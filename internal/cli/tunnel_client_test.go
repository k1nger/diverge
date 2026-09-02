package cli

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	pb "github.com/divergedev/diverge/api/gen/diverge/v1alpha1"
	"github.com/divergedev/diverge/api/gen/diverge/v1alpha1/divergev1alpha1connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockTunnelServer struct {
	divergev1alpha1connect.UnimplementedTunnelServiceHandler
	msgCh chan *pb.TunnelServiceTunnelRequest
}

func (m *mockTunnelServer) Tunnel(ctx context.Context, stream *connect.BidiStream[pb.TunnelServiceTunnelRequest, pb.TunnelServiceTunnelResponse]) error {
	// First message should be Register
	msg, err := stream.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive register: %w", err)
	}
	if msg.GetRegister() == nil {
		return fmt.Errorf("expected register message, got %T", msg.Payload)
	}
	m.msgCh <- msg

	// Send Ready
	_ = stream.Send(&pb.TunnelServiceTunnelResponse{
		Payload: &pb.TunnelServiceTunnelResponse_Ready{
			Ready: &pb.TunnelReady{
				TunnelId: "tunnel-123",
				Endpoint: "http://test-preview.diverge.local",
			},
		},
	})

	// Wait for further messages (e.g. Pong, Response)
	go func() {
		for {
			msg, err := stream.Receive()
			if err != nil {
				return
			}
			m.msgCh <- msg
		}
	}()

	// Simulate sending a Ping
	_ = stream.Send(&pb.TunnelServiceTunnelResponse{
		Payload: &pb.TunnelServiceTunnelResponse_Ping{
			Ping: &pb.TunnelPing{
				Timestamp: timestamppb.Now(),
			},
		},
	})

	// Simulate sending a Request
	_ = stream.Send(&pb.TunnelServiceTunnelResponse{
		Payload: &pb.TunnelServiceTunnelResponse_HttpRequest{
			HttpRequest: &pb.TunnelHTTPRequest{
				RequestId: "req-1",
				Method:    "GET",
				Path:      "/foo",
			},
		},
	})

	<-ctx.Done()
	return nil
}

func TestTunnelClient(t *testing.T) {
	// Setup a local mock application server
	appServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/foo", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("mock app response"))
	}))
	defer appServer.Close()

	// Extract port
	var port int
	_, _ = fmt.Sscanf(appServer.Listener.Addr().String(), "127.0.0.1:%d", &port)

	// Setup a mock tunnel server
	msgCh := make(chan *pb.TunnelServiceTunnelRequest, 10)
	mockServer := &mockTunnelServer{msgCh: msgCh}
	mux := http.NewServeMux()
	path, handler := divergev1alpha1connect.NewTunnelServiceHandler(mockServer)
	mux.Handle(path, handler)

	ts := httptest.NewUnstartedServer(mux)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	tc := NewTunnelClient(ts.URL, port, "test-preview", "test-service", "default", "test-token",
		slog.New(slog.NewTextHandler(os.Stdout, nil)))
	tc.httpClient = ts.Client() // use test server's TLS client
	tc.tunnelHTTPClient = ts.Client()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run client
	go tc.ConnectWithRetry(ctx)

	// Wait for Register
	var regMsg *pb.TunnelServiceTunnelRequest
	select {
	case regMsg = <-msgCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for register")
	}
	assert.Equal(t, "test-preview", regMsg.GetRegister().PreviewId)
	assert.Equal(t, int32(2), regMsg.GetRegister().ProtocolVersion)

	// Wait for Pong
	var pongMsg *pb.TunnelServiceTunnelRequest
	select {
	case pongMsg = <-msgCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for pong")
	}
	assert.NotNil(t, pongMsg.GetPong())

	// Wait for Response
	var respMsg *pb.TunnelServiceTunnelRequest
	select {
	case respMsg = <-msgCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for response")
	}
	require.NotNil(t, respMsg.GetHttpResponse())
	assert.Equal(t, "req-1", respMsg.GetHttpResponse().RequestId)
	assert.Equal(t, int32(200), respMsg.GetHttpResponse().StatusCode)
	assert.Equal(t, "mock app response", string(respMsg.GetHttpResponse().Body))

	// Clean up
	cancel()
	time.Sleep(200 * time.Millisecond) // Let it exit gracefully
}

func TestTunnelClient_ReadyChannel(t *testing.T) {
	appServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer appServer.Close()
	var port int
	_, _ = fmt.Sscanf(appServer.Listener.Addr().String(), "127.0.0.1:%d", &port)

	msgCh := make(chan *pb.TunnelServiceTunnelRequest, 10)
	mockServer := &mockTunnelServer{msgCh: msgCh}
	mux := http.NewServeMux()
	path, handler := divergev1alpha1connect.NewTunnelServiceHandler(mockServer)
	mux.Handle(path, handler)
	ts := httptest.NewUnstartedServer(mux)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	tc := NewTunnelClient(ts.URL, port, "test-preview", "test-service", "default", "test-token", slog.Default())
	tc.httpClient = ts.Client()
	tc.tunnelHTTPClient = ts.Client()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go tc.ConnectWithRetry(ctx)

	select {
	case <-tc.Ready:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("Ready channel was not closed")
	}
}

type mockReconnectServer struct {
	divergev1alpha1connect.UnimplementedTunnelServiceHandler
	msgCh chan *pb.TunnelServiceTunnelRequest
	t     *testing.T
	count int
}

func (m *mockReconnectServer) Tunnel(ctx context.Context, stream *connect.BidiStream[pb.TunnelServiceTunnelRequest, pb.TunnelServiceTunnelResponse]) error {
	m.count++
	msg, err := stream.Receive()
	if err != nil {
		return err
	}
	m.msgCh <- msg

	if m.count == 1 {
		// First connection, simulate server close
		return fmt.Errorf("server closed stream")
	}

	// Second connection, send ready and wait
	_ = stream.Send(&pb.TunnelServiceTunnelResponse{
		Payload: &pb.TunnelServiceTunnelResponse_Ready{
			Ready: &pb.TunnelReady{
				TunnelId: "tunnel-456",
				Endpoint: "http://test-preview.diverge.local",
			},
		},
	})
	<-ctx.Done()
	return nil
}

func TestTunnelClient_Reconnect(t *testing.T) {
	appServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer appServer.Close()
	var port int
	_, _ = fmt.Sscanf(appServer.Listener.Addr().String(), "127.0.0.1:%d", &port)

	msgCh := make(chan *pb.TunnelServiceTunnelRequest, 10)
	mockServer := &mockReconnectServer{msgCh: msgCh, t: t}
	mux := http.NewServeMux()
	path, handler := divergev1alpha1connect.NewTunnelServiceHandler(mockServer)
	mux.Handle(path, handler)
	ts := httptest.NewUnstartedServer(mux)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	tc := NewTunnelClient(ts.URL, port, "test-preview", "test-service", "default", "test-token", slog.Default())
	tc.httpClient = ts.Client()
	tc.tunnelHTTPClient = ts.Client()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go tc.ConnectWithRetry(ctx)

	// Wait for first Register
	select {
	case <-msgCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for first register")
	}

	// Wait for second Register
	select {
	case <-msgCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for second register")
	}
}

// TestNewTunnelClient_SendsAuthorizationHeader is the regression guard for the
// tunnel connecting with no credential at all. The server authenticates every
// Tunnel RPC by TokenReview and rejects an unauthenticated request with 401,
// so the header has to reach the wire.
//
// The stub serves unencrypted HTTP/2, because the real plaintext server does and the client
// now dials http:// with prior-knowledge HTTP/2 — the only version connect-go
// carries a bidirectional stream over. A plain HTTP/1.1 stub here would fail
// on the preface, which is precisely the 505 this transport exists to fix.
func TestNewTunnelClient_SendsAuthorizationHeader(t *testing.T) {
	var gotAuth string
	var gotProto string
	var mu sync.Mutex
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		gotProto = r.Proto
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	srv.Config.Protocols.SetUnencryptedHTTP2(true)
	srv.Start()
	defer srv.Close()

	tc := NewTunnelClient(srv.URL, 8080, "preview-1", "svc", "ns", "s3cret-token", slog.Default())

	req, err := http.NewRequest(http.MethodPost, srv.URL, nil)
	require.NoError(t, err)
	resp, err := tc.tunnelHTTPClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "Bearer s3cret-token", gotAuth)
	// The header arriving over HTTP/1.1 would pass the assertion above while
	// the actual tunnel stream still got 505, so the version is pinned too.
	assert.Equal(t, "HTTP/2.0", gotProto)
}

// TestTunnelAuthTransport_DoesNotMutateRequest pins the RoundTripper contract:
// the caller's request must not be modified in place.
func TestTunnelAuthTransport_DoesNotMutateRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	transport := &tunnelAuthTransport{base: http.DefaultTransport, tokenSource: StaticTokenSource("tok")}
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Empty(t, req.Header.Get("Authorization"),
		"RoundTrip must not modify the request it is given")
}

// TestTunnelAuthTransport_DynamicTokenSource verifies that the transport calls
// TokenSource on every request, allowing dynamically refreshed credentials
// (such as rotated ServiceAccount tokens) to be used without reconnecting.
func TestTunnelAuthTransport_DynamicTokenSource(t *testing.T) {
	var gotAuths []string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuths = append(gotAuths, r.Header.Get("Authorization"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var currentToken string
	var tokenMu sync.Mutex
	dynamicTS := &testFuncTokenSource{
		fn: func(ctx context.Context) (string, error) {
			tokenMu.Lock()
			defer tokenMu.Unlock()
			return currentToken, nil
		},
	}

	tokenMu.Lock()
	currentToken = "token-phase-1"
	tokenMu.Unlock()

	tc := NewTunnelClientWithTokenSource(srv.URL, 8080, "preview-1", "svc", "ns", dynamicTS, nil, slog.Default())

	// First request with phase 1 token
	req1, err := http.NewRequest(http.MethodPost, srv.URL, nil)
	require.NoError(t, err)
	resp1, err := tc.tunnelHTTPClient.Do(req1)
	require.NoError(t, err)
	_ = resp1.Body.Close()

	// Rotate token
	tokenMu.Lock()
	currentToken = "token-phase-2-rotated"
	tokenMu.Unlock()

	// Second request should dynamically present phase 2 token
	req2, err := http.NewRequest(http.MethodPost, srv.URL, nil)
	require.NoError(t, err)
	resp2, err := tc.tunnelHTTPClient.Do(req2)
	require.NoError(t, err)
	_ = resp2.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, gotAuths, 2)
	assert.Equal(t, "Bearer token-phase-1", gotAuths[0])
	assert.Equal(t, "Bearer token-phase-2-rotated", gotAuths[1])
}

type testFuncTokenSource struct {
	fn func(ctx context.Context) (string, error)
}

func (f *testFuncTokenSource) Token(ctx context.Context) (string, error) {
	return f.fn(ctx)
}

func TestTunnelAuthTransport_RejectsNonLoopbackHTTP(t *testing.T) {
	transport := &tunnelAuthTransport{
		base:        http.DefaultTransport,
		tokenSource: StaticTokenSource("secret-token"),
	}

	// Non-loopback HTTP must be rejected without sending credentials
	req, err := http.NewRequest(http.MethodGet, "http://remote-server.example.com/tunnel", nil)
	require.NoError(t, err)

	_, err = transport.RoundTrip(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insecure HTTP scheme is only allowed for loopback addresses")

	// Loopback IPv4 HTTP is allowed through transport validation
	loopbackReq, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:8080/tunnel", nil)
	require.NoError(t, err)
	_, err = transport.RoundTrip(loopbackReq)
	if err != nil {
		assert.NotContains(t, err.Error(), "insecure HTTP scheme is only allowed for loopback addresses")
	}

	// Localhost is allowed through transport validation
	localhostReq, err := http.NewRequest(http.MethodGet, "http://localhost:8080/tunnel", nil)
	require.NoError(t, err)
	_, err = transport.RoundTrip(localhostReq)
	if err != nil {
		assert.NotContains(t, err.Error(), "insecure HTTP scheme is only allowed for loopback addresses")
	}
}

func TestTunnelClient_RejectsCrossHostRedirect(t *testing.T) {
	targetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer targetSrv.Close()

	redirectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, targetSrv.URL+"/redirected", http.StatusFound)
	}))
	defer redirectSrv.Close()

	tc := NewTunnelClientWithTokenSource(redirectSrv.URL, 8080, "p1", "svc", "ns", StaticTokenSource("tok"), nil, slog.Default())
	req, err := http.NewRequest(http.MethodGet, redirectSrv.URL, nil)
	require.NoError(t, err)

	_, err = tc.tunnelHTTPClient.Do(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to send credentials across redirects")
}
