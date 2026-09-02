package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"

	pb "github.com/divergedev/diverge/api/gen/diverge/v1alpha1"
	"github.com/divergedev/diverge/api/gen/diverge/v1alpha1/divergev1alpha1connect"
)

const (
	// tunnelChunkSize is the max body size to inline in a single message.
	// Bodies larger than this are streamed in chunks.
	tunnelChunkSize = 32 << 10 // 32KB

	// tunnelLocalTimeout is the HTTP client timeout for localhost requests.
	// P1 #11: Prevents goroutine leaks from slow/stuck localhost services.
	tunnelLocalTimeout = 30 * time.Second
)

type TunnelClient struct {
	serverAddr       string
	localPort        int
	previewID        string
	service          string
	namespace        string
	logger           *slog.Logger
	httpClient       *http.Client
	tunnelHTTPClient *http.Client
	Ready            chan struct{}
	readyOnce        sync.Once
}

func isLoopbackHost(host string) bool {
	h := host
	if strings.Contains(h, ":") {
		if sh, _, err := net.SplitHostPort(h); err == nil {
			h = sh
		}
	}
	h = strings.Trim(h, "[]")
	if strings.EqualFold(h, "localhost") {
		return true
	}
	if ip := net.ParseIP(h); ip != nil && ip.IsLoopback() {
		return true
	}
	return false
}

// tunnelAuthTransport attaches the bearer credential from TokenSource to every tunnel request.
type tunnelAuthTransport struct {
	base        http.RoundTripper
	tokenSource TokenSource
}

func (t *tunnelAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.tokenSource == nil {
		return t.base.RoundTrip(req)
	}

	// Security: Do not transmit credentials in cleartext over non-loopback HTTP.
	if req.URL != nil && strings.EqualFold(req.URL.Scheme, "http") && !isLoopbackHost(req.URL.Host) {
		return nil, fmt.Errorf("insecure HTTP scheme is only allowed for loopback addresses, got %q", req.URL.Host)
	}

	tok, err := t.tokenSource.Token(req.Context())
	if err != nil {
		return nil, fmt.Errorf("failed to get tunnel auth token: %w", err)
	}

	clone := req.Clone(req.Context())
	if tok != "" {
		clone.Header.Set("Authorization", "Bearer "+tok)
	}
	return t.base.RoundTrip(clone)
}

// tunnelBaseTransport picks a transport that can actually carry the Tunnel
// stream. It is bidirectional, which connect-go only supports over HTTP/2 —
// and without TLS there is no ALPN, so the default transport stays on
// HTTP/1.1 and the server answers the stream with 505 HTTP Version Not
// Supported. For a plaintext server the only option is prior-knowledge HTTP/2
// (h2c): a transport whose only protocol for http:// is unencrypted HTTP/2,
// which net/http speaks as prior knowledge. Over https the
// default transport already negotiates HTTP/2 through ALPN, so it is kept.
// (tunnelAuthTransport separately refuses plaintext to anything but loopback,
// so in practice h2c only ever reaches a port-forward.) The constructors that
// build a real tunnel pass this in; a nil base transport still means the
// default, so callers that only need plain requests are unaffected.
func tunnelBaseTransport(serverAddr string) http.RoundTripper {
	if !strings.HasPrefix(serverAddr, "http://") {
		return http.DefaultTransport
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Protocols = new(http.Protocols)
	t.Protocols.SetUnencryptedHTTP2(true)
	return t
}

// NewTunnelClientWithTokenSource creates a TunnelClient using a dynamic TokenSource and optional
// base transport (defaults to http.DefaultTransport).
func NewTunnelClientWithTokenSource(
	serverAddr string,
	localPort int,
	previewID, service, namespace string,
	tokenSource TokenSource,
	baseTransport http.RoundTripper,
	logger *slog.Logger,
) *TunnelClient {
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}

	return &TunnelClient{
		serverAddr: serverAddr,
		localPort:  localPort,
		previewID:  previewID,
		service:    service,
		namespace:  namespace,
		logger:     logger,
		httpClient: &http.Client{Timeout: tunnelLocalTimeout}, // P1 #11
		tunnelHTTPClient: &http.Client{
			Transport: &tunnelAuthTransport{
				base:        baseTransport,
				tokenSource: tokenSource,
			},
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) > 0 {
					orig := via[0]
					if !strings.EqualFold(req.URL.Scheme, orig.URL.Scheme) || !strings.EqualFold(req.URL.Host, orig.URL.Host) {
						return fmt.Errorf("refusing to send credentials across redirects to different scheme or host: %s -> %s", orig.URL, req.URL)
					}
				}
				return nil
			},
		},
		Ready: make(chan struct{}),
	}
}

// NewTunnelClient is a backward-compatible wrapper creating a TunnelClient with a static token.
func NewTunnelClient(serverAddr string, localPort int, previewID, service, namespace, token string, logger *slog.Logger) *TunnelClient {
	return NewTunnelClientWithTokenSource(serverAddr, localPort, previewID, service, namespace, StaticTokenSource(token), tunnelBaseTransport(serverAddr), logger)
}

func (tc *TunnelClient) ConnectWithRetry(ctx context.Context) {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		connStart := time.Now()
		err := tc.Connect(ctx)
		connDuration := time.Since(connStart)

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Reset backoff if the connection lived long enough — it wasn't a startup failure
			if connDuration > maxBackoff {
				backoff = 1 * time.Second
			}
			tc.logger.Error("tunnel disconnected, reconnecting...", "err", err, "backoff", backoff, "was_connected", connDuration)
			fmt.Printf("⚠️  Tunnel disconnected: %v (reconnecting in %v)\n", err, backoff)
		} else {
			return // graceful shutdown
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (tc *TunnelClient) Connect(ctx context.Context) error {
	client := divergev1alpha1connect.NewTunnelServiceClient(
		tc.tunnelHTTPClient,
		tc.serverAddr,
	)

	stream := client.Tunnel(ctx)

	var sendMu sync.Mutex

	sendMu.Lock()
	err := stream.Send(&pb.TunnelServiceTunnelRequest{
		Payload: &pb.TunnelServiceTunnelRequest_Register{
			Register: &pb.TunnelRegister{
				Service:         tc.service,
				Namespace:       tc.namespace,
				Port:            int32(tc.localPort),
				PreviewId:       tc.previewID,
				ProtocolVersion: 2, // v2 = chunked streaming support
			},
		},
	})
	sendMu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to send register: %w", err)
	}

	msg, err := stream.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive ready: %w", err)
	}

	ready := msg.GetReady()
	if ready == nil {
		return fmt.Errorf("expected TunnelReady, got %T", msg.Payload)
	}

	tc.logger.Info("tunnel ready", "endpoint", ready.Endpoint, "tunnel_id", ready.TunnelId)
	fmt.Printf("🚇 Tunnel active (id: %s, endpoint: %s)\n", ready.TunnelId, ready.Endpoint)
	tc.readyOnce.Do(func() { close(tc.Ready) })

	errCh := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				tc.logger.Error("panic in tunnel receive loop", "err", r)
				errCh <- fmt.Errorf("panic: %v", r)
			}
		}()

		for {
			msg, err := stream.Receive()
			if err != nil {
				tc.logger.Error("stream receive error", "err", err)
				errCh <- err
				return
			}

			switch p := msg.Payload.(type) {
			case *pb.TunnelServiceTunnelResponse_Ping:
				sendMu.Lock()
				_ = stream.Send(&pb.TunnelServiceTunnelRequest{
					Payload: &pb.TunnelServiceTunnelRequest_Pong{
						Pong: &pb.TunnelPong{
							Timestamp: p.Ping.Timestamp,
						},
					},
				})
				sendMu.Unlock()
			case *pb.TunnelServiceTunnelResponse_HttpRequest:
				go func(req *pb.TunnelHTTPRequest) {
					defer func() {
						if r := recover(); r != nil {
							tc.logger.Error("panic handling tunnel request", "err", r)
						}
					}()

					tc.handleAndStreamResponse(ctx, req, &sendMu, stream)
				}(p.HttpRequest)
			case *pb.TunnelServiceTunnelResponse_Close:
				reason := p.Close.Reason
				tc.logger.Info("tunnel closed by server", "reason", reason)
				if reason != "" {
					fmt.Printf("🚇 Tunnel closed by server: %s\n", reason)
				}
				errCh <- nil
				return
			}
		}
	}()

	var streamErr error
	select {
	case <-ctx.Done():
	case streamErr = <-errCh:
	}

	sendMu.Lock()
	defer sendMu.Unlock()

	_ = stream.Send(&pb.TunnelServiceTunnelRequest{
		Payload: &pb.TunnelServiceTunnelRequest_Close{
			Close: &pb.TunnelClose{
				Reason: "client shutting down",
			},
		},
	})

	err = stream.CloseRequest()
	if streamErr != nil {
		return streamErr
	}
	return err
}

// handleAndStreamResponse forwards a request to localhost and streams the response
// back in chunks if it's large.
func (tc *TunnelClient) handleAndStreamResponse(ctx context.Context, req *pb.TunnelHTTPRequest, sendMu *sync.Mutex, stream *connect.BidiStreamForClient[pb.TunnelServiceTunnelRequest, pb.TunnelServiceTunnelResponse]) {
	urlStr := fmt.Sprintf("http://localhost:%d%s", tc.localPort, req.Path)
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, urlStr, bytes.NewReader(req.Body))
	if err != nil {
		tc.sendErrorResponse(sendMu, stream, req.RequestId, 500, fmt.Sprintf("failed to create request: %v", err))
		return
	}

	// P1 #9: Preserve multi-value headers
	for _, h := range req.Headers {
		for _, v := range h.Values {
			httpReq.Header.Add(h.Key, v)
		}
	}

	resp, err := tc.httpClient.Do(httpReq)
	if err != nil {
		tc.sendErrorResponse(sendMu, stream, req.RequestId, 502, "local service unavailable")
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	headers := headersToProto(resp.Header)

	// Read the first chunk to see if we can inline it
	buf := make([]byte, tunnelChunkSize)
	n, readErr := io.ReadFull(resp.Body, buf)
	firstChunk := buf[:n]

	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		tc.sendErrorResponse(sendMu, stream, req.RequestId, 502, "error reading local response body")
		return
	}

	if readErr == io.EOF || readErr == io.ErrUnexpectedEOF || n < tunnelChunkSize {
		// Small response — send inline
		sendMu.Lock()
		_ = stream.Send(&pb.TunnelServiceTunnelRequest{
			Payload: &pb.TunnelServiceTunnelRequest_HttpResponse{
				HttpResponse: &pb.TunnelHTTPResponse{
					RequestId:     req.RequestId,
					StatusCode:    int32(resp.StatusCode),
					Headers:       headers,
					Body:          firstChunk,
					HasMoreChunks: false,
				},
			},
		})
		sendMu.Unlock()
		return
	}

	// Large response — send header + stream chunks
	sendMu.Lock()
	_ = stream.Send(&pb.TunnelServiceTunnelRequest{
		Payload: &pb.TunnelServiceTunnelRequest_HttpResponse{
			HttpResponse: &pb.TunnelHTTPResponse{
				RequestId:     req.RequestId,
				StatusCode:    int32(resp.StatusCode),
				Headers:       headers,
				Body:          firstChunk,
				HasMoreChunks: true,
			},
		},
	})
	sendMu.Unlock()

	// Stream remaining body
	for {
		n, readErr = resp.Body.Read(buf)
		if n > 0 {
			isLast := readErr == io.EOF
			sendMu.Lock()
			_ = stream.Send(&pb.TunnelServiceTunnelRequest{
				Payload: &pb.TunnelServiceTunnelRequest_ResponseChunk{
					ResponseChunk: &pb.TunnelResponseChunk{
						RequestId: req.RequestId,
						Data:      buf[:n],
						IsLast:    isLast,
					},
				},
			})
			sendMu.Unlock()
			if isLast {
				return
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				tc.logger.Error("error reading remaining local response body", "err", readErr)
			}
			// Send final empty chunk to signal end
			sendMu.Lock()
			_ = stream.Send(&pb.TunnelServiceTunnelRequest{
				Payload: &pb.TunnelServiceTunnelRequest_ResponseChunk{
					ResponseChunk: &pb.TunnelResponseChunk{
						RequestId: req.RequestId,
						Data:      nil,
						IsLast:    true,
					},
				},
			})
			sendMu.Unlock()
			return
		}
	}
}

func (tc *TunnelClient) sendErrorResponse(sendMu *sync.Mutex, stream *connect.BidiStreamForClient[pb.TunnelServiceTunnelRequest, pb.TunnelServiceTunnelResponse], requestID string, statusCode int32, msg string) {
	sendMu.Lock()
	_ = stream.Send(&pb.TunnelServiceTunnelRequest{
		Payload: &pb.TunnelServiceTunnelRequest_HttpResponse{
			HttpResponse: &pb.TunnelHTTPResponse{
				RequestId:  requestID,
				StatusCode: statusCode,
				Body:       []byte(msg),
			},
		},
	})
	sendMu.Unlock()
}

// headersToProto converts http.Header to repeated HeaderEntry (preserves multi-values).
func headersToProto(h http.Header) []*pb.HeaderEntry {
	entries := make([]*pb.HeaderEntry, 0, len(h))
	for k, v := range h {
		entries = append(entries, &pb.HeaderEntry{
			Key:    k,
			Values: v,
		})
	}
	return entries
}
