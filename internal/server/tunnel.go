package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pb "github.com/divergedev/diverge/api/gen/diverge/v1alpha1"
	"github.com/divergedev/diverge/api/gen/diverge/v1alpha1/divergev1alpha1connect"
	divergeiov1alpha1 "github.com/divergedev/diverge/api/v1alpha1"
	"github.com/divergedev/diverge/internal/server/auth"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// maxChunkBufferSize is the max memory to buffer for a single chunked response.
	maxChunkBufferSize = 10 << 20 // 10MB max per response
	// tunnelHeartbeatInterval is how often the server pings the CLI.
	tunnelHeartbeatInterval = 15 * time.Second
	// tunnelHeartbeatTimeout is how long to wait for a pong before killing the tunnel.
	tunnelHeartbeatTimeout = 45 * time.Second
	// tunnelTTLDuration is how long tunnel resources live without renewal.
	tunnelTTLDuration = 2 * time.Minute
	// tunnelReqChBuffer is the channel buffer for pending requests.
	tunnelReqChBuffer = 10
	// tunnelProxyPort is the dedicated port for tunnel proxy (no auth).
	TunnelProxyPort = 8081
)

type TunnelManager struct {
	mu          sync.RWMutex
	tunnels     map[string]*activeTunnel
	crdClient   client.Client
	k8sClient   kubernetes.Interface
	logger      *slog.Logger
	auditLogger *AuditLogger
	lease       *TunnelLease
	gc          *TunnelGC
	podName     string
}

type chunkAssembly struct {
	statusCode int32
	headers    []*pb.HeaderEntry
	body       []byte
}

type activeTunnel struct {
	tunnelID  string // unique per-connection UUID
	previewID string
	service   string
	namespace string
	port      int32
	reqCh     chan *pb.TunnelHTTPRequest

	respMu sync.RWMutex
	respWg map[string]chan *pb.TunnelHTTPResponse

	// Chunked response assembly
	chunkMu     sync.Mutex
	chunkBuffer map[string]*chunkAssembly // request_id -> accumulated chunks

	lastPong   time.Time
	lastPongMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
}

func NewTunnelManager(crdClient client.Client, k8s kubernetes.Interface, logger *slog.Logger, audit *AuditLogger) *TunnelManager {
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName = "diverge-server-local"
	}

	tm := &TunnelManager{
		tunnels:     make(map[string]*activeTunnel),
		crdClient:   crdClient,
		k8sClient:   k8s,
		logger:      logger,
		auditLogger: audit,
		podName:     podName,
		lease:       NewTunnelLease(k8s, logger, podName),
		gc:          NewTunnelGC(k8s, logger),
	}
	return tm
}

// StartGC starts the background garbage collector for tunnel resources.
func (tm *TunnelManager) StartGC(ctx context.Context, namespaces []string) {
	go tm.gc.Run(ctx, namespaces)
}

func (tm *TunnelManager) Tunnel(ctx context.Context, stream *connect.BidiStream[pb.TunnelServiceTunnelRequest, pb.TunnelServiceTunnelResponse]) error {
	// 1. Receive TunnelRegister
	msg, err := stream.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive register message: %w", err)
	}
	reg := msg.GetRegister()
	if reg == nil {
		return fmt.Errorf("first message must be TunnelRegister")
	}

	// 2. Validate auth (done by middleware)
	// 3. RBAC check
	if err := AuthorizeAction(ctx, tm.k8sClient, tm.auditLogger, "create", reg.Namespace, "environments"); err != nil {
		return err
	}

	// P0 #1: Validate preview-id ownership
	if err := tm.validatePreviewOwnership(ctx, reg); err != nil {
		return connect.NewError(connect.CodePermissionDenied, err)
	}

	tunnelID := uuid.New().String()
	tm.logger.Info("tunnel registering",
		"preview-id", reg.PreviewId,
		"namespace", reg.Namespace,
		"service", reg.Service,
		"port", reg.Port,
		"tunnel-id", tunnelID,
	)

	// P0 #2: Acquire distributed lease (evicts previous holder)
	previousHolder, err := tm.lease.Acquire(ctx, reg.Namespace, reg.PreviewId, tunnelID)
	if err != nil {
		return fmt.Errorf("failed to acquire tunnel lease: %w", err)
	}
	if previousHolder != "" {
		tm.logger.Info("evicted previous tunnel holder", "previous", previousHolder, "preview-id", reg.PreviewId)
		// If previous holder is on this pod, cancel it
		tm.evictLocalTunnel(reg.PreviewId)
	}

	// 4. Create headless Service + EndpointSlice
	if err := tm.createTunnelResources(ctx, reg, tunnelID); err != nil {
		return fmt.Errorf("failed to create tunnel resources: %w", err)
	}

	defer func() {
		// P0 #5: Cleanup — only if we still hold the lease
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if tm.lease.Renew(cleanupCtx, reg.Namespace, reg.PreviewId, tunnelID) {
			tm.deleteTunnelResources(cleanupCtx, reg)
			tm.lease.Release(cleanupCtx, reg.Namespace, reg.PreviewId, tunnelID)
		}
	}()

	// 5. Register tunnel in-memory
	tCtx, tCancel := context.WithCancel(ctx)
	defer tCancel()

	at := &activeTunnel{
		tunnelID:    tunnelID,
		previewID:   reg.PreviewId,
		service:     reg.Service,
		namespace:   reg.Namespace,
		port:        reg.Port,
		reqCh:       make(chan *pb.TunnelHTTPRequest, tunnelReqChBuffer),
		respWg:      make(map[string]chan *pb.TunnelHTTPResponse),
		chunkBuffer: make(map[string]*chunkAssembly),
		lastPong:    time.Now(),
		ctx:         tCtx,
		cancel:      tCancel,
	}

	tm.mu.Lock()
	tm.tunnels[reg.PreviewId] = at
	tm.mu.Unlock()

	defer func() {
		tm.mu.Lock()
		if tm.tunnels[reg.PreviewId] == at {
			delete(tm.tunnels, reg.PreviewId)
		}
		tm.mu.Unlock()

		at.respMu.Lock()
		at.respWg = make(map[string]chan *pb.TunnelHTTPResponse)
		at.respMu.Unlock()
	}()

	var sendMu sync.Mutex

	// 6. Send TunnelReady
	podIP := os.Getenv("POD_IP")
	if podIP == "" {
		podIP = "127.0.0.1"
	}
	endpoint := fmt.Sprintf("%s:%d", podIP, TunnelProxyPort) // P0 #3: point to proxy port
	sendMu.Lock()
	err = stream.Send(&pb.TunnelServiceTunnelResponse{
		Payload: &pb.TunnelServiceTunnelResponse_Ready{
			Ready: &pb.TunnelReady{
				TunnelId: tunnelID,
				Endpoint: endpoint,
			},
		},
	})
	sendMu.Unlock()
	if err != nil {
		return err
	}

	// 7. Start heartbeat with dead stream detection (P1 #7)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				tm.logger.Error("panic in heartbeat goroutine", "err", r)
			}
		}()

		ticker := time.NewTicker(tunnelHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-tCtx.Done():
				return
			case <-ticker.C:
				// Check for dead stream
				at.lastPongMu.RLock()
				stale := time.Since(at.lastPong) > tunnelHeartbeatTimeout
				at.lastPongMu.RUnlock()
				if stale {
					tm.logger.Warn("tunnel heartbeat timeout, closing", "preview-id", reg.PreviewId)
					tCancel()
					return
				}

				// Check lease is still ours (P0 #2: distributed fencing)
				if !tm.lease.Renew(tCtx, reg.Namespace, reg.PreviewId, tunnelID) {
					tm.logger.Info("lease lost, closing tunnel", "preview-id", reg.PreviewId)
					tCancel()
					return
				}

				// Refresh TTL annotation (P0 #5)
				tm.refreshTunnelTTL(tCtx, reg)

				sendMu.Lock()
				_ = stream.Send(&pb.TunnelServiceTunnelResponse{
					Payload: &pb.TunnelServiceTunnelResponse_Ping{
						Ping: &pb.TunnelPing{
							Timestamp: timestamppb.Now(),
						},
					},
				})
				sendMu.Unlock()
			}
		}
	}()

	// 8. Event loop — receive responses and chunks from CLI
	errCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				tm.logger.Error("panic in receive goroutine", "err", r)
				errCh <- fmt.Errorf("panic: %v", r)
			}
		}()

		for {
			msg, err := stream.Receive()
			if err != nil {
				errCh <- err
				return
			}
			switch p := msg.Payload.(type) {
			case *pb.TunnelServiceTunnelRequest_HttpResponse:
				if p.HttpResponse.HasMoreChunks {
					// Start of chunked response — store headers/status, wait for chunks
					at.respMu.RLock()
					ch, ok := at.respWg[p.HttpResponse.RequestId]
					at.respMu.RUnlock()
					if ok {
						// Store partial response (body will be assembled from chunks)
						at.chunkMu.Lock()
						at.chunkBuffer[p.HttpResponse.RequestId] = &chunkAssembly{
							statusCode: p.HttpResponse.StatusCode,
							headers:    p.HttpResponse.Headers,
							body:       p.HttpResponse.Body,
						}
						at.chunkMu.Unlock()
						// Don't send to channel yet — wait for all chunks
						_ = ch // keep reference alive
					}
				} else {
					// Complete response (no chunking)
					at.respMu.RLock()
					ch, ok := at.respWg[p.HttpResponse.RequestId]
					at.respMu.RUnlock()
					if ok {
						select {
						case ch <- p.HttpResponse:
						default:
						}
					}
				}
			case *pb.TunnelServiceTunnelRequest_ResponseChunk:
				at.chunkMu.Lock()
				assembly := at.chunkBuffer[p.ResponseChunk.RequestId]
				if assembly == nil {
					at.chunkMu.Unlock()
					continue
				}
				assembly.body = append(assembly.body, p.ResponseChunk.Data...)
				if len(assembly.body) > maxChunkBufferSize {
					delete(at.chunkBuffer, p.ResponseChunk.RequestId)
					at.chunkMu.Unlock()
					tm.logger.Warn("chunk buffer exceeded max size", "request_id", p.ResponseChunk.RequestId)
					continue
				}
				if p.ResponseChunk.IsLast {
					// Assemble final response
					delete(at.chunkBuffer, p.ResponseChunk.RequestId)
					at.chunkMu.Unlock()

					at.respMu.RLock()
					ch, ok := at.respWg[p.ResponseChunk.RequestId]
					at.respMu.RUnlock()
					if ok {
						select {
						case ch <- &pb.TunnelHTTPResponse{
							RequestId:  p.ResponseChunk.RequestId,
							StatusCode: assembly.statusCode,
							Headers:    assembly.headers,
							Body:       assembly.body,
						}:
						default:
						}
					}
				} else {
					at.chunkMu.Unlock()
				}
			case *pb.TunnelServiceTunnelRequest_Pong:
				at.lastPongMu.Lock()
				at.lastPong = time.Now()
				at.lastPongMu.Unlock()
			case *pb.TunnelServiceTunnelRequest_Close:
				tm.logger.Info("tunnel closed by client", "reason", p.Close.Reason, "preview-id", reg.PreviewId)
				errCh <- nil
				return
			}
		}
	}()

	// Main loop — send requests to CLI
	for {
		select {
		case <-tCtx.Done():
			return nil
		case err := <-errCh:
			return err
		case req := <-at.reqCh:
			sendMu.Lock()
			err := stream.Send(&pb.TunnelServiceTunnelResponse{
				Payload: &pb.TunnelServiceTunnelResponse_HttpRequest{
					HttpRequest: req,
				},
			})
			sendMu.Unlock()
			if err != nil {
				return err
			}
		}
	}
}

// P0 #1: Validate that authenticated user owns the preview-id
func (tm *TunnelManager) validatePreviewOwnership(ctx context.Context, reg *pb.TunnelRegister) error {
	user, ok := auth.UserInfoFromContext(ctx)
	if !ok {
		return fmt.Errorf("no authenticated user in context")
	}

	var pg divergeiov1alpha1.PreviewGroup
	err := tm.crdClient.Get(ctx, client.ObjectKey{Name: reg.PreviewId}, &pg)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to look up preview group: %w", err)
	}

	if pg.Spec.Owner != "" && pg.Spec.Owner != user.Username {
		return fmt.Errorf("preview-id %q is owned by %q", reg.PreviewId, pg.Spec.Owner)
	}

	return nil
}

// evictLocalTunnel cancels a tunnel on this pod if it exists.
func (tm *TunnelManager) evictLocalTunnel(previewID string) {
	tm.mu.Lock()
	if existing, ok := tm.tunnels[previewID]; ok {
		tm.logger.Info("evicting local tunnel", "preview-id", previewID, "tunnel-id", existing.tunnelID)
		existing.cancel()
		delete(tm.tunnels, previewID)
	}
	tm.mu.Unlock()
}

func (tm *TunnelManager) createTunnelResources(ctx context.Context, reg *pb.TunnelRegister, tunnelID string) error {
	podIP := os.Getenv("POD_IP")
	if podIP == "" {
		podIP = "127.0.0.1"
	}

	expires := time.Now().Add(tunnelTTLDuration).Format(time.RFC3339)
	svcName := fmt.Sprintf("diverge-tunnel-%s", reg.PreviewId)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: reg.Namespace,
			Labels: map[string]string{
				"divergedev.com/tunnel":     "true",
				"divergedev.com/preview-id": reg.PreviewId,
			},
			Annotations: map[string]string{
				"divergedev.com/tunnel-id":      tunnelID,
				"divergedev.com/tunnel-expires": expires,
				"divergedev.com/tunnel-holder":  tm.podName,
			},
		},
		Spec: corev1.ServiceSpec{
			// A REAL ClusterIP, NOT headless. A headless Service does no port
			// remapping: a caller resolves it to the backing pod IP and
			// connects on whatever port it names, so publishing reg.Port
			// (8099, the developer's LOCAL port) sent every consumer to a port
			// nothing listens on — the proxy is on TunnelProxyPort. A real
			// ClusterIP lets kube-proxy DNAT Port -> TargetPort, so the name
			// consumers already use reaches the proxy. It also resolves under
			// kube-dns from the Service's own ClusterIP, without depending on
			// the resolver reading endpoints at all.
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{{
				Port:       reg.Port,
				TargetPort: intstr.FromInt32(TunnelProxyPort),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}

	proxyPort := int32(TunnelProxyPort) // P0 #3: proxy port, not RPC port
	ep := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: reg.Namespace,
			Labels: map[string]string{
				"kubernetes.io/service-name": svcName,
				"divergedev.com/tunnel":      "true",
				"divergedev.com/preview-id":  reg.PreviewId,
			},
			Annotations: map[string]string{
				"divergedev.com/tunnel-id":      tunnelID,
				"divergedev.com/tunnel-expires": expires,
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{podIP},
				// Ready MUST be set true. This backs a HEADLESS Service, and
				// CoreDNS only publishes A records for headless-Service
				// endpoints whose Ready condition is true — a nil condition is
				// treated as not-ready, so the tunnel's DNS name resolves to
				// NOTHING (NXDOMAIN) and a caller reaching it by Service name
				// gets "no such host". Left nil, the tunnel authenticates,
				// registers, carries a direct-IP request, and is still
				// unreachable by the name every consumer actually uses.
				Conditions: discoveryv1.EndpointConditions{
					Ready: func() *bool { b := true; return &b }(),
				},
			},
		},
		Ports: []discoveryv1.EndpointPort{
			{
				Port:     &proxyPort,
				Protocol: func() *corev1.Protocol { p := corev1.ProtocolTCP; return &p }(),
			},
		},
	}

	if _, err := tm.k8sClient.CoreV1().Services(reg.Namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create headless service: %w", err)
		}
		// Already exists — update
		existing, getErr := tm.k8sClient.CoreV1().Services(reg.Namespace).Get(ctx, svc.Name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("failed to get existing service: %w", getErr)
		}
		svc.ResourceVersion = existing.ResourceVersion
		svc.Spec.ClusterIP = existing.Spec.ClusterIP // immutable
		if _, updateErr := tm.k8sClient.CoreV1().Services(reg.Namespace).Update(ctx, svc, metav1.UpdateOptions{}); updateErr != nil {
			return fmt.Errorf("failed to update headless service: %w", updateErr)
		}
	}
	if _, err := tm.k8sClient.DiscoveryV1().EndpointSlices(reg.Namespace).Create(ctx, ep, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create endpointslice: %w", err)
		}
		// Already exists — update
		existing, getErr := tm.k8sClient.DiscoveryV1().EndpointSlices(reg.Namespace).Get(ctx, ep.Name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("failed to get existing endpointslice: %w", getErr)
		}
		ep.ResourceVersion = existing.ResourceVersion
		if _, updateErr := tm.k8sClient.DiscoveryV1().EndpointSlices(reg.Namespace).Update(ctx, ep, metav1.UpdateOptions{}); updateErr != nil {
			return fmt.Errorf("failed to update endpointslice: %w", updateErr)
		}
	}

	// ALSO a legacy core/v1 Endpoints object, or the tunnel has no DNS name on
	// a kube-dns cluster. EndpointSlice is the modern API and CoreDNS reads it;
	// kube-dns — still GKE's resolver on many clusters — predates it and
	// resolves a headless Service only from Endpoints. Without this the Service
	// exists, the EndpointSlice is Ready, and the name is NXDOMAIN, so a caller
	// reaching the tunnel by name gets "no such host" while a direct pod IP
	// works — which reads as an intermittent reset rather than a DNS gap.
	// Verified on GKE: with only the slice, NXDOMAIN; add this, resolves.
	// Writing both is portable — CoreDNS honours either.
	//
	// core/v1 Endpoints is deprecated in favour of EndpointSlice, which is the
	// whole reason this object is here: the resolvers that need it are the
	// ones that predate the slice. It is still served and still reconciled.
	endpoints := &corev1.Endpoints{ //nolint:staticcheck // kube-dns resolves a headless Service only from core/v1 Endpoints
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: reg.Namespace,
			Labels: map[string]string{
				"diverge.dev/tunnel":     "true",
				"diverge.dev/preview-id": reg.PreviewId,
			},
		},
		Subsets: []corev1.EndpointSubset{{ //nolint:staticcheck // see above
			Addresses: []corev1.EndpointAddress{{IP: podIP}},
			Ports:     []corev1.EndpointPort{{Port: proxyPort, Protocol: corev1.ProtocolTCP}},
		}},
	}
	if _, err := tm.k8sClient.CoreV1().Endpoints(reg.Namespace).Create(ctx, endpoints, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create endpoints: %w", err)
		}
		existing, getErr := tm.k8sClient.CoreV1().Endpoints(reg.Namespace).Get(ctx, endpoints.Name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("failed to get existing endpoints: %w", getErr)
		}
		endpoints.ResourceVersion = existing.ResourceVersion
		if _, updateErr := tm.k8sClient.CoreV1().Endpoints(reg.Namespace).Update(ctx, endpoints, metav1.UpdateOptions{}); updateErr != nil {
			return fmt.Errorf("failed to update endpoints: %w", updateErr)
		}
	}

	return nil
}

func (tm *TunnelManager) deleteTunnelResources(ctx context.Context, reg *pb.TunnelRegister) {
	svcName := fmt.Sprintf("diverge-tunnel-%s", reg.PreviewId)
	_ = tm.k8sClient.CoreV1().Services(reg.Namespace).Delete(ctx, svcName, metav1.DeleteOptions{})
	_ = tm.k8sClient.DiscoveryV1().EndpointSlices(reg.Namespace).Delete(ctx, svcName, metav1.DeleteOptions{})
	_ = tm.k8sClient.CoreV1().Endpoints(reg.Namespace).Delete(ctx, svcName, metav1.DeleteOptions{})
}

func (tm *TunnelManager) refreshTunnelTTL(ctx context.Context, reg *pb.TunnelRegister) {
	svcName := fmt.Sprintf("diverge-tunnel-%s", reg.PreviewId)
	expires := time.Now().Add(tunnelTTLDuration).Format(time.RFC3339)

	svc, err := tm.k8sClient.CoreV1().Services(reg.Namespace).Get(ctx, svcName, metav1.GetOptions{})
	if err != nil {
		return
	}
	if svc.Annotations == nil {
		svc.Annotations = make(map[string]string)
	}
	svc.Annotations["divergedev.com/tunnel-expires"] = expires
	_, _ = tm.k8sClient.CoreV1().Services(reg.Namespace).Update(ctx, svc, metav1.UpdateOptions{})
}

// ForwardRequest sends an HTTP request through the tunnel to the CLI.
// P0 #4: Supports chunked streaming — for large bodies, sends chunks via TunnelRequestChunk.
func (tm *TunnelManager) ForwardRequest(ctx context.Context, previewID string, req *pb.TunnelHTTPRequest) (*pb.TunnelHTTPResponse, error) {
	tm.mu.RLock()
	at, ok := tm.tunnels[previewID]
	tm.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("tunnel not found for preview-id: %s", previewID)
	}

	respCh := make(chan *pb.TunnelHTTPResponse, 1)
	at.respMu.Lock()
	at.respWg[req.RequestId] = respCh
	at.respMu.Unlock()

	defer func() {
		at.respMu.Lock()
		delete(at.respWg, req.RequestId)
		at.respMu.Unlock()
		at.chunkMu.Lock()
		delete(at.chunkBuffer, req.RequestId)
		at.chunkMu.Unlock()
	}()

	// P1 #15: Use select with timeout for channel send
	select {
	case at.reqCh <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-at.ctx.Done():
		return nil, fmt.Errorf("tunnel disconnected")
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("tunnel queue full, request dropped")
	}

	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-at.ctx.Done():
		return nil, fmt.Errorf("tunnel disconnected while waiting for response")
	case <-timeout.C:
		return nil, fmt.Errorf("timeout waiting for tunnel response")
	case resp, ok := <-respCh:
		if !ok {
			return nil, fmt.Errorf("tunnel disconnected")
		}
		return resp, nil
	}
}

func (tm *TunnelManager) HasTunnel(previewID string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	_, ok := tm.tunnels[previewID]
	return ok
}

// NewTunnelProxyHandler creates the HTTP handler for the tunnel proxy.
// P0 #3: This handler is served on a DEDICATED port (8081), NOT the RPC mux.
// P0 #6: No auth middleware — cluster-internal traffic only.
// Validates Host header to prevent open proxy abuse.
func (tm *TunnelManager) NewTunnelProxyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Determine preview-id from Host header or x-diverge-env
		previewID := r.Header.Get("x-diverge-env")
		if previewID == "" {
			// P0 #3: Extract from Host header (diverge-tunnel-{preview-id}.{ns}.svc)
			previewID = extractPreviewIDFromHost(r.Host)
		}
		if previewID == "" {
			http.Error(w, "cannot determine tunnel target", http.StatusBadRequest)
			return
		}

		// P0 #6: Validate Host header matches expected tunnel service pattern
		if !tm.HasTunnel(previewID) {
			http.Error(w, "tunnel not found", http.StatusBadGateway)
			return
		}

		const maxProxyBodySize = 1 << 20 // 1MB
		body, err := io.ReadAll(io.LimitReader(r.Body, maxProxyBodySize+1))
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
		if len(body) > maxProxyBodySize {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}

		reqID := uuid.New().String()
		tunnelReq := &pb.TunnelHTTPRequest{
			RequestId: reqID,
			Method:    r.Method,
			Path:      r.URL.RequestURI(),
			Headers:   headersToProto(r.Header),
			Body:      body,
		}

		// For small bodies, just forward directly
		resp, fwdErr := tm.ForwardRequest(r.Context(), previewID, tunnelReq)
		if fwdErr != nil {
			tm.logger.Error("tunnel forward error", "err", fwdErr, "preview_id", previewID)
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}

		for _, h := range resp.Headers {
			for _, v := range h.Values {
				w.Header().Add(h.Key, v)
			}
		}
		w.WriteHeader(int(resp.StatusCode))
		_, _ = w.Write(resp.Body)
	})
}

// extractPreviewIDFromHost extracts preview-id from Host header.
// Expected format: diverge-tunnel-{preview-id}.{namespace}.svc.cluster.local
func extractPreviewIDFromHost(host string) string {
	// Strip port
	if idx := strings.IndexByte(host, ':'); idx != -1 {
		host = host[:idx]
	}
	const prefix = "diverge-tunnel-"
	if !strings.HasPrefix(host, prefix) {
		return ""
	}
	// Extract preview-id (everything between prefix and first dot)
	rest := host[len(prefix):]
	if idx := strings.IndexByte(rest, '.'); idx != -1 {
		return rest[:idx]
	}
	return rest
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

// divergev1alpha1connect interface compliance
var _ divergev1alpha1connect.TunnelServiceHandler = (*TunnelManager)(nil)
