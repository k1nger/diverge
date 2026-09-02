package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	divergev1alpha1 "github.com/divergedev/diverge/api/v1alpha1"
	"github.com/divergedev/diverge/internal/observability"
	"github.com/divergedev/diverge/internal/server"
	"github.com/divergedev/diverge/internal/server/auth"
	"github.com/divergedev/diverge/internal/server/streaming"
)

var (
	scheme  = runtime.NewScheme()
	version = "dev" // Overridden via ldflags: -X main.version=...
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(divergev1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		addr               string
		metricsAddr        string
		tlsCertFile        string
		tlsKeyFile         string
		secureCookiesMode  string
		tokenCacheTTL      time.Duration
		maxStreams         int
		maxStreamsPerUser  int
		audiences          string
		corsAllowedOrigins string
		corsMaxAge         int
		shutdownTimeout    time.Duration
		dashboardEnabled   bool
		// OIDC SSO flags
		oidcIssuerURL     string
		oidcClientID      string
		oidcClientSecret  string
		oidcRedirectURL   string
		oidcScopes        string
		oidcProviderName  string
		oidcUsernameClaim string
		oidcGroupsClaim   string
		oidcAllowedGroups string
		// Session flags
		sessionSecret string
		sessionMaxAge time.Duration
	)

	flag.StringVar(&addr, "addr", ":8443", "Main server listen address")
	flag.StringVar(&metricsAddr, "metrics-addr", ":9090", "Prometheus metrics endpoint address")
	flag.StringVar(&tlsCertFile, "tls-cert-file", "", "TLS certificate file (optional)")
	flag.StringVar(&tlsKeyFile, "tls-key-file", "", "TLS private key file (optional)")
	flag.StringVar(&secureCookiesMode, "secure-cookies", server.SecureCookiesAuto,
		"Set the Secure flag on session cookies: 'auto' (when this server terminates TLS, or the OIDC redirect URL is https), 'true', or 'false'")
	flag.DurationVar(&tokenCacheTTL, "token-cache-ttl", 5*time.Second, "TokenReview cache TTL")
	flag.IntVar(&maxStreams, "max-streams", 250, "Maximum concurrent streams (global)")
	flag.IntVar(&maxStreamsPerUser, "max-streams-per-user", 20, "Maximum concurrent streams per user")
	flag.StringVar(&audiences, "audiences", "diverge-server", "Comma-separated list of valid token audiences")
	// WARNING: Default "*" allows all origins. In production, set this to your
	// specific domain(s) to prevent unauthorized cross-origin access.
	flag.StringVar(&corsAllowedOrigins, "cors-allowed-origins", "*", "Comma-separated list of allowed CORS origins")
	flag.IntVar(&corsMaxAge, "cors-max-age", 86400, "CORS max age in seconds")
	flag.DurationVar(&shutdownTimeout, "shutdown-timeout", 25*time.Second, "Graceful shutdown timeout (should be < K8s terminationGracePeriodSeconds)")
	flag.BoolVar(&dashboardEnabled, "dashboard", true, "Enable the embedded web dashboard at /")
	// OIDC SSO
	flag.StringVar(&oidcIssuerURL, "oidc-issuer-url", "", "OIDC provider issuer URL (empty = SSO disabled)")
	flag.StringVar(&oidcClientID, "oidc-client-id", "", "OIDC client ID")
	flag.StringVar(&oidcClientSecret, "oidc-client-secret", "", "OIDC client secret (or set DIVERGE_OIDC_CLIENT_SECRET env)")
	flag.StringVar(&oidcRedirectURL, "oidc-redirect-url", "", "OIDC callback URL (e.g. https://diverge.example.com/auth/callback)")
	flag.StringVar(&oidcScopes, "oidc-scopes", "openid,profile,email,groups", "Comma-separated OIDC scopes")
	flag.StringVar(&oidcProviderName, "oidc-provider-name", "SSO", "Display name for the SSO button (e.g. Okta, Google)")
	flag.StringVar(&oidcUsernameClaim, "oidc-username-claim", "preferred_username", "OIDC JWT claim for username")
	flag.StringVar(&oidcGroupsClaim, "oidc-groups-claim", "groups", "OIDC JWT claim for groups")
	flag.StringVar(&oidcAllowedGroups, "oidc-allowed-groups", "", "Comma-separated list of allowed OIDC groups (empty = all)")
	// Session
	flag.StringVar(&sessionSecret, "session-secret", "", "HMAC signing key for session JWTs (base64-encoded, or set DIVERGE_SESSION_SECRET env)")
	flag.DurationVar(&sessionMaxAge, "session-max-age", 24*time.Hour, "Session cookie max age")
	flag.Parse()

	// Structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Signal-driven graceful shutdown (moved up for tracing)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	shutdownTracing, err := observability.Setup(ctx, "diverge-server")
	if err != nil {
		logger.Error("failed to setup tracing", "error", err)
		os.Exit(1)
	}

	logger.Info("starting diverge-server",
		"addr", addr,
		"metrics_addr", metricsAddr,
		"token_cache_ttl", tokenCacheTTL,
		"max_streams", maxStreams,
		"max_streams_per_user", maxStreamsPerUser,
	)

	// Validate stream limits
	if maxStreamsPerUser <= 0 || maxStreamsPerUser > maxStreams {
		logger.Error("invalid stream limits: max-streams-per-user must be in (0, max-streams]",
			"max-streams", maxStreams, "max-streams-per-user", maxStreamsPerUser)
		os.Exit(1)
	}

	// Build in-cluster K8s config
	cfg := ctrl.GetConfigOrDie()

	// Create typed clientset (for TokenReview, SubjectAccessReview)
	k8sClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		logger.Error("failed to create kubernetes clientset", "error", err)
		os.Exit(1)
	}

	// Create controller-runtime client (for CRD operations).
	// We intentionally use client.New() (direct, uncached client) instead of
	// a cached client to avoid read-your-writes cache lag. This ensures API
	// clients always read the most up-to-date state immediately after mutations.
	crClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		logger.Error("failed to create controller-runtime client", "error", err)
		os.Exit(1)
	}

	// Initialize streaming components
	broadcasterMetrics := server.GetBroadcasterMetrics()
	informerMgr := streaming.NewInformerManager(logger, broadcasterMetrics)
	logStreamer := streaming.NewLogStreamer(k8sClient)

	// Create stream limiter (per-user + global quotas)
	streamLimiter := server.NewStreamLimiter(maxStreams, maxStreamsPerUser, server.GetStreamLimiterMetrics())
	server.SetStreamLimiterMax(maxStreams)

	// Auth setup
	tokenCache := auth.NewTokenCache(1024, tokenCacheTTL)
	tokenReviewProvider := auth.NewTokenReviewProvider(k8sClient, []string{audiences})

	authMetrics := server.NewAuthMetrics()
	auditLogger := server.NewAuditLogger(logger)

	// Session manager (for OIDC session JWTs)
	var signingKey []byte
	if sessionSecret != "" {
		var err error
		signingKey, err = base64.StdEncoding.DecodeString(sessionSecret)
		if err != nil {
			logger.Error("failed to decode session secret (must be base64)", "error", err)
			os.Exit(1)
		}
	} else if envSecret := os.Getenv("DIVERGE_SESSION_SECRET"); envSecret != "" {
		var err error
		signingKey, err = base64.StdEncoding.DecodeString(envSecret)
		if err != nil {
			logger.Error("failed to decode DIVERGE_SESSION_SECRET (must be base64)", "error", err)
			os.Exit(1)
		}
	}
	// If no key provided, SessionManager will auto-generate (ephemeral)
	sessionMgr, err := auth.NewSessionManager(auth.SessionConfig{
		SigningKey: signingKey,
		MaxAge:     sessionMaxAge,
	})
	if err != nil {
		logger.Error("failed to create session manager", "error", err)
		os.Exit(1)
	}

	// Build composite auth provider
	var authProvider auth.AuthProvider
	exemptPrefixes := func() []string {
		prefixes := []string{}
		if dashboardEnabled {
			prefixes = append(prefixes, "/assets/")
		}
		return prefixes
	}()

	// Resolve OIDC client secret from env if not set via flag
	if oidcClientSecret == "" {
		oidcClientSecret = os.Getenv("DIVERGE_OIDC_CLIENT_SECRET")
	}

	// OIDC SSO setup
	var oidcHandler *server.OIDCHandler
	if oidcIssuerURL != "" {
		logger.Info("OIDC SSO enabled", "issuer", oidcIssuerURL, "provider", oidcProviderName)

		var allowedGroups []string
		if oidcAllowedGroups != "" {
			for _, g := range strings.Split(oidcAllowedGroups, ",") {
				if trimmed := strings.TrimSpace(g); trimmed != "" {
					allowedGroups = append(allowedGroups, trimmed)
				}
			}
		}

		scopes := strings.Split(oidcScopes, ",")

		// Create OIDC auth provider for JWT verification
		oidcProvider, err := auth.NewOIDCProvider(context.Background(), auth.OIDCProviderConfig{
			IssuerURL:     oidcIssuerURL,
			ClientID:      oidcClientID,
			UsernameClaim: oidcUsernameClaim,
			GroupsClaim:   oidcGroupsClaim,
			AllowedGroups: allowedGroups,
		}, sessionMgr, logger)
		if err != nil {
			logger.Error("failed to create OIDC provider", "error", err)
			os.Exit(1)
		}

		// Composite: OIDC/session first, then TokenReview
		composite := auth.NewCompositeProvider(logger)
		composite.Add("oidc", oidcProvider)
		composite.Add("tokenreview", tokenReviewProvider)
		authProvider = composite

		// Create OIDC HTTP handler
		cookieResolver := server.CookiePolicyResolver{
			Mode:       secureCookiesMode,
			TLSEnabled: tlsCertFile != "",
			PublicURL:  oidcRedirectURL,
		}
		cookiePolicy, secErr := cookieResolver.Resolve()
		if secErr != nil {
			logger.Error("invalid secure cookie configuration", "err", secErr)
			os.Exit(1)
		}
		if !cookiePolicy.Secure {
			logger.Warn("session cookies will not have the Secure flag",
				"hint", "set --secure-cookies=true when TLS is terminated in front of this server")
		}
		oidcHandler, err = server.NewOIDCHandler(server.OIDCHandlerConfig{
			IssuerURL:      oidcIssuerURL,
			ClientID:       oidcClientID,
			ClientSecret:   oidcClientSecret,
			RedirectURL:    oidcRedirectURL,
			Scopes:         scopes,
			ProviderName:   oidcProviderName,
			SessionManager: sessionMgr,
			SessionMaxAge:  sessionMaxAge,
			SecureCookies:  cookiePolicy.Secure,
			UsernameClaim:  oidcUsernameClaim,
			GroupsClaim:    oidcGroupsClaim,
			Logger:         logger,
		})
		if err != nil {
			logger.Error("failed to create OIDC handler", "error", err)
			os.Exit(1)
		}

		exemptPrefixes = append(exemptPrefixes, "/auth/")
	} else {
		authProvider = tokenReviewProvider
		logger.Info("OIDC SSO disabled (no --oidc-issuer-url), using TokenReview only")
	}

	authMiddleware := auth.NewMiddleware(auth.MiddlewareConfig{
		Provider: authProvider,
		Cache:    tokenCache,
		Logger:   logger,
		Metrics: &auth.AuthMetrics{
			Latency:     authMetrics.Latency,
			CacheHits:   authMetrics.CacheHits,
			CacheMisses: authMetrics.CacheMisses,
			Attempts:    authMetrics.Attempts,
		},
		ExemptPaths:    []string{"/healthz", "/readyz"},
		ExemptPrefixes: exemptPrefixes,
		// Only meaningful when SSO is configured: a signed-out browser
		// navigation goes to sign-in instead of a bare 401 page. Every
		// session expires, so this is the routine path, not an edge.
		LoginURL: func() string {
			if oidcIssuerURL != "" {
				return "/auth/login"
			}
			return ""
		}(),
	})

	// Build the ConnectRPC mux
	mux, tunnelMgr := server.NewServeMux(server.ServeMuxConfig{
		Client:           crClient,
		K8sClient:        k8sClient,
		InformerMgr:      informerMgr,
		LogStreamer:      logStreamer,
		StreamLimiter:    streamLimiter,
		Logger:           logger,
		AuditLogger:      auditLogger,
		Version:          version,
		DashboardEnabled: dashboardEnabled,
	})

	// Register OIDC auth routes (exempt from auth middleware)
	if oidcHandler != nil {
		oidcHandler.RegisterRoutes(mux)
	}

	// Health check (exempt from auth)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// Protocol telemetry (records wire protocol + client SDK after auth)
	handler := server.ProtocolTelemetryMiddleware(mux)

	// Wrap with auth middleware (runs before telemetry)
	handler = authMiddleware(handler)

	rawOrigins := strings.Split(corsAllowedOrigins, ",")
	origins := make([]string, 0, len(rawOrigins))
	for _, o := range rawOrigins {
		trimmed := strings.TrimSpace(o)
		if trimmed != "" {
			origins = append(origins, trimmed)
		}
	}

	opts := cors.Options{
		AllowedMethods:   []string{"GET", "POST"},
		AllowedHeaders:   []string{"Authorization", "Content-Type", "Connect-Protocol-Version", "Connect-Timeout-Ms", "X-Grpc-Web", "X-User-Agent", "Grpc-Timeout"},
		ExposedHeaders:   []string{"Grpc-Status", "Grpc-Message", "Grpc-Status-Details-Bin"},
		AllowCredentials: false, // Disabling AllowCredentials prevents CSRF vulnerability with wildcard origins
		MaxAge:           corsMaxAge,
	}
	if len(origins) == 1 && origins[0] == "*" {
		opts.AllowOriginFunc = func(origin string) bool { return true }
	} else {
		opts.AllowedOrigins = origins
	}

	corsHandler := cors.New(opts)

	handler = corsHandler.Handler(handler)

	// Main server — do NOT bind signal context to BaseContext
	// as it would cancel in-flight requests immediately on SIGTERM,
	// bypassing graceful shutdown. Let Shutdown() handle draining.
	// NO ReadTimeout, and that is not a relaxation of a security control — it
	// is the difference between the tunnel working and not. ReadTimeout bounds
	// the WHOLE request including the body, and TunnelService/Tunnel is a
	// bidirectional stream whose request body never ends. A 30s ReadTimeout
	// therefore severed every tunnel at exactly 30 seconds — measured,
	// was_connected=30.31s then "deadline_exceeded: i/o timeout" then
	// reconnect, forever — which made previews usable only for sub-30s bursts
	// and left a dead window on every cycle.
	//
	// ReadHeaderTimeout still bounds the headers, so slowloris is still
	// refused; IdleTimeout still reaps idle keep-alive connections. What is
	// removed is only the deadline on a body that is a stream by design. The
	// dashboard's ordinary requests finish well inside IdleTimeout and are
	// unaffected.
	mainSrv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Without TLS there is no ALPN, so net/http alone never negotiates HTTP/2
	// and answers a prior-knowledge HTTP/2 preface with 505 HTTP Version Not
	// Supported. TunnelService/Tunnel is a bidirectional stream, which
	// connect-go refuses over HTTP/1.1 — so a plaintext deployment (the
	// chart's default; it sets no --tls-cert-file) could authenticate a tunnel
	// and then never establish one. Unencrypted HTTP/2 and HTTP/1.1 are served
	// side by side on the same listener, leaving the dashboard's browser
	// traffic untouched. With TLS the standard ALPN path already provides
	// HTTP/2, so the default protocol set is kept.
	if tlsCertFile == "" || tlsKeyFile == "" {
		mainSrv.Protocols = new(http.Protocols)
		mainSrv.Protocols.SetHTTP1(true)
		mainSrv.Protocols.SetUnencryptedHTTP2(true)
	}

	// Metrics server
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(crmetrics.Registry, promhttp.HandlerOpts{}))
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	metricsSrv := &http.Server{
		Addr:              metricsAddr,
		Handler:           metricsMux,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Start tunnel proxy server on dedicated port (no auth, cluster-internal)
	tunnelProxySrv := server.NewTunnelProxyServer(tunnelMgr, server.TunnelProxyPort, logger)

	// Use errgroup to tie server lifecycles together
	g, gCtx := errgroup.WithContext(ctx)

	// Start main server
	g.Go(func() error {
		logger.Info("server listening", "addr", addr)
		var listenErr error
		if tlsCertFile != "" && tlsKeyFile != "" {
			listenErr = mainSrv.ListenAndServeTLS(tlsCertFile, tlsKeyFile)
		} else {
			listenErr = mainSrv.ListenAndServe()
		}
		if listenErr != nil && listenErr != http.ErrServerClosed {
			return fmt.Errorf("server listen failed: %w", listenErr)
		}
		return nil
	})

	// Start metrics server
	g.Go(func() error {
		logger.Info("metrics server listening", "addr", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("metrics server listen failed: %w", err)
		}
		return nil
	})

	// Start tunnel proxy server
	g.Go(func() error {
		logger.Info("tunnel proxy listening", "port", server.TunnelProxyPort)
		if err := tunnelProxySrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("tunnel proxy listen failed: %w", err)
		}
		return nil
	})

	// Graceful shutdown goroutine
	g.Go(func() error {
		<-gCtx.Done()
		logger.Info("shutdown signal received, draining connections")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer shutdownCancel()

		// 1. Close broadcasters FIRST to unblock watch handlers.
		// http.Server.Shutdown() waits for active handlers but does NOT
		// cancel their request contexts. Watch handlers block on
		// <-sub.Events(), so we must close the channels first to let
		// them return, otherwise Shutdown() hangs for the full timeout.
		informerMgr.Close()

		// 2. Drain HTTP servers concurrently (sends GOAWAY, waits for handlers)
		var shutdownWg sync.WaitGroup
		servers := []*http.Server{mainSrv, metricsSrv, tunnelProxySrv}
		for _, srv := range servers {
			shutdownWg.Add(1)
			go func(s *http.Server) {
				defer shutdownWg.Done()
				if err := s.Shutdown(shutdownCtx); err != nil {
					logger.Error("server shutdown error", "addr", s.Addr, "error", err)
				}
			}(srv)
		}
		shutdownWg.Wait()

		logger.Info("shutdown complete")
		return nil
	})

	errWait := g.Wait()

	tracingCtx, tracingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tracingCancel()
	if err := shutdownTracing(tracingCtx); err != nil {
		logger.Error("tracing shutdown error", "error", err)
	}

	if errWait != nil {
		logger.Error("server exited with error", "error", errWait)
		os.Exit(1)
	}
}
