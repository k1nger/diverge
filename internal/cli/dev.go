package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	divergeiov1alpha1 "github.com/divergedev/diverge/api/v1alpha1"
	"github.com/divergedev/diverge/internal/config"
	"github.com/divergedev/diverge/internal/git"
	"github.com/divergedev/diverge/internal/proxy"
	"github.com/divergedev/diverge/pkg/devsession"
	"github.com/divergedev/diverge/pkg/license"
)

// ErrCollision indicates that a preview group with the same name already exists but belongs to a different owner.
var ErrCollision = errors.New("preview group collision")

const (
	envDivergeProxyURL  = "DIVERGE_PROXY_URL"
	envDivergeProxyMode = "DIVERGE_PROXY_MODE"
	proxyModePath       = "path"
	proxyModeHost       = "host"
)

// DevOptions holds optional configuration for the dev command.
type DevOptions struct {
	Detector       EnvironmentDetector
	Discoverer     ServerDiscoverer
	SessionManager devsession.Manager
	resolvedEnvMap map[string]string
}

// DevOption configures DevOptions (e.g. WithDetector, WithPort, WithService).
type DevOption func(*DevOptions)

// WithEnvironmentDetector allows injecting a custom EnvironmentDetector for testing.
func WithEnvironmentDetector(d EnvironmentDetector) DevOption {
	return func(o *DevOptions) { o.Detector = d }
}

// WithServerDiscoverer allows injecting a custom ServerDiscoverer for testing.
func WithServerDiscoverer(d ServerDiscoverer) DevOption {
	return func(o *DevOptions) { o.Discoverer = d }
}

// WithSessionManager allows injecting a custom devsession.Manager for testing.
func WithSessionManager(m devsession.Manager) DevOption {
	return func(o *DevOptions) { o.SessionManager = m }
}

func newDevCmd(app *App) *cobra.Command {
	var (
		serviceFlag    string
		portFlag       int32
		endpointFlag   string
		devspaceFlag   bool
		previewIdFlag  string
		noTunnelFlag   bool
		serverFlag     string
		tokenFlag      string
		proxyPortFlag  int
		noProxyFlag    bool
		proxyModeFlag  string
		watchEnvFlag   bool
		onConflictFlag string
		forceFlag      bool
	)

	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Route cluster traffic for a service to your local machine",
		Long: `Start a local development session by creating a PreviewGroup that routes
traffic for the specified service to your local machine's Tailscale IP or via a tunnel.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if proxyPortFlag < 0 || proxyPortFlag > 65535 {
				return fmt.Errorf("--proxy-port must be between 0 and 65535, got %d", proxyPortFlag)
			}
			if proxyModeFlag != proxyModePath && proxyModeFlag != proxyModeHost {
				return fmt.Errorf("--proxy-mode must be %q or %q, got %q", proxyModePath, proxyModeHost, proxyModeFlag)
			}
			if onConflictFlag != "" && onConflictFlag != "warn" && onConflictFlag != "block" && onConflictFlag != "allow" {
				return fmt.Errorf("--on-conflict must be \"warn\", \"block\", or \"allow\", got %q", onConflictFlag)
			}
			return runDev(runDevParams{
				App: app, Service: serviceFlag, Port: portFlag,
				Endpoint: endpointFlag, Devspace: devspaceFlag,
				PreviewID: previewIdFlag, Args: args, Cmd: cmd,
				NoTunnel: noTunnelFlag, Server: serverFlag, Token: tokenFlag,
				ProxyPort: proxyPortFlag, NoProxy: noProxyFlag,
				ProxyMode: proxyModeFlag, WatchEnv: watchEnvFlag,
				OnConflict: onConflictFlag, Force: forceFlag,
			})
		},
	}
	cmd.Flags().StringVar(&serviceFlag, "service", "", "Service name (default: auto-detect)")
	cmd.Flags().Int32Var(&portFlag, "port", 0, "Local port (default: 8080)")
	cmd.Flags().StringVar(&endpointFlag, "endpoint", "", "Local endpoint IP (default: auto-detect or tunnel)")
	cmd.Flags().BoolVar(&devspaceFlag, "devspace", false, "Generate a devspace.yaml template and show DevSpace instructions")
	cmd.Flags().StringVar(&previewIdFlag, "preview-id", "", "Preview ID for routing (default: git branch name)")
	cmd.Flags().BoolVar(&noTunnelFlag, "no-tunnel", false, "Disable ConnectRPC tunnel and use direct routing (e.g., Tailscale)")
	cmd.Flags().StringVar(&serverFlag, "server", "", "Diverge server address for tunnel (default: auto-detect via port-forward)")
	cmd.Flags().StringVar(&tokenFlag, "token", "", "Bearer token for the diverge server (default: $DIVERGE_TOKEN, then the kubeconfig credential)")
	cmd.Flags().IntVar(&proxyPortFlag, "proxy-port", 19001, "Local loopback proxy port for outbound service routing")
	cmd.Flags().BoolVar(&noProxyFlag, "no-proxy", false, "Disable local loopback proxy")
	cmd.Flags().StringVar(&proxyModeFlag, "proxy-mode", proxyModePath, "Proxy routing mode: 'path' (default) or 'host' (requires *.localhost DNS)")
	cmd.Flags().BoolVar(&watchEnvFlag, "watch-env", false, "Auto-restart child process when environment configuration changes")
	cmd.Flags().StringVar(&onConflictFlag, "on-conflict", "", "Conflict resolution policy when another user is developing this service: 'warn' (default), 'block', or 'allow'")
	cmd.Flags().BoolVar(&forceFlag, "force", false, "Force start dev session, overriding any active collision")

	cmd.AddCommand(newDevSessionsCmd(app))

	return cmd
}

type runDevParams struct {
	App        *App
	Service    string
	Port       int32
	Endpoint   string
	Devspace   bool
	PreviewID  string
	Args       []string
	Cmd        *cobra.Command
	NoTunnel   bool
	Server     string
	Token      string
	Options    []DevOption
	ProxyPort  int
	NoProxy    bool
	ProxyMode  string
	WatchEnv   bool
	OnConflict string
	Force      bool
}

func runDev(p runDevParams) error {
	ctx := p.Cmd.Context()

	if p.Devspace {
		defaultService := "my-service"
		if p.Service != "" {
			defaultService = p.Service
		}

		// editorconfig-checker-disable
		devspaceTemplate := fmt.Sprintf(`version: v2beta1
name: diverge-dev

# Import Diverge dev vars
vars:
  DIVERGE_SERVICE: ${DIVERGE_SERVICE:-%s}
  DIVERGE_BRANCH:
    command: git branch --show-current

pipelines:
  diverge-dev:
    run: |-
      diverge dev --service ${DIVERGE_SERVICE} -- devspace dev

dev:
  app:
    imageSelector: ${DIVERGE_SERVICE}
    sync:
      - path: ./:/app
        excludePaths:
          - .git/
          - node_modules/
    terminal:
      command: ./start-dev.sh
    ports:
      - port: "8080:8080"
`, defaultService)
		// editorconfig-checker-enable

		if _, err := os.Stat("devspace.yaml"); err == nil {
			fmt.Println("ℹ️  devspace.yaml already exists, skipping creation.")
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("checking devspace.yaml: %w", err)
		} else {
			if err := os.WriteFile("devspace.yaml", []byte(devspaceTemplate), 0644); err != nil {
				return fmt.Errorf("failed to create devspace.yaml: %w", err)
			}
			fmt.Println("✅ Created devspace.yaml template in current directory.")
		}
		fmt.Println("\nTo start developing with DevSpace and Diverge:")
		fmt.Println("  1. Edit devspace.yaml to match your service.")
		fmt.Println("  2. Run: DIVERGE_SERVICE=your-service devspace run diverge-dev")
		fmt.Println("\nFor more details, see docs/guides/devspace-integration.md")
		return nil
	}

	devOpts := &DevOptions{
		Detector: &DefaultEnvironmentDetector{},
	}
	for _, opt := range p.Options {
		opt(devOpts)
	}
	detector := devOpts.Detector

	// 1. Auto-detect service name
	serviceName := p.Service
	if serviceName == "" {
		s, err := detector.DetectServiceName(ctx)
		if err != nil {
			return fmt.Errorf("failed to detect service name: %w. Use --service flag", err)
		}
		serviceName = s
	}
	if serviceName == "" {
		return fmt.Errorf("could not determine service name: use --service flag")
	}

	// 2. Auto-detect endpoint
	endpointIP := p.Endpoint
	if endpointIP == "" {
		ip, err := detector.DetectLocalIP(ctx)
		if err != nil {
			return err
		}
		endpointIP = ip
	}

	// 3. Auto-detect port
	port := p.Port
	if port == 0 {
		port = 8080
	}
	endpoint := fmt.Sprintf("%s:%d", endpointIP, port)

	// 4. Construct header value
	headerValue := "local-dev"
	if p.PreviewID != "" {
		headerValue = p.PreviewID
	} else {
		branch, err := detector.DetectGitBranch(ctx)
		if err == nil && branch != "" {
			headerValue = git.SlugifyBranch(branch)
		} else if err != nil {
			slog.Debug("failed to detect git branch", "error", err)
		}
	}

	// 5. Construct group name
	username, _ := detector.DetectUsername(ctx)
	groupName := fmt.Sprintf("dev-%s-%s", username, serviceName)

	// Normalize group name
	groupName = strings.ReplaceAll(groupName, "_", "-")
	groupName = strings.ToLower(groupName)
	groupName = strings.TrimRight(groupName, "-")

	c, clientset, err := p.App.KubeClient()
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	ns := p.App.Namespace
	if ns == "" {
		ns = "default"
	}

	// 5b. Multi-User Dev Session Conflict Detection (Pro Tier)
	conflictPolicy := devsession.ConflictPolicyWarn
	if p.Force {
		conflictPolicy = devsession.ConflictPolicyAllow
	} else if p.OnConflict != "" {
		conflictPolicy = devsession.ConflictPolicy(p.OnConflict)
	} else if cfg, err := config.Load(".diverge.yaml"); err == nil && cfg != nil {
		if cfg.Defaults.Dev != nil && cfg.Defaults.Dev.OnConflict != "" {
			conflictPolicy = devsession.ConflictPolicy(cfg.Defaults.Dev.OnConflict)
		}
	}

	licInfo, _ := license.CheckFeature("", nil, license.FeatureConflictDetection)
	if licInfo.IsTrial {
		slog.Debug("multi-user conflict detection active (community trial mode)")
	} else if licInfo.IsGrace {
		slog.Warn("Diverge Pro license is within 7-day grace period", "graceUntil", licInfo.GraceUntil)
	}

	sessionMgr := devOpts.SessionManager
	if sessionMgr == nil {
		sessionMgr = devsession.NewSessionManager(c)
	}

	sessionID := uuid.NewString()
	sess := devsession.DevSession{
		ID:        sessionID,
		Service:   serviceName,
		Namespace: ns,
		Developer: username,
		Hostname:  devsession.DetectHostname(),
		Branch:    headerValue,
		PreviewID: headerValue,
		GroupName: groupName,
		PID:       os.Getpid(),
	}

	existingSess, conflicted, acquireErr := sessionMgr.Acquire(ctx, sess, conflictPolicy, p.Force)
	if acquireErr != nil {
		var confErr *devsession.ConflictError
		if errors.As(acquireErr, &confErr) {
			fmt.Print(confErr.FormatWarning(conflictPolicy))
			return fmt.Errorf("dev session blocked: %w", acquireErr)
		}
		return fmt.Errorf("failed to acquire dev session lock: %w", acquireErr)
	}

	if conflicted && existingSess != nil {
		confErr := &devsession.ConflictError{
			Service:          serviceName,
			ExistingSession:  *existingSess,
			CurrentDeveloper: username,
		}
		fmt.Print(confErr.FormatWarning(conflictPolicy))
	}

	if !p.NoTunnel {
		fmt.Println("▸ Establishing tunnel...          ")

		sAddr := p.Server

		// Discovery needs a rest config; with an explicit --server it is only
		// consulted as a credential fallback, so a missing kubeconfig there
		// must not be fatal.
		restCfg, cfgErr := p.App.RestConfig()
		if sAddr == "" {
			if cfgErr != nil {
				return fmt.Errorf("failed to get rest config: %w", cfgErr)
			}

			var stopCh chan struct{}
			var dErr error
			discoverer := devOpts.Discoverer
			if discoverer == nil {
				discoverer = &K8sServerDiscoverer{
					K8sClient:  clientset,
					RestConfig: restCfg,
					Namespace:  p.App.Namespace,
				}
			}
			sAddr, stopCh, dErr = discoverer.Discover(ctx)
			if dErr != nil {
				return fmt.Errorf("failed to discover server: %w", dErr)
			}
			if stopCh != nil {
				defer close(stopCh)
			}
		} else if cfgErr != nil {
			restCfg = nil
		}

		if errs := validation.IsDNS1123Label(headerValue); len(errs) > 0 {
			return fmt.Errorf("preview-id %q is not a valid DNS label: %s", headerValue, strings.Join(errs, "; "))
		}

		// The server authenticates every Tunnel RPC by TokenReview. Fail here
		// with something actionable rather than in a 401 reconnect loop.
		tokenSource, tokenErr := resolveTunnelTokenSource(p.Token, restCfg)
		if tokenErr != nil {
			if errors.Is(tokenErr, ErrNoTunnelCredential) {
				return fmt.Errorf("%w: pass --token, set %s, configure OpenBao/Vault, or use a kubeconfig with a valid bearer or exec credential. "+
					"The token must be accepted by the server's --audiences (default: diverge-server)",
					tokenErr, tunnelTokenEnvVar)
			}
			return tokenErr
		}

		tc := NewTunnelClientWithTokenSource(sAddr, int(port), headerValue, serviceName, ns, tokenSource, tunnelBaseTransport(sAddr), slog.Default())
		go tc.ConnectWithRetry(ctx)

		select {
		case <-tc.Ready:
		case <-time.After(15 * time.Second):
			return fmt.Errorf("tunnel connection timed out")
		case <-ctx.Done():
			return ctx.Err()
		}

		// If using tunnel, the PreviewGroup should point to the tunnel's Headless Service.
		endpoint = fmt.Sprintf("diverge-tunnel-%s.%s.svc.cluster.local:%d", headerValue, ns, port)
	}

	// 6. Create PreviewGroup CR
	pg := &divergeiov1alpha1.PreviewGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: groupName,
		},
		Spec: divergeiov1alpha1.PreviewGroupSpec{
			Owner: username,
			Services: []divergeiov1alpha1.PreviewGroupServiceSpec{
				{
					Name:      serviceName,
					Namespace: p.App.Namespace,
					Mode:      divergeiov1alpha1.ServiceModeLocal,
					Endpoint:  endpoint,
				},
			},
			Routing: divergeiov1alpha1.PreviewGroupRouting{
				HeaderKey:   "x-diverge-env",
				HeaderValue: headerValue,
			},
			Source: divergeiov1alpha1.EnvironmentSource{
				Provider: "local",
				Branch:   headerValue,
			},
		},
	}

	// Load async routes from config
	if cfg, err := config.Load(".diverge.yaml"); err == nil && cfg != nil {
		if svcCfg, ok := cfg.Services[serviceName]; ok && len(svcCfg.AsyncRoutes) > 0 {
			pg.Spec.Services[0].AsyncRoutes = svcCfg.AsyncRoutes
		}
	}

	fmt.Printf("Starting dev session for service %q...\n", serviceName)
	fmt.Printf("Routing traffic with header %s: %s to %s\n", "x-diverge-env", headerValue, endpoint)

	// Atomic Create — handle collision

	if err := c.Create(ctx, pg); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Fetch existing PG
			var existing divergeiov1alpha1.PreviewGroup
			if getErr := c.Get(ctx, types.NamespacedName{Name: groupName}, &existing); getErr != nil {
				return fmt.Errorf("failed to check existing PreviewGroup: %w", getErr)
			}
			if existing.Spec.Owner != "" && existing.Spec.Owner != username {
				return fmt.Errorf("%w: group %q is owned by %q (you are %q). Delete it first with: diverge preview delete %s",
					ErrCollision, groupName, existing.Spec.Owner, username, groupName)
			}
			// Same owner — update instead
			existing.Spec = pg.Spec
			if updateErr := c.Update(ctx, &existing); updateErr != nil {
				return fmt.Errorf("failed to update PreviewGroup: %w", updateErr)
			}
		} else if apierrors.IsInvalid(err) {
			// A DEGRADED SESSION, NOT A DEAD ONE. The installed CRD refuses
			// this spec — today that is guaranteed: dev sets
			// spec.source.provider to "local" and the CRD's enum has only
			// gitlab and github, so this create has never once succeeded
			// against the product's own definition. The PreviewGroup drives
			// controller-side routing and ownership; the TUNNEL depends on
			// neither, and a consumer that routes by the tunnel's Service name
			// works without it. Killing the session here tears down a tunnel
			// that just finished establishing, for the sake of an object the
			// session can live without.
			fmt.Printf("⚠️  PreviewGroup not created (the installed CRD refused it): %v\n", err)
			fmt.Println("   Continuing without one: controller-managed routing and ownership")
			fmt.Println("   checks are unavailable; the tunnel itself is unaffected.")
		} else {
			return fmt.Errorf("failed to create PreviewGroup: %w", err)
		}
	}

	// Start loopback proxy and config watcher
	var cwOpts []ConfigWatcherOption
	var loopbackProxy *proxy.LoopbackProxy

	if !p.NoProxy {
		proxyMode := proxy.ProxyMode(p.ProxyMode)
		if proxyMode == proxy.ModeHost && !proxy.CheckLocalhostDNS() {
			slog.Warn("*.localhost DNS not available on this system, falling back to path mode")
			proxyMode = proxy.ModePath
		}

		loopbackProxy = proxy.NewLoopbackProxy("x-diverge-env", headerValue, p.ProxyPort, proxyMode)

		proxyErrCh := make(chan error, 1)
		go func() {
			proxyErrCh <- loopbackProxy.Start(ctx)
		}()
		defer func() { _ = loopbackProxy.Shutdown(context.Background()) }()

		// Wait for proxy to be ready or fail
		select {
		case <-loopbackProxy.Ready():
			fmt.Printf("▸ Local proxy listening on %s (%s mode)\n", loopbackProxy.Addr(), loopbackProxy.Mode())
			if loopbackProxy.Mode() == proxy.ModeHost {
				fmt.Println("  Try: curl http://<service>.localhost:19001/health")
			} else {
				fmt.Println("  Try: curl http://127.0.0.1:19001/<service>/health")
			}
		case err := <-proxyErrCh:
			if err == nil {
				return fmt.Errorf("loopback proxy stopped unexpectedly")
			}
			return fmt.Errorf("failed to start loopback proxy: %w", err)
		case <-time.After(5 * time.Second):
			return fmt.Errorf("loopback proxy startup timed out")
		}

		// Monitor for late serve errors
		go func() {
			if err := <-proxyErrCh; err != nil {
				slog.Error("loopback proxy error", "error", err)
			}
		}()

		cwOpts = append(cwOpts,
			WithProxyAddr(loopbackProxy.Addr()),
			WithProxyMode(string(loopbackProxy.Mode())),
			WithOnUpdate(func(services []divergeiov1alpha1.PreviewGroupServiceStatus) {
				routes := make([]proxy.ServiceRoute, 0, len(services))
				for _, svc := range services {
					if svc.URL != "" {
						routes = append(routes, proxy.ServiceRoute{Name: svc.Name, URL: svc.URL})
					}
				}
				loopbackProxy.UpdateRoutes(routes)
			}),
		)
	}

	// Create ConfigWatcher first — supervisor needs it for fresh env reads.
	cw := NewConfigWatcher(c, groupName, ".env.diverge", cwOpts...)

	// Set up supervisor for --watch-env, wiring it to the ConfigWatcher.
	var supervisor *Supervisor
	if p.WatchEnv && len(p.Args) > 0 {
		envBuilder := func() map[string]string {
			// Rebuild env from scratch each restart — no stale accumulation.
			// Start with the static baseline env.
			env := make(map[string]string)
			for k, v := range devOpts.resolvedEnvMap {
				env[k] = v
			}
			// Merge latest watcher env (DIVERGE_SVC_*_URL etc.) — overrides baseline.
			if latest := cw.LatestEnvMap(); latest != nil {
				for k, v := range latest {
					env[k] = v
				}
			}
			if loopbackProxy != nil {
				env[envDivergeProxyURL] = loopbackProxy.Addr()
				env[envDivergeProxyMode] = string(loopbackProxy.Mode())
			}
			return env
		}
		supervisor = NewSupervisor(p.Args, envBuilder)
		cw.SetOnEnvChange(func(diff map[string]string) {
			supervisor.RequestRestart("service config changed", diff)
		})
	}

	go func() { _ = cw.Watch(ctx) }()

	// Start lease heartbeat
	heartbeatTicker := time.NewTicker(20 * time.Second)
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()

	go func() {
		defer heartbeatTicker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-heartbeatTicker.C:
				hbCallCtx, hbCallCancel := context.WithTimeout(hbCtx, 5*time.Second)
				_ = sessionMgr.Heartbeat(hbCallCtx, ns, serviceName, sessionID)
				retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
					var current divergeiov1alpha1.PreviewGroup
					if err := c.Get(hbCallCtx, types.NamespacedName{Name: groupName}, &current); err != nil {
						return err
					}
					now := metav1.Now()
					current.Status.LeaseRenewedAt = &now
					return c.Status().Update(hbCallCtx, &current)
				})
				hbCallCancel()
				if retryErr != nil {
					slog.Error("heartbeat failed", "error", retryErr)
				}
			}
		}
	}()

	defer func() {
		sessCtx, sessCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := sessionMgr.Release(sessCtx, ns, serviceName, sessionID); err != nil {
			slog.Debug("failed to release dev session", "error", err)
		}
		sessCancel()

		fmt.Printf("\nCleaning up PreviewGroup %q...\n", groupName)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Delete(cleanupCtx, pg); err != nil {
			slog.Error("failed to clean up PreviewGroup", "name", groupName, "error", err)
		} else {
			fmt.Println("Goodbye!")
		}
	}()

	// 7. Print status
	_ = runPreviewStatus(ctx, p.App, groupName, p.Cmd.OutOrStdout())

	asyncVars, err := waitForAsyncRoutes(ctx, c, groupName, serviceName, ns)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("failed waiting for async routes: %w", err)
	}

	if len(p.Args) == 0 {
		var envBuf bytes.Buffer
		synced, syncErr := syncBaselineEnv(ctx, clientset, syncEnvOptions{
			Namespace:   ns,
			ServiceName: serviceName,
			Overrides:   asyncVars,
		}, &envBuf)

		if syncErr == nil && synced > 0 {
			fmt.Printf("📋 Synced %d env vars from baseline\n", synced)
		} else if syncErr != nil {
			fmt.Printf("⚠️  Could not sync env vars: %v\n", syncErr)
		}
	} else {
		pod, err := findBaselinePod(ctx, clientset, ns, serviceName)
		if err != nil {
			return fmt.Errorf("failed to find baseline pod: %w", err)
		}
		resolvedEnv, err := resolveBaselineEnv(ctx, clientset, pod)
		if err != nil {
			return fmt.Errorf("failed to resolve baseline env: %w", err)
		}
		for k, v := range asyncVars {
			resolvedEnv[k] = v
		}
		if loopbackProxy != nil {
			resolvedEnv[envDivergeProxyURL] = loopbackProxy.Addr()
			resolvedEnv[envDivergeProxyMode] = string(loopbackProxy.Mode())
		}
		fmt.Printf("📋 Resolved %d env vars from baseline (in-memory, no file written)\n", len(resolvedEnv))
		devOpts.resolvedEnvMap = resolvedEnv
	}

	if len(p.Args) > 0 {
		if supervisor != nil {
			// Supervised mode: Supervisor handles restarts on env change.
			fmt.Println("🚀 Starting child process with --watch-env (auto-restart on config change)")
			return supervisor.Run(ctx)
		}

		// Legacy non-supervised mode.
		fmt.Printf("🚀 Starting child process: %v\n", p.Args)
		childCmd, err := runChildProcess(ctx, p.Args, devOpts.resolvedEnvMap)
		if err != nil {
			return fmt.Errorf("failed to start child process: %w", err)
		}
		if childCmd != nil {
			// Wait for the child command to finish or context to be canceled
			if err := childCmd.Wait(); err != nil {
				if ctx.Err() != nil {
					return nil // graceful shutdown
				}
				return fmt.Errorf("child process failed: %w", err)
			}
		}
	} else {
		fmt.Println("Press Ctrl+C to stop dev session...")
		// Wait for Ctrl+C
		<-ctx.Done()
	}

	return nil
}

func newPreviewInterceptCmd(app *App) *cobra.Command {
	var groupName string
	var endpoint string
	cmd := &cobra.Command{
		Use:   "intercept <service>",
		Short: "Intercept a service in a preview group and route it locally",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPreviewIntercept(app, args[0], groupName, endpoint, cmd.Context())
		},
	}
	cmd.Flags().StringVar(&groupName, "group", "", "PreviewGroup name")
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "Local endpoint (e.g. 100.1.2.3:8080)")
	_ = cmd.MarkFlagRequired("group")
	_ = cmd.MarkFlagRequired("endpoint")
	return cmd
}

func runPreviewIntercept(app *App, service, groupName, endpoint string, ctx context.Context) error {
	if service == "" {
		return fmt.Errorf("service name is required")
	}
	c, _, err := app.KubeClient()
	if err != nil {
		return err
	}

	var pg divergeiov1alpha1.PreviewGroup
	if err := c.Get(ctx, types.NamespacedName{Name: groupName}, &pg); err != nil {
		return err
	}

	i := slices.IndexFunc(pg.Spec.Services, func(svc divergeiov1alpha1.PreviewGroupServiceSpec) bool {
		return svc.Name == service
	})

	if i != -1 {
		pg.Spec.Services[i].Mode = divergeiov1alpha1.ServiceModeLocal
		pg.Spec.Services[i].Endpoint = endpoint
	} else {
		pg.Spec.Services = append(pg.Spec.Services, divergeiov1alpha1.PreviewGroupServiceSpec{
			Name:     service,
			Mode:     divergeiov1alpha1.ServiceModeLocal,
			Endpoint: endpoint,
		})
	}

	if err := c.Update(ctx, &pg); err != nil {
		return err
	}

	fmt.Printf("Intercepting %s to %s in group %s\n", service, endpoint, groupName)
	return nil
}

func newPreviewReleaseCmd(app *App) *cobra.Command {
	var groupName string
	cmd := &cobra.Command{
		Use:   "release <service>",
		Short: "Stop intercepting a service and revert to image mode",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPreviewRelease(app, args[0], groupName, cmd.Context())
		},
	}
	cmd.Flags().StringVar(&groupName, "group", "", "PreviewGroup name")
	_ = cmd.MarkFlagRequired("group")
	return cmd
}

func runPreviewRelease(app *App, service, groupName string, ctx context.Context) error {
	c, _, err := app.KubeClient()
	if err != nil {
		return err
	}

	var pg divergeiov1alpha1.PreviewGroup
	if err := c.Get(ctx, types.NamespacedName{Name: groupName}, &pg); err != nil {
		return err
	}

	i := slices.IndexFunc(pg.Spec.Services, func(svc divergeiov1alpha1.PreviewGroupServiceSpec) bool {
		return svc.Name == service
	})

	if i != -1 {
		if pg.Spec.Services[i].Image == "" {
			pg.Spec.Services[i].Mode = divergeiov1alpha1.ServiceModeBaseline
		} else {
			pg.Spec.Services[i].Mode = divergeiov1alpha1.ServiceModeImage
		}
		pg.Spec.Services[i].Endpoint = "" // clear endpoint
	} else {
		return fmt.Errorf("service %q not found in group %q", service, groupName)
	}

	if err := c.Update(ctx, &pg); err != nil {
		return err
	}

	fmt.Printf("Released intercept for %s in group %s\n", service, groupName)
	return nil
}

func waitForAsyncRoutes(ctx context.Context, c client.Client, groupName string, serviceName string, defaultNs string) (map[string]string, error) {
	// Add a maximum timeout for the entire polling operation
	pollCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	var initialPg divergeiov1alpha1.PreviewGroup
	if err := c.Get(pollCtx, types.NamespacedName{Name: groupName}, &initialPg); err == nil {
		hasAsync := false
		for _, svc := range initialPg.Spec.Services {
			if svc.Name == serviceName && len(svc.AsyncRoutes) > 0 {
				hasAsync = true
				break
			}
		}
		if !hasAsync {
			return nil, nil
		}
	}

	var envName string
	var envNs string
	backoff := 100 * time.Millisecond
	maxBackoff := 1500 * time.Millisecond

	backoffTimer := time.NewTimer(backoff)
	defer backoffTimer.Stop()

	for {
		var pg divergeiov1alpha1.PreviewGroup
		if err := c.Get(pollCtx, types.NamespacedName{Name: groupName}, &pg); err == nil {
			for _, svc := range pg.Status.Services {
				if svc.Name == serviceName && svc.EnvironmentName != "" {
					envName = svc.EnvironmentName
					envNs = svc.Namespace
					if envNs == "" {
						envNs = defaultNs
					}
					break
				}
			}
			if pg.Status.Phase == divergeiov1alpha1.PreviewGroupPhaseRunning {
				break
			}
		}
		if envName != "" {
			break
		}

		select {
		case <-pollCtx.Done():
			if ctx.Err() == nil {
				// Our timeout fired, not user cancellation
				return nil, fmt.Errorf("timed out waiting for async routes after 2m")
			}
			return nil, ctx.Err()
		case <-backoffTimer.C:
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			backoffTimer.Reset(backoff)
		}
	}

	// Wait for the specific Environment's async routes to be ready
	var env divergeiov1alpha1.Environment
	fmt.Println("⏳ Waiting for async routes...")

	backoff = 100 * time.Millisecond
	backoffTimer2 := time.NewTimer(backoff)
	defer backoffTimer2.Stop()

	for {
		if err := c.Get(pollCtx, types.NamespacedName{Name: envName, Namespace: envNs}, &env); err != nil {
			if apierrors.IsNotFound(err) || apierrors.IsServerTimeout(err) || apierrors.IsServiceUnavailable(err) {
				// retry on transient errors
			} else {
				return nil, fmt.Errorf("failed to refresh Environment %q: %w", envName, err)
			}
		} else {
			ready := false
			for _, cond := range env.Status.Conditions {
				if cond.Type == "AsyncRoutingReady" {
					if cond.Status == metav1.ConditionTrue {
						ready = true
					}
					if cond.Status == metav1.ConditionFalse {
						if cond.Reason == "AsyncProvisionFailed" || cond.Reason == "EnvVarConflict" {
							return nil, fmt.Errorf("async route provisioning failed: %s (%s)", cond.Message, cond.Reason)
						}
					}
					break
				}
			}

			if ready {
				break
			}
		}

		select {
		case <-pollCtx.Done():
			if ctx.Err() == nil {
				// Our timeout fired, not user cancellation
				return nil, fmt.Errorf("timed out waiting for async routes after 2m")
			}
			return nil, ctx.Err()
		case <-backoffTimer2.C:
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			backoffTimer2.Reset(backoff)
		}
	}

	fmt.Printf("✅ Async routes ready (%d routes provisioned)\n", len(env.Spec.Routing.AsyncRoutes))
	for _, route := range env.Spec.Routing.AsyncRoutes {
		envVar := route.EnvVarMapping[route.Target]
		if envVar == "" {
			envVar = divergeiov1alpha1.DefaultEnvVarForProtocol(route.Protocol)
		}
		fmt.Printf("  ✓ %s: %s → %s\n", route.Protocol, route.Target, envVar)
	}

	asyncVars := make(map[string]string)
	for k, v := range env.Status.AsyncEnvVars {
		asyncVars[k] = v
	}
	return asyncVars, nil
}
