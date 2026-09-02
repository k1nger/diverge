package auth

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// AuthMetrics holds Prometheus metrics for the auth middleware.
type AuthMetrics struct {
	Latency     *prometheus.HistogramVec // provider, result
	CacheHits   prometheus.Counter
	CacheMisses prometheus.Counter
	Attempts    *prometheus.CounterVec // provider, result
}

type AuditLogger interface {
	LogAuth(ctx context.Context, event string, user *UserInfo, r *http.Request, attrs ...slog.Attr)
}

// MiddlewareConfig configures the auth middleware.
type MiddlewareConfig struct {
	Provider       AuthProvider
	Cache          *TokenCache
	Logger         *slog.Logger
	AuditLogger    AuditLogger
	Metrics        *AuthMetrics
	ExemptPaths    []string
	ExemptPrefixes []string
	// LoginURL, when set, is where an unauthenticated BROWSER NAVIGATION is
	// redirected instead of receiving the raw 401 below. A signed-out human
	// loading the dashboard is the routine case — every session expires — and
	// "missing or invalid authorization header" as a bare page is an error
	// message for a state that is not an error. API callers, and anything not
	// asking for text/html, keep the 401 they can act on.
	LoginURL string
}

// NewMiddleware creates net/http middleware that authenticates requests via
// Kubernetes TokenReview with LRU caching.
func NewMiddleware(cfg MiddlewareConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Check exempt paths (exact match)
			for _, path := range cfg.ExemptPaths {
				if r.URL.Path == path {
					next.ServeHTTP(w, r)
					return
				}
			}
			// Check exempt prefixes (prefix match)
			for _, prefix := range cfg.ExemptPrefixes {
				if strings.HasPrefix(r.URL.Path, prefix) {
					next.ServeHTTP(w, r)
					return
				}
			}

			token := extractBearerToken(r.Header.Get("Authorization"))
			// Fall back to session cookie if no Authorization header
			if token == "" {
				if cookie, err := r.Cookie("diverge_token"); err == nil {
					token = cookie.Value
				}
			}
			if token == "" {
				if cfg.Metrics != nil && cfg.Metrics.Attempts != nil {
					cfg.Metrics.Attempts.WithLabelValues("none", "failure").Inc()
				}
				if cfg.AuditLogger != nil {
					cfg.AuditLogger.LogAuth(r.Context(), "auth.failure", nil, r, slog.String("reason", "missing_token"))
				} else {
					cfg.Logger.Warn("auth.failure", "reason", "missing_token", "path", r.URL.Path, "source_ip", r.RemoteAddr)
				}
				// A browser navigating to a page gets sent to sign in; only
				// GET is redirected, so a state-changing request can never be
				// silently replayed through a login flow.
				if cfg.LoginURL != "" && r.Method == http.MethodGet &&
					strings.Contains(r.Header.Get("Accept"), "text/html") {
					http.Redirect(w, r, cfg.LoginURL, http.StatusSeeOther)
					return
				}
				http.Error(w, "missing or invalid authorization header", http.StatusUnauthorized)
				return
			}

			// Check cache first
			if user := cfg.Cache.Get(token); user != nil {
				if cfg.Metrics != nil {
					if cfg.Metrics.CacheHits != nil {
						cfg.Metrics.CacheHits.Inc()
					}
					if cfg.Metrics.Attempts != nil {
						cfg.Metrics.Attempts.WithLabelValues("cache", "success").Inc()
					}
				}
				ctx := ContextWithUserInfo(r.Context(), user)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			if cfg.Metrics != nil && cfg.Metrics.CacheMisses != nil {
				cfg.Metrics.CacheMisses.Inc()
			}

			// TokenReview
			start := time.Now()
			user, err := cfg.Provider.Authenticate(r.Context(), token)
			duration := time.Since(start).Seconds()

			if cfg.Metrics != nil && cfg.Metrics.Latency != nil {
				result := "success"
				if err != nil {
					result = "failure"
				}
				cfg.Metrics.Latency.WithLabelValues("tokenreview", result).Observe(duration)
			}

			if err != nil {
				if cfg.Metrics != nil && cfg.Metrics.Attempts != nil {
					cfg.Metrics.Attempts.WithLabelValues("tokenreview", "failure").Inc()
				}
				if cfg.AuditLogger != nil {
					cfg.AuditLogger.LogAuth(r.Context(), "auth.failure", nil, r, slog.String("reason", "token_review_rejected"), slog.Any("error", err))
				} else {
					cfg.Logger.Warn("auth.failure", "reason", "token_review_rejected", "path", r.URL.Path, "source_ip", r.RemoteAddr, "error", err)
				}
				http.Error(w, "authentication failed", http.StatusUnauthorized)
				return
			}

			// Cache successful auth (never cache failures)
			cfg.Cache.Set(token, user.DeepCopy())

			if cfg.Metrics != nil && cfg.Metrics.Attempts != nil {
				cfg.Metrics.Attempts.WithLabelValues("tokenreview", "success").Inc()
			}
			if cfg.AuditLogger != nil {
				cfg.AuditLogger.LogAuth(r.Context(), "auth.success", user, r)
			} else {
				cfg.Logger.Info("auth.success", "user", user.Username, "groups", user.Groups, "path", r.URL.Path, "source_ip", r.RemoteAddr)
			}

			ctx := ContextWithUserInfo(r.Context(), user)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func extractBearerToken(authHeader string) string {
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return ""
	}
	token := strings.TrimSpace(parts[1])
	if token == "" {
		return ""
	}
	return token
}
