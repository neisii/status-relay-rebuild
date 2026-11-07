// Package proxy implements the HTTP reverse-proxy that routes traffic to
// either the primary or the fallback upstream based on the DecisionEngine state.
//
// The routing decision is an atomic read — zero lock contention on the
// hot path, so latency overhead is negligible even under high concurrency.
package proxy

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/neisii/status-relay/internal/engine"
	"github.com/neisii/status-relay/internal/metrics"
)

// Router is an http.Handler that forwards requests to the active upstream.
type Router struct {
	primaryProxy  *httputil.ReverseProxy
	fallbackProxy *httputil.ReverseProxy
	engine        *engine.DecisionEngine
	metrics       *metrics.Metrics
}

// New creates a Router.  primaryURL and fallbackURL must be valid absolute URLs.
func New(primaryURL, fallbackURL string, eng *engine.DecisionEngine) (*Router, error) {
	primary, err := url.Parse(primaryURL)
	if err != nil {
		return nil, err
	}
	fallback, err := url.Parse(fallbackURL)
	if err != nil {
		return nil, err
	}

	return &Router{
		primaryProxy:  newReverseProxy(primary),
		fallbackProxy: newReverseProxy(fallback),
		engine:        eng,
	}, nil
}

// SetMetrics attaches a Metrics instance to record request statistics.
func (r *Router) SetMetrics(m *metrics.Metrics) {
	r.metrics = m
}

// ServeHTTP satisfies http.Handler.  It reads the current routing target
// (one atomic load) and dispatches to the appropriate reverse proxy.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	target := r.engine.Target()
	start := time.Now()

	// Wrap ResponseWriter to capture status code
	wrapped := &statusCodeCapture{ResponseWriter: w, statusCode: http.StatusOK}

	switch target {
	case engine.Primary:
		r.primaryProxy.ServeHTTP(wrapped, req)
		if r.metrics != nil {
			latency := time.Since(start)
			success := wrapped.statusCode >= 200 && wrapped.statusCode < 500
			r.metrics.RecordRequest("primary", success, latency)
		}
	case engine.Fallback:
		log.Printf("[proxy] routing to FALLBACK  path=%s", req.URL.Path)
		r.fallbackProxy.ServeHTTP(wrapped, req)
		if r.metrics != nil {
			latency := time.Since(start)
			success := wrapped.statusCode >= 200 && wrapped.statusCode < 500
			r.metrics.RecordRequest("fallback", success, latency)
		}
	}
}

// statusCodeCapture wraps ResponseWriter to capture the HTTP status code.
type statusCodeCapture struct {
	http.ResponseWriter
	statusCode int
}

func (s *statusCodeCapture) WriteHeader(code int) {
	s.statusCode = code
	s.ResponseWriter.WriteHeader(code)
}

// newReverseProxy builds a standard reverse proxy for the given base URL,
// rewriting the Host header so the upstream receives a proper request.
func newReverseProxy(base *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(base)

	// Preserve the original director and layer on Host rewriting.
	original := proxy.Director
	proxy.Director = func(req *http.Request) {
		original(req)
		req.Host = base.Host // make sure Host matches the upstream
	}

	// Log upstream errors without crashing the proxy.
	proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
		log.Printf("[proxy] upstream error host=%s path=%s err=%v", base.Host, req.URL.Path, err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
	}

	return proxy
}
