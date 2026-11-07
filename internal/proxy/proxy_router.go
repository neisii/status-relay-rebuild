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

	"github.com/neisii/status-relay/internal/engine"
)

// Router is an http.Handler that forwards requests to the active upstream.
type Router struct {
	primaryProxy  *httputil.ReverseProxy
	fallbackProxy *httputil.ReverseProxy
	engine        *engine.DecisionEngine
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

// ServeHTTP satisfies http.Handler.  It reads the current routing target
// (one atomic load) and dispatches to the appropriate reverse proxy.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	target := r.engine.Target()

	switch target {
	case engine.Primary:
		r.primaryProxy.ServeHTTP(w, req)
	case engine.Fallback:
		log.Printf("[proxy] routing to FALLBACK  path=%s", req.URL.Path)
		r.fallbackProxy.ServeHTTP(w, req)
	}
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
