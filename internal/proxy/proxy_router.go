// Package proxy implements the HTTP reverse-proxy that routes traffic to
// either the primary or the fallback upstream based on the DecisionEngine state.
//
// The routing decision is an atomic read — zero lock contention on the
// hot path, so latency overhead is negligible even under high concurrency.
package proxy

import (
	"context"
	"errors"
	"log"
	"mime"
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

	// ModifyResponse only sees the *http.Response; hand it the controller of
	// this request's writer through the context (see releaseWriteDeadlineForSSE).
	rc := http.NewResponseController(wrapped)
	req = req.WithContext(context.WithValue(req.Context(), responseControllerKey{}, rc))

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

// Unwrap exposes the underlying ResponseWriter so http.ResponseController
// (used by httputil.ReverseProxy) can reach Flush, Hijack and friends.
// Without it, SSE responses are not flushed and Upgrade requests fail.
func (s *statusCodeCapture) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
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

	proxy.ModifyResponse = releaseWriteDeadlineForSSE

	// Log upstream errors without crashing the proxy.
	proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
		log.Printf("[proxy] upstream error host=%s path=%s err=%v", base.Host, req.URL.Path, err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
	}

	return proxy
}

// responseControllerKey is the request-context key under which Router.ServeHTTP
// stores the *http.ResponseController of the client-facing writer.
type responseControllerKey struct{}

// releaseWriteDeadlineForSSE lifts http.Server.WriteTimeout for a response whose
// Content-Type is text/event-stream.  WriteTimeout is an absolute limit counted
// from the request header, so it would cut any stream longer than that even
// while events keep flowing.  Other responses keep the limit.
//
// It never fails the response: without a controller in the context (a proxy
// used outside Router) or with a writer that cannot set deadlines, the
// response is left as it is.
func releaseWriteDeadlineForSSE(resp *http.Response) error {
	if resp.Request == nil || !isEventStream(resp.Header.Get("Content-Type")) {
		return nil
	}
	rc, ok := resp.Request.Context().Value(responseControllerKey{}).(*http.ResponseController)
	if !ok {
		return nil
	}
	if err := rc.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		log.Printf("[proxy] cannot lift write deadline for SSE path=%s err=%v", resp.Request.URL.Path, err)
	}
	return nil
}

// isEventStream reports whether a Content-Type header value is text/event-stream,
// ignoring case and parameters such as charset.
func isEventStream(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && mediaType == "text/event-stream"
}
