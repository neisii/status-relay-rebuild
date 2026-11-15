package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/neisii/status-relay/internal/checker"
	"github.com/neisii/status-relay/internal/engine"
	"github.com/neisii/status-relay/internal/metrics"
)

func upstream(name string, code int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", name)
		w.Header().Set("X-Seen-Host", r.Host)
		w.Header().Set("X-Seen-Path", r.URL.RequestURI())
		w.WriteHeader(code)
		io.WriteString(w, name)
	}))
}

var cfg = engine.Config{FailThreshold: 2, LatencyThreshold: time.Second, RecoverThreshold: 2}

// failOver drives the engine to Fallback through its public Run API.
func failOver(t *testing.T, eng *engine.DecisionEngine) {
	t.Helper()
	ch := make(chan checker.CheckResult, cfg.FailThreshold)
	for i := 0; i < cfg.FailThreshold; i++ {
		ch <- checker.CheckResult{StatusCode: 500}
	}
	close(ch)
	eng.Run(context.Background(), ch)
	if eng.Target() != engine.Fallback {
		t.Fatal("engine did not fail over")
	}
}

func do(r http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestNewRejectsInvalidURLs(t *testing.T) {
	eng := engine.New(cfg)
	if _, err := New("http://a b", "http://ok", eng); err == nil {
		t.Error("expected error for bad primary URL")
	}
	if _, err := New("http://ok", "http://a b", eng); err == nil {
		t.Error("expected error for bad fallback URL")
	}
}

func TestRoutesToPrimaryThenFallback(t *testing.T) {
	p, f := upstream("primary", 200), upstream("fallback", 200)
	defer p.Close()
	defer f.Close()

	eng := engine.New(cfg)
	r, err := New(p.URL, f.URL, eng)
	if err != nil {
		t.Fatal(err)
	}

	if got := do(r, "/x").Header().Get("X-Upstream"); got != "primary" {
		t.Errorf("before failover: %q, want primary", got)
	}
	failOver(t, eng)
	if got := do(r, "/x").Header().Get("X-Upstream"); got != "fallback" {
		t.Errorf("after failover: %q, want fallback", got)
	}
}

func TestForwardsPathQueryAndRewritesHost(t *testing.T) {
	p := upstream("primary", 200)
	defer p.Close()
	r, _ := New(p.URL, p.URL, engine.New(cfg))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://client.example/a/b?q=1", nil)
	r.ServeHTTP(rec, req)

	if got := rec.Header().Get("X-Seen-Path"); got != "/a/b?q=1" {
		t.Errorf("path = %q", got)
	}
	wantHost := p.Listener.Addr().String()
	if got := rec.Header().Get("X-Seen-Host"); got != wantHost {
		t.Errorf("upstream saw Host %q, want %q", got, wantHost)
	}
}

func TestPassesThroughUpstreamResponse(t *testing.T) {
	p := upstream("primary", 418)
	defer p.Close()
	r, _ := New(p.URL, p.URL, engine.New(cfg))

	rec := do(r, "/")
	if rec.Code != 418 || rec.Body.String() != "primary" {
		t.Errorf("code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestUpstreamDownReturnsBadGateway(t *testing.T) {
	p := upstream("primary", 200)
	url := p.URL
	p.Close()

	r, _ := New(url, url, engine.New(cfg))
	if rec := do(r, "/"); rec.Code != http.StatusBadGateway {
		t.Errorf("code = %d, want 502", rec.Code)
	}
}

func TestMetricsRecordedPerTarget(t *testing.T) {
	p, f := upstream("primary", 200), upstream("fallback", 503)
	defer p.Close()
	defer f.Close()

	eng := engine.New(cfg)
	r, _ := New(p.URL, f.URL, eng)
	m := metrics.New()
	r.SetMetrics(m)

	do(r, "/")
	do(r, "/")
	failOver(t, eng)
	do(r, "/")

	rs := m.Snapshot()["request_stats"].(map[string]interface{})
	pri := rs["primary"].(map[string]interface{})
	fb := rs["fallback"].(map[string]interface{})

	if pri["total_requests"] != 2 || pri["successful_requests"] != 2 {
		t.Errorf("primary = %v", pri)
	}
	if fb["total_requests"] != 1 || fb["failed_requests"] != 1 {
		t.Errorf("fallback (503) should count as failed: %v", fb)
	}
}

func TestStatusCodeCaptureDefaultsTo200(t *testing.T) {
	rec := httptest.NewRecorder()
	s := &statusCodeCapture{ResponseWriter: rec, statusCode: http.StatusOK}
	s.WriteHeader(http.StatusTeapot)
	if s.statusCode != http.StatusTeapot || rec.Code != http.StatusTeapot {
		t.Errorf("captured=%d recorder=%d", s.statusCode, rec.Code)
	}
}
