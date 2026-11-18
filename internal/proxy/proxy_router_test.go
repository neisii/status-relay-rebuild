package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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

// sseUpstream flushes one event, then waits for release before sending the
// second one.  The wait is channel-based so the test has no timing dependency.
func sseUpstream(release <-chan struct{}) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "data: one\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		fmt.Fprint(w, "data: two\n\n")
	}))
}

// upgradeUpstream accepts an Upgrade request, hijacks the connection, sends a
// greeting and echoes one line back.
func upgradeUpstream() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, brw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: test\r\nConnection: Upgrade\r\n\r\n")
		brw.WriteString("hello\n")
		brw.Flush()
		line, _ := brw.ReadString('\n')
		brw.WriteString("echo:" + line)
		brw.Flush()
	}))
}

// newRouterFor returns a router (with metrics) whose engine is in the requested state.
func newRouterFor(t *testing.T, primaryURL, fallbackURL string, fallback bool) (*Router, *metrics.Metrics) {
	t.Helper()
	eng := engine.New(cfg)
	r, err := New(primaryURL, fallbackURL, eng)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New()
	r.SetMetrics(m)
	if fallback {
		failOver(t, eng)
	}
	return r, m
}

func TestStreamsSSEWithoutWaitingForResponseEnd(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		name := map[bool]string{false: "primary", true: "fallback"}[fallback]
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			up := sseUpstream(release)
			defer up.Close()
			defer close(release)

			r, _ := newRouterFor(t, up.URL, up.URL, fallback)
			srv := httptest.NewServer(r)
			defer srv.Close()

			client := &http.Client{Timeout: 3 * time.Second}
			resp, err := client.Get(srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			got := make(chan string, 1)
			go func() {
				line, _ := bufio.NewReader(resp.Body).ReadString('\n')
				got <- strings.TrimSpace(line)
			}()
			select {
			case line := <-got:
				if line != "data: one" {
					t.Errorf("first line = %q", line)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("first SSE event was not flushed before the response ended")
			}
		})
	}
}

// dialUpgrade sends an Upgrade request over raw TCP and returns the status
// line, the greeting and the echo of "ping".
func dialUpgrade(t *testing.T, base string) (status, greeting, echo string) {
	t.Helper()
	addr := strings.TrimPrefix(base, "http://")
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprintf(c, "GET / HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n", addr)

	br := bufio.NewReader(c)
	status, _ = br.ReadString('\n')
	status = strings.TrimSpace(status)
	if !strings.Contains(status, "101") {
		return status, "", ""
	}
	for {
		l, err := br.ReadString('\n')
		if err != nil || strings.TrimSpace(l) == "" {
			break
		}
	}
	greeting, _ = br.ReadString('\n')
	fmt.Fprint(c, "ping\n")
	echo, _ = br.ReadString('\n')
	return status, strings.TrimSpace(greeting), strings.TrimSpace(echo)
}

func TestPassesThroughUpgradeRequests(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		name := map[bool]string{false: "primary", true: "fallback"}[fallback]
		t.Run(name, func(t *testing.T) {
			up := upgradeUpstream()
			defer up.Close()
			r, m := newRouterFor(t, up.URL, up.URL, fallback)
			srv := httptest.NewServer(r)
			defer srv.Close()

			status, greeting, echo := dialUpgrade(t, srv.URL)
			if !strings.Contains(status, "101") || greeting != "hello" || echo != "echo:ping" {
				t.Fatalf("status=%q greeting=%q echo=%q", status, greeting, echo)
			}

			// Metrics are recorded after ServeHTTP returns, i.e. after the
			// hijacked session ends; wait for it.
			deadline := time.Now().Add(2 * time.Second)
			for {
				rs := m.Snapshot()["request_stats"].(map[string]interface{})
				st := rs[name].(map[string]interface{})
				if st["total_requests"] == 1 {
					// A successful upgrade must not count as a failure.  The
					// recorded status code is the initial 200 because
					// WriteHeader is never called on a hijacked connection
					// (known limitation, see the blueprint).
					if st["failed_requests"] != 0 || st["successful_requests"] != 1 {
						t.Errorf("%s stats = %v", name, st)
					}
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("request not recorded: %v", st)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func TestStatusCodeCaptureExposesUnderlyingWriter(t *testing.T) {
	rec := httptest.NewRecorder()
	s := &statusCodeCapture{ResponseWriter: rec, statusCode: http.StatusOK}

	if err := http.NewResponseController(s).Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !rec.Flushed {
		t.Error("Flush was not forwarded to the underlying writer")
	}
	if _, _, err := http.NewResponseController(s).Hijack(); !errors.Is(err, http.ErrNotSupported) {
		t.Errorf("Hijack on non-Hijacker = %v, want http.ErrNotSupported", err)
	}
}
