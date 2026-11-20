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
	"net/url"
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

// testWriteTimeout stands in for the 30s WriteTimeout of main.serve; the
// streams below last well beyond it.
const testWriteTimeout = 300 * time.Millisecond

// timeoutServer starts h behind an http.Server with a short WriteTimeout.
func timeoutServer(h http.Handler) *httptest.Server {
	srv := httptest.NewUnstartedServer(h)
	srv.Config.WriteTimeout = testWriteTimeout
	srv.Start()
	return srv
}

// tickUpstream sends n events, flushing each one, with interval between them.
func tickUpstream(contentType string, n int, interval time.Duration) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		f := w.(http.Flusher)
		for i := 0; i < n; i++ {
			if i > 0 {
				select {
				case <-time.After(interval):
				case <-r.Context().Done():
					return
				}
			}
			fmt.Fprintf(w, "data: %d\n\n", i)
			f.Flush()
		}
	}))
}

// readEvents GETs url and returns how many data lines arrived and the error
// that ended the read (io.EOF on a clean end).  No Accept header is sent.
func readEvents(url string) (int, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	n := 0
	for {
		line, err := br.ReadString('\n')
		if strings.HasPrefix(line, "data:") {
			n++
		}
		if err != nil {
			return n, err
		}
	}
}

// 8 events 100ms apart last about 700ms, more than twice testWriteTimeout.
const (
	streamEvents   = 8
	streamInterval = 100 * time.Millisecond
)

func TestSSEOutlivesWriteTimeout(t *testing.T) {
	for _, ct := range []string{"text/event-stream", "Text/Event-Stream; charset=utf-8"} {
		for _, fallback := range []bool{false, true} {
			name := fmt.Sprintf("%s/fallback=%v", ct, fallback)
			t.Run(name, func(t *testing.T) {
				up := tickUpstream(ct, streamEvents, streamInterval)
				defer up.Close()
				r, _ := newRouterFor(t, up.URL, up.URL, fallback)
				srv := timeoutServer(r)
				defer srv.Close()

				n, err := readEvents(srv.URL)
				if n != streamEvents || err != io.EOF {
					t.Errorf("got %d/%d events, end error = %v; want a complete stream", n, streamEvents, err)
				}
			})
		}
	}
}

// The release is limited to SSE: any other streamed response is still cut by
// WriteTimeout.  This pins the boundary that the blueprint documents as a
// known limitation.
func TestNonSSEStreamStillHitsWriteTimeout(t *testing.T) {
	up := tickUpstream("text/plain", streamEvents, streamInterval)
	defer up.Close()
	r, _ := newRouterFor(t, up.URL, up.URL, false)
	srv := timeoutServer(r)
	defer srv.Close()

	n, err := readEvents(srv.URL)
	if err == io.EOF && n == streamEvents {
		t.Errorf("non-SSE stream survived WriteTimeout (%d events)", n)
	}
}

func TestSSEStreamRecordedAsSuccess(t *testing.T) {
	up := tickUpstream("text/event-stream", streamEvents, streamInterval)
	defer up.Close()
	r, m := newRouterFor(t, up.URL, up.URL, false)
	srv := timeoutServer(r)
	defer srv.Close()

	if n, err := readEvents(srv.URL); n != streamEvents || err != io.EOF {
		t.Fatalf("got %d events, err = %v", n, err)
	}
	// The final chunk is written after ServeHTTP returns, so the request is
	// already recorded when the client sees EOF.  The latency is the whole
	// session (known limitation).
	st := m.Snapshot()["request_stats"].(map[string]interface{})["primary"].(map[string]interface{})
	if st["total_requests"] != 1 || st["successful_requests"] != 1 || st["failed_requests"] != 0 {
		t.Errorf("primary stats = %v", st)
	}
}

// ReverseProxy used outside Router has no ResponseController in the request
// context; ModifyResponse must leave the response alone instead of failing.
func TestSSEWithoutRouterContext(t *testing.T) {
	up := tickUpstream("text/event-stream", 2, time.Millisecond)
	defer up.Close()
	base, _ := url.Parse(up.URL)
	srv := httptest.NewServer(newReverseProxy(base))
	defer srv.Close()

	if n, err := readEvents(srv.URL); n != 2 || err != io.EOF {
		t.Errorf("got %d events, err = %v", n, err)
	}
}

// deadlineErrWriter refuses SetWriteDeadline with an error other than
// http.ErrNotSupported.
type deadlineErrWriter struct{ *httptest.ResponseRecorder }

func (deadlineErrWriter) SetWriteDeadline(time.Time) error { return errors.New("deadline refused") }

// A writer that cannot release the deadline (recorder: ErrNotSupported, or a
// failing SetWriteDeadline) must not break the response.
func TestSSEWhenDeadlineCannotBeReleased(t *testing.T) {
	up := tickUpstream("text/event-stream", 2, time.Millisecond)
	defer up.Close()
	r, _ := newRouterFor(t, up.URL, up.URL, false)

	writers := map[string]func() (http.ResponseWriter, *httptest.ResponseRecorder){
		"not supported": func() (http.ResponseWriter, *httptest.ResponseRecorder) {
			rec := httptest.NewRecorder()
			return rec, rec
		},
		"error": func() (http.ResponseWriter, *httptest.ResponseRecorder) {
			rec := httptest.NewRecorder()
			return deadlineErrWriter{rec}, rec
		},
	}
	for name, mk := range writers {
		t.Run(name, func(t *testing.T) {
			w, rec := mk()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "data: 1") {
				t.Errorf("code=%d body=%q", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestIsEventStream(t *testing.T) {
	for ct, want := range map[string]bool{
		"text/event-stream":                true,
		"text/event-stream; charset=utf-8": true,
		"Text/Event-Stream":                true,
		"text/plain":                       false,
		"application/json":                 false,
		"":                                 false,
		"text/event-stream-extra":          false,
		"not a media type;;":               false,
	} {
		if got := isEventStream(ct); got != want {
			t.Errorf("isEventStream(%q) = %v, want %v", ct, got, want)
		}
	}
}

// echoUpstream hijacks, answers 101 and echoes every line until the peer closes.
func echoUpstream() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, brw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: test\r\nConnection: Upgrade\r\n\r\n")
		brw.Flush()
		for {
			line, err := brw.ReadString('\n')
			if err != nil {
				return
			}
			brw.WriteString("echo:" + line)
			brw.Flush()
		}
	}))
}

// A hijacked connection is not subject to WriteTimeout (regression guard: the
// SSE release must not be needed, and must not be required, for Upgrade).
func TestUpgradeOutlivesWriteTimeout(t *testing.T) {
	up := echoUpstream()
	defer up.Close()
	r, _ := newRouterFor(t, up.URL, up.URL, false)
	srv := timeoutServer(r)
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(c, "GET / HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n", addr)
	br := bufio.NewReader(c)
	status, _ := br.ReadString('\n')
	if !strings.Contains(status, "101") {
		t.Fatalf("status = %q", status)
	}
	for {
		l, err := br.ReadString('\n')
		if err != nil || strings.TrimSpace(l) == "" {
			break
		}
	}

	ping := func(msg string) {
		t.Helper()
		fmt.Fprintf(c, "%s\n", msg)
		got, err := br.ReadString('\n')
		if strings.TrimSpace(got) != "echo:"+msg {
			t.Fatalf("echo for %q = %q, err = %v", msg, got, err)
		}
	}
	ping("before")
	time.Sleep(testWriteTimeout + 200*time.Millisecond)
	ping("after")
}

// ── CloseStreams (graceful shutdown) ─────────────────────────────────────

// openStream sends one event and then keeps the response open until the
// client or the proxy goes away.
func openStream(contentType string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		fmt.Fprint(w, "data: hello\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
}

// streamResult is how a client read of an open stream ended.
type streamResult struct {
	events int
	err    error // io.EOF on a clean end
}

// watchStream GETs url, waits for the first event and returns a channel that
// delivers how the stream ended.
func watchStream(t *testing.T, url string) <-chan streamResult {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	br := bufio.NewReader(resp.Body)
	if line, err := br.ReadString('\n'); err != nil || !strings.HasPrefix(line, "data:") {
		t.Fatalf("first event: %q, %v", line, err)
	}
	br.ReadString('\n') // blank line that ends the event
	out := make(chan streamResult, 1)
	go func() {
		n := 1
		for {
			line, err := br.ReadString('\n')
			if strings.HasPrefix(line, "data:") {
				n++
			}
			if err != nil {
				out <- streamResult{n, err}
				return
			}
		}
	}()
	return out
}

func waitStream(t *testing.T, ch <-chan streamResult) streamResult {
	t.Helper()
	select {
	case res := <-ch:
		return res
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not end after CloseStreams")
		return streamResult{}
	}
}

func TestCloseStreamsEndsOpenSSECleanly(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("fallback=%v", fallback), func(t *testing.T) {
			up := openStream("text/event-stream")
			t.Cleanup(up.Close)
			r, m := newRouterFor(t, up.URL, up.URL, fallback)
			srv := httptest.NewServer(r)
			t.Cleanup(srv.Close)

			ended := watchStream(t, srv.URL)
			r.CloseStreams()
			res := waitStream(t, ended)
			if res.err != io.EOF || res.events != 1 {
				t.Errorf("stream ended with %d events, err = %v; want 1 event and io.EOF", res.events, res.err)
			}

			// The final chunk is written after ServeHTTP returns, so the
			// request is already recorded when the client sees the end.
			target := map[bool]string{false: "primary", true: "fallback"}[fallback]
			st := m.Snapshot()["request_stats"].(map[string]interface{})[target].(map[string]interface{})
			if st["total_requests"] != 1 || st["successful_requests"] != 1 || st["failed_requests"] != 0 {
				t.Errorf("%s stats = %v", target, st)
			}
		})
	}
}

func TestCloseStreamsEndsEveryOpenSSE(t *testing.T) {
	up := openStream("text/event-stream")
	t.Cleanup(up.Close)
	r, _ := newRouterFor(t, up.URL, up.URL, false)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	var all []<-chan streamResult
	for i := 0; i < 3; i++ {
		all = append(all, watchStream(t, srv.URL))
	}
	r.CloseStreams()
	for i, ch := range all {
		if res := waitStream(t, ch); res.err != io.EOF {
			t.Errorf("stream %d ended with err = %v, want io.EOF", i, res.err)
		}
	}
}

// A stream that starts while shutdown is already under way (accepted just
// before the listener closed) must end too, not stay open.
func TestSSEStartedAfterCloseStreamsEnds(t *testing.T) {
	up := openStream("text/event-stream")
	t.Cleanup(up.Close)
	r, _ := newRouterFor(t, up.URL, up.URL, false)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	r.CloseStreams()
	done := make(chan error, 1)
	go func() {
		_, err := readEvents(srv.URL)
		done <- err
	}()
	select {
	case err := <-done:
		if err != io.EOF {
			t.Errorf("late stream ended with err = %v, want io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream started after CloseStreams stayed open")
	}
}

// CloseStreams only touches SSE: a response that is still being written
// finishes, because Shutdown waits for it.
func TestCloseStreamsLeavesOtherResponsesAlone(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "first,")
		w.(http.Flusher).Flush()
		close(started)
		<-release
		io.WriteString(w, "second")
	}))
	t.Cleanup(up.Close)
	r, _ := newRouterFor(t, up.URL, up.URL, false)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	got := make(chan string, 1)
	go func() {
		resp, err := http.Get(srv.URL)
		if err != nil {
			got <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		got <- fmt.Sprintf("%s|%v", b, err)
	}()
	<-started
	r.CloseStreams()
	close(release)
	select {
	case s := <-got:
		if s != "first,second|<nil>" {
			t.Errorf("plain response = %q, want it complete", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("plain response did not finish")
	}
}

func TestCloseStreamsIsIdempotent(t *testing.T) {
	r, _ := newRouterFor(t, "http://127.0.0.1:1", "http://127.0.0.1:1", false)
	r.CloseStreams()
	r.CloseStreams()
}
