package checker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/neisii/status-relay/internal/metrics"
)

func TestProbeStatusCodes(t *testing.T) {
	for _, code := range []int{200, 404, 500, 503} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		}))
		h := New(srv.URL, time.Second, time.Second)
		r := h.probe(context.Background(), time.Now())
		srv.Close()

		if r.Err != nil || r.StatusCode != code {
			t.Errorf("code %d: got status=%d err=%v", code, r.StatusCode, r.Err)
		}
		if r.Latency <= 0 {
			t.Errorf("code %d: latency not recorded", code)
		}
	}
}

func TestProbeSetsUserAgentAndMethod(t *testing.T) {
	var ua, method string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ua, method = r.UserAgent(), r.Method
	}))
	defer srv.Close()

	New(srv.URL, time.Second, time.Second).probe(context.Background(), time.Now())
	if ua != "StatusRelay/1.0" || method != http.MethodGet {
		t.Errorf("ua=%q method=%q", ua, method)
	}
}

func TestProbeDoesNotFollowRedirects(t *testing.T) {
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/final" {
			hit = true
			return
		}
		http.Redirect(w, r, "/final", http.StatusFound)
	}))
	defer srv.Close()

	r := New(srv.URL, time.Second, time.Second).probe(context.Background(), time.Now())
	if r.Err != nil || r.StatusCode != http.StatusFound {
		t.Errorf("status=%d err=%v, want 302 with no error", r.StatusCode, r.Err)
	}
	if hit {
		t.Error("redirect target was requested")
	}
}

func TestProbeTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()

	r := New(srv.URL, time.Second, 50*time.Millisecond).probe(context.Background(), time.Now())
	if r.Err == nil || r.StatusCode != 0 {
		t.Errorf("status=%d err=%v, want timeout error", r.StatusCode, r.Err)
	}
}

func TestProbeUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	r := New(url, time.Second, time.Second).probe(context.Background(), time.Now())
	if r.Err == nil || r.StatusCode != 0 {
		t.Errorf("status=%d err=%v, want connection error", r.StatusCode, r.Err)
	}
}

func TestProbeInvalidURL(t *testing.T) {
	r := New("http://bad host/\x7f", time.Second, time.Second).probe(context.Background(), time.Now())
	if r.Err == nil {
		t.Error("expected request-construction error")
	}
}

func TestStartStreamsResultsAndClosesOnCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	h := New(srv.URL, 10*time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	h.Start(ctx)

	for i := 0; i < 3; i++ {
		select {
		case r, ok := <-h.Results():
			if !ok || r.StatusCode != 200 {
				t.Fatalf("result %d: ok=%v status=%d", i, ok, r.StatusCode)
			}
		case <-time.After(time.Second):
			t.Fatalf("no result %d within 1s", i)
		}
	}

	cancel()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-h.Results():
			if !ok {
				return // closed as expected
			}
		case <-deadline:
			t.Fatal("results channel not closed after cancel")
		}
	}
}

func TestMetricsRecordedPerProbe(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}))
	defer srv.Close()

	m := metrics.New()
	h := New(srv.URL, 10*time.Millisecond, time.Second)
	h.SetMetrics(m)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.Start(ctx)

	<-h.Results()
	<-h.Results()
	cancel()
	for range h.Results() { // drain until closed so the loop has finished recording
	}

	hs := m.Snapshot()["health_check_stats"].(map[string]interface{})
	total, _ := hs["total_probes"].(int)
	if total < 2 || hs["failed_probes"] != 0 || hs["successful_probes"] != total {
		t.Errorf("health_check_stats = %v", hs)
	}
}

// blockingUpstream returns a server whose handler signals on started and then
// blocks until the request is cancelled (or a safety timeout fires).
func blockingUpstream(started chan<- struct{}) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
}

// countLoops returns the number of live HealthChecker.loop goroutines.
func countLoops() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "HealthChecker).loop")
}

func TestCancelDuringProbeClosesPromptly(t *testing.T) {
	started := make(chan struct{}, 1)
	srv := blockingUpstream(started)
	defer srv.Close()

	h := New(srv.URL, 10*time.Millisecond, 3*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.Start(ctx)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("probe did not reach upstream")
	}
	cancel()

	deadline := time.After(500 * time.Millisecond)
	for {
		select {
		case _, ok := <-h.Results():
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("results not closed within 500ms of cancel (probe not aborted)")
		}
	}
}

func TestCancelledProbeIsNotRecorded(t *testing.T) {
	started := make(chan struct{}, 1)
	srv := blockingUpstream(started)
	defer srv.Close()

	m := metrics.New()
	h := New(srv.URL, 10*time.Millisecond, 3*time.Second)
	h.SetMetrics(m)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.Start(ctx)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("probe did not reach upstream")
	}
	cancel()

	var got []CheckResult
	timeout := time.After(5 * time.Second)
drain:
	for {
		select {
		case r, ok := <-h.Results():
			if !ok {
				break drain
			}
			got = append(got, r)
		case <-timeout:
			t.Fatal("results not closed within 5s of cancel")
		}
	}

	if len(got) != 0 {
		t.Errorf("got %d results after cancel, want 0: %+v", len(got), got)
	}
	hs := m.Snapshot()["health_check_stats"].(map[string]interface{})
	if hs["total_probes"] != 0 {
		t.Errorf("total_probes = %v after cancel, want 0 (aborted probe must not be recorded)", hs["total_probes"])
	}
}

func TestRealTimeoutStillRecordedAsFailure(t *testing.T) {
	srv := blockingUpstream(make(chan struct{}, 1))
	defer srv.Close()

	m := metrics.New()
	h := New(srv.URL, 10*time.Millisecond, 50*time.Millisecond)
	h.SetMetrics(m)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.Start(ctx)

	select {
	case r := <-h.Results():
		if r.Err == nil || r.StatusCode != 0 {
			t.Errorf("status=%d err=%v, want timeout error", r.StatusCode, r.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no timeout result within 2s")
	}
	cancel()
	for range h.Results() {
	}

	hs := m.Snapshot()["health_check_stats"].(map[string]interface{})
	if f, _ := hs["failed_probes"].(int); f < 1 {
		t.Errorf("failed_probes = %v, want >= 1 (real timeout is a genuine failure)", hs["failed_probes"])
	}
}

func TestCancelWithFullBufferStopsLoop(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	base := countLoops()
	h := New(srv.URL, 5*time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.Start(ctx)

	// Nobody reads Results(): wait until the buffer is full and the loop is
	// blocked on the next send.
	for i := 0; len(h.results) < cap(h.results); i++ {
		if i > 200 {
			t.Fatal("buffer did not fill")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()

	for i := 0; countLoops() > base; i++ {
		if i > 100 {
			t.Fatal("loop goroutine still alive 1s after cancel with a full buffer")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestProbeCancelledContextDoesNotReachUpstream(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := New(srv.URL, time.Second, time.Second).probe(ctx, time.Now())
	if r.Err == nil || r.StatusCode != 0 {
		t.Errorf("status=%d err=%v, want cancellation error", r.StatusCode, r.Err)
	}
	if hits != 0 {
		t.Errorf("upstream hit %d times with a cancelled context", hits)
	}
}
