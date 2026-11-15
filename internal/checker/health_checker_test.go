package checker

import (
	"context"
	"net/http"
	"net/http/httptest"
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
		r := h.probe(time.Now())
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

	New(srv.URL, time.Second, time.Second).probe(time.Now())
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

	r := New(srv.URL, time.Second, time.Second).probe(time.Now())
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

	r := New(srv.URL, time.Second, 50*time.Millisecond).probe(time.Now())
	if r.Err == nil || r.StatusCode != 0 {
		t.Errorf("status=%d err=%v, want timeout error", r.StatusCode, r.Err)
	}
}

func TestProbeUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	r := New(url, time.Second, time.Second).probe(time.Now())
	if r.Err == nil || r.StatusCode != 0 {
		t.Errorf("status=%d err=%v, want connection error", r.StatusCode, r.Err)
	}
}

func TestProbeInvalidURL(t *testing.T) {
	r := New("http://bad host/\x7f", time.Second, time.Second).probe(time.Now())
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
