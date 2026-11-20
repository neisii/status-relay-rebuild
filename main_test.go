package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func quietLog(t *testing.T) {
	t.Helper()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
}

// fakeUpstream returns a server that answers every request with name as the
// body and 200, or 503 while *down is true.
func fakeUpstream(t *testing.T, name string, down *atomic.Bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if down != nil && down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, name)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func fastConfig(primary, fallback string) config {
	return config{
		primaryURL:       primary,
		fallbackURL:      fallback,
		port:             0,
		interval:         20 * time.Millisecond,
		probeTimeout:     500 * time.Millisecond,
		failThreshold:    2,
		latencyThreshold: 1000,
		recoverThreshold: 2,
	}
}

func get(t *testing.T, url string) (int, string, http.Header) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// ── parseConfig ──────────────────────────────────────────────────────────

func TestParseConfigDefaults(t *testing.T) {
	var buf bytes.Buffer
	cfg, err := parseConfig([]string{"--primary", "http://p", "--fallback", "http://f"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := config{
		primaryURL:       "http://p",
		fallbackURL:      "http://f",
		port:             8080,
		interval:         200 * time.Millisecond,
		probeTimeout:     1500 * time.Millisecond,
		failThreshold:    3,
		latencyThreshold: 1000,
		recoverThreshold: 2,
	}
	if cfg != want {
		t.Errorf("cfg = %+v, want %+v", cfg, want)
	}
}

func TestParseConfigAllFlags(t *testing.T) {
	var buf bytes.Buffer
	cfg, err := parseConfig([]string{
		"--primary", "http://p", "--fallback", "http://f",
		"--port", "9090", "--interval", "50ms", "--probe-timeout", "2s",
		"--fail-threshold", "5", "--latency-threshold", "250", "--recover-threshold", "4",
	}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := config{
		primaryURL:       "http://p",
		fallbackURL:      "http://f",
		port:             9090,
		interval:         50 * time.Millisecond,
		probeTimeout:     2 * time.Second,
		failThreshold:    5,
		latencyThreshold: 250,
		recoverThreshold: 4,
	}
	if cfg != want {
		t.Errorf("cfg = %+v, want %+v", cfg, want)
	}
}

func TestParseConfigMissingRequired(t *testing.T) {
	cases := map[string][]string{
		"none":          {},
		"only primary":  {"--primary", "http://p"},
		"only fallback": {"--fallback", "http://f"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			_, err := parseConfig(args, &buf)
			if err != errMissingRequired {
				t.Fatalf("err = %v, want errMissingRequired", err)
			}
			out := buf.String()
			if !strings.Contains(out, "Error: --primary and --fallback are required") {
				t.Errorf("missing error message in output: %q", out)
			}
			if !strings.Contains(out, "Usage of start:") {
				t.Errorf("missing usage in output: %q", out)
			}
		})
	}
}

func TestParseConfigUnknownFlag(t *testing.T) {
	var buf bytes.Buffer
	_, err := parseConfig([]string{"--bogus"}, &buf)
	if err == nil || err == errMissingRequired || err == flag.ErrHelp {
		t.Fatalf("err = %v, want a flag parse error", err)
	}
	if !strings.Contains(buf.String(), "flag provided but not defined") {
		t.Errorf("output = %q", buf.String())
	}
}

func TestParseConfigInvalidValue(t *testing.T) {
	var buf bytes.Buffer
	_, err := parseConfig([]string{"--port", "abc"}, &buf)
	if err == nil || err == errMissingRequired || err == flag.ErrHelp {
		t.Fatalf("err = %v, want a flag parse error", err)
	}
}

func TestParseConfigHelp(t *testing.T) {
	for _, h := range []string{"-h", "-help", "--help"} {
		var buf bytes.Buffer
		_, err := parseConfig([]string{h}, &buf)
		if err != flag.ErrHelp {
			t.Errorf("%s: err = %v, want flag.ErrHelp", h, err)
		}
		if !strings.Contains(buf.String(), "Usage of start:") {
			t.Errorf("%s: output = %q", h, buf.String())
		}
	}
}

// ── parseConfig: value validation ────────────────────────────────────────

var validBase = []string{"--primary", "http://p", "--fallback", "http://f"}

func withArgs(extra ...string) []string {
	return append(append([]string{}, validBase...), extra...)
}

func TestParseConfigAcceptsBoundaryValues(t *testing.T) {
	cases := map[string][]string{
		"port min":                withArgs("--port", "0"),
		"port max":                withArgs("--port", "65535"),
		"interval min":            withArgs("--interval", "10ms"),
		"probe-timeout min":       withArgs("--probe-timeout", "10ms"),
		"fail-threshold min":      withArgs("--fail-threshold", "1"),
		"recover-threshold min":   withArgs("--recover-threshold", "1"),
		"latency-threshold min":   withArgs("--latency-threshold", "1"),
		"latency-threshold max":   withArgs("--latency-threshold", "9223372036854"),
		"same primary / fallback": {"--primary", "http://p", "--fallback", "http://p"},
		"https with port, path":   {"--primary", "https://h:8443/base", "--fallback", "http://f"},
		"upper-case scheme":       {"--primary", "HTTP://H", "--fallback", "http://f"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			if _, err := parseConfig(args, &buf); err != nil {
				t.Fatalf("unexpected error: %v (output=%q)", err, buf.String())
			}
		})
	}
}

func TestParseConfigRejectsOutOfRangeValues(t *testing.T) {
	cases := []struct {
		name string
		args []string
		flag string
	}{
		{"port negative", withArgs("--port", "-1"), "--port"},
		{"port too large", withArgs("--port", "65536"), "--port"},
		{"interval zero", withArgs("--interval", "0"), "--interval"},
		{"interval negative", withArgs("--interval", "-1s"), "--interval"},
		{"interval below min", withArgs("--interval", "9ms"), "--interval"},
		{"probe-timeout zero", withArgs("--probe-timeout", "0"), "--probe-timeout"},
		{"probe-timeout negative", withArgs("--probe-timeout", "-1s"), "--probe-timeout"},
		{"probe-timeout below min", withArgs("--probe-timeout", "9ms"), "--probe-timeout"},
		{"fail-threshold zero", withArgs("--fail-threshold", "0"), "--fail-threshold"},
		{"fail-threshold negative", withArgs("--fail-threshold", "-3"), "--fail-threshold"},
		{"recover-threshold zero", withArgs("--recover-threshold", "0"), "--recover-threshold"},
		{"recover-threshold negative", withArgs("--recover-threshold", "-1"), "--recover-threshold"},
		{"latency-threshold zero", withArgs("--latency-threshold", "0"), "--latency-threshold"},
		{"latency-threshold negative", withArgs("--latency-threshold", "-5"), "--latency-threshold"},
		{"latency-threshold overflow edge", withArgs("--latency-threshold", "9223372036855"), "--latency-threshold"},
		{"latency-threshold int64 max", withArgs("--latency-threshold", "9223372036854775807"), "--latency-threshold"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			_, err := parseConfig(tc.args, &buf)
			if !errors.Is(err, errInvalidConfig) {
				t.Fatalf("err = %v, want errInvalidConfig", err)
			}
			out := buf.String()
			if !strings.Contains(out, "Error: invalid "+tc.flag+" ") {
				t.Errorf("output lacks error for %s: %q", tc.flag, out)
			}
			if !strings.Contains(out, "Usage of start:") {
				t.Errorf("output lacks usage: %q", out)
			}
		})
	}
}

func TestParseConfigRejectsInvalidURLs(t *testing.T) {
	bad := []string{" ", " http://p", "not-a-url", "localhost:9000", "ftp://x", "http://", "::bad", "http://a b"}
	for _, u := range bad {
		for _, name := range []string{"primary", "fallback"} {
			t.Run(fmt.Sprintf("%s=%q", name, u), func(t *testing.T) {
				args := []string{"--primary", "http://p", "--fallback", "http://f"}
				if name == "primary" {
					args[1] = u
				} else {
					args[3] = u
				}
				var buf bytes.Buffer
				_, err := parseConfig(args, &buf)
				if !errors.Is(err, errInvalidConfig) {
					t.Fatalf("err = %v, want errInvalidConfig", err)
				}
				if !strings.Contains(buf.String(), "Error: invalid --"+name+" ") {
					t.Errorf("output = %q", buf.String())
				}
			})
		}
	}
}

func TestParseConfigReportsAllErrors(t *testing.T) {
	var buf bytes.Buffer
	_, err := parseConfig([]string{
		"--primary", "ftp://x", "--fallback", "http://f",
		"--interval", "0", "--fail-threshold", "0",
	}, &buf)
	if !errors.Is(err, errInvalidConfig) {
		t.Fatalf("err = %v, want errInvalidConfig", err)
	}
	out := buf.String()
	for _, want := range []string{"invalid --primary", "invalid --interval", "invalid --fail-threshold"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q: %q", want, out)
		}
	}
}

func TestParseConfigRejectsPositionalArgs(t *testing.T) {
	cases := map[string][]string{
		"after flags":  withArgs("extra-arg"),
		"before flags": append([]string{"extra-arg"}, validBase...),
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			_, err := parseConfig(args, &buf)
			if !errors.Is(err, errInvalidConfig) {
				t.Fatalf("err = %v, want errInvalidConfig", err)
			}
			if !strings.Contains(buf.String(), `unexpected argument "extra-arg"`) {
				t.Errorf("output = %q", buf.String())
			}
		})
	}
}

// ── run: exit codes ──────────────────────────────────────────────────────

func TestRunInvalidConfigMessage(t *testing.T) {
	var buf bytes.Buffer
	got := run(context.Background(), []string{"start", "--primary", "http://p", "--fallback", "http://f", "--interval", "0"}, &buf)
	if got != 2 {
		t.Errorf("exit = %d, want 2", got)
	}
	if !strings.Contains(buf.String(), "Error: invalid --interval 0s: must be at least 10ms") {
		t.Errorf("stderr = %q", buf.String())
	}
}

func TestRunExitCodes(t *testing.T) {
	quietLog(t)
	cases := []struct {
		name string
		args []string
		want int
	}{
		{"no sub-command", nil, 1},
		{"not start", []string{"stop"}, 1},
		{"missing both", []string{"start"}, 1},
		{"missing fallback", []string{"start", "--primary", "http://p"}, 1},
		{"unknown flag", []string{"start", "--bogus"}, 2},
		{"invalid flag value", []string{"start", "--port", "abc"}, 2},
		{"help", []string{"start", "-h"}, 0},
		// Invalid values are rejected before anything starts (exit 2, same as a
		// flag parse error). "invalid URL" and the port range used to exit 1.
		{"invalid URL", []string{"start", "--primary", "::bad", "--fallback", "http://f"}, 2},
		{"URL without scheme", []string{"start", "--primary", "localhost:9000", "--fallback", "http://f"}, 2},
		{"port out of range", []string{"start", "--primary", "http://p", "--fallback", "http://f", "--port", "65536"}, 2},
		{"interval zero", []string{"start", "--primary", "http://p", "--fallback", "http://f", "--interval", "0"}, 2},
		{"probe-timeout zero", []string{"start", "--primary", "http://p", "--fallback", "http://f", "--probe-timeout", "0"}, 2},
		{"fail-threshold zero", []string{"start", "--primary", "http://p", "--fallback", "http://f", "--fail-threshold", "0"}, 2},
		{"unexpected positional argument", []string{"start", "--primary", "http://p", "--fallback", "http://f", "extra"}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			got := run(context.Background(), tc.args, &buf)
			if got != tc.want {
				t.Errorf("exit = %d, want %d (stderr=%q)", got, tc.want, buf.String())
			}
		})
	}
}

func TestRunUsageMessage(t *testing.T) {
	var buf bytes.Buffer
	run(context.Background(), nil, &buf)
	if got := buf.String(); got != "Usage: status-relay start [flags]\n" {
		t.Errorf("stderr = %q", got)
	}
}

func TestRunListenFailure(t *testing.T) {
	quietLog(t)
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	var buf bytes.Buffer
	got := run(context.Background(), []string{
		"start", "--primary", "http://127.0.0.1:1", "--fallback", "http://127.0.0.1:2",
		"--port", fmt.Sprint(port),
	}, &buf)
	if got != 1 {
		t.Errorf("exit = %d, want 1", got)
	}
}

func TestRunGracefulStop(t *testing.T) {
	quietLog(t)
	up := fakeUpstream(t, "primary", nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"start", "--primary", up.URL, "--fallback", up.URL, "--port", "0"}, io.Discard)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("exit = %d, want 0", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after ctx cancel")
	}
}

// ── handler ──────────────────────────────────────────────────────────────

func TestHandlerMetricsJSON(t *testing.T) {
	up := fakeUpstream(t, "primary", nil)
	a, err := newApp(fastConfig(up.URL, up.URL))
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	a.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics/json", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
	var snap map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	for _, k := range []string{"current_state", "failover_events", "health_check_stats", "request_stats", "version"} {
		if _, ok := snap[k]; !ok {
			t.Errorf("snapshot missing key %q", k)
		}
	}
}

func TestHandlerProxiesRoot(t *testing.T) {
	up := fakeUpstream(t, "primary-body", nil)
	a, err := newApp(fastConfig(up.URL, up.URL))
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	a.handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/some/path", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "primary-body" {
		t.Errorf("status=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestNewAppInvalidURL(t *testing.T) {
	if _, err := newApp(fastConfig("::bad", "http://f")); err == nil {
		t.Error("expected error for invalid primary URL")
	}
}

// ── serve: integration ───────────────────────────────────────────────────

func TestServeFailoverAndShutdown(t *testing.T) {
	quietLog(t)
	var primaryDown atomic.Bool
	primary := fakeUpstream(t, "primary", &primaryDown)
	fallback := fakeUpstream(t, "fallback", nil)

	a, err := newApp(fastConfig(primary.URL, fallback.URL))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, a, ln) }()

	// healthy primary -> served by primary
	eventually(t, "primary serves requests", func() bool {
		code, body, _ := get(t, base+"/")
		return code == http.StatusOK && body == "primary"
	})

	// primary goes down -> fallback takes over
	primaryDown.Store(true)
	eventually(t, "failover to fallback", func() bool {
		_, body, _ := get(t, base+"/")
		return body == "fallback"
	})

	// metrics reflect exactly one failover
	code, body, hdr := get(t, base+"/metrics/json")
	if code != http.StatusOK {
		t.Fatalf("metrics status = %d", code)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
	var snap struct {
		FailoverEvents struct {
			TotalFailovers int `json:"total_failovers"`
		} `json:"failover_events"`
		CurrentState struct {
			ActiveTarget string `json:"active_target"`
		} `json:"current_state"`
	}
	if err := json.Unmarshal([]byte(body), &snap); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if snap.FailoverEvents.TotalFailovers != 1 {
		t.Errorf("total_failovers = %d, want 1", snap.FailoverEvents.TotalFailovers)
	}
	if snap.CurrentState.ActiveTarget != "fallback" {
		t.Errorf("active_target = %q, want fallback", snap.CurrentState.ActiveTarget)
	}

	// ctx cancel -> graceful shutdown without error
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after ctx cancel")
	}
	if _, err := http.Get(base + "/"); err == nil {
		t.Error("server still accepting connections after shutdown")
	}
}

func TestServeReturnsServerError(t *testing.T) {
	quietLog(t)
	up := fakeUpstream(t, "primary", nil)
	a, err := newApp(fastConfig(up.URL, up.URL))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln.Close() // Serve fails immediately on a closed listener

	done := make(chan error, 1)
	go func() { done <- serve(context.Background(), a, ln) }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("expected server error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return on server error")
	}
}

// ── serve: shutdown with open streams ────────────────────────────────────

// streamUpstream answers /sse with one event and then keeps the response open
// until the client goes away.  Every other path (the health probe) answers
// "ok" at once, or "done" after release is closed on /slow.
func streamUpstream(t *testing.T, slowStarted chan<- struct{}, release <-chan struct{}) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sse":
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: hello\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case "/slow":
			slowStarted <- struct{}{}
			<-release
			fmt.Fprint(w, "done")
		default:
			fmt.Fprint(w, "ok")
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// startServe runs serve on a free port and returns its base URL, the cancel
// function and the channel that receives serve's result with the time it took
// after cancel.
func startServe(t *testing.T, up *httptest.Server) (base string, cancel func(), done <-chan error) {
	t.Helper()
	a, err := newApp(fastConfig(up.URL, up.URL))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancelCtx := context.WithCancel(context.Background())
	t.Cleanup(cancelCtx)
	ch := make(chan error, 1)
	go func() { ch <- serve(ctx, a, ln) }()
	return "http://" + ln.Addr().String(), cancelCtx, ch
}

// openSSEClient reads /sse from base until the first event, then reports how
// the stream ends.
func openSSEClient(t *testing.T, base string) <-chan error {
	t.Helper()
	var resp *http.Response
	eventually(t, "proxy serves /sse", func() bool {
		r, err := http.Get(base + "/sse")
		if err != nil {
			return false
		}
		resp = r
		return true
	})
	t.Cleanup(func() { resp.Body.Close() })
	br := bufio.NewReader(resp.Body)
	if line, err := br.ReadString('\n'); err != nil || !strings.HasPrefix(line, "data:") {
		t.Fatalf("first event: %q, %v", line, err)
	}
	end := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, br)
		end <- err
	}()
	return end
}

// An open SSE stream must not make shutdown use up the 5 second limit, and the
// client must see a clean end of stream.
func TestServeShutdownEndsOpenSSE(t *testing.T) {
	quietLog(t)
	up := streamUpstream(t, nil, nil)
	base, cancel, done := startServe(t, up)
	end := openSSEClient(t, base)

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after ctx cancel")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("shutdown took %v with an open SSE stream, want well under the 5s limit", took)
	}
	select {
	case err := <-end:
		if err != nil {
			t.Errorf("client saw %v, want a clean end of stream", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client stream did not end")
	}
}

// A plain response that is still in progress is waited for, not cut.
func TestServeShutdownWaitsForPlainResponse(t *testing.T) {
	quietLog(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	up := streamUpstream(t, started, release)
	base, cancel, done := startServe(t, up)

	body := make(chan string, 1)
	go func() {
		var resp *http.Response
		var err error
		for i := 0; i < 500; i++ {
			if resp, err = http.Get(base + "/slow"); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			body <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		body <- fmt.Sprintf("%s|%v", b, err)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("slow request did not reach upstream")
	}

	cancel()
	select {
	case err := <-done:
		t.Fatalf("serve returned (%v) while a response was in progress", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	if got := <-body; got != "done|<nil>" {
		t.Errorf("response = %q, want it delivered in full", got)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after the response finished")
	}
}

// The exit code stays 0 with an open stream.
func TestRunGracefulStopWithOpenSSE(t *testing.T) {
	quietLog(t)
	up := streamUpstream(t, nil, nil)
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	code := make(chan int, 1)
	go func() {
		code <- run(ctx, []string{"start", "--primary", up.URL, "--fallback", up.URL,
			"--port", fmt.Sprint(port)}, io.Discard)
	}()
	end := openSSEClient(t, fmt.Sprintf("http://127.0.0.1:%d", port))

	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("exit = %d, want 0", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after ctx cancel")
	}
	select {
	case <-end:
	case <-time.After(2 * time.Second):
		t.Error("client stream did not end")
	}
}
