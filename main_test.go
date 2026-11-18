package main

import (
	"bytes"
	"context"
	"encoding/json"
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

// ── run: exit codes ──────────────────────────────────────────────────────

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
		{"invalid URL", []string{"start", "--primary", "::bad", "--fallback", "http://f"}, 1},
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
