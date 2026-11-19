// Package checker implements a lightweight, non-blocking HTTP health checker.
// It probes a target URL at a fixed interval and streams CheckResults to a
// buffered channel so callers never block the probe goroutine.
package checker

import (
	"context"
	"net/http"
	"time"

	"github.com/neisii/status-relay/internal/metrics"
)

// CheckResult is the outcome of a single health probe.
type CheckResult struct {
	Timestamp  time.Time
	StatusCode int // 0 if the request never completed
	Latency    time.Duration
	Err        error // non-nil on timeout or network error
}

// HealthChecker probes a URL at a regular interval.
type HealthChecker struct {
	target   string
	interval time.Duration
	timeout  time.Duration
	client   *http.Client
	results  chan CheckResult
	metrics  *metrics.Metrics
}

// New creates a HealthChecker.
//
//	target   – URL to probe (e.g. "https://api.example.com/health")
//	interval – how often to probe (e.g. 200ms)
//	timeout  – per-request deadline (e.g. 1500ms)
func New(target string, interval, timeout time.Duration) *HealthChecker {
	return &HealthChecker{
		target:   target,
		interval: interval,
		timeout:  timeout,
		client: &http.Client{
			Timeout: timeout,
			// Skip redirect chains — we only care whether the immediate
			// target is reachable, not whether it eventually redirects.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		// Buffer = 10 keeps the channel from blocking the ticker goroutine
		// if the consumer (DecisionEngine) is momentarily busy.
		results: make(chan CheckResult, 10),
	}
}

// Start launches the probe loop.  It returns immediately; probing runs in a
// dedicated goroutine that stops when ctx is cancelled.
func (h *HealthChecker) Start(ctx context.Context) {
	go h.loop(ctx)
}

// Results returns the read-only channel of probe outcomes.
func (h *HealthChecker) Results() <-chan CheckResult {
	return h.results
}

// SetMetrics attaches a Metrics instance to record probe results.
func (h *HealthChecker) SetMetrics(m *metrics.Metrics) {
	h.metrics = m
}

func (h *HealthChecker) loop(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	defer close(h.results)

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			// select picks at random when both cases are ready; make
			// cancellation win so shutdown never starts another probe.
			if ctx.Err() != nil {
				return
			}

			result := h.probe(ctx, t)

			// A probe aborted by shutdown is not a real failure: drop it so
			// it reaches neither metrics nor the decision engine.  The parent
			// ctx is checked instead of the error type so a genuine
			// per-probe timeout (client.Timeout) is still recorded.
			if result.Err != nil && ctx.Err() != nil {
				return
			}

			// Record probe result to metrics
			if h.metrics != nil {
				success := result.Err == nil && result.StatusCode >= 200 && result.StatusCode < 500
				h.metrics.RecordProbe(success, result.Latency)
			}

			// Never block on a full buffer past shutdown.
			select {
			case h.results <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (h *HealthChecker) probe(ctx context.Context, t time.Time) CheckResult {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.target, nil)
	if err != nil {
		return CheckResult{Timestamp: t, Err: err, Latency: time.Since(start)}
	}
	req.Header.Set("User-Agent", "StatusRelay/1.0")

	resp, err := h.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		return CheckResult{Timestamp: t, Err: err, Latency: latency}
	}
	resp.Body.Close()

	return CheckResult{
		Timestamp:  t,
		StatusCode: resp.StatusCode,
		Latency:    latency,
	}
}
