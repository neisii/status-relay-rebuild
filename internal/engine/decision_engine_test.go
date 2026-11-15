package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/neisii/status-relay/internal/checker"
	"github.com/neisii/status-relay/internal/metrics"
)

var testCfg = Config{
	FailThreshold:    3,
	LatencyThreshold: 100 * time.Millisecond,
	RecoverThreshold: 2,
}

func ok() checker.CheckResult {
	return checker.CheckResult{StatusCode: 200, Latency: 10 * time.Millisecond}
}

func bad() checker.CheckResult {
	return checker.CheckResult{StatusCode: 500, Latency: 10 * time.Millisecond}
}

func feed(d *DecisionEngine, rs ...checker.CheckResult) {
	for _, r := range rs {
		d.process(r)
	}
}

func TestRouteTargetString(t *testing.T) {
	if Primary.String() != "primary" || Fallback.String() != "fallback" {
		t.Errorf("got %q / %q", Primary, Fallback)
	}
}

func TestStartsOnPrimary(t *testing.T) {
	if got := New(testCfg).Target(); got != Primary {
		t.Errorf("Target = %v, want primary", got)
	}
}

func TestIsFailed(t *testing.T) {
	d := New(testCfg)
	tests := []struct {
		name string
		r    checker.CheckResult
		want bool
	}{
		{"200 fast", checker.CheckResult{StatusCode: 200, Latency: 10 * time.Millisecond}, false},
		{"404 is not a failure", checker.CheckResult{StatusCode: 404, Latency: 10 * time.Millisecond}, false},
		{"499 is not a failure", checker.CheckResult{StatusCode: 499, Latency: 10 * time.Millisecond}, false},
		{"500 fails", checker.CheckResult{StatusCode: 500, Latency: 10 * time.Millisecond}, true},
		{"199 fails", checker.CheckResult{StatusCode: 199, Latency: 10 * time.Millisecond}, true},
		{"status 0 fails", checker.CheckResult{StatusCode: 0, Latency: 10 * time.Millisecond}, true},
		{"error fails even with 200", checker.CheckResult{StatusCode: 200, Err: errors.New("boom")}, true},
		{"latency equal to threshold passes", checker.CheckResult{StatusCode: 200, Latency: testCfg.LatencyThreshold}, false},
		{"latency over threshold fails", checker.CheckResult{StatusCode: 200, Latency: testCfg.LatencyThreshold + time.Millisecond}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := d.isFailed(tt.r); got != tt.want {
				t.Errorf("isFailed = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFailoverAfterThreshold(t *testing.T) {
	d := New(testCfg)
	feed(d, bad(), bad())
	if d.Target() != Primary {
		t.Fatalf("switched before threshold")
	}
	feed(d, bad())
	if d.Target() != Fallback {
		t.Errorf("Target = %v after %d failures, want fallback", d.Target(), testCfg.FailThreshold)
	}
}

func TestSuccessResetsFailureStreak(t *testing.T) {
	d := New(testCfg)
	feed(d, bad(), bad(), ok(), bad(), bad())
	if d.Target() != Primary {
		t.Errorf("non-consecutive failures must not trigger failover")
	}
}

func TestLatencyTriggersFailover(t *testing.T) {
	d := New(testCfg)
	slow := checker.CheckResult{StatusCode: 200, Latency: time.Second}
	feed(d, slow, slow, slow)
	if d.Target() != Fallback {
		t.Errorf("slow responses should fail over")
	}
}

func TestRecoveryAfterThreshold(t *testing.T) {
	d := New(testCfg)
	feed(d, bad(), bad(), bad())
	feed(d, ok())
	if d.Target() != Fallback {
		t.Fatalf("recovered before threshold")
	}
	feed(d, ok())
	if d.Target() != Primary {
		t.Errorf("Target = %v, want primary after recovery", d.Target())
	}
}

func TestFailureResetsRecoveryStreak(t *testing.T) {
	d := New(testCfg)
	feed(d, bad(), bad(), bad())
	feed(d, ok(), bad(), ok())
	if d.Target() != Fallback {
		t.Errorf("interrupted success streak must not recover")
	}
}

func TestCounterResetsAfterSwitch(t *testing.T) {
	d := New(testCfg)
	feed(d, bad(), bad(), bad()) // -> fallback, counter reset
	// Failures while already on fallback must not build toward anything.
	feed(d, bad(), bad(), bad(), bad())
	if d.Target() != Fallback {
		t.Fatalf("should stay on fallback")
	}
	feed(d, ok(), ok())
	if d.Target() != Primary {
		t.Errorf("should recover after 2 successes regardless of earlier failures")
	}
}

func TestMetricsRecorded(t *testing.T) {
	m := metrics.New()
	d := New(testCfg)
	d.SetMetrics(m)

	feed(d, bad(), bad(), bad(), ok(), ok())

	snap := m.Snapshot()
	fe := snap["failover_events"].(map[string]interface{})
	if fe["total_failovers"] != 1 || fe["total_recoveries"] != 1 {
		t.Errorf("failover_events = %v, want 1 failover and 1 recovery", fe)
	}
}

func TestRunProcessesUntilChannelClosed(t *testing.T) {
	d := New(testCfg)
	ch := make(chan checker.CheckResult, 3)
	for i := 0; i < 3; i++ {
		ch <- bad()
	}
	close(ch)

	done := make(chan struct{})
	go func() {
		d.Run(context.Background(), ch)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after channel close")
	}
	if d.Target() != Fallback {
		t.Errorf("Target = %v, want fallback", d.Target())
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	d := New(testCfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx, make(chan checker.CheckResult))
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
