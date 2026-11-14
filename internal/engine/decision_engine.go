// Package engine implements the threshold-based failure detection logic.
//
// The DecisionEngine consumes CheckResults from the health checker and
// writes the current RouteTarget into an atomic value so the proxy can
// read it on the hot path without any locking.
package engine

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/neisii/status-relay/internal/checker"
	"github.com/neisii/status-relay/internal/metrics"
)

// RouteTarget identifies which upstream to use.
type RouteTarget int32

const (
	Primary  RouteTarget = 0
	Fallback RouteTarget = 1
)

func (r RouteTarget) String() string {
	if r == Primary {
		return "primary"
	}
	return "fallback"
}

// Config holds tuneable thresholds.
type Config struct {
	// FailThreshold is the number of consecutive failed checks before
	// switching to the fallback.
	FailThreshold int

	// LatencyThreshold is the per-check latency that counts as a failure.
	LatencyThreshold time.Duration

	// RecoverThreshold is the number of consecutive healthy checks
	// required before switching back to primary.
	RecoverThreshold int
}

// DecisionEngine reads CheckResults and updates the atomic routing target.
type DecisionEngine struct {
	cfg         Config
	target      atomic.Int32 // stores RouteTarget
	consecutive int          // consecutive fail (negative) or success (positive) count
	metrics     *metrics.Metrics
}

// New creates a DecisionEngine starting on the primary.
func New(cfg Config) *DecisionEngine {
	d := &DecisionEngine{cfg: cfg}
	d.target.Store(int32(Primary))
	return d
}

// SetMetrics attaches a Metrics instance to record failover/recovery events.
func (d *DecisionEngine) SetMetrics(m *metrics.Metrics) {
	d.metrics = m
}

// Run consumes results until ctx is cancelled or the channel is closed.
// It blocks; call it in a goroutine.
func (d *DecisionEngine) Run(ctx context.Context, results <-chan checker.CheckResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case r, ok := <-results:
			if !ok {
				return
			}
			d.process(r)
		}
	}
}

// Target returns the current routing decision.  Safe to call from any goroutine.
func (d *DecisionEngine) Target() RouteTarget {
	return RouteTarget(d.target.Load())
}

func (d *DecisionEngine) process(r checker.CheckResult) {
	failed := d.isFailed(r)

	if failed {
		if d.consecutive > 0 {
			d.consecutive = 0 // reset recovery streak
		}
		d.consecutive-- // move further negative
	} else {
		if d.consecutive < 0 {
			d.consecutive = 0 // reset failure streak
		}
		d.consecutive++ // move further positive
	}

	current := d.Target()

	switch current {
	case Primary:
		// Switch to fallback after FailThreshold consecutive failures.
		if -d.consecutive >= d.cfg.FailThreshold {
			d.target.Store(int32(Fallback))
			log.Printf("[engine] FAILOVER → fallback  (consecutive_failures=%d, last_latency=%s, err=%v)",
				-d.consecutive, r.Latency, r.Err)
			if d.metrics != nil {
				d.metrics.RecordFailover()
			}
			d.consecutive = 0
		} else if d.metrics != nil {
			d.metrics.RecordConsecutiveCount(-d.consecutive, "failures")
		}
	case Fallback:
		// Return to primary after RecoverThreshold consecutive successes.
		if d.consecutive >= d.cfg.RecoverThreshold {
			d.target.Store(int32(Primary))
			log.Printf("[engine] RECOVER  → primary   (consecutive_successes=%d)", d.consecutive)
			if d.metrics != nil {
				d.metrics.RecordRecovery()
			}
			d.consecutive = 0
		} else if d.metrics != nil {
			d.metrics.RecordConsecutiveCount(d.consecutive, "successes")
		}
	}
}

// isFailed returns true if this result counts as a failure.
func (d *DecisionEngine) isFailed(r checker.CheckResult) bool {
	if r.Err != nil {
		return true
	}
	if r.StatusCode < 200 || r.StatusCode >= 500 {
		return true
	}
	if r.Latency > d.cfg.LatencyThreshold {
		return true
	}
	return false
}
