package metrics

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func section(t *testing.T, snap map[string]interface{}, keys ...string) map[string]interface{} {
	t.Helper()
	cur := snap
	for _, k := range keys {
		next, ok := cur[k].(map[string]interface{})
		if !ok {
			t.Fatalf("missing section %q in %v", k, keys)
		}
		cur = next
	}
	return cur
}

func TestNewInitialState(t *testing.T) {
	snap := New().Snapshot()

	if snap["version"] != "1.0" {
		t.Errorf("version = %v, want 1.0", snap["version"])
	}
	st := section(t, snap, "current_state")
	if st["active_target"] != "primary" || st["consecutive_type"] != "successes" || st["consecutive_count"] != 0 {
		t.Errorf("unexpected initial state: %v", st)
	}
	fe := section(t, snap, "failover_events")
	if fe["last_failover_at"] != nil || fe["last_recovery_at"] != nil {
		t.Errorf("last_* should be nil before any event: %v", fe)
	}
	p := section(t, snap, "request_stats", "primary")
	if p["total_requests"] != 0 || p["error_rate"] != 0.0 || p["p95_latency_ms"] != 0.0 {
		t.Errorf("empty primary stats should be zero: %v", p)
	}
}

func TestFailoverAndRecovery(t *testing.T) {
	m := New()

	m.RecordFailover()
	snap := m.Snapshot()
	if got := section(t, snap, "current_state")["active_target"]; got != "fallback" {
		t.Errorf("after failover active_target = %v", got)
	}
	fe := section(t, snap, "failover_events")
	if fe["total_failovers"] != 1 || fe["last_failover_at"] == nil {
		t.Errorf("failover not recorded: %v", fe)
	}

	m.RecordRecovery()
	snap = m.Snapshot()
	if got := section(t, snap, "current_state")["active_target"]; got != "primary" {
		t.Errorf("after recovery active_target = %v", got)
	}
	fe = section(t, snap, "failover_events")
	if fe["total_recoveries"] != 1 || fe["last_recovery_at"] == nil {
		t.Errorf("recovery not recorded: %v", fe)
	}
}

func TestRecordConsecutiveCount(t *testing.T) {
	m := New()
	m.RecordConsecutiveCount(2, "failures")
	st := section(t, m.Snapshot(), "current_state")
	if st["consecutive_count"] != 2 || st["consecutive_type"] != "failures" {
		t.Errorf("got %v", st)
	}
}

func TestRecordRequestPerTarget(t *testing.T) {
	m := New()
	m.RecordRequest("primary", true, 10*time.Millisecond)
	m.RecordRequest("primary", true, 30*time.Millisecond)
	m.RecordRequest("primary", false, 20*time.Millisecond)
	m.RecordRequest("fallback", true, 50*time.Millisecond)
	m.RecordRequest("unknown", true, time.Second) // must be ignored

	rs := section(t, m.Snapshot(), "request_stats")
	p := rs["primary"].(map[string]interface{})
	f := rs["fallback"].(map[string]interface{})

	if p["total_requests"] != 3 || p["successful_requests"] != 2 || p["failed_requests"] != 1 {
		t.Errorf("primary counts wrong: %v", p)
	}
	if p["avg_latency_ms"] != 20.0 {
		t.Errorf("primary avg = %v, want 20", p["avg_latency_ms"])
	}
	if p["error_rate"] != 0.3333 {
		t.Errorf("primary error_rate = %v, want 0.3333", p["error_rate"])
	}
	if f["total_requests"] != 1 || f["failed_requests"] != 0 || f["error_rate"] != 0.0 {
		t.Errorf("fallback stats wrong: %v", f)
	}
}

func TestRecordProbe(t *testing.T) {
	m := New()
	m.RecordProbe(true, 10*time.Millisecond)
	m.RecordProbe(true, 20*time.Millisecond)
	m.RecordProbe(true, 30*time.Millisecond)
	m.RecordProbe(false, 40*time.Millisecond)

	h := section(t, m.Snapshot(), "health_check_stats")
	if h["total_probes"] != 4 || h["successful_probes"] != 3 || h["failed_probes"] != 1 {
		t.Errorf("probe counts wrong: %v", h)
	}
	if h["success_rate"] != 0.75 {
		t.Errorf("success_rate = %v, want 0.75", h["success_rate"])
	}
	if h["avg_probe_latency_ms"] != 25.0 {
		t.Errorf("avg = %v, want 25", h["avg_probe_latency_ms"])
	}
}

func TestPercentiles(t *testing.T) {
	m := New()

	// 1..100ms: with index = p*n/100 on a sorted slice, P95 -> 96ms, P99 -> 100ms.
	var lat []time.Duration
	for i := 1; i <= 100; i++ {
		lat = append(lat, time.Duration(i)*time.Millisecond)
	}
	p95, p99 := m.percentiles(lat)
	if p95 != 96.0 || p99 != 100.0 {
		t.Errorf("p95=%v p99=%v, want 96/100", p95, p99)
	}

	// Single sample: index clamps to the last element.
	p95, p99 = m.percentiles([]time.Duration{7 * time.Millisecond})
	if p95 != 7.0 || p99 != 7.0 {
		t.Errorf("single sample p95=%v p99=%v, want 7/7", p95, p99)
	}

	// Input order must not matter and input must not be mutated.
	unsorted := []time.Duration{30 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}
	p95, _ = m.percentiles(unsorted)
	if p95 != 30.0 {
		t.Errorf("unsorted p95 = %v, want 30", p95)
	}
	if unsorted[0] != 30*time.Millisecond {
		t.Errorf("percentiles mutated its input: %v", unsorted)
	}
}

func TestLatencyWindowCappedAt1000(t *testing.T) {
	m := New()
	// 1500 samples: first 500 are 1s (should be evicted), last 1000 are 1ms.
	for i := 0; i < 500; i++ {
		m.RecordRequest("primary", true, time.Second)
	}
	for i := 0; i < 1000; i++ {
		m.RecordRequest("primary", true, time.Millisecond)
	}

	p := section(t, m.Snapshot(), "request_stats", "primary")
	if p["total_requests"] != 1500 {
		t.Errorf("total_requests = %v, want 1500 (cumulative)", p["total_requests"])
	}
	if len(m.primaryLatencies) != maxLatencySamples {
		t.Errorf("window len = %d, want %d", len(m.primaryLatencies), maxLatencySamples)
	}
	if p["avg_latency_ms"] != 1.0 || p["p99_latency_ms"] != 1.0 {
		t.Errorf("old samples leaked into window: %v", p)
	}
}

func TestSnapshotMarshalsToJSON(t *testing.T) {
	m := New()
	m.RecordFailover()
	m.RecordRequest("primary", true, 5*time.Millisecond)
	m.RecordProbe(true, 5*time.Millisecond)

	b, err := json.Marshal(m.Snapshot())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{"version", "timestamp", "uptime_seconds", "current_state",
		"failover_events", "request_stats", "health_check_stats"} {
		if _, ok := out[k]; !ok {
			t.Errorf("missing top-level key %q", k)
		}
	}
	if _, err := time.Parse(time.RFC3339, out["timestamp"].(string)); err != nil {
		t.Errorf("timestamp not RFC3339: %v", err)
	}
	// nil last_recovery_at must serialize as JSON null.
	fe := out["failover_events"].(map[string]interface{})
	if v, ok := fe["last_recovery_at"]; !ok || v != nil {
		t.Errorf("last_recovery_at = %v (present=%v), want null", v, ok)
	}
}

// Run with -race. Writers and Snapshot() readers hit every code path at once.
func TestConcurrentAccess(t *testing.T) {
	m := New()
	const workers, iters = 8, 500

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				target := "primary"
				if (w+i)%2 == 0 {
					target = "fallback"
				}
				m.RecordRequest(target, i%5 != 0, time.Duration(i)*time.Microsecond)
				m.RecordProbe(i%7 != 0, time.Duration(i)*time.Microsecond)
				if i%50 == 0 {
					m.RecordFailover()
					m.RecordRecovery()
					m.RecordConsecutiveCount(i, "failures")
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_ = m.Snapshot()
			}
		}()
	}
	wg.Wait()

	rs := section(t, m.Snapshot(), "request_stats")
	total := rs["primary"].(map[string]interface{})["total_requests"].(int) +
		rs["fallback"].(map[string]interface{})["total_requests"].(int)
	if total != workers*iters {
		t.Errorf("lost updates: total requests = %d, want %d", total, workers*iters)
	}
	if got := section(t, m.Snapshot(), "health_check_stats")["total_probes"]; got != workers*iters {
		t.Errorf("lost probes: %v, want %d", got, workers*iters)
	}
}

// Values taken from the blueprint's example response: they only match if
// round() rounds to nearest rather than truncating.
func TestRoundMatchesBlueprintExamples(t *testing.T) {
	cases := []struct {
		in       float64
		decimals int
		want     float64
	}{
		{40.0 / 5420.0, 4, 0.0074},
		{17850.0 / 18000.0, 4, 0.9917},
		{2.0 / 3.0, 4, 0.6667},
		{45.25, 1, 45.3},
		{0, 4, 0},
	}
	for _, c := range cases {
		if got := round(c.in, c.decimals); got != c.want {
			t.Errorf("round(%v, %d) = %v, want %v", c.in, c.decimals, got, c.want)
		}
	}
}
