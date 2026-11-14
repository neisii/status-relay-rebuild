// Package metrics provides real-time and cumulative statistics for the failover proxy.
package metrics

import (
	"sort"
	"sync"
	"time"
)

// Metrics tracks proxy statistics: failover events, request counts, health check results.
type Metrics struct {
	mu sync.RWMutex

	startTime time.Time

	// Current routing state
	activeTarget     string // "primary" or "fallback"
	consecutiveCount int
	consecutiveType  string // "successes" or "failures"

	// Failover events
	totalFailovers   int
	totalRecoveries  int
	lastFailoverTime time.Time
	lastRecoveryTime time.Time

	// Request statistics (Primary)
	primaryRequests      int
	primarySuccessful    int
	primaryFailed        int
	primaryLatencies     []time.Duration // keep last 1000
	primaryLatenciesLock sync.Mutex

	// Request statistics (Fallback)
	fallbackRequests      int
	fallbackSuccessful    int
	fallbackFailed        int
	fallbackLatencies     []time.Duration // keep last 1000
	fallbackLatenciesLock sync.Mutex

	// Health check statistics
	totalProbes        int
	successfulProbes   int
	failedProbes       int
	probeLatencies     []time.Duration // keep last 1000
	probeLatenciesLock sync.Mutex
}

const maxLatencySamples = 1000

// New creates a new Metrics instance.
func New() *Metrics {
	return &Metrics{
		startTime:       time.Now(),
		activeTarget:    "primary",
		consecutiveType: "successes",
	}
}

// RecordFailover records a failover event (primary → fallback).
func (m *Metrics) RecordFailover() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.activeTarget = "fallback"
	m.totalFailovers++
	m.lastFailoverTime = time.Now()
	m.consecutiveCount = 0
	m.consecutiveType = "failures"
}

// RecordRecovery records a recovery event (fallback → primary).
func (m *Metrics) RecordRecovery() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.activeTarget = "primary"
	m.totalRecoveries++
	m.lastRecoveryTime = time.Now()
	m.consecutiveCount = 0
	m.consecutiveType = "successes"
}

// RecordConsecutiveCount updates the consecutive count and type.
func (m *Metrics) RecordConsecutiveCount(count int, typ string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.consecutiveCount = count
	m.consecutiveType = typ
}

// RecordRequest records a request to primary or fallback.
// target: "primary" or "fallback", success: true if request succeeded, latency: request duration.
func (m *Metrics) RecordRequest(target string, success bool, latency time.Duration) {
	if target == "primary" {
		m.recordPrimaryRequest(success, latency)
	} else if target == "fallback" {
		m.recordFallbackRequest(success, latency)
	}
}

func (m *Metrics) recordPrimaryRequest(success bool, latency time.Duration) {
	m.mu.Lock()
	m.primaryRequests++
	if success {
		m.primarySuccessful++
	} else {
		m.primaryFailed++
	}
	m.mu.Unlock()

	m.primaryLatenciesLock.Lock()
	m.primaryLatencies = append(m.primaryLatencies, latency)
	if len(m.primaryLatencies) > maxLatencySamples {
		m.primaryLatencies = m.primaryLatencies[1:]
	}
	m.primaryLatenciesLock.Unlock()
}

func (m *Metrics) recordFallbackRequest(success bool, latency time.Duration) {
	m.mu.Lock()
	m.fallbackRequests++
	if success {
		m.fallbackSuccessful++
	} else {
		m.fallbackFailed++
	}
	m.mu.Unlock()

	m.fallbackLatenciesLock.Lock()
	m.fallbackLatencies = append(m.fallbackLatencies, latency)
	if len(m.fallbackLatencies) > maxLatencySamples {
		m.fallbackLatencies = m.fallbackLatencies[1:]
	}
	m.fallbackLatenciesLock.Unlock()
}

// RecordProbe records a health check probe result.
func (m *Metrics) RecordProbe(success bool, latency time.Duration) {
	m.mu.Lock()
	m.totalProbes++
	if success {
		m.successfulProbes++
	} else {
		m.failedProbes++
	}
	m.mu.Unlock()

	m.probeLatenciesLock.Lock()
	m.probeLatencies = append(m.probeLatencies, latency)
	if len(m.probeLatencies) > maxLatencySamples {
		m.probeLatencies = m.probeLatencies[1:]
	}
	m.probeLatenciesLock.Unlock()
}

// Snapshot returns a JSON-ready snapshot of current metrics.
func (m *Metrics) Snapshot() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	uptime := time.Since(m.startTime).Seconds()

	// Calculate percentiles
	primaryP95, primaryP99 := m.percentiles(m.primaryLatencies)
	fallbackP95, fallbackP99 := m.percentiles(m.fallbackLatencies)
	probeP95, probeP99 := m.percentiles(m.probeLatencies)

	// Calculate averages
	primaryAvg := m.average(m.primaryLatencies)
	fallbackAvg := m.average(m.fallbackLatencies)
	probeAvg := m.average(m.probeLatencies)

	// Calculate error rates
	primaryErrorRate := 0.0
	if m.primaryRequests > 0 {
		primaryErrorRate = float64(m.primaryFailed) / float64(m.primaryRequests)
	}
	fallbackErrorRate := 0.0
	if m.fallbackRequests > 0 {
		fallbackErrorRate = float64(m.fallbackFailed) / float64(m.fallbackRequests)
	}
	probeSuccessRate := 0.0
	if m.totalProbes > 0 {
		probeSuccessRate = float64(m.successfulProbes) / float64(m.totalProbes)
	}

	return map[string]interface{}{
		"version":        "1.0",
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"uptime_seconds": uptime,
		"current_state": map[string]interface{}{
			"active_target":     m.activeTarget,
			"consecutive_count": m.consecutiveCount,
			"consecutive_type":  m.consecutiveType,
		},
		"failover_events": map[string]interface{}{
			"total_failovers":  m.totalFailovers,
			"total_recoveries": m.totalRecoveries,
			"last_failover_at": timePtr(m.lastFailoverTime),
			"last_recovery_at": timePtr(m.lastRecoveryTime),
		},
		"request_stats": map[string]interface{}{
			"primary": map[string]interface{}{
				"total_requests":      m.primaryRequests,
				"successful_requests": m.primarySuccessful,
				"failed_requests":     m.primaryFailed,
				"avg_latency_ms":      primaryAvg,
				"p95_latency_ms":      primaryP95,
				"p99_latency_ms":      primaryP99,
				"error_rate":          round(primaryErrorRate, 4),
			},
			"fallback": map[string]interface{}{
				"total_requests":      m.fallbackRequests,
				"successful_requests": m.fallbackSuccessful,
				"failed_requests":     m.fallbackFailed,
				"avg_latency_ms":      fallbackAvg,
				"p95_latency_ms":      fallbackP95,
				"p99_latency_ms":      fallbackP99,
				"error_rate":          round(fallbackErrorRate, 4),
			},
		},
		"health_check_stats": map[string]interface{}{
			"total_probes":         m.totalProbes,
			"successful_probes":    m.successfulProbes,
			"failed_probes":        m.failedProbes,
			"success_rate":         round(probeSuccessRate, 4),
			"avg_probe_latency_ms": probeAvg,
			"p95_probe_latency_ms": probeP95,
			"p99_probe_latency_ms": probeP99,
		},
	}
}

// Helper functions

func (m *Metrics) percentiles(latencies []time.Duration) (p95, p99 float64) {
	if len(latencies) == 0 {
		return 0, 0
	}

	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	p95Index := (95 * len(sorted)) / 100
	p99Index := (99 * len(sorted)) / 100
	if p95Index >= len(sorted) {
		p95Index = len(sorted) - 1
	}
	if p99Index >= len(sorted) {
		p99Index = len(sorted) - 1
	}

	p95 = round(sorted[p95Index].Seconds()*1000, 1)
	p99 = round(sorted[p99Index].Seconds()*1000, 1)
	return p95, p99
}

func (m *Metrics) average(latencies []time.Duration) float64 {
	if len(latencies) == 0 {
		return 0
	}

	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	return round(sum.Seconds()*1000/float64(len(latencies)), 1)
}

func round(f float64, decimals int) float64 {
	mult := 1.0
	for i := 0; i < decimals; i++ {
		mult *= 10
	}
	return float64(int64(f*mult)) / mult
}

func timePtr(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}
