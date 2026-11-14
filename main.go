// StatusRelay — real-time HTTP failover proxy.
//
// Usage:
//
//	status-relay start \
//	  --primary   https://api.primary.com \
//	  --fallback  https://api.backup.com \
//	  --port      8080
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/neisii/status-relay/internal/checker"
	"github.com/neisii/status-relay/internal/engine"
	"github.com/neisii/status-relay/internal/metrics"
	"github.com/neisii/status-relay/internal/proxy"
)

func main() {
	// ── sub-command dispatch ──────────────────────────────────────────────
	if len(os.Args) < 2 || os.Args[1] != "start" {
		fmt.Fprintf(os.Stderr, "Usage: status-relay start [flags]\n")
		os.Exit(1)
	}

	// ── flags ─────────────────────────────────────────────────────────────
	fs := flag.NewFlagSet("start", flag.ExitOnError)

	primaryURL := fs.String("primary", "", "Primary upstream URL (required)")
	fallbackURL := fs.String("fallback", "", "Fallback upstream URL (required)")
	port := fs.Int("port", 8080, "Local port to listen on")
	interval := fs.Duration("interval", 200*time.Millisecond, "Health-check interval")
	probeTimeout := fs.Duration("probe-timeout", 1500*time.Millisecond, "Per-probe HTTP timeout")
	failN := fs.Int("fail-threshold", 3, "Consecutive failures before failover")
	latencyMS := fs.Int("latency-threshold", 1000, "Latency (ms) that counts as a failure")
	recoverN := fs.Int("recover-threshold", 2, "Consecutive successes before recovering primary")

	if err := fs.Parse(os.Args[2:]); err != nil {
		os.Exit(1)
	}

	if *primaryURL == "" || *fallbackURL == "" {
		fmt.Fprintln(os.Stderr, "Error: --primary and --fallback are required")
		fs.Usage()
		os.Exit(1)
	}

	// ── build components ──────────────────────────────────────────────────
	m := metrics.New()

	eng := engine.New(engine.Config{
		FailThreshold:    *failN,
		LatencyThreshold: time.Duration(*latencyMS) * time.Millisecond,
		RecoverThreshold: *recoverN,
	})
	eng.SetMetrics(m)

	hc := checker.New(*primaryURL, *interval, *probeTimeout)
	hc.SetMetrics(m)

	router, err := proxy.New(*primaryURL, *fallbackURL, eng)
	if err != nil {
		log.Fatalf("failed to create proxy router: %v", err)
	}
	router.SetMetrics(m)

	// ── context + signal handling ─────────────────────────────────────────
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// ── start health checker ──────────────────────────────────────────────
	hc.Start(ctx)

	// ── start decision engine ─────────────────────────────────────────────
	go eng.Run(ctx, hc.Results())

	// ── start HTTP proxy server ───────────────────────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(m.Snapshot())
	})
	mux.Handle("/", router)

	addr := fmt.Sprintf(":%d", *port)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
		// Conservative timeouts — the proxy will stream whatever the
		// upstream sends, so we keep these generous.
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("[relay] listening on %s  primary=%s  fallback=%s", addr, *primaryURL, *fallbackURL)
		log.Printf("[relay] thresholds: fail=%d  latency=%dms  recover=%d  interval=%s",
			*failN, *latencyMS, *recoverN, *interval)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// ── wait for shutdown signal ──────────────────────────────────────────
	<-ctx.Done()
	log.Println("[relay] shutting down …")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[relay] graceful shutdown error: %v", err)
	}
	log.Println("[relay] stopped")
}
