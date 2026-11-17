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
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
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

// config holds the parsed command-line settings of the start sub-command.
type config struct {
	primaryURL       string
	fallbackURL      string
	port             int
	interval         time.Duration
	probeTimeout     time.Duration
	failThreshold    int
	latencyThreshold int // milliseconds
	recoverThreshold int
}

// errMissingRequired is returned by parseConfig when --primary or --fallback
// is not given.  run maps it to exit code 1.
var errMissingRequired = errors.New("--primary and --fallback are required")

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stderr)
	cancel()
	os.Exit(code)
}

// run executes the program and returns the process exit code.
// args excludes the program name.
func run(ctx context.Context, args []string, stderr io.Writer) int {
	// ── sub-command dispatch ──────────────────────────────────────────────
	if len(args) < 1 || args[0] != "start" {
		fmt.Fprintf(stderr, "Usage: status-relay start [flags]\n")
		return 1
	}

	cfg, err := parseConfig(args[1:], stderr)
	switch {
	case err == nil:
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errMissingRequired):
		return 1
	default: // flag parse error (flag.ExitOnError used to exit 2)
		return 2
	}

	app, err := newApp(cfg)
	if err != nil {
		log.Printf("failed to create proxy router: %v", err)
		return 1
	}

	addr := fmt.Sprintf(":%d", cfg.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("server error: %v", err)
		return 1
	}

	log.Printf("[relay] listening on %s  primary=%s  fallback=%s", addr, cfg.primaryURL, cfg.fallbackURL)
	log.Printf("[relay] thresholds: fail=%d  latency=%dms  recover=%d  interval=%s",
		cfg.failThreshold, cfg.latencyThreshold, cfg.recoverThreshold, cfg.interval)

	if err := serve(ctx, app, ln); err != nil {
		log.Printf("server error: %v", err)
		return 1
	}
	return 0
}

// parseConfig parses the flags of the start sub-command (args excludes "start").
// Flag errors are returned as-is (flag.ErrHelp for -h); a missing required
// flag returns errMissingRequired after printing the message and usage.
func parseConfig(args []string, stderr io.Writer) (config, error) {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var cfg config
	fs.StringVar(&cfg.primaryURL, "primary", "", "Primary upstream URL (required)")
	fs.StringVar(&cfg.fallbackURL, "fallback", "", "Fallback upstream URL (required)")
	fs.IntVar(&cfg.port, "port", 8080, "Local port to listen on")
	fs.DurationVar(&cfg.interval, "interval", 200*time.Millisecond, "Health-check interval")
	fs.DurationVar(&cfg.probeTimeout, "probe-timeout", 1500*time.Millisecond, "Per-probe HTTP timeout")
	fs.IntVar(&cfg.failThreshold, "fail-threshold", 3, "Consecutive failures before failover")
	fs.IntVar(&cfg.latencyThreshold, "latency-threshold", 1000, "Latency (ms) that counts as a failure")
	fs.IntVar(&cfg.recoverThreshold, "recover-threshold", 2, "Consecutive successes before recovering primary")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	if cfg.primaryURL == "" || cfg.fallbackURL == "" {
		fmt.Fprintln(stderr, "Error: --primary and --fallback are required")
		fs.Usage()
		return config{}, errMissingRequired
	}
	return cfg, nil
}

// app bundles the wired components.
type app struct {
	cfg     config
	metrics *metrics.Metrics
	engine  *engine.DecisionEngine
	checker *checker.HealthChecker
	router  *proxy.Router
}

// newApp builds and wires the components.
func newApp(cfg config) (*app, error) {
	m := metrics.New()

	eng := engine.New(engine.Config{
		FailThreshold:    cfg.failThreshold,
		LatencyThreshold: time.Duration(cfg.latencyThreshold) * time.Millisecond,
		RecoverThreshold: cfg.recoverThreshold,
	})
	eng.SetMetrics(m)

	hc := checker.New(cfg.primaryURL, cfg.interval, cfg.probeTimeout)
	hc.SetMetrics(m)

	router, err := proxy.New(cfg.primaryURL, cfg.fallbackURL, eng)
	if err != nil {
		return nil, err
	}
	router.SetMetrics(m)

	return &app{cfg: cfg, metrics: m, engine: eng, checker: hc, router: router}, nil
}

// handler returns the HTTP routes: /metrics/json and the failover proxy on /.
func (a *app) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(a.metrics.Snapshot())
	})
	mux.Handle("/", a.router)
	return mux
}

// serve starts the health checker, the decision engine and the HTTP server on
// ln.  It returns nil after a graceful shutdown triggered by ctx, or the
// server error if the server fails first.
func serve(ctx context.Context, a *app, ln net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// ── start health checker ──────────────────────────────────────────────
	a.checker.Start(ctx)

	// ── start decision engine ─────────────────────────────────────────────
	go a.engine.Run(ctx, a.checker.Results())

	// ── start HTTP proxy server ───────────────────────────────────────────
	srv := &http.Server{
		Addr:    ln.Addr().String(),
		Handler: a.handler(),
		// Conservative timeouts — the proxy will stream whatever the
		// upstream sends, so we keep these generous.
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// ── wait for shutdown signal or server failure ────────────────────────
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	log.Println("[relay] shutting down …")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[relay] graceful shutdown error: %v", err)
	}
	log.Println("[relay] stopped")
	return nil
}
