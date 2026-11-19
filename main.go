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
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
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

// errInvalidConfig is returned by parseConfig when a flag value is out of its
// allowed range or malformed.  The per-flag messages are already printed to
// stderr; run maps it to exit code 2 (same as a flag parse error).
var errInvalidConfig = errors.New("invalid configuration")

// Lower and upper bounds for flag values.  Defaults are well inside them.
const (
	minInterval     = 10 * time.Millisecond // default 200ms
	minProbeTimeout = 10 * time.Millisecond // default 1500ms
	maxPort         = 65535

	// maxLatencyThresholdMS keeps time.Duration(ms) * time.Millisecond from
	// overflowing int64.
	maxLatencyThresholdMS = math.MaxInt64 / int64(time.Millisecond)
)

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
	case errors.Is(err, errInvalidConfig):
		return 2
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
// flag returns errMissingRequired after printing the message and usage.  Values
// that parse but are out of range or malformed return errInvalidConfig after
// printing one "Error:" line per problem and the usage.
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

	// A positional argument stops flag parsing, so flags after it would be
	// dropped silently.  Report it instead of a misleading "required" error.
	var posErr error
	if fs.NArg() > 0 {
		posErr = fmt.Errorf("unexpected argument %q: put all flags before positional arguments", fs.Arg(0))
	}

	if posErr == nil && (cfg.primaryURL == "" || cfg.fallbackURL == "") {
		fmt.Fprintln(stderr, "Error: --primary and --fallback are required")
		fs.Usage()
		return config{}, errMissingRequired
	}

	if err := errors.Join(posErr, validateConfig(cfg)); err != nil {
		for _, line := range strings.Split(err.Error(), "\n") {
			fmt.Fprintf(stderr, "Error: %s\n", line)
		}
		fs.Usage()
		return config{}, errInvalidConfig
	}
	return cfg, nil
}

// validateConfig checks flag values and returns all problems joined together
// (nil if none).  Empty URLs are skipped; parseConfig reports those as missing.
func validateConfig(cfg config) error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if cfg.port < 0 || cfg.port > maxPort {
		add("invalid --port %d: must be between 0 and %d", cfg.port, maxPort)
	}
	if cfg.interval < minInterval {
		add("invalid --interval %s: must be at least %s", cfg.interval, minInterval)
	}
	if cfg.probeTimeout < minProbeTimeout {
		add("invalid --probe-timeout %s: must be at least %s", cfg.probeTimeout, minProbeTimeout)
	}
	if cfg.failThreshold < 1 {
		add("invalid --fail-threshold %d: must be at least 1", cfg.failThreshold)
	}
	if cfg.recoverThreshold < 1 {
		add("invalid --recover-threshold %d: must be at least 1", cfg.recoverThreshold)
	}
	if cfg.latencyThreshold < 1 || int64(cfg.latencyThreshold) > maxLatencyThresholdMS {
		add("invalid --latency-threshold %d: must be between 1 and %d (milliseconds)",
			cfg.latencyThreshold, maxLatencyThresholdMS)
	}
	for _, u := range []struct{ flag, val string }{{"primary", cfg.primaryURL}, {"fallback", cfg.fallbackURL}} {
		if u.val != "" && !isHTTPURL(u.val) {
			add("invalid --%s %q: must be an http or https URL with a host", u.flag, u.val)
		}
	}
	return errors.Join(errs...)
}

// isHTTPURL reports whether raw is an absolute http or https URL with a host.
func isHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != ""
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

	// Shutdown waits for active requests, and an open SSE stream never ends on
	// its own.  Close the streams first so Shutdown does not use up its limit.
	srv.RegisterOnShutdown(a.router.CloseStreams)

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
