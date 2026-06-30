// Command trino-query-log-sink ingests Trino QueryCompletedEvents, persists
// them to an Iceberg table through Trino, and serves a browse UI.
//
// Subcommands:
//
//	serve     run both the ingest endpoint and the UI (default)
//	ingest    run only the ingest endpoint (write path)
//	ui        run only the read UI (read path)
//	init      apply the schema/table DDL once
//	prune     delete rows older than RETENTION_DAYS (run as a CronJob)
//	maintain  compact files and reclaim space via Iceberg procedures (CronJob)
//	ddl       print the DDL to stdout
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shadi/trino-query-log-sink/internal/config"
	"github.com/Shadi/trino-query-log-sink/internal/ingest"
	"github.com/Shadi/trino-query-log-sink/internal/observability"
	"github.com/Shadi/trino-query-log-sink/internal/prune"
	"github.com/Shadi/trino-query-log-sink/internal/server"
	"github.com/Shadi/trino-query-log-sink/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	if cmd == "-h" || cmd == "--help" || cmd == "help" {
		usage()
		return nil
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	log := observability.NewLogger(cfg.LogLevel)
	slog.SetDefault(log)

	switch cmd {
	case "serve":
		return serve(cfg, log)
	case "ingest":
		return runIngest(cfg, log)
	case "ui":
		return runUI(cfg, log)
	case "init":
		return initDDL(cfg, log)
	case "prune":
		return runPrune(cfg, log)
	case "maintain":
		return runMaintain(cfg, log)
	case "ddl":
		return printDDL(cfg)
	default:
		usage()
		return fmt.Errorf("unknown subcommand %q", cmd)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `trino-query-log-sink <subcommand>

  serve     run both the ingest endpoint and the web UI (default)
  ingest    run only the ingest endpoint (write path)
  ui        run only the read UI (read path)
  init      apply the schema/table DDL once
  prune     delete rows older than RETENTION_DAYS
  maintain  compact files and reclaim space via Iceberg procedures
  ddl       print the DDL to stdout

Configuration is read from environment variables (see README).
`)
}

func serve(cfg *config.Config, log *slog.Logger) error {
	return runServer(cfg, log, server.Options{Ingest: true, UI: true})
}

func runIngest(cfg *config.Config, log *slog.Logger) error {
	return runServer(cfg, log, server.Options{Ingest: true})
}

func runUI(cfg *config.Config, log *slog.Logger) error {
	return runServer(cfg, log, server.Options{UI: true})
}

func runServer(cfg *config.Config, log *slog.Logger, opts server.Options) error {
	st, err := store.New(cfg.Trino)
	if err != nil {
		return err
	}
	defer st.Close()

	metrics := observability.NewMetrics()

	var buf *ingest.Buffer
	if opts.Ingest {
		buf = ingest.New(st, ingest.Config{
			BatchSize:      cfg.BatchSize,
			FlushInterval:  cfg.FlushInterval,
			BufferCapacity: cfg.BufferCapacity,
			MaxRetries:     cfg.FlushMaxRetries,
			FlushTimeout:   cfg.Trino.QueryTimeout,
		}, log, metrics)
		metrics.SetBufferSource(func() observability.BufferStats {
			depth, capacity, lastMs, durMs := buf.Stats()
			return observability.BufferStats{
				Depth: depth, Capacity: capacity,
				LastFlushUnixMs: lastMs, LastFlushDurationMs: durMs,
			}
		})
	}

	srv := server.New(*cfg, st, buf, metrics, log, opts)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go srv.RunReadiness(ctx)

	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() { serverErr <- httpSrv.ListenAndServe() }()
	log.Info("listening",
		"addr", cfg.ListenAddr,
		"roles", rolesLabel(opts),
		"trino", fmt.Sprintf("%s:%d", cfg.Trino.Host, cfg.Trino.Port),
		"table", fmt.Sprintf("%s.%s.%s", cfg.Trino.Catalog, cfg.Trino.Schema, cfg.Trino.Table))

	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
	}

	srv.MarkNotReady()

	shutCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		log.Error("http shutdown", "error", err)
	}
	if buf != nil {
		drainCtx, dcancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer dcancel()
		if err := buf.Close(drainCtx); err != nil {
			log.Error("buffer drain incomplete; some events may be lost", "error", err)
		}
	}
	log.Info("shutdown complete")
	return nil
}

func rolesLabel(opts server.Options) string {
	switch {
	case opts.Ingest && opts.UI:
		return "ingest,ui"
	case opts.Ingest:
		return "ingest"
	default:
		return "ui"
	}
}

func initDDL(cfg *config.Config, log *slog.Logger) error {
	st, err := store.New(cfg.Trino)
	if err != nil {
		return err
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	log.Info("applying DDL",
		"catalog", cfg.Trino.Catalog, "schema", cfg.Trino.Schema, "table", cfg.Trino.Table,
		"location", cfg.IcebergLocation)
	if err := st.Init(ctx, cfg.IcebergLocation); err != nil {
		return err
	}
	log.Info("DDL applied")
	return nil
}

func runPrune(cfg *config.Config, log *slog.Logger) error {
	st, err := store.New(cfg.Trino)
	if err != nil {
		return err
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	return prune.Run(ctx, st, cfg.RetentionDays, time.Now().UTC(), log)
}

func runMaintain(cfg *config.Config, log *slog.Logger) error {
	st, err := store.New(cfg.Trino)
	if err != nil {
		return err
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	log.Info("maintaining table", "table", cfg.Trino.Table, "retention_threshold", cfg.MaintainRetention)
	if err := st.Maintain(ctx, cfg.MaintainRetention); err != nil {
		return err
	}
	log.Info("maintenance complete")
	return nil
}

func printDDL(cfg *config.Config) error {
	st, err := store.New(cfg.Trino)
	if err != nil {
		return err
	}
	defer st.Close()
	fmt.Print(st.DDLScript(cfg.IcebergLocation))
	return nil
}
