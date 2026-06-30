// Package server exposes the HTTP surface: the /ingest endpoint, the
// browse/detail UI (server-rendered html/template enhanced with HTMX), and the
// health, readiness, and metrics endpoints.
package server

import (
	"bytes"
	"context"
	"embed"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Shadi/trino-query-log-sink/internal/config"
	"github.com/Shadi/trino-query-log-sink/internal/observability"
	"github.com/Shadi/trino-query-log-sink/internal/store"
)

//go:embed templates/*.html static/*
var assets embed.FS

// Enqueuer accepts rows for asynchronous, batched persistence.
type Enqueuer interface {
	Add(store.Row)
}

type Options struct {
	Ingest bool
	UI     bool
}

type Server struct {
	cfg     config.Config
	store   store.Store
	buf     Enqueuer
	metrics *observability.Metrics
	log     *slog.Logger
	opts    Options

	ready atomic.Bool

	dashboardTmpl *template.Template
	detailTmpl    *template.Template
	partialTmpl   *template.Template
}

func New(cfg config.Config, st store.Store, buf Enqueuer, metrics *observability.Metrics, log *slog.Logger, opts Options) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{
		cfg:     cfg,
		store:   st,
		buf:     buf,
		metrics: metrics,
		log:     log,
		opts:    opts,
	}
	if opts.UI {
		funcs := template.FuncMap{
			"bytesH":            humanBytes,
			"bytesHp":           humanBytesPtr,
			"msH":               humanMillis,
			"ts":                formatTime,
			"tsp":               formatStartTime,
			"tsOpt":             formatTimeOpt,
			"num":               humanNum,
			"nump":              humanNumPtr,
			"stateBadge":        stateBadge,
			"sortlink":          sortlink,
			"topHref":           topHref,
			"topHrefPartial":    topHrefPartial,
			"failedHref":        failedHref,
			"failedHrefPartial": failedHrefPartial,
			"prevHref":          prevHref,
			"prevHrefPartial":   prevHrefPartial,
			"nextHref":          nextHref,
			"nextHrefPartial":   nextHrefPartial,
		}
		parse := func(files ...string) *template.Template {
			return template.Must(template.New("base").Funcs(funcs).ParseFS(assets, files...))
		}
		s.dashboardTmpl = parse("templates/base.html", "templates/dashboard.html", "templates/table.html")
		s.detailTmpl = parse("templates/base.html", "templates/detail.html")
		s.partialTmpl = parse("templates/table.html")
	}
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	if s.opts.Ingest {
		mux.HandleFunc("POST /ingest", s.handleIngest)
	}

	if s.opts.UI {
		mux.HandleFunc("GET /{$}", s.handleDashboard)
		mux.HandleFunc("GET /partials/queries", s.handleQueriesPartial)
		mux.HandleFunc("GET /query/{queryId}", s.handleQueryDetail)

		staticFS, _ := fs.Sub(assets, "static")
		fileServer := http.StripPrefix("/static/", http.FileServer(http.FS(staticFS)))
		mux.Handle("GET /static/", noDirListing(fileServer))
	}

	if s.cfg.MetricsEnabled && s.metrics != nil {
		mux.Handle("GET /metrics", s.metrics)
	}

	return s.logRequests(securityHeaders(mux))
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; base-uri 'self'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "same-origin")
		next.ServeHTTP(w, r)
	})
}

func noDirListing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		s.log.Debug("http",
			"method", r.Method, "path", r.URL.Path, "status", sw.status,
			"duration_ms", time.Since(start).Milliseconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("not ready: query log table unreachable; apply the DDL (init subcommand or ddl/trino_query_log.sql)"))
}

func (s *Server) MarkNotReady() { s.ready.Store(false) }

func (s *Server) RunReadiness(ctx context.Context) {
	check := func() {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := s.store.Validate(cctx); err != nil {
			if s.ready.Swap(false) {
				s.log.Warn("readiness lost: query log table unreachable", "error", err)
			} else {
				s.log.Warn("not ready: query log table unreachable", "error", err)
			}
			return
		}
		if !s.ready.Swap(true) {
			s.log.Info("ready: query log table reachable")
		}
	}

	check()
	for {
		d := 30 * time.Second
		if !s.ready.Load() {
			d = 5 * time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
			check()
		}
	}
}

func (s *Server) render(w http.ResponseWriter, t *template.Template, name string, data any) {
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, name, data); err != nil {
		s.log.Error("template render failed", "template", name, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}
