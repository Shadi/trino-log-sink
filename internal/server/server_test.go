package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shadi/trino-log-sink/internal/config"
	"github.com/Shadi/trino-log-sink/internal/observability"
	"github.com/Shadi/trino-log-sink/internal/store"
)

type fakeStore struct {
	list    []store.QuerySummary
	detail  *store.Row
	listErr error
	getErr  error
}

func (f *fakeStore) Validate(context.Context) error                 { return nil }
func (f *fakeStore) InsertBatch(context.Context, []store.Row) error { return nil }
func (f *fakeStore) ListQueries(context.Context, store.QueryFilter) ([]store.QuerySummary, error) {
	return f.list, f.listErr
}
func (f *fakeStore) GetQuery(_ context.Context, id string) (*store.Row, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.detail != nil && f.detail.QueryID == id {
		return f.detail, nil
	}
	return nil, nil
}
func (f *fakeStore) Prune(context.Context, time.Time) error    { return nil }
func (f *fakeStore) Maintain(context.Context, string) error    { return nil }
func (f *fakeStore) Optimize(context.Context, time.Time) error { return nil }
func (f *fakeStore) Close() error                              { return nil }

type fakeEnqueuer struct {
	mu   sync.Mutex
	rows []store.Row
}

func (e *fakeEnqueuer) Add(r store.Row) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rows = append(e.rows, r)
}
func (e *fakeEnqueuer) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.rows)
}

func newTestServer(st store.Store, enq Enqueuer) *Server {
	return newTestServerWith(st, enq, Options{Ingest: true, UI: true})
}

func newTestServerWith(st store.Store, enq Enqueuer, opts Options) *Server {
	cfg := config.Config{MetricsEnabled: true}
	cfg.Trino.Source = "trino-query-log"
	return New(cfg, st, enq, observability.NewMetrics(), nil, opts)
}

const validEvent = `{
  "metadata": {"queryId": "q-123", "query": "SELECT 1", "queryState": "FINISHED"},
  "statistics": {"cpuTime": "PT0.5S", "wallTime": "PT1.2S"},
  "context": {"user": "alice", "source": "jdbc", "serverVersion": "478", "environment": "prod"},
  "ioMetadata": {"inputs": []},
  "createTime": "2026-06-30T12:00:00.000Z",
  "endTime": "2026-06-30T12:00:01.200Z"
}`

func postIngest(t *testing.T, s *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

func TestIngestValid(t *testing.T) {
	enq := &fakeEnqueuer{}
	s := newTestServer(&fakeStore{}, enq)
	rec := postIngest(t, s, validEvent)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if enq.count() != 1 {
		t.Fatalf("expected 1 enqueued row, got %d", enq.count())
	}
	if enq.rows[0].QueryID != "q-123" || enq.rows[0].CPUMS != 500 {
		t.Errorf("row mapped wrong: %+v", enq.rows[0])
	}
}

func TestIngestTruncatesOversizedFields(t *testing.T) {
	enq := &fakeEnqueuer{}
	cfg := config.Config{MetricsEnabled: true, MaxFieldBytes: 2048}
	cfg.Trino.Source = "trino-query-log"
	s := New(cfg, &fakeStore{}, enq, observability.NewMetrics(), nil, Options{Ingest: true})

	huge := strings.Repeat("x", 100_000)
	body := strings.Replace(validEvent, `"query": "SELECT 1"`, `"query": "`+huge+`"`, 1)
	rec := postIngest(t, s, body)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if enq.count() != 1 {
		t.Fatalf("expected 1 enqueued row, got %d", enq.count())
	}
	if got := enq.rows[0].QueryText; len(got) > 2048 || !strings.Contains(got, "[truncated ") {
		t.Errorf("query_text not truncated to MaxFieldBytes: %d bytes", len(got))
	}
}

func TestIngestSuppressed(t *testing.T) {
	enq := &fakeEnqueuer{}
	s := newTestServer(&fakeStore{}, enq)
	body := strings.Replace(validEvent, `"source": "jdbc"`, `"source": "trino-query-log"`, 1)
	rec := postIngest(t, s, body)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if enq.count() != 0 {
		t.Fatalf("self-sourced event should be suppressed, got %d rows", enq.count())
	}
}

func TestIngestMalformed(t *testing.T) {
	enq := &fakeEnqueuer{}
	s := newTestServer(&fakeStore{}, enq)
	rec := postIngest(t, s, `{not json`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if enq.count() != 0 {
		t.Fatalf("malformed event should not enqueue")
	}
}

func TestIngestInvalidMissingFields(t *testing.T) {
	enq := &fakeEnqueuer{}
	s := newTestServer(&fakeStore{}, enq)
	rec := postIngest(t, s, `{"metadata":{"queryState":"FINISHED"},"statistics":{},"context":{},"ioMetadata":{}}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if enq.count() != 0 {
		t.Fatalf("event missing queryId/createTime should be skipped")
	}
}

func TestDashboardRenders(t *testing.T) {
	st := &fakeStore{list: []store.QuerySummary{
		{QueryID: "q-1", CreateTime: time.Now().UTC(), UserName: "alice", QueryState: "FINISHED", QueryPreview: "SELECT 1", WallMS: 1200, PhysicalInputBytes: 2048},
		{QueryID: "q-2", CreateTime: time.Now().UTC(), UserName: "bob", QueryState: "FAILED", QueryPreview: "SELECT bad", ErrorCode: "SYNTAX_ERROR"},
	}}
	s := newTestServer(st, &fakeEnqueuer{})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"q-1", "alice", "FAILED", "Top by", "/query/q-1"} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard body missing %q", want)
		}
	}
}

func TestPartialRenders(t *testing.T) {
	st := &fakeStore{list: []store.QuerySummary{{QueryID: "q-9", CreateTime: time.Now().UTC(), UserName: "carol", QueryState: "FINISHED"}}}
	s := newTestServer(st, &fakeEnqueuer{})

	req := httptest.NewRequest(http.MethodGet, "/partials/queries?sort=wall&dir=desc", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "q-9") || strings.Contains(body, "<html") {
		t.Errorf("partial should contain row but not full page: %q", body[:min(200, len(body))])
	}
}

func TestQueryDetailRenders(t *testing.T) {
	st := &fakeStore{detail: &store.Row{
		QueryID: "q-detail", QueryState: "FINISHED", UserName: "dan", QueryText: "SELECT * FROM t",
		CreateTime: time.Now().UTC(), WallMS: 5000, CPUMS: 2500, PhysicalInputBytes: 1 << 20,
		InputsJSON: `[{"catalogName":"c","schema":"s","table":"t","physicalInputBytes":1048576}]`,
		Plan:       "- Output[1]",
	}}
	s := newTestServer(st, &fakeEnqueuer{})

	req := httptest.NewRequest(http.MethodGet, "/query/q-detail", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"q-detail", "SELECT * FROM t", "Input tables", "- Output[1]"} {
		if !strings.Contains(body, want) {
			t.Errorf("detail body missing %q", want)
		}
	}
}

func TestQueryDetailNotFound(t *testing.T) {
	s := newTestServer(&fakeStore{}, &fakeEnqueuer{})
	req := httptest.NewRequest(http.MethodGet, "/query/missing", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestReadyz(t *testing.T) {
	s := newTestServer(&fakeStore{}, &fakeEnqueuer{})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before probe should be 503, got %d", rec.Code)
	}

	s.RunReadiness(canceledCtx())
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz after successful probe should be 200, got %d", rec.Code)
	}
}

func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestSecurityHeaders(t *testing.T) {
	s := newTestServer(&fakeStore{}, &fakeEnqueuer{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	h := rec.Header()
	if !strings.Contains(h.Get("Content-Security-Policy"), "default-src 'self'") {
		t.Errorf("missing/weak CSP: %q", h.Get("Content-Security-Policy"))
	}
	if h.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("missing nosniff header")
	}
	if h.Get("X-Frame-Options") != "DENY" {
		t.Errorf("missing X-Frame-Options")
	}
}

func TestIngestRoleExcludesUI(t *testing.T) {
	s := newTestServerWith(&fakeStore{}, &fakeEnqueuer{}, Options{Ingest: true})

	if rec := postIngest(t, s, validEvent); rec.Code != http.StatusAccepted {
		t.Fatalf("ingest role should accept /ingest, got %d", rec.Code)
	}
	for _, path := range []string{"/", "/query/x", "/partials/queries", "/static/app.css", "/api/v1/queries", "/api/v1/queries/x"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("ingest role should not serve %s, got %d", path, rec.Code)
		}
	}
}

func TestUIRoleExcludesIngest(t *testing.T) {
	st := &fakeStore{list: []store.QuerySummary{{QueryID: "q-1", CreateTime: time.Now().UTC()}}}
	s := newTestServerWith(st, &fakeEnqueuer{}, Options{UI: true})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ui role should serve dashboard, got %d", rec.Code)
	}
	if rec := postIngest(t, s, validEvent); rec.Code != http.StatusMethodNotAllowed && rec.Code != http.StatusNotFound {
		t.Errorf("ui role should not accept /ingest, got %d", rec.Code)
	}
}

func TestStaticServedButNoDirListing(t *testing.T) {
	s := newTestServer(&fakeStore{}, &fakeEnqueuer{})

	req := httptest.NewRequest(http.MethodGet, "/static/app.css", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("app.css should be served, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/static/", nil)
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("directory listing should be 404, got %d", rec.Code)
	}
}

func TestAPIListQueries(t *testing.T) {
	st := &fakeStore{list: []store.QuerySummary{
		{QueryID: "q-1", CreateTime: time.Now().UTC(), UserName: "alice", QueryState: "FINISHED", CPUMS: 1000},
		{QueryID: "q-2", CreateTime: time.Now().UTC(), UserName: "bob", QueryState: "FAILED"},
	}}
	s := newTestServer(st, &fakeEnqueuer{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/queries?range=24h&sort=cpu", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content-type = %q, want application/json", ct)
	}
	var got listResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Count != 2 || len(got.Queries) != 2 {
		t.Errorf("count = %d, queries = %d, want 2/2", got.Count, len(got.Queries))
	}
	if got.HasNext {
		t.Errorf("hasNext = true, want false")
	}
	if got.Queries[0].QueryID != "q-1" {
		t.Errorf("first queryId = %q, want q-1", got.Queries[0].QueryID)
	}
	if !strings.Contains(rec.Body.String(), `"queryId"`) {
		t.Errorf("expected camelCase queryId field in body")
	}
}

func TestAPIListQueriesPaging(t *testing.T) {
	st := &fakeStore{list: []store.QuerySummary{
		{QueryID: "q-1", CreateTime: time.Now().UTC()},
		{QueryID: "q-2", CreateTime: time.Now().UTC()},
	}}
	s := newTestServer(st, &fakeEnqueuer{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/queries?limit=1", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	var got listResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Queries) != 1 || !got.HasNext {
		t.Errorf("len = %d, hasNext = %v, want 1/true", len(got.Queries), got.HasNext)
	}
	if got.Limit != 1 {
		t.Errorf("limit = %d, want 1", got.Limit)
	}
}

func TestAPIListQueriesError(t *testing.T) {
	st := &fakeStore{listErr: errors.New("boom")}
	s := newTestServer(st, &fakeEnqueuer{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/queries", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	var got apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got.Error == "" {
		t.Errorf("expected json error body, got %q (err %v)", rec.Body.String(), err)
	}
}

func TestAPIGetQuery(t *testing.T) {
	st := &fakeStore{detail: &store.Row{
		QueryID: "q-detail", QueryState: "FINISHED", UserName: "dan", QueryText: "SELECT * FROM t",
		CreateTime: time.Now().UTC(), WallMS: 5000, CPUMS: 2500,
		InputsJSON: `[{"catalogName":"c","schema":"s","table":"t","physicalInputBytes":1048576}]`,
		Plan:       "- Output[1]",
	}}
	s := newTestServer(st, &fakeEnqueuer{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/queries/q-detail", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got queryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Row == nil || got.QueryID != "q-detail" {
		t.Fatalf("queryId not decoded: %+v", got.Row)
	}
	if got.Plan != "- Output[1]" {
		t.Errorf("plan = %q", got.Plan)
	}
	if len(got.Inputs) != 1 || got.Inputs[0].CatalogName != "c" {
		t.Errorf("inputs not parsed: %+v", got.Inputs)
	}
	body := rec.Body.String()
	for _, want := range []string{`"queryId"`, `"plan"`, `"inputs"`} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q", want)
		}
	}
}

func TestAPIGetQueryNotFound(t *testing.T) {
	s := newTestServer(&fakeStore{}, &fakeEnqueuer{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/queries/missing", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content-type = %q, want application/json", ct)
	}
}

func TestAPIGetQueryError(t *testing.T) {
	st := &fakeStore{getErr: errors.New("boom")}
	s := newTestServer(st, &fakeEnqueuer{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/queries/q-x", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}
