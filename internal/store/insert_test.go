package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
)

type execRecord struct {
	query    string
	args     int
	argBytes int // total bytes of string args
	ids      []string
	failed   bool
}

// fakeConnector satisfies driver.Connector and hands out conns that record
// every ExecContext call. errAt is a 0-based exec index that returns err
// instead of succeeding; -1 disables failures.
type fakeConnector struct {
	mu    sync.Mutex
	execs []execRecord
	errAt int
	err   error
}

func (c *fakeConnector) Connect(context.Context) (driver.Conn, error) { return &fakeConn{c: c}, nil }
func (c *fakeConnector) Driver() driver.Driver                        { return nil }

func (c *fakeConnector) records() []execRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]execRecord(nil), c.execs...)
}

func (c *fakeConnector) stopFailing() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err, c.errAt = nil, -1
}

func (c *fakeConnector) committedIDs() []string {
	var out []string
	for _, e := range c.records() {
		if !e.failed {
			out = append(out, e.ids...)
		}
	}
	return out
}

type fakeConn struct{ c *fakeConnector }

func (fc *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("prepare unused") }
func (fc *fakeConn) Close() error                        { return nil }
func (fc *fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("tx unused") }

func (fc *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c := fc.c
	c.mu.Lock()
	defer c.mu.Unlock()
	rec := execRecord{query: query, args: len(args)}
	for i, a := range args {
		if s, ok := a.Value.(string); ok {
			rec.argBytes += len(s)
			if i%len(schemaColumns) == 0 {
				rec.ids = append(rec.ids, s)
			}
		}
	}
	rec.failed = c.err != nil && len(c.execs) == c.errAt
	c.execs = append(c.execs, rec)
	if rec.failed {
		return nil, c.err
	}
	return driver.RowsAffected(0), nil
}

// newFakeDBStore returns a store whose *sql.DB records ExecContext calls
// instead of talking to Trino.
func newFakeDBStore(t *testing.T, fc *fakeConnector) *TrinoStore {
	t.Helper()
	s := newTestStore(t)
	_ = s.db.Close()
	s.db = sql.OpenDB(fc)
	return s
}

func TestInsertBatchSplitsOversizedBatch(t *testing.T) {
	fc := &fakeConnector{errAt: -1}
	s := newFakeDBStore(t, fc)
	s.maxStmtBytes = 40_000

	rows := make([]Row, 6)
	for i := range rows {
		rows[i] = Row{QueryID: fmt.Sprintf("q-%d", i), Plan: strings.Repeat("p", 12_000), CreateTime: testTime}
	}
	if err := s.InsertBatch(context.Background(), rows); err != nil {
		t.Fatal(err)
	}

	execs := fc.records()
	if len(execs) < 2 {
		t.Fatalf("oversized batch not split: %d ExecContext calls", len(execs))
	}
	total := 0
	for _, e := range execs {
		total += e.args
		if !strings.HasPrefix(e.query, s.insertPrefix) {
			t.Errorf("statement missing insert prefix: %.60q", e.query)
		}
	}
	if want := 6 * len(schemaColumns); total != want {
		t.Errorf("total args %d, want %d — rows lost or duplicated across chunks", total, want)
	}
}

func TestInsertBatchSmallBatchSingleStatement(t *testing.T) {
	fc := &fakeConnector{errAt: -1}
	s := newFakeDBStore(t, fc)

	rows := []Row{
		{QueryID: "q-1", QueryText: "SELECT 1", CreateTime: testTime},
		{QueryID: "q-2", QueryText: "SELECT 2", CreateTime: testTime},
	}
	if err := s.InsertBatch(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	if execs := fc.records(); len(execs) != 1 || execs[0].args != 2*len(schemaColumns) {
		t.Errorf("small batch should stay one statement: %+v", fc.records())
	}
}

func TestInsertBatchFailFastNonRetryable(t *testing.T) {
	fc := &fakeConnector{
		errAt: 0,
		err:   errors.New("USER_ERROR: Query text length (2000000) exceeds the maximum length (1000000)"),
	}
	s := newFakeDBStore(t, fc)
	s.maxStmtBytes = 40_000

	rows := make([]Row, 6)
	for i := range rows {
		rows[i] = Row{QueryID: fmt.Sprintf("q-%d", i), Plan: strings.Repeat("p", 12_000), CreateTime: testTime}
	}
	err := s.InsertBatch(context.Background(), rows)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, ErrNonRetryable) {
		t.Errorf("length-exceeded error should surface as non-retryable: %v", err)
	}
	if n := len(fc.records()); n != 1 {
		t.Errorf("fail-fast violated: %d execs after first chunk failed", n)
	}
}

func multiChunkBatch(t *testing.T, s *TrinoStore) []Row {
	t.Helper()
	rows := make([]Row, 6)
	for i := range rows {
		rows[i] = Row{QueryID: fmt.Sprintf("q-%d", i), Plan: strings.Repeat("p", 12_000), CreateTime: testTime}
	}
	if chunks := s.SplitBatch(rows); len(chunks) < 2 {
		t.Fatalf("test needs a batch that splits, got %d chunk(s)", len(chunks))
	}
	return rows
}

func TestInsertBatchReportsCommittedPrefix(t *testing.T) {
	failure := errors.New("trino unavailable")
	fc := &fakeConnector{errAt: 1, err: failure}
	s := newFakeDBStore(t, fc)
	s.maxStmtBytes = 40_000
	rows := multiChunkBatch(t, s)
	firstChunk := len(s.SplitBatch(rows)[0])

	err := s.InsertBatch(context.Background(), rows)

	if err == nil {
		t.Fatal("expected an error")
	}
	var pce *PartialCommitError
	if !errors.As(err, &pce) {
		t.Fatalf("mid-batch failure must report its committed prefix, got %T: %v", err, err)
	}
	if pce.Committed != firstChunk || CommittedRows(err) != firstChunk {
		t.Errorf("committed = %d (CommittedRows %d), want %d", pce.Committed, CommittedRows(err), firstChunk)
	}
	if !errors.Is(err, failure) {
		t.Errorf("underlying failure must stay in the chain: %v", err)
	}
	if n := len(fc.records()); n != 2 {
		t.Errorf("insert should stop at the first failed chunk: %d execs", n)
	}
}

func TestInsertBatchOfSplitChunkStaysOneStatement(t *testing.T) {
	fc := &fakeConnector{errAt: -1}
	s := newFakeDBStore(t, fc)
	s.maxStmtBytes = 40_000
	chunks := s.SplitBatch(multiChunkBatch(t, s))

	for _, chunk := range chunks {
		if err := s.InsertBatch(context.Background(), chunk); err != nil {
			t.Fatal(err)
		}
	}

	if got := len(fc.records()); got != len(chunks) {
		t.Errorf("%d statements for %d chunks: a chunk must map to one commit for callers to resume on chunk boundaries", got, len(chunks))
	}
}

func TestInsertBatchRetryOfRemainderDoesNotDuplicate(t *testing.T) {
	fc := &fakeConnector{errAt: 1, err: errors.New("trino unavailable")}
	s := newFakeDBStore(t, fc)
	s.maxStmtBytes = 40_000
	rows := multiChunkBatch(t, s)

	committed := CommittedRows(s.InsertBatch(context.Background(), rows))
	if committed == 0 || committed >= len(rows) {
		t.Fatalf("committed = %d, want a strict prefix of %d rows", committed, len(rows))
	}

	fc.stopFailing()
	if err := s.InsertBatch(context.Background(), rows[committed:]); err != nil {
		t.Fatalf("retry of the uncommitted remainder: %v", err)
	}

	want := make([]string, len(rows))
	for i, r := range rows {
		want[i] = r.QueryID
	}
	if got := fc.committedIDs(); !slices.Equal(got, want) {
		t.Errorf("committed rows = %v, want %v exactly once each", got, want)
	}
}

func TestInsertBatchClampsOversizedRow(t *testing.T) {
	fc := &fakeConnector{errAt: -1}
	s := newFakeDBStore(t, fc)
	s.maxStmtBytes = 100_000

	rows := []Row{{QueryID: "q-big", JSONPlan: strings.Repeat("j", 2<<20), CreateTime: testTime}}
	if err := s.InsertBatch(context.Background(), rows); err != nil {
		t.Fatal(err)
	}

	execs := fc.records()
	if len(execs) != 1 {
		t.Fatalf("single row should be one statement, got %d", len(execs))
	}
	if execs[0].argBytes > s.maxStmtBytes {
		t.Errorf("row not clamped before exec: %d bytes of string args > %d budget", execs[0].argBytes, s.maxStmtBytes)
	}
}
