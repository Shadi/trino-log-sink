package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type execRecord struct {
	query    string
	args     int
	argBytes int // total bytes of string args
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

type fakeConn struct{ c *fakeConnector }

func (fc *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("prepare unused") }
func (fc *fakeConn) Close() error                        { return nil }
func (fc *fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("tx unused") }

func (fc *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c := fc.c
	c.mu.Lock()
	defer c.mu.Unlock()
	rec := execRecord{query: query, args: len(args)}
	for _, a := range args {
		if s, ok := a.Value.(string); ok {
			rec.argBytes += len(s)
		}
	}
	idx := len(c.execs)
	c.execs = append(c.execs, rec)
	if c.err != nil && idx == c.errAt {
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
