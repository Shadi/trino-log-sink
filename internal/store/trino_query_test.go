package store

import (
	"context"
	"database/sql/driver"
	"io"
	"reflect"
	"testing"
	"time"
)

type queryRecord struct {
	query string
	args  []any
}

func (fc *fakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c := fc.c
	c.mu.Lock()
	defer c.mu.Unlock()
	rec := queryRecord{query: query}
	for _, a := range args {
		rec.args = append(rec.args, a.Value)
	}
	c.queries = append(c.queries, rec)
	return emptyRows{}, nil
}

func (c *fakeConnector) queryRecords() []queryRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]queryRecord(nil), c.queries...)
}

type emptyRows struct{}

func (emptyRows) Columns() []string                     { return nil }
func (emptyRows) Close() error                          { return nil }
func (emptyRows) Next(_ []driver.Value) error           { return io.EOF }
func (emptyRows) ColumnTypeDatabaseTypeName(int) string { return "" }

func TestTrinoListQueriesSQL(t *testing.T) {
	since := time.Date(2026, 9, 15, 10, 0, 0, 123456000, time.UTC)
	until := since.Add(time.Hour)
	cases := []struct {
		name       string
		filter     QueryFilter
		wantSuffix string
		wantArgs   []any
	}{
		{
			name:       "defaults",
			filter:     QueryFilter{},
			wantSuffix: ` ORDER BY "create_time" ASC, "query_id" ASC LIMIT 100`,
		},
		{
			name: "all filters descending with offset",
			filter: QueryFilter{
				Since: since, Until: until, User: "alice", Catalog: "hive", State: "FAILED",
				Sort: SortCPU, Desc: true, Limit: 101, Offset: 200,
			},
			wantSuffix: ` WHERE "create_time" >= ? AND "create_time" < ? AND "user_name" = ? AND "catalog" = ? AND "query_state" = ?` +
				` ORDER BY "cpu_ms" DESC, "query_id" DESC OFFSET 200 LIMIT 101`,
			wantArgs: []any{since, until, "alice", "hive", "FAILED"},
		},
		{
			name:       "unknown sort falls back to create_time",
			filter:     QueryFilter{Sort: "bogus", Desc: true},
			wantSuffix: ` ORDER BY "create_time" DESC, "query_id" DESC LIMIT 100`,
		},
		{
			name:       "oversized limit is clamped",
			filter:     QueryFilter{Sort: SortBytes, Limit: maxListLimit + 1},
			wantSuffix: ` ORDER BY "physical_input_bytes" ASC, "query_id" ASC LIMIT 100`,
		},
		{
			name:       "sort keys map to columns",
			filter:     QueryFilter{Sort: SortMemory, Desc: true, Limit: 5},
			wantSuffix: ` ORDER BY "peak_user_memory_bytes" DESC, "query_id" DESC LIMIT 5`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := &fakeConnector{errAt: -1}
			s := newFakeDBStore(t, fc)
			rows, err := s.ListQueries(context.Background(), tc.filter)
			if err != nil {
				t.Fatal(err)
			}
			if rows != nil {
				t.Errorf("empty result must be nil, got %v", rows)
			}
			recs := fc.queryRecords()
			if len(recs) != 1 {
				t.Fatalf("expected one query, got %d", len(recs))
			}
			if want := s.summarySelect + tc.wantSuffix; recs[0].query != want {
				t.Errorf("query mismatch\n got: %s\nwant: %s", recs[0].query, want)
			}
			if !reflect.DeepEqual(recs[0].args, tc.wantArgs) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", recs[0].args, tc.wantArgs)
			}
		})
	}
}

func TestTrinoGetQuerySQL(t *testing.T) {
	fc := &fakeConnector{errAt: -1}
	s := newFakeDBStore(t, fc)
	id := "20260915_101500_00042_abcde"
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)

	row, err := s.GetQuery(context.Background(), id)
	if err != nil || row != nil {
		t.Fatalf("expected no row and no error, got %v, %v", row, err)
	}
	recs := fc.queryRecords()
	if len(recs) != 1 {
		t.Fatalf("expected one query, got %d", len(recs))
	}
	want := "SELECT " + s.rowSelectList + " FROM " + s.table +
		` WHERE "query_id" = ? AND "create_time" >= ? AND "create_time" < ? LIMIT 1`
	if recs[0].query != want {
		t.Errorf("query mismatch\n got: %s\nwant: %s", recs[0].query, want)
	}
	wantArgs := []any{id, day.AddDate(0, 0, -1), day.AddDate(0, 0, 2)}
	if !reflect.DeepEqual(recs[0].args, wantArgs) {
		t.Errorf("args mismatch\n got: %#v\nwant: %#v", recs[0].args, wantArgs)
	}
}

func TestTrinoGetQueryMalformedIDSkipsQuery(t *testing.T) {
	fc := &fakeConnector{errAt: -1}
	s := newFakeDBStore(t, fc)
	row, err := s.GetQuery(context.Background(), "not-an-id")
	if err != nil || row != nil {
		t.Fatalf("expected nil, nil, got %v, %v", row, err)
	}
	if n := len(fc.queryRecords()); n != 0 {
		t.Errorf("malformed id must not reach the database, got %d queries", n)
	}
}
