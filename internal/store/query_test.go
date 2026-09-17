package store

import (
	"reflect"
	"testing"
	"time"
)

func TestListPredicates(t *testing.T) {
	since := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)
	got := listPredicates(QueryFilter{Since: since, Until: until, User: "u", Catalog: "c", State: "FAILED"})
	want := []predicate{
		{"create_time", ">=", since},
		{"create_time", "<", until},
		{"user_name", "=", "u"},
		{"catalog", "=", "c"},
		{"query_state", "=", "FAILED"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("predicates = %#v\nwant %#v", got, want)
	}
	if got := listPredicates(QueryFilter{}); got != nil {
		t.Errorf("empty filter must yield no predicates, got %#v", got)
	}
}

func TestOrderByAndLimit(t *testing.T) {
	cases := []struct {
		f    QueryFilter
		want string
	}{
		{QueryFilter{}, `"create_time" ASC, "query_id" ASC`},
		{QueryFilter{Sort: SortWall, Desc: true}, `"wall_ms" DESC, "query_id" DESC`},
		{QueryFilter{Sort: SortRows}, `"output_rows" ASC, "query_id" ASC`},
		{QueryFilter{Sort: "nonsense", Desc: true}, `"create_time" DESC, "query_id" DESC`},
	}
	for _, tc := range cases {
		if got := orderBy(tc.f); got != tc.want {
			t.Errorf("orderBy(%+v) = %s, want %s", tc.f, got, tc.want)
		}
	}
	for limit, want := range map[int]int{0: 100, -5: 100, 1: 1, 5000: 5000, 5001: 100} {
		if got := pageLimit(QueryFilter{Limit: limit}); got != want {
			t.Errorf("pageLimit(%d) = %d, want %d", limit, got, want)
		}
	}
}

func TestQueryIDWindow(t *testing.T) {
	from, to, ok := queryIDWindow("20260915_101500_00042_abcde")
	if !ok {
		t.Fatal("valid id rejected")
	}
	if from != time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC) || to != time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC) {
		t.Errorf("window = [%s, %s)", from, to)
	}
	if _, _, ok := queryIDWindow("bogus"); ok {
		t.Error("malformed id accepted")
	}
}

func TestSummaryScanDestsMatchColumns(t *testing.T) {
	var q QuerySummary
	byColumn := map[string]any{
		"query_id": &q.QueryID, "create_time": &q.CreateTime, "execution_start_time": &q.ExecutionStartTime,
		"user_name": &q.UserName, "source": &q.Source, "catalog": &q.Catalog, "query_state": &q.QueryState,
		"query_type": &q.QueryType, "query_preview": &q.QueryPreview, "wall_ms": &q.WallMS, "cpu_ms": &q.CPUMS,
		"physical_input_bytes": &q.PhysicalInputBytes, "peak_user_memory_bytes": &q.PeakUserMemoryBytes,
		"output_rows": &q.OutputRows, "processed_input_rows": &q.ProcessedInputRows, "error_code": &q.ErrorCode,
	}
	dests := q.scanDests()
	if len(dests) != len(summaryColumns) || len(byColumn) != len(summaryColumns) {
		t.Fatalf("QuerySummary.scanDests() has %d values, byColumn %d, summaryColumns %d", len(dests), len(byColumn), len(summaryColumns))
	}
	for i, name := range summaryColumns {
		if dests[i] != byColumn[name] {
			t.Errorf("position %d: scanDests points at the wrong field for column %s", i, name)
		}
	}
}
