package store

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/trinodb/trino-go-client/trino"
)

var testTime = time.Date(2026, 7, 29, 13, 33, 7, 123456789, time.FixedZone("", 5*3600+1800))

func TestEstimateArgBytesUpperBoundsSerial(t *testing.T) {
	for _, a := range []any{
		"",
		"plain text",
		"it's got 'many' quotes '''",
		int64(-9223372036854775808),
		testTime,
		nil,
	} {
		ser, err := trino.Serial(a)
		if err != nil {
			t.Fatalf("Serial(%v): %v", a, err)
		}
		// +2 covers the ", " separator between USING args.
		if est := estimateArgBytes(a); est < len(ser)+2 {
			t.Errorf("estimate %d < serialized %d+2 for %#v (%q)", est, len(ser), a, ser)
		}
	}
}

func TestEstimateRowBytesUpperBound(t *testing.T) {
	s := newTestStore(t)
	r := Row{
		QueryID:            "20260729_133307_00001_abcde",
		QueryText:          "select 'a''b' from t",
		Plan:               strings.Repeat("- Output[1]\n", 50),
		CreateTime:         testTime,
		ExecutionStartTime: &testTime,
	}

	actual := len(s.rowPlaceholder) + 2
	for _, a := range r.args() {
		ser, err := trino.Serial(a)
		if err != nil {
			t.Fatalf("Serial: %v", err)
		}
		actual += len(ser) + 2
	}
	if est := s.estimateRowBytes(r); est < actual {
		t.Errorf("estimate %d < actual serialized size %d", est, actual)
	}
}

func TestChunkRowsSplitsAtBudget(t *testing.T) {
	byText := func(r Row) int { return len(r.QueryText) }
	rows := make([]Row, 5)
	for i := range rows {
		rows[i] = Row{QueryID: fmt.Sprintf("q-%d", i), QueryText: "aaaa"}
	}

	chunks := chunkRows(rows, 10, byText)

	if len(chunks) != 3 || len(chunks[0]) != 2 || len(chunks[1]) != 2 || len(chunks[2]) != 1 {
		t.Fatalf("chunk sizes wrong: %v", chunkLens(chunks))
	}
	i := 0
	for _, c := range chunks {
		size := 0
		for _, r := range c {
			if want := fmt.Sprintf("q-%d", i); r.QueryID != want {
				t.Errorf("order broken: got %s, want %s", r.QueryID, want)
			}
			size += byText(r)
			i++
		}
		if size > 10 {
			t.Errorf("chunk over budget: %d", size)
		}
	}
	if i != len(rows) {
		t.Errorf("row count changed: %d != %d", i, len(rows))
	}
}

func TestChunkRowsOversizeRowAlone(t *testing.T) {
	byText := func(r Row) int { return len(r.QueryText) }
	rows := []Row{
		{QueryID: "small-1", QueryText: "aa"},
		{QueryID: "huge", QueryText: strings.Repeat("x", 50)},
		{QueryID: "small-2", QueryText: "aaa"},
	}

	chunks := chunkRows(rows, 10, byText)

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %v", chunkLens(chunks))
	}
	if len(chunks[1]) != 1 || chunks[1][0].QueryID != "huge" {
		t.Errorf("oversize row should be a singleton chunk: %v", chunkLens(chunks))
	}
	for _, c := range chunks {
		if len(c) == 0 {
			t.Error("empty chunk emitted")
		}
	}
}

func chunkLens(chunks [][]Row) []int {
	out := make([]int, len(chunks))
	for i, c := range chunks {
		out[i] = len(c)
	}
	return out
}

func TestClampRowFitsBudget(t *testing.T) {
	s := newTestStore(t)
	r := Row{
		QueryID:    "q-clamp",
		QueryText:  strings.Repeat("q", 400_000),
		Plan:       strings.Repeat("p", 400_000),
		JSONPlan:   strings.Repeat("j", 400_000),
		CreateTime: testTime,
	}
	const budget = 660_000

	got := s.clampRow(r, budget)

	if est := s.estimateRowBytes(got); est > budget {
		t.Fatalf("estimate %d > budget %d after clamp", est, budget)
	}
	if got.QueryID != "q-clamp" {
		t.Errorf("query_id changed: %q", got.QueryID)
	}
	truncated := 0
	for _, f := range []string{got.QueryText, got.Plan, got.JSONPlan} {
		if strings.Contains(f, "[truncated ") {
			truncated++
		}
	}
	if truncated == 0 {
		t.Error("expected at least one field to carry a truncation marker")
	}
}

func TestClampRowTerminatesOnNonPayloadField(t *testing.T) {
	s := newTestStore(t)
	r := Row{
		QueryID:    "q-tags",
		ClientTags: strings.Repeat("t", 1<<20), // 1MB in a field TruncateFields treats as small
	}

	got := s.clampRow(r, 50_000)

	if est := s.estimateRowBytes(got); est > 50_000 {
		t.Fatalf("estimate %d > budget after clamp", est)
	}
	if got.QueryID != "q-tags" {
		t.Errorf("query_id changed: %q", got.QueryID)
	}
}

func TestClampRowPrefersPlanOverInputs(t *testing.T) {
	s := newTestStore(t)
	r := Row{
		QueryID:    "q-tie",
		Plan:       strings.Repeat("p", 300_000),
		InputsJSON: strings.Repeat("i", 300_000),
		CreateTime: testTime,
	}
	budget := s.estimateRowBytes(r) - 50_000

	got := s.clampRow(r, budget)

	if len(got.InputsJSON) != 300_000 {
		t.Errorf("inputs_json should be untouched when the plan can absorb the deficit: %d bytes", len(got.InputsJSON))
	}
	if !strings.Contains(got.Plan, "[truncated ") {
		t.Error("plan should have been the truncation victim")
	}
}

func TestClampRowTerminatesOnImpossibleBudget(t *testing.T) {
	s := newTestStore(t)
	overhead := s.estimateRowBytes(Row{})
	r := Row{QueryID: "q", Plan: strings.Repeat("p", 10_000), CreateTime: testTime}

	// Budget below the fixed per-row overhead can never be met; the clamp must
	// still terminate (the row then fails as its own chunk) rather than loop.
	// Every string field ends up empty, leaving exactly the fixed overhead.
	got := s.clampRow(r, overhead-100)
	if est := s.estimateRowBytes(got); est != overhead {
		t.Errorf("estimate %d after exhaustive clamp, want fixed overhead %d", est, overhead)
	}

	// Just above the overhead is satisfiable: everything shrinkable shrinks.
	got = s.clampRow(r, overhead+50)
	if est := s.estimateRowBytes(got); est > overhead+50 {
		t.Errorf("estimate %d > budget %d", est, overhead+50)
	}
}

func TestClassifyInsertErr(t *testing.T) {
	trinoErr := &trino.ErrQueryFailed{
		StatusCode: 200,
		Reason: &trino.ErrTrino{
			ErrorName: "QUERY_TEXT_TOO_LARGE",
			ErrorType: "USER_ERROR",
			Message:   "Query text length (3151383) exceeds the maximum length (1000000)",
		},
	}
	// Wrapped exactly as insertChunk wraps before the buffer sees it.
	wrapped := fmt.Errorf("insert 323 rows: %w", classifyInsertErr(trinoErr))
	if !errors.Is(wrapped, ErrNonRetryable) {
		t.Error("ErrTrino QUERY_TEXT_TOO_LARGE should be non-retryable")
	}

	plain := fmt.Errorf("insert 1 rows: %w",
		classifyInsertErr(errors.New("Query text length (2000000) exceeds the maximum length (1000000)")))
	if !errors.Is(plain, ErrNonRetryable) {
		t.Error("length-exceeded message without ErrTrino should be non-retryable via fallback")
	}

	// Message deliberately avoids the fallback substring so this pins the
	// errors.As + ErrorName branch on its own.
	nameOnly := classifyInsertErr(&trino.ErrQueryFailed{
		Reason: &trino.ErrTrino{ErrorName: "QUERY_TEXT_TOO_LARGE", ErrorType: "USER_ERROR", Message: "the query is too big"},
	})
	if !errors.Is(nameOnly, ErrNonRetryable) {
		t.Error("ErrorName match alone should be non-retryable")
	}

	if errors.Is(classifyInsertErr(errors.New("connection refused")), ErrNonRetryable) {
		t.Error("unrelated errors must stay retryable")
	}
}
