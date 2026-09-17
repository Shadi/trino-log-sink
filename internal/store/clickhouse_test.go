package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Shadi/trino-log-sink/internal/config"
)

func testClickHouseConfig() config.ClickHouse {
	return config.ClickHouse{
		Addr: "localhost:9000", Database: "obs", Table: "trino_query_log", User: "default",
		DialTimeout: time.Second, QueryTimeout: 30 * time.Second, MaxBatchBytes: 32 << 20,
	}
}

func TestClickHouseDDL(t *testing.T) {
	s := newClickHouseStore(nil, testClickHouseConfig())
	script := s.DDLScript()
	for _, want := range []string{
		`CREATE DATABASE IF NOT EXISTS "obs"`,
		`CREATE TABLE IF NOT EXISTS "obs"."trino_query_log"`,
		`"query_id" String`,
		`"query_state" LowCardinality(String)`,
		`"source" String`,
		`"principal" String`,
		`"create_time" DateTime64(6, 'UTC')`,
		`"execution_start_time" Nullable(DateTime64(6, 'UTC'))`,
		`"end_time" Nullable(DateTime64(6, 'UTC'))`,
		`"wall_ms" Int64`,
		`"query_text" String CODEC(ZSTD(3))`,
		`"plan" String CODEC(ZSTD(6))`,
		`"json_plan" String CODEC(ZSTD(6))`,
		`"ingested_at" DateTime64(6, 'UTC') DEFAULT now64(6)`,
		`INDEX qid_bf "query_id" TYPE bloom_filter(0.01) GRANULARITY 1`,
		`ENGINE = ReplacingMergeTree("ingested_at")`,
		`PARTITION BY toDate("create_time")`,
		`ORDER BY ("create_time", "query_id")`,
		`SETTINGS non_replicated_deduplication_window = 1000`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("DDL missing %q\n---\n%s", want, script)
		}
	}
	if strings.Contains(script, "TTL") {
		t.Errorf("DDL must not define a TTL; retention is the prune subcommand's job\n%s", script)
	}
}

func TestClickHouseTypeMappingCoversEveryColumn(t *testing.T) {
	for _, c := range schemaColumns {
		typ := clickhouseType(c)
		switch {
		case strings.HasPrefix(c.sqlType, "timestamp"):
			if !strings.Contains(typ, "DateTime64(6, 'UTC')") {
				t.Errorf("%s: %s -> %s", c.name, c.sqlType, typ)
			}
		case c.sqlType == "bigint":
			if typ != "Int64" {
				t.Errorf("%s: %s -> %s", c.name, c.sqlType, typ)
			}
		default:
			if !strings.HasSuffix(typ, "String") && !strings.HasSuffix(typ, "String)") {
				t.Errorf("%s: %s -> %s", c.name, c.sqlType, typ)
			}
		}
		if clickhouseNullable[c.name] && !strings.HasPrefix(typ, "Nullable(") {
			t.Errorf("%s must be Nullable, got %s", c.name, typ)
		}
	}
}

func TestClickHouseStatements(t *testing.T) {
	s := newClickHouseStore(nil, testClickHouseConfig())
	if !strings.HasPrefix(s.insertStmt, `INSERT INTO "obs"."trino_query_log" ("query_id", `) {
		t.Errorf("insert statement: %s", s.insertStmt)
	}
	if strings.Contains(s.insertStmt, clickhouseVersionColumn) {
		t.Errorf("insert must leave the version column to its DEFAULT: %s", s.insertStmt)
	}
	if !strings.HasSuffix(s.summarySelect, ` FROM "obs"."trino_query_log" FINAL`) {
		t.Errorf("summary select must read FINAL: %s", s.summarySelect)
	}
	if strings.Contains(s.summarySelect, "plan") {
		t.Errorf("summary select must not read large columns: %s", s.summarySelect)
	}
}

func TestClickHouseWhereRendersServerSideParameters(t *testing.T) {
	since := time.Date(2026, 9, 15, 10, 0, 0, 123456789, time.FixedZone("x", 2*3600))
	params := clickhouse.Parameters{}
	where := clickhouseWhere(listPredicates(QueryFilter{Since: since, User: "alice"}), params)
	want := ` WHERE "create_time" >= {p0:DateTime64(6, 'UTC')} AND "user_name" = {p1:String}`
	if where != want {
		t.Errorf("where = %s\nwant %s", where, want)
	}
	if params["p0"] != "2026-09-15 08:00:00.123456" {
		t.Errorf("timestamp parameter must be UTC at microsecond precision, got %q", params["p0"])
	}
	if params["p1"] != "alice" {
		t.Errorf("string parameter = %q", params["p1"])
	}
	if clickhouseWhere(nil, params) != "" {
		t.Error("no predicates must render no WHERE clause")
	}
}

func TestClickHouseStringParamEscaping(t *testing.T) {
	params := clickhouse.Parameters{}
	clickhouseParam("p", "DOMAIN\\user\tx\ny\r\x00'q", params)
	if want := `DOMAIN\\user\tx\ny\r\0'q`; params["p"] != want {
		t.Errorf("escaped = %q, want %q", params["p"], want)
	}
}

func TestClickHouseGetQueryPredicates(t *testing.T) {
	params := clickhouse.Parameters{}
	preds, ok := queryIDPredicates("20260915_101500_00042_abcde")
	if !ok {
		t.Fatal("valid id rejected")
	}
	where := clickhouseWhere(preds, params)
	if !strings.Contains(where, `"query_id" = {p0:String}`) || params["p0"] != "20260915_101500_00042_abcde" {
		t.Errorf("id predicate: %s %v", where, params)
	}
	if params["p1"] != "2026-09-14 00:00:00.000000" || params["p2"] != "2026-09-17 00:00:00.000000" {
		t.Errorf("day window params: %v", params)
	}
}

func TestBatchTokenContentBased(t *testing.T) {
	a := Row{QueryID: "q1", WallMS: 10, CreateTime: testTime}
	b := Row{QueryID: "q2", WallMS: 20, CreateTime: testTime, ExecutionStartTime: &testTime}
	if batchToken([]Row{a, b}) != batchToken([]Row{b, a}) {
		t.Error("token must not depend on row order")
	}
	changed := a
	changed.WallMS = 11
	if batchToken([]Row{a, b}) == batchToken([]Row{changed, b}) {
		t.Error("token must change when row content changes")
	}
	nilTime := b
	nilTime.ExecutionStartTime = nil
	if batchToken([]Row{b}) == batchToken([]Row{nilTime}) {
		t.Error("token must distinguish nil and set timestamps")
	}
	if len(batchToken(nil)) != 64 {
		t.Error("token must be a hex sha256 digest")
	}
}

func TestClickHouseSplitBatchHonoursBudget(t *testing.T) {
	s := newClickHouseStore(nil, testClickHouseConfig())
	s.maxBatchBytes = 10_000
	rows := make([]Row, 5)
	for i := range rows {
		rows[i] = Row{QueryID: "q", QueryText: strings.Repeat("x", 4_000), CreateTime: testTime}
	}
	chunks := s.SplitBatch(rows)
	if len(chunks) < 3 {
		t.Fatalf("expected the batch to split into >= 3 chunks, got %d", len(chunks))
	}
	total := 0
	for _, c := range chunks {
		total += len(c)
		size := 0
		for _, r := range c {
			size += clickhouseRowBytes(r)
		}
		if len(c) > 1 && size > s.maxBatchBytes {
			t.Errorf("chunk of %d rows estimated at %d bytes exceeds budget", len(c), size)
		}
	}
	if total != len(rows) {
		t.Errorf("rows lost or duplicated across chunks: %d != %d", total, len(rows))
	}
	if got := s.SplitBatch(nil); len(got) != 0 {
		t.Errorf("empty batch must yield no chunks, got %d", len(got))
	}
}

func TestClassifyClickHouseErr(t *testing.T) {
	cases := []struct {
		code int32
		want bool
	}{
		{60, true}, {81, true}, {16, true}, {53, true}, {62, true}, {497, true}, {516, true},
		{252, false}, {241, false}, {209, false},
	}
	for _, tc := range cases {
		err := classifyClickHouseErr(&clickhouse.Exception{Code: tc.code, Name: "X", Message: "m"})
		if got := errors.Is(err, ErrNonRetryable); got != tc.want {
			t.Errorf("code %d: non-retryable=%v, want %v", tc.code, got, tc.want)
		}
	}
	plain := errors.New("dial tcp: connection refused")
	if errors.Is(classifyClickHouseErr(plain), ErrNonRetryable) {
		t.Error("transport errors must stay retryable")
	}
}

func TestPartitionHelpers(t *testing.T) {
	cutoff := time.Date(2026, 9, 14, 13, 45, 0, 0, time.FixedZone("x", 3*3600))
	if got := partitionID(cutoff); got != "20260914" {
		t.Errorf("partitionID = %s", got)
	}
	if got := dropPartitionStmt(`"obs"."t"`, "20260910"); got != `ALTER TABLE "obs"."t" DROP PARTITION ID '20260910'` {
		t.Errorf("dropPartitionStmt = %s", got)
	}
	for _, bad := range []string{"", "2026091", "2026-09-10", "20260910'", "all", "202609100"} {
		if partitionIDPattern.MatchString(bad) {
			t.Errorf("partition id %q must be rejected", bad)
		}
	}
}

func TestClickHouseNoOpMaintenance(t *testing.T) {
	s := newClickHouseStore(nil, testClickHouseConfig())
	if err := s.Optimize(t.Context(), time.Now()); err != nil {
		t.Errorf("Optimize: %v", err)
	}
	if err := s.Maintain(t.Context(), "7d"); err != nil {
		t.Errorf("Maintain: %v", err)
	}
}

func TestClickHouseOptionsTLS(t *testing.T) {
	cfg := testClickHouseConfig()
	opts, err := clickhouseOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if opts.TLS != nil || opts.Auth.Database != "" || opts.Compression == nil {
		t.Errorf("unexpected plain options: %+v", opts)
	}
	if opts.Settings["max_execution_time"] != 30 || opts.Settings["max_result_rows"] != clickhouseMaxResultRows {
		t.Errorf("settings = %v", opts.Settings)
	}
	if opts.MaxOpenConns != clickhouseMaxOpenConns || opts.MaxCompressionBuffer != clickhouseCompressBuf {
		t.Errorf("pool/compression options = %d/%d", opts.MaxOpenConns, opts.MaxCompressionBuffer)
	}

	cfg.TLS, cfg.TLSInsecure = true, true
	opts, err = clickhouseOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if opts.TLS == nil || opts.TLS.ServerName != "localhost" || !opts.TLS.InsecureSkipVerify || opts.TLS.MinVersion == 0 {
		t.Errorf("unexpected TLS config: %+v", opts.TLS)
	}

	cfg.TLSCA = "/nonexistent/ca.pem"
	if _, err := clickhouseOptions(cfg); err == nil {
		t.Error("missing CA file must fail fast")
	}
}
