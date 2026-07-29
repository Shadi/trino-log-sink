package store

import (
	"strings"
	"testing"

	"github.com/Shadi/trino-log-sink/internal/config"
)

func newTestStore(t *testing.T) *TrinoStore {
	t.Helper()
	s, err := New(config.Trino{
		Host: "localhost", Port: 8080, User: "tester",
		Source: "trino-query-log", Catalog: "gravitino", Schema: "observability", Table: "trino_query_log",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestArgsMatchColumnCount(t *testing.T) {
	r := Row{}
	if len(r.args()) != len(schemaColumns) {
		t.Fatalf("Row.args() has %d values but schemaColumns has %d", len(r.args()), len(schemaColumns))
	}
}

func TestDDLScript(t *testing.T) {
	s := newTestStore(t)
	script := s.DDLScript("gs://my-bucket/observability")

	for _, want := range []string{
		`CREATE SCHEMA IF NOT EXISTS "gravitino"."observability"`,
		`location = 'gs://my-bucket/observability'`,
		`CREATE TABLE IF NOT EXISTS "gravitino"."observability"."trino_query_log"`,
		`"create_time" timestamp(6) with time zone`,
		`"wall_ms" bigint`,
		`partitioning = ARRAY['day(create_time)']`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("DDL missing %q\n---\n%s", want, script)
		}
	}
}

func TestSchemaDDLWithoutLocation(t *testing.T) {
	s := newTestStore(t)
	if strings.Contains(s.SchemaDDL(""), "location") {
		t.Errorf("empty location should omit WITH clause: %s", s.SchemaDDL(""))
	}
}

func TestQueryIDDay(t *testing.T) {
	d, ok := queryIDDay("20260630_143418_00512_abcde")
	if !ok || d.Format("2006-01-02") != "2026-06-30" {
		t.Errorf("queryIDDay parsed wrong: %v ok=%v", d, ok)
	}
	for _, bad := range []string{"", "short", "notadate_1234", "2026ab30_x"} {
		if _, ok := queryIDDay(bad); ok {
			t.Errorf("queryIDDay(%q) should fail", bad)
		}
	}
}

func TestQuoteIdentEscaping(t *testing.T) {
	if got := quoteIdent(`a"b`); got != `"a""b"` {
		t.Errorf("quoteIdent escaping wrong: %s", got)
	}
}
