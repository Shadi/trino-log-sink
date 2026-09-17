package store

import (
	"os"
	"strings"
	"testing"

	"github.com/Shadi/trino-log-sink/internal/config"
)

func shippedDDL(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "--") {
			continue
		}
		lines = append(lines, line)
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func TestShippedDDLMatchesGenerators(t *testing.T) {
	trino, err := NewTrino(config.Trino{
		Host: "localhost", Port: 8080, User: "tester", Source: "trino-query-log",
		Catalog: "gravitino", Schema: "observability", Table: "trino_query_log",
		IcebergLocation: "gs://CHANGE_ME-warehouse/observability",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trino.Close() })
	if got, want := strings.TrimSpace(trino.DDLScript()), shippedDDL(t, "../../ddl/trino_query_log.sql"); got != want {
		t.Errorf("ddl/trino_query_log.sql is stale; regenerate with `trino-query-log-sink ddl`\n got:\n%s\nwant:\n%s", got, want)
	}

	ch := newClickHouseStore(nil, config.ClickHouse{Database: "observability", Table: "trino_query_log"})
	if got, want := strings.TrimSpace(ch.DDLScript()), shippedDDL(t, "../../ddl/clickhouse_query_log.sql"); got != want {
		t.Errorf("ddl/clickhouse_query_log.sql is stale; regenerate with `STORE_BACKEND=clickhouse trino-query-log-sink ddl`\n got:\n%s\nwant:\n%s", got, want)
	}
}
