package config

import (
	"strings"
	"testing"
	"time"
)

func loadWith(t *testing.T, vars map[string]string) (*Config, error) {
	t.Helper()
	for k, v := range vars {
		t.Setenv(k, v)
	}
	return Load()
}

func TestDefaultsSelectTrino(t *testing.T) {
	cfg, err := loadWith(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StoreBackend != BackendTrino {
		t.Errorf("default backend = %q, want %q", cfg.StoreBackend, BackendTrino)
	}
	if got := cfg.StoreQueryTimeout(); got != 60*time.Second {
		t.Errorf("StoreQueryTimeout = %s, want 60s", got)
	}
	if !strings.HasPrefix(cfg.StoreTarget(), "trino://") {
		t.Errorf("StoreTarget = %q, want trino://...", cfg.StoreTarget())
	}
}

func TestClickHouseDefaults(t *testing.T) {
	cfg, err := loadWith(t, map[string]string{
		"STORE_BACKEND":            "clickhouse",
		"CLICKHOUSE_ADDR":          "ch.example:9440",
		"CLICKHOUSE_QUERY_TIMEOUT": "45s",
	})
	if err != nil {
		t.Fatal(err)
	}
	ch := cfg.ClickHouse
	if ch.Database != "observability" || ch.Table != "trino_query_log" || ch.User != "default" {
		t.Errorf("unexpected ClickHouse defaults: %+v", ch)
	}
	if ch.DialTimeout != 5*time.Second || ch.MaxBatchBytes != 8<<20 {
		t.Errorf("unexpected ClickHouse timing/size defaults: %+v", ch)
	}
	if got := cfg.StoreQueryTimeout(); got != 45*time.Second {
		t.Errorf("StoreQueryTimeout = %s, want 45s", got)
	}
	if want := "clickhouse://ch.example:9440/observability.trino_query_log"; cfg.StoreTarget() != want {
		t.Errorf("StoreTarget = %q, want %q", cfg.StoreTarget(), want)
	}
}

func TestIPv6ClickHouseAddr(t *testing.T) {
	if _, err := loadWith(t, map[string]string{
		"STORE_BACKEND":   "clickhouse",
		"CLICKHOUSE_ADDR": "[::1]:9000",
	}); err != nil {
		t.Errorf("bracketed IPv6 address must be accepted: %v", err)
	}
}

func TestInvalidConfigs(t *testing.T) {
	cases := []struct {
		name string
		vars map[string]string
		want string
	}{
		{"unknown backend", map[string]string{"STORE_BACKEND": "postgres"}, "STORE_BACKEND"},
		{"addr missing", map[string]string{"STORE_BACKEND": "clickhouse"}, "CLICKHOUSE_ADDR"},
		{"retry backoff zero", map[string]string{"FLUSH_RETRY_BACKOFF": "0s"}, "FLUSH_RETRY_BACKOFF"},
		{"retry max below initial", map[string]string{"FLUSH_RETRY_BACKOFF": "10s", "FLUSH_RETRY_MAX_BACKOFF": "5s"}, "FLUSH_RETRY_MAX_BACKOFF"},
		{"addr without port", map[string]string{"STORE_BACKEND": "clickhouse", "CLICKHOUSE_ADDR": "ch.example"}, "CLICKHOUSE_ADDR"},
		{"addr port out of range", map[string]string{"STORE_BACKEND": "clickhouse", "CLICKHOUSE_ADDR": "ch:70000"}, "CLICKHOUSE_ADDR"},
		{"database with quote", map[string]string{"STORE_BACKEND": "clickhouse", "CLICKHOUSE_ADDR": "ch:9000", "CLICKHOUSE_DATABASE": `x"y`}, "CLICKHOUSE_DATABASE"},
		{"table with backslash", map[string]string{"STORE_BACKEND": "clickhouse", "CLICKHOUSE_ADDR": "ch:9000", "CLICKHOUSE_TABLE": `t\`}, "CLICKHOUSE_TABLE"},
		{"table with dot", map[string]string{"STORE_BACKEND": "clickhouse", "CLICKHOUSE_ADDR": "ch:9000", "CLICKHOUSE_TABLE": "db.t"}, "CLICKHOUSE_TABLE"},
		{"table starting with digit", map[string]string{"STORE_BACKEND": "clickhouse", "CLICKHOUSE_ADDR": "ch:9000", "CLICKHOUSE_TABLE": "1t"}, "CLICKHOUSE_TABLE"},
		{"zero query timeout", map[string]string{"STORE_BACKEND": "clickhouse", "CLICKHOUSE_ADDR": "ch:9000", "CLICKHOUSE_QUERY_TIMEOUT": "0s"}, "CLICKHOUSE_QUERY_TIMEOUT"},
		{"zero dial timeout", map[string]string{"STORE_BACKEND": "clickhouse", "CLICKHOUSE_ADDR": "ch:9000", "CLICKHOUSE_DIAL_TIMEOUT": "0s"}, "CLICKHOUSE_DIAL_TIMEOUT"},
		{"batch too small", map[string]string{"STORE_BACKEND": "clickhouse", "CLICKHOUSE_ADDR": "ch:9000", "CLICKHOUSE_MAX_BATCH_BYTES": "1000"}, "CLICKHOUSE_MAX_BATCH_BYTES"},
		{"insecure without tls", map[string]string{"STORE_BACKEND": "clickhouse", "CLICKHOUSE_ADDR": "ch:9000", "CLICKHOUSE_TLS_INSECURE": "true"}, "CLICKHOUSE_TLS_INSECURE"},
		{"ca without tls", map[string]string{"STORE_BACKEND": "clickhouse", "CLICKHOUSE_ADDR": "ch:9000", "CLICKHOUSE_TLS_CA": "/ca.pem"}, "CLICKHOUSE_TLS_CA"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadWith(t, tc.vars)
			if err == nil {
				t.Fatal("expected a validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %s", err, tc.want)
			}
		})
	}
}

func TestClickHouseSettingsIgnoredForTrino(t *testing.T) {
	if _, err := loadWith(t, map[string]string{"CLICKHOUSE_ADDR": "garbage"}); err != nil {
		t.Errorf("ClickHouse settings must not be validated for the trino backend: %v", err)
	}
}

func TestIcebergLocationLivesInTrinoConfig(t *testing.T) {
	cfg, err := loadWith(t, map[string]string{"ICEBERG_LOCATION": "gs://bucket/obs"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Trino.IcebergLocation != "gs://bucket/obs" {
		t.Errorf("IcebergLocation = %q", cfg.Trino.IcebergLocation)
	}
}
