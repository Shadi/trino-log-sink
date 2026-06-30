// Package config loads and validates service configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Trino struct {
	Host         string
	Port         int
	User         string
	Source       string
	Catalog      string
	Schema       string
	Table        string
	Password     string
	AccessToken  string
	SSL          bool
	QueryTimeout time.Duration
}

type Config struct {
	ListenAddr string

	Trino Trino

	BatchSize       int
	FlushInterval   time.Duration
	BufferCapacity  int
	FlushMaxRetries int

	RetentionDays int

	MaintainRetention string
	OptimizeDays      int

	MetricsEnabled bool
	LogLevel       string

	IcebergLocation string
}

func Load() (*Config, error) {
	e := &env{}

	cfg := &Config{
		ListenAddr: e.str("LISTEN_ADDR", ":8080"),
		Trino: Trino{
			Host:         e.str("TRINO_HOST", "trino-cluster-trino.trino"),
			Port:         e.intVal("TRINO_PORT", 8080),
			User:         e.str("TRINO_USER", "trino-query-log"),
			Source:       e.str("TRINO_SOURCE", "trino-query-log"),
			Catalog:      e.str("TRINO_CATALOG", "gravitino"),
			Schema:       e.str("TRINO_SCHEMA", "observability"),
			Table:        e.str("TRINO_TABLE", "trino_query_log"),
			Password:     e.str("TRINO_PASSWORD", ""),
			AccessToken:  e.str("TRINO_ACCESS_TOKEN", ""),
			SSL:          e.boolVal("TRINO_SSL", false),
			QueryTimeout: e.duration("TRINO_QUERY_TIMEOUT", 60*time.Second),
		},
		BatchSize:         e.intVal("BATCH_SIZE", 100),
		FlushInterval:     e.duration("FLUSH_INTERVAL", 5*time.Second),
		BufferCapacity:    e.intVal("BUFFER_CAPACITY", 10000),
		FlushMaxRetries:   e.intVal("FLUSH_MAX_RETRIES", 3),
		RetentionDays:     e.intVal("RETENTION_DAYS", 7),
		MaintainRetention: e.str("MAINTAIN_RETENTION", "7d"),
		OptimizeDays:      e.intVal("OPTIMIZE_DAYS", 1),
		MetricsEnabled:    e.boolVal("METRICS_ENABLED", true),
		LogLevel:          e.str("LOG_LEVEL", "info"),
		IcebergLocation:   e.str("ICEBERG_LOCATION", ""),
	}

	if err := errors.Join(append(e.errs, cfg.validate()...)...); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() []error {
	var errs []error
	add := func(cond bool, msg string) {
		if cond {
			errs = append(errs, errors.New(msg))
		}
	}

	add(c.Trino.Host == "", "TRINO_HOST must not be empty")
	add(c.Trino.Port < 1 || c.Trino.Port > 65535, "TRINO_PORT must be in 1..65535")
	add(c.Trino.User == "", "TRINO_USER must not be empty")
	add(c.Trino.Source == "", "TRINO_SOURCE must not be empty")
	add(c.Trino.Catalog == "", "TRINO_CATALOG must not be empty")
	add(c.Trino.Schema == "", "TRINO_SCHEMA must not be empty")
	add(c.Trino.Table == "", "TRINO_TABLE must not be empty")
	add(c.BatchSize < 1, "BATCH_SIZE must be >= 1")
	add(c.FlushInterval <= 0, "FLUSH_INTERVAL must be > 0")
	add(c.BufferCapacity < 1, "BUFFER_CAPACITY must be >= 1")
	add(c.FlushMaxRetries < 0, "FLUSH_MAX_RETRIES must be >= 0")
	add(c.RetentionDays < 0, "RETENTION_DAYS must be >= 0")
	add(c.OptimizeDays < 0, "OPTIMIZE_DAYS must be >= 0")

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		add(true, "LOG_LEVEL must be one of debug, info, warn, error")
	}
	return errs
}

type env struct {
	errs []error
}

func (e *env) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (e *env) intVal(key string, def int) int {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: invalid integer %q", key, raw))
		return def
	}
	return v
}

func (e *env) boolVal(key string, def bool) bool {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	v, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: invalid boolean %q", key, raw))
		return def
	}
	return v
}

func (e *env) duration(key string, def time.Duration) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	v, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: invalid duration %q", key, raw))
		return def
	}
	return v
}
