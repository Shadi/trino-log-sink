package store

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Shadi/trino-log-sink/internal/config"
)

type chConn interface {
	Exec(ctx context.Context, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
	Close() error
}

type ClickHouseStore struct {
	conn          chConn
	cfg           config.ClickHouse
	table         string
	insertStmt    string
	summarySelect string
	rowSelect     string
	maxBatchBytes int
}

const (
	clickhouseVersionColumn = "ingested_at"
	clickhouseTimestampType = "DateTime64(6, 'UTC')"
	clickhouseParamTime     = "2006-01-02 15:04:05.000000"
	clickhouseDedupWindow   = 1000
	clickhouseMaxOpenConns  = 4
	clickhouseMaxIdleConns  = 2
	clickhouseCompressBuf   = 4 << 20
	clickhouseMaxResultRows = 10_000
	clickhouseDefaultBatch  = 8 << 20
	clickhouseClientName    = "trino-query-log-sink"
)

var (
	clickhouseNullable = map[string]bool{"execution_start_time": true, "end_time": true}

	clickhouseLowCardinality = map[string]bool{
		"query_state": true, "query_type": true, "update_type": true, "error_code": true,
		"error_type": true, "catalog": true, "schema_name": true, "resource_group": true,
		"server_version": true, "environment": true, "user_name": true,
	}

	clickhouseCodecs = map[string]string{
		"query_text": "ZSTD(3)", "error_message": "ZSTD(3)", "inputs_json": "ZSTD(3)",
		"plan": "ZSTD(6)", "json_plan": "ZSTD(6)",
	}

	clickhouseNonRetryableCodes = map[int32]bool{
		16: true, 53: true, 60: true, 62: true, 81: true, 497: true, 516: true,
	}

	partitionIDPattern = regexp.MustCompile(`^\d{8}$`)
)

func NewClickHouse(cfg config.ClickHouse) (*ClickHouseStore, error) {
	opts, err := clickhouseOptions(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	return newClickHouseStore(conn, cfg), nil
}

func newClickHouseStore(conn chConn, cfg config.ClickHouse) *ClickHouseStore {
	s := &ClickHouseStore{
		conn:          conn,
		cfg:           cfg,
		table:         quoteIdent(cfg.Database) + "." + quoteIdent(cfg.Table),
		maxBatchBytes: cfg.MaxBatchBytes,
	}
	if s.maxBatchBytes <= 0 {
		s.maxBatchBytes = clickhouseDefaultBatch
	}
	cols := make([]string, len(schemaColumns))
	for i, c := range schemaColumns {
		cols[i] = quoteIdent(c.name)
	}
	summary := make([]string, len(summaryColumns))
	for i, name := range summaryColumns {
		summary[i] = quoteIdent(name)
	}
	s.insertStmt = "INSERT INTO " + s.table + " (" + strings.Join(cols, ", ") + ")"
	s.summarySelect = "SELECT " + strings.Join(summary, ", ") + " FROM " + s.table + " FINAL"
	s.rowSelect = "SELECT " + strings.Join(cols, ", ") + " FROM " + s.table + " FINAL"
	return s
}

func clickhouseOptions(cfg config.ClickHouse) (*clickhouse.Options, error) {
	opts := &clickhouse.Options{
		Addr:                 []string{cfg.Addr},
		Auth:                 clickhouse.Auth{Username: cfg.User, Password: cfg.Password},
		DialTimeout:          cfg.DialTimeout,
		ReadTimeout:          cfg.QueryTimeout,
		MaxOpenConns:         clickhouseMaxOpenConns,
		MaxIdleConns:         clickhouseMaxIdleConns,
		MaxCompressionBuffer: clickhouseCompressBuf,
		Compression:          &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		ClientInfo:           clickhouse.ClientInfo{Products: []struct{ Name, Version string }{{Name: clickhouseClientName}}},
		Settings: clickhouse.Settings{
			"max_execution_time": max(int(cfg.QueryTimeout.Seconds()), 1),
			"max_result_rows":    clickhouseMaxResultRows,
		},
	}
	if cfg.TLS {
		tlsConfig, err := clickhouseTLS(cfg)
		if err != nil {
			return nil, err
		}
		opts.TLS = tlsConfig
	}
	return opts, nil
}

func clickhouseTLS(cfg config.ClickHouse) (*tls.Config, error) {
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("clickhouse addr: %w", err)
	}
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         host,
		InsecureSkipVerify: cfg.TLSInsecure,
	}
	if cfg.TLSCA != "" {
		pem, err := os.ReadFile(cfg.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("read CLICKHOUSE_TLS_CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("CLICKHOUSE_TLS_CA: no certificates found")
		}
		tlsConfig.RootCAs = pool
	}
	return tlsConfig, nil
}

func clickhouseType(c column) string {
	var t string
	switch {
	case strings.HasPrefix(c.sqlType, "timestamp"):
		t = clickhouseTimestampType
	case c.sqlType == "bigint":
		t = "Int64"
	default:
		t = "String"
	}
	switch {
	case clickhouseNullable[c.name]:
		return "Nullable(" + t + ")"
	case clickhouseLowCardinality[c.name]:
		return "LowCardinality(" + t + ")"
	default:
		return t
	}
}

func clickhouseColumnDDL(c column) string {
	ddl := quoteIdent(c.name) + " " + clickhouseType(c)
	if codec, ok := clickhouseCodecs[c.name]; ok {
		ddl += " CODEC(" + codec + ")"
	}
	return ddl
}

func (s *ClickHouseStore) DatabaseDDL() string {
	return "CREATE DATABASE IF NOT EXISTS " + quoteIdent(s.cfg.Database)
}

func (s *ClickHouseStore) TableDDL() string {
	lines := make([]string, 0, len(schemaColumns)+2)
	for _, c := range schemaColumns {
		lines = append(lines, "  "+clickhouseColumnDDL(c))
	}
	lines = append(lines,
		"  "+quoteIdent(clickhouseVersionColumn)+" "+clickhouseTimestampType+" DEFAULT now64(6)",
		"  INDEX qid_bf "+quoteIdent("query_id")+" TYPE bloom_filter(0.01) GRANULARITY 1",
	)
	return "CREATE TABLE IF NOT EXISTS " + s.table + " (\n" + strings.Join(lines, ",\n") + "\n)" +
		"\nENGINE = ReplacingMergeTree(" + quoteIdent(clickhouseVersionColumn) + ")" +
		"\nPARTITION BY toDate(" + quoteIdent("create_time") + ")" +
		"\nORDER BY (" + quoteIdent("create_time") + ", " + quoteIdent("query_id") + ")" +
		"\nSETTINGS non_replicated_deduplication_window = " + strconv.Itoa(clickhouseDedupWindow)
}

func (s *ClickHouseStore) DDLScript() string {
	return s.DatabaseDDL() + ";\n\n" + s.TableDDL() + ";\n"
}

func (s *ClickHouseStore) Init(ctx context.Context) error {
	if err := s.conn.Exec(ctx, s.DatabaseDDL()); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	if err := s.conn.Exec(ctx, s.TableDDL()); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

func (s *ClickHouseStore) Validate(ctx context.Context) error {
	if err := s.discard(ctx, "SELECT 1", nil); err != nil {
		return fmt.Errorf("clickhouse not reachable: %w", err)
	}
	return nil
}

func (s *ClickHouseStore) ValidateTable(ctx context.Context) error {
	if err := s.discard(ctx, "SELECT 1 FROM "+s.table+" WHERE 0", nil); err != nil {
		return fmt.Errorf("query log table %s not reachable; apply the DDL (init subcommand or ddl/clickhouse_query_log.sql): %w", s.table, err)
	}
	return nil
}

func (s *ClickHouseStore) discard(ctx context.Context, query string, params clickhouse.Parameters) error {
	rows, err := s.conn.Query(s.queryContext(ctx, params), query)
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	return rows.Close()
}

func clickhouseRowBytes(r Row) int {
	n := 0
	for _, a := range r.args() {
		n += estimateArgBytes(a)
	}
	return n
}

func (s *ClickHouseStore) SplitBatch(rows []Row) [][]Row {
	return chunkRows(rows, s.maxBatchBytes, clickhouseRowBytes)
}

func (s *ClickHouseStore) InsertBatch(ctx context.Context, rows []Row) error {
	committed := 0
	for _, chunk := range s.SplitBatch(rows) {
		if err := s.insertChunk(ctx, chunk); err != nil {
			if committed > 0 {
				return &PartialCommitError{Committed: committed, Err: err}
			}
			return err
		}
		committed += len(chunk)
	}
	return nil
}

func (s *ClickHouseStore) insertChunk(ctx context.Context, rows []Row) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_deduplication_token": batchToken(rows),
	}))
	batch, err := s.conn.PrepareBatch(ctx, s.insertStmt)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", classifyClickHouseErr(err))
	}
	for _, r := range rows {
		if err := batch.Append(r.args()...); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append row %s: %w", r.QueryID, classifyClickHouseErr(err))
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert %d rows: %w", len(rows), classifyClickHouseErr(err))
	}
	return nil
}

func batchToken(rows []Row) string {
	order := make([]int, len(rows))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return rows[order[a]].QueryID < rows[order[b]].QueryID })
	h := sha256.New()
	for _, i := range order {
		for _, a := range rows[i].args() {
			hashArg(h, a)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func hashArg(h hash.Hash, a any) {
	switch v := a.(type) {
	case string:
		fmt.Fprintf(h, "s%d:%s;", len(v), v)
	case int64:
		fmt.Fprintf(h, "i%d;", v)
	case time.Time:
		fmt.Fprintf(h, "t%d;", v.UnixNano())
	case nil:
		_, _ = io.WriteString(h, "n;")
	default:
		fmt.Fprintf(h, "v%v;", v)
	}
}

func classifyClickHouseErr(err error) error {
	var ex *clickhouse.Exception
	if errors.As(err, &ex) && clickhouseNonRetryableCodes[ex.Code] {
		return fmt.Errorf("%w: %w", ErrNonRetryable, err)
	}
	return err
}

func (s *ClickHouseStore) ListQueries(ctx context.Context, f QueryFilter) ([]QuerySummary, error) {
	params := clickhouse.Parameters{}
	where := clickhouseWhere(listPredicates(f), params)
	stmt := s.summarySelect + where + " ORDER BY " + orderBy(f) + " LIMIT " + strconv.Itoa(pageLimit(f))
	if f.Offset > 0 {
		stmt += " OFFSET " + strconv.Itoa(f.Offset)
	}

	rows, err := s.conn.Query(s.queryContext(ctx, params), stmt)
	if err != nil {
		return nil, fmt.Errorf("list queries: %w", err)
	}
	defer rows.Close()

	var out []QuerySummary
	for rows.Next() {
		var q QuerySummary
		if err := rows.Scan(q.scanDests()...); err != nil {
			return nil, fmt.Errorf("scan summary: %w", err)
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

func (s *ClickHouseStore) GetQuery(ctx context.Context, queryID string) (*Row, error) {
	preds, ok := queryIDPredicates(queryID)
	if !ok {
		return nil, nil
	}
	params := clickhouse.Parameters{}
	stmt := s.rowSelect + clickhouseWhere(preds, params) + " LIMIT 1"

	rows, err := s.conn.Query(s.queryContext(ctx, params), stmt)
	if err != nil {
		return nil, fmt.Errorf("get query %s: %w", queryID, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, rows.Err()
	}
	var r Row
	if err := rows.Scan(r.scanDests()...); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}
	return &r, nil
}

func clickhouseWhere(preds []predicate, params clickhouse.Parameters) string {
	if len(preds) == 0 {
		return ""
	}
	parts := make([]string, len(preds))
	for i, p := range preds {
		parts[i] = quoteIdent(p.column) + " " + p.op + " " + clickhouseParam("p"+strconv.Itoa(i), p.value, params)
	}
	return " WHERE " + strings.Join(parts, " AND ")
}

func clickhouseParam(name string, v any, params clickhouse.Parameters) string {
	switch x := v.(type) {
	case time.Time:
		params[name] = x.UTC().Format(clickhouseParamTime)
		return "{" + name + ":" + clickhouseTimestampType + "}"
	case int64:
		params[name] = strconv.FormatInt(x, 10)
		return "{" + name + ":Int64}"
	case int:
		params[name] = strconv.Itoa(x)
		return "{" + name + ":Int64}"
	case string:
		params[name] = clickhouseParamValue(x)
		return "{" + name + ":String}"
	default:
		params[name] = clickhouseParamValue(fmt.Sprint(x))
		return "{" + name + ":String}"
	}
}

var clickhouseTSVEscaper = strings.NewReplacer(
	`\`, `\\`, "\n", `\n`, "\t", `\t`, "\r", `\r`, "\x00", `\0`, "\b", `\b`, "\f", `\f`,
)

// clickhouseParamValue encodes a String query-parameter value in the
// TSV-escaped format ClickHouse uses for query parameters; the driver adds the
// quoted field-dump layer of the native protocol itself (clickhouse-go >= 2.48).
func clickhouseParamValue(s string) string {
	return clickhouseTSVEscaper.Replace(s)
}

func (s *ClickHouseStore) queryContext(ctx context.Context, params clickhouse.Parameters) context.Context {
	opts := []clickhouse.QueryOption{
		clickhouse.WithSettings(clickhouse.Settings{"do_not_merge_across_partitions_select_final": 1}),
	}
	if len(params) > 0 {
		opts = append(opts, clickhouse.WithParameters(params))
	}
	return clickhouse.Context(ctx, opts...)
}

func (s *ClickHouseStore) Prune(ctx context.Context, olderThan time.Time) error {
	ids, err := s.partitionsBefore(ctx, olderThan)
	if err != nil {
		return fmt.Errorf("prune rows older than %s: %w", olderThan.Format(time.RFC3339), err)
	}
	for _, id := range ids {
		if err := s.conn.Exec(ctx, dropPartitionStmt(s.table, id)); err != nil {
			return fmt.Errorf("drop partition %s: %w", id, err)
		}
	}
	return nil
}

func (s *ClickHouseStore) partitionsBefore(ctx context.Context, cutoff time.Time) ([]string, error) {
	params := clickhouse.Parameters{}
	where := clickhouseWhere([]predicate{
		{"database", "=", s.cfg.Database},
		{"table", "=", s.cfg.Table},
		{"partition_id", "<", partitionID(cutoff)},
	}, params)
	rows, err := s.conn.Query(clickhouse.Context(ctx, clickhouse.WithParameters(params)),
		"SELECT DISTINCT partition_id FROM system.parts"+where+" AND active ORDER BY partition_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if !partitionIDPattern.MatchString(id) {
			return nil, fmt.Errorf("unexpected partition id %q", id)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func partitionID(day time.Time) string {
	return day.UTC().Format("20060102")
}

func dropPartitionStmt(table, id string) string {
	return "ALTER TABLE " + table + " DROP PARTITION ID '" + id + "'"
}

func (s *ClickHouseStore) Optimize(context.Context, time.Time) error { return nil }

func (s *ClickHouseStore) Maintain(context.Context, string) error { return nil }

func (s *ClickHouseStore) Close() error {
	return s.conn.Close()
}
