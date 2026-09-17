package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Shadi/trino-log-sink/internal/config"
	"github.com/trinodb/trino-go-client/trino"
)

const optimizeFileSizeThreshold = "64MB"

type TrinoStore struct {
	db  *sql.DB
	cfg config.Trino

	table          string
	insertColumns  string
	insertPrefix   string
	rowPlaceholder string
	rowSelectList  string
	summarySelect  string
	stmtOverhead   int
	maxStmtBytes   int
}

var summaryColumns = []string{
	"query_id", "create_time", "execution_start_time", "user_name", "source",
	"catalog", "query_state", "query_type", "query_preview", "wall_ms", "cpu_ms",
	"physical_input_bytes", "peak_user_memory_bytes", "output_rows",
	"processed_input_rows", "error_code",
}

func NewTrino(cfg config.Trino) (*TrinoStore, error) {
	scheme := "http"
	if cfg.SSL {
		scheme = "https"
	}
	u := url.URL{Scheme: scheme, Host: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)}
	if cfg.Password != "" {
		u.User = url.UserPassword(cfg.User, cfg.Password)
	} else {
		u.User = url.User(cfg.User)
	}

	tc := &trino.Config{
		ServerURI:              u.String(),
		Source:                 cfg.Source,
		Catalog:                cfg.Catalog,
		Schema:                 cfg.Schema,
		DisableExplicitPrepare: true,
	}
	if cfg.AccessToken != "" {
		tc.AccessToken = cfg.AccessToken
	}
	if cfg.QueryTimeout > 0 {
		qt := cfg.QueryTimeout
		tc.QueryTimeout = &qt
	}

	dsn, err := tc.FormatDSN()
	if err != nil {
		return nil, fmt.Errorf("build trino dsn: %w", err)
	}
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, fmt.Errorf("open trino: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxIdleTime(5 * time.Minute)

	s := &TrinoStore{
		db:           db,
		cfg:          cfg,
		table:        qualifiedName(cfg.Catalog, cfg.Schema, cfg.Table),
		maxStmtBytes: cfg.MaxStatementBytes,
	}
	if s.maxStmtBytes <= 0 {
		s.maxStmtBytes = defaultMaxStatementBytes
	}
	s.buildStatements()
	return s, nil
}

func (s *TrinoStore) buildStatements() {
	byName := make(map[string]column, len(schemaColumns))
	quoted := make([]string, len(schemaColumns))
	rowParts := make([]string, len(schemaColumns))
	for i, c := range schemaColumns {
		byName[c.name] = c
		q := quoteIdent(c.name)
		quoted[i] = q
		rowParts[i] = coalesced(c, q)
	}
	s.insertColumns = strings.Join(quoted, ", ")
	s.rowSelectList = strings.Join(rowParts, ", ")
	s.rowPlaceholder = "(" + strings.TrimSuffix(strings.Repeat("?,", len(schemaColumns)), ",") + ")"
	s.insertPrefix = "INSERT INTO " + s.table + " (" + s.insertColumns + ") VALUES "
	s.stmtOverhead = executeImmediateOverhead + len(s.insertPrefix) + strings.Count(s.insertPrefix, "'")

	summary := make([]string, len(summaryColumns))
	for i, name := range summaryColumns {
		q := quoteIdent(name)
		summary[i] = coalesced(byName[name], q)
	}
	s.summarySelect = "SELECT " + strings.Join(summary, ", ") + " FROM " + s.table
}

func coalesced(c column, quoted string) string {
	switch {
	case strings.HasPrefix(c.sqlType, "timestamp"):
		return quoted
	case c.sqlType == "bigint":
		return "COALESCE(" + quoted + ", 0)"
	default:
		return "COALESCE(" + quoted + ", '')"
	}
}

func (s *TrinoStore) Validate(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "SELECT 1")
	if err != nil {
		return fmt.Errorf("trino not reachable: %w", err)
	}
	return rows.Close()
}

func (s *TrinoStore) ValidateTable(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "SELECT 1 FROM "+s.table+" WHERE false")
	if err != nil {
		return fmt.Errorf("query log table %s not reachable; apply the DDL (init subcommand or ddl/trino_query_log.sql): %w", s.table, err)
	}
	return rows.Close()
}

// The driver runs with DisableExplicitPrepare, so every statement ships as
// EXECUTE IMMEDIATE '<insert text>' USING <args serialized to SQL literals> —
// one string whose size must stay under the cluster's query.max-length. The
// per-arg bounds live in chunk.go; the constants below cover the statement
// framing.
const (
	defaultMaxStatementBytes = 700_000
	minStatementBudget       = 4096
	clampMaxIterations       = 1000

	executeImmediateOverhead = 27 // "EXECUTE IMMEDIATE '" + closing "'" + " USING "
)

func (s *TrinoStore) SplitBatch(rows []Row) [][]Row {
	if len(rows) == 0 {
		return nil
	}
	budget := max(s.maxStmtBytes-s.stmtOverhead, minStatementBudget)
	clamped := make([]Row, len(rows))
	for i, r := range rows {
		clamped[i] = s.clampRow(r, budget)
	}
	return chunkRows(clamped, budget, s.estimateRowBytes)
}

func (s *TrinoStore) InsertBatch(ctx context.Context, rows []Row) error {
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

func (s *TrinoStore) insertChunk(ctx context.Context, rows []Row) error {
	groups := make([]string, len(rows))
	args := make([]any, 0, len(rows)*len(schemaColumns))
	for i, r := range rows {
		groups[i] = s.rowPlaceholder
		args = append(args, r.args()...)
	}

	if _, err := s.db.ExecContext(ctx, s.insertPrefix+strings.Join(groups, ", "), args...); err != nil {
		return fmt.Errorf("insert %d rows: %w", len(rows), classifyInsertErr(err))
	}
	return nil
}

// classifyInsertErr wraps Trino's query-text-too-large rejection with
// ErrNonRetryable. The string fallback covers HTTP-level rejections whose
// error chain carries no *trino.ErrTrino.
func classifyInsertErr(err error) error {
	var te *trino.ErrTrino
	if (errors.As(err, &te) && te.ErrorName == "QUERY_TEXT_TOO_LARGE") ||
		strings.Contains(err.Error(), "exceeds the maximum length") {
		return fmt.Errorf("%w: %w", ErrNonRetryable, err)
	}
	return err
}

func (s *TrinoStore) estimateRowBytes(r Row) int {
	n := len(s.rowPlaceholder) + 2 // placeholder group + ", " between groups
	for _, a := range r.args() {
		n += estimateArgBytes(a)
	}
	return n
}

// clampRow guarantees a single row's estimated statement contribution fits the
// budget by repeatedly shrinking the currently-largest string field (payload
// fields win ties over identifier-ish ones via stringFields order). The loop
// only continues while the estimate strictly decreases and is iteration-capped:
// a stuck clamp would hang the flusher goroutine, which is strictly worse than
// letting an unshrinkable row fail as its own chunk.
func (s *TrinoStore) clampRow(r Row, budget int) Row {
	est := s.estimateRowBytes(r)
	for i := 0; est > budget && i < clampMaxIterations; i++ {
		var largest *string
		for _, f := range r.stringFields() {
			if largest == nil || len(*f) > len(*largest) {
				largest = f
			}
		}
		if largest == nil || *largest == "" {
			break
		}
		target := max(len(*largest)-(est-budget), len(*largest)/2)
		*largest = truncate(*largest, target)
		next := s.estimateRowBytes(r)
		if next >= est {
			break
		}
		est = next
	}
	return r
}

func (s *TrinoStore) ListQueries(ctx context.Context, f QueryFilter) ([]QuerySummary, error) {
	var sb strings.Builder
	sb.WriteString(s.summarySelect)
	where, args := trinoWhere(listPredicates(f))
	sb.WriteString(where)
	sb.WriteString(" ORDER BY ")
	sb.WriteString(orderBy(f))
	if f.Offset > 0 {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.Itoa(f.Offset))
	}
	sb.WriteString(" LIMIT ")
	sb.WriteString(strconv.Itoa(pageLimit(f)))

	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
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

func (s *TrinoStore) GetQuery(ctx context.Context, queryID string) (*Row, error) {
	preds, ok := queryIDPredicates(queryID)
	if !ok {
		return nil, nil
	}
	where, args := trinoWhere(preds)
	stmt := "SELECT " + s.rowSelectList + " FROM " + s.table + where + " LIMIT 1"
	rows, err := s.db.QueryContext(ctx, stmt, args...)
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

func trinoWhere(preds []predicate) (string, []any) {
	if len(preds) == 0 {
		return "", nil
	}
	parts := make([]string, len(preds))
	args := make([]any, len(preds))
	for i, p := range preds {
		parts[i] = quoteIdent(p.column) + " " + p.op + " ?"
		args[i] = p.value
	}
	return " WHERE " + strings.Join(parts, " AND "), args
}

func (s *TrinoStore) Prune(ctx context.Context, olderThan time.Time) error {
	stmt := "DELETE FROM " + s.table + " WHERE " + quoteIdent("create_time") + " < ?"
	if _, err := s.db.ExecContext(ctx, stmt, olderThan); err != nil {
		return fmt.Errorf("prune rows older than %s: %w", olderThan.Format(time.RFC3339), err)
	}
	return nil
}

func (s *TrinoStore) Optimize(ctx context.Context, since time.Time) error {
	lit := since.UTC().Format("2006-01-02 15:04:05.000")
	stmt := "ALTER TABLE " + s.table + " EXECUTE optimize(file_size_threshold => '" + optimizeFileSizeThreshold + "')" +
		" WHERE " + quoteIdent("create_time") + " >= TIMESTAMP '" + lit + " UTC'"
	if _, err := s.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("optimize since %s: %w", lit, err)
	}
	return nil
}

func (s *TrinoStore) Maintain(ctx context.Context, retentionThreshold string) error {
	thr := "'" + strings.ReplaceAll(retentionThreshold, "'", "''") + "'"
	steps := []struct {
		name string
		stmt string
	}{
		{"expire_snapshots", "ALTER TABLE " + s.table + " EXECUTE expire_snapshots(retention_threshold => " + thr + ")"},
		{"remove_orphan_files", "ALTER TABLE " + s.table + " EXECUTE remove_orphan_files(retention_threshold => " + thr + ")"},
	}
	var errs []error
	for _, step := range steps {
		if _, err := s.db.ExecContext(ctx, step.stmt); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", step.name, err))
		}
	}
	return errors.Join(errs...)
}

func (s *TrinoStore) Init(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, s.SchemaDDL()); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, s.TableDDL()); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

func (s *TrinoStore) SchemaDDL() string {
	stmt := "CREATE SCHEMA IF NOT EXISTS " + quoteIdent(s.cfg.Catalog) + "." + quoteIdent(s.cfg.Schema)
	if s.cfg.IcebergLocation != "" {
		stmt += " WITH (location = '" + strings.ReplaceAll(s.cfg.IcebergLocation, "'", "''") + "')"
	}
	return stmt
}

func (s *TrinoStore) TableDDL() string {
	cols := make([]string, len(schemaColumns))
	for i, c := range schemaColumns {
		cols[i] = "  " + quoteIdent(c.name) + " " + c.sqlType
	}
	props := []string{
		"partitioning = ARRAY['day(create_time)']",
		"sorted_by = ARRAY['create_time']",
		"extra_properties = MAP(ARRAY['write.metadata.delete-after-commit.enabled'], ARRAY['true'])",
	}
	return "CREATE TABLE IF NOT EXISTS " + s.table + " (\n" +
		strings.Join(cols, ",\n") +
		"\n) WITH (\n  " + strings.Join(props, ",\n  ") + "\n)"
}

func (s *TrinoStore) DDLScript() string {
	return s.SchemaDDL() + ";\n\n" + s.TableDDL() + ";\n"
}

func (s *TrinoStore) Close() error {
	return s.db.Close()
}
