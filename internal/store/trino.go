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

	"github.com/Shadi/trino-query-log-sink/internal/config"
	"github.com/trinodb/trino-go-client/trino"
)

const (
	defaultListLimit = 100
	maxListLimit     = 5000
)

type TrinoStore struct {
	db  *sql.DB
	cfg config.Trino

	table          string
	insertColumns  string
	rowPlaceholder string
	rowSelectList  string
	summarySelect  string
}

var summaryColumns = []string{
	"query_id", "create_time", "execution_start_time", "user_name", "source",
	"catalog", "query_state", "query_type", "query_text", "wall_ms", "cpu_ms",
	"physical_input_bytes", "peak_user_memory_bytes", "output_rows",
	"processed_input_rows", "error_code",
}

func New(cfg config.Trino) (*TrinoStore, error) {
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
		db:    db,
		cfg:   cfg,
		table: qualifiedName(cfg.Catalog, cfg.Schema, cfg.Table),
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

	summary := make([]string, len(summaryColumns))
	for i, name := range summaryColumns {
		q := quoteIdent(name)
		if name == "query_text" {
			summary[i] = "substr(COALESCE(" + q + ", ''), 1, 200)"
			continue
		}
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
	rows, err := s.db.QueryContext(ctx, "SELECT 1 FROM "+s.table+" WHERE false")
	if err != nil {
		return fmt.Errorf("query log table %s not reachable; apply the DDL (init subcommand or ddl/trino_query_log.sql): %w", s.table, err)
	}
	return rows.Close()
}

func (s *TrinoStore) InsertBatch(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	colCount := len(schemaColumns)
	groups := make([]string, len(rows))
	args := make([]any, 0, len(rows)*colCount)
	for i, r := range rows {
		groups[i] = s.rowPlaceholder
		args = append(args, r.args()...)
	}

	stmt := "INSERT INTO " + s.table + " (" + s.insertColumns + ") VALUES " + strings.Join(groups, ", ")
	if _, err := s.db.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("insert %d rows: %w", len(rows), err)
	}
	return nil
}

func (s *TrinoStore) ListQueries(ctx context.Context, f QueryFilter) ([]QuerySummary, error) {
	var where []string
	var args []any
	if !f.Since.IsZero() {
		where = append(where, quoteIdent("create_time")+" >= ?")
		args = append(args, f.Since)
	}
	if !f.Until.IsZero() {
		where = append(where, quoteIdent("create_time")+" < ?")
		args = append(args, f.Until)
	}
	if f.User != "" {
		where = append(where, quoteIdent("user_name")+" = ?")
		args = append(args, f.User)
	}
	if f.Catalog != "" {
		where = append(where, quoteIdent("catalog")+" = ?")
		args = append(args, f.Catalog)
	}
	if f.State != "" {
		where = append(where, quoteIdent("query_state")+" = ?")
		args = append(args, f.State)
	}

	sortCol, ok := sortColumns[f.Sort]
	if !ok {
		sortCol = "create_time"
	}
	dir := "ASC"
	if f.Desc {
		dir = "DESC"
	}
	limit := f.Limit
	if limit <= 0 || limit > maxListLimit {
		limit = defaultListLimit
	}

	var sb strings.Builder
	sb.WriteString(s.summarySelect)
	if len(where) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(where, " AND "))
	}
	sb.WriteString(" ORDER BY ")
	sb.WriteString(quoteIdent(sortCol))
	sb.WriteString(" ")
	sb.WriteString(dir)
	if sortCol != "query_id" {
		sb.WriteString(", ")
		sb.WriteString(quoteIdent("query_id"))
		sb.WriteString(" ")
		sb.WriteString(dir)
	}
	if f.Offset > 0 {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.Itoa(f.Offset))
	}
	sb.WriteString(" LIMIT ")
	sb.WriteString(strconv.Itoa(limit))

	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list queries: %w", err)
	}
	defer rows.Close()

	var out []QuerySummary
	for rows.Next() {
		var q QuerySummary
		if err := rows.Scan(
			&q.QueryID, &q.CreateTime, &q.ExecutionStartTime, &q.UserName, &q.Source,
			&q.Catalog, &q.QueryState, &q.QueryType, &q.QueryPreview, &q.WallMS, &q.CPUMS,
			&q.PhysicalInputBytes, &q.PeakUserMemoryBytes, &q.OutputRows, &q.ProcessedInputRows, &q.ErrorCode,
		); err != nil {
			return nil, fmt.Errorf("scan summary: %w", err)
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

func (s *TrinoStore) GetQuery(ctx context.Context, queryID string) (*Row, error) {
	where := quoteIdent("query_id") + " = ?"
	args := []any{queryID}
	if day, ok := queryIDDay(queryID); ok {
		where += " AND " + quoteIdent("create_time") + " >= ? AND " + quoteIdent("create_time") + " < ?"
		args = append(args, day.AddDate(0, 0, -1), day.AddDate(0, 0, 2))
	}

	stmt := "SELECT " + s.rowSelectList + " FROM " + s.table + " WHERE " + where + " LIMIT 1"
	rows, err := s.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("get query %s: %w", queryID, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, rows.Err()
	}
	var r Row
	if err := rows.Scan(
		&r.QueryID, &r.QueryState, &r.QueryType, &r.UserName, &r.Source, &r.Principal, &r.ClientTags,
		&r.Catalog, &r.SchemaName, &r.QueryText, &r.UpdateType, &r.CreateTime, &r.ExecutionStartTime, &r.EndTime,
		&r.QueuedMS, &r.AnalysisMS, &r.PlanningMS, &r.ExecutionMS, &r.WallMS, &r.CPUMS,
		&r.PeakUserMemoryBytes, &r.PeakTotalMemoryBytes, &r.PhysicalInputBytes, &r.PhysicalInputRows,
		&r.ProcessedInputBytes, &r.ProcessedInputRows, &r.OutputBytes, &r.OutputRows, &r.WrittenBytes, &r.WrittenRows,
		&r.CompletedSplits, &r.ErrorCode, &r.ErrorType, &r.ErrorMessage, &r.Plan, &r.JSONPlan, &r.InputsJSON,
		&r.ResourceGroup, &r.ServerVersion, &r.Environment,
	); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}
	return &r, nil
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
	stmt := "ALTER TABLE " + s.table + " EXECUTE optimize WHERE " + quoteIdent("create_time") + " >= TIMESTAMP '" + lit + " UTC'"
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

func (s *TrinoStore) Init(ctx context.Context, location string) error {
	if _, err := s.db.ExecContext(ctx, s.SchemaDDL(location)); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, s.TableDDL()); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

func (s *TrinoStore) SchemaDDL(location string) string {
	stmt := "CREATE SCHEMA IF NOT EXISTS " + quoteIdent(s.cfg.Catalog) + "." + quoteIdent(s.cfg.Schema)
	if location != "" {
		stmt += " WITH (location = '" + strings.ReplaceAll(location, "'", "''") + "')"
	}
	return stmt
}

func (s *TrinoStore) TableDDL() string {
	cols := make([]string, len(schemaColumns))
	for i, c := range schemaColumns {
		cols[i] = "  " + quoteIdent(c.name) + " " + c.sqlType
	}
	return "CREATE TABLE IF NOT EXISTS " + s.table + " (\n" +
		strings.Join(cols, ",\n") +
		"\n) WITH (\n  partitioning = ARRAY['day(create_time)']\n)"
}

func (s *TrinoStore) DDLScript(location string) string {
	return s.SchemaDDL(location) + ";\n\n" + s.TableDDL() + ";\n"
}

func (s *TrinoStore) Close() error {
	return s.db.Close()
}
