// Package store defines the query-log persistence contract and a Trino-backed
// implementation. All callers depend on the Store interface, not the concrete
// type, so the buffer and HTTP handlers are trivially testable with a fake.
package store

import (
	"context"
	"time"
)

// Store persists and reads back query-log rows. Writes and reads both go
// through Trino.
type Store interface {
	// Validate confirms the configured catalog/schema/table are reachable.
	Validate(ctx context.Context) error
	// InsertBatch writes rows in a single multi-row INSERT.
	InsertBatch(ctx context.Context, rows []Row) error
	// ListQueries returns lightweight summaries matching the filter.
	ListQueries(ctx context.Context, f QueryFilter) ([]QuerySummary, error)
	// GetQuery returns the full row for a query id, or nil if not found.
	GetQuery(ctx context.Context, queryID string) (*Row, error)
	// Prune deletes rows whose create_time is strictly before olderThan.
	Prune(ctx context.Context, olderThan time.Time) error
	// Maintain reclaims space via Iceberg expire_snapshots + remove_orphan_files.
	Maintain(ctx context.Context, retentionThreshold string) error
	// Optimize compacts data files whose create_time is at or after since.
	Optimize(ctx context.Context, since time.Time) error
	Close() error
}

type column struct {
	name    string
	sqlType string
}

var schemaColumns = []column{
	{"query_id", "varchar"},
	{"query_state", "varchar"},
	{"query_type", "varchar"},
	{"user_name", "varchar"},
	{"source", "varchar"},
	{"principal", "varchar"},
	{"client_tags", "varchar"},
	{"catalog", "varchar"},
	{"schema_name", "varchar"},
	{"query_text", "varchar"},
	{"update_type", "varchar"},
	{"create_time", "timestamp(6) with time zone"},
	{"execution_start_time", "timestamp(6) with time zone"},
	{"end_time", "timestamp(6) with time zone"},
	{"queued_ms", "bigint"},
	{"analysis_ms", "bigint"},
	{"planning_ms", "bigint"},
	{"execution_ms", "bigint"},
	{"wall_ms", "bigint"},
	{"cpu_ms", "bigint"},
	{"peak_user_memory_bytes", "bigint"},
	{"peak_total_memory_bytes", "bigint"},
	{"physical_input_bytes", "bigint"},
	{"physical_input_rows", "bigint"},
	{"processed_input_bytes", "bigint"},
	{"processed_input_rows", "bigint"},
	{"output_bytes", "bigint"},
	{"output_rows", "bigint"},
	{"written_bytes", "bigint"},
	{"written_rows", "bigint"},
	{"completed_splits", "bigint"},
	{"error_code", "varchar"},
	{"error_type", "varchar"},
	{"error_message", "varchar"},
	{"plan", "varchar"},
	{"json_plan", "varchar"},
	{"inputs_json", "varchar"},
	{"resource_group", "varchar"},
	{"server_version", "varchar"},
	{"environment", "varchar"},
}

type Row struct {
	QueryID              string
	QueryState           string
	QueryType            string
	UserName             string
	Source               string
	Principal            string
	ClientTags           string
	Catalog              string
	SchemaName           string
	QueryText            string
	UpdateType           string
	CreateTime           time.Time
	ExecutionStartTime   *time.Time
	EndTime              *time.Time
	QueuedMS             int64
	AnalysisMS           int64
	PlanningMS           int64
	ExecutionMS          int64
	WallMS               int64
	CPUMS                int64
	PeakUserMemoryBytes  int64
	PeakTotalMemoryBytes int64
	PhysicalInputBytes   int64
	PhysicalInputRows    int64
	ProcessedInputBytes  int64
	ProcessedInputRows   int64
	OutputBytes          int64
	OutputRows           int64
	WrittenBytes         int64
	WrittenRows          int64
	CompletedSplits      int64
	ErrorCode            string
	ErrorType            string
	ErrorMessage         string
	Plan                 string
	JSONPlan             string
	InputsJSON           string
	ResourceGroup        string
	ServerVersion        string
	Environment          string
}

func (r Row) args() []any {
	return []any{
		r.QueryID, r.QueryState, r.QueryType, r.UserName, r.Source, r.Principal, r.ClientTags,
		r.Catalog, r.SchemaName, r.QueryText, r.UpdateType, r.CreateTime, timeArg(r.ExecutionStartTime), timeArg(r.EndTime),
		r.QueuedMS, r.AnalysisMS, r.PlanningMS, r.ExecutionMS, r.WallMS, r.CPUMS,
		r.PeakUserMemoryBytes, r.PeakTotalMemoryBytes, r.PhysicalInputBytes, r.PhysicalInputRows,
		r.ProcessedInputBytes, r.ProcessedInputRows, r.OutputBytes, r.OutputRows, r.WrittenBytes, r.WrittenRows,
		r.CompletedSplits, r.ErrorCode, r.ErrorType, r.ErrorMessage, r.Plan, r.JSONPlan, r.InputsJSON,
		r.ResourceGroup, r.ServerVersion, r.Environment,
	}
}

type QuerySummary struct {
	QueryID             string
	CreateTime          time.Time
	ExecutionStartTime  *time.Time
	UserName            string
	Source              string
	Catalog             string
	QueryState          string
	QueryType           string
	QueryPreview        string
	WallMS              int64
	CPUMS               int64
	PhysicalInputBytes  int64
	PeakUserMemoryBytes int64
	OutputRows          int64
	ProcessedInputRows  int64
	ErrorCode           string
}

type SortKey string

const (
	SortStart  SortKey = "start"
	SortWall   SortKey = "wall"
	SortCPU    SortKey = "cpu"
	SortBytes  SortKey = "bytes"
	SortMemory SortKey = "mem"
	SortRows   SortKey = "rows"
)

var sortColumns = map[SortKey]string{
	SortStart:  "create_time",
	SortWall:   "wall_ms",
	SortCPU:    "cpu_ms",
	SortBytes:  "physical_input_bytes",
	SortMemory: "peak_user_memory_bytes",
	SortRows:   "output_rows",
}

type QueryFilter struct {
	Since   time.Time
	Until   time.Time
	User    string
	Catalog string
	State   string
	Sort    SortKey
	Desc    bool
	Limit   int
	Offset  int
}

func timeArg(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}
