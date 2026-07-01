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
	QueryID              string     `json:"queryId"`
	QueryState           string     `json:"queryState"`
	QueryType            string     `json:"queryType"`
	UserName             string     `json:"userName"`
	Source               string     `json:"source"`
	Principal            string     `json:"principal"`
	ClientTags           string     `json:"clientTags"`
	Catalog              string     `json:"catalog"`
	SchemaName           string     `json:"schemaName"`
	QueryText            string     `json:"queryText"`
	UpdateType           string     `json:"updateType"`
	CreateTime           time.Time  `json:"createTime"`
	ExecutionStartTime   *time.Time `json:"executionStartTime"`
	EndTime              *time.Time `json:"endTime"`
	QueuedMS             int64      `json:"queuedMs"`
	AnalysisMS           int64      `json:"analysisMs"`
	PlanningMS           int64      `json:"planningMs"`
	ExecutionMS          int64      `json:"executionMs"`
	WallMS               int64      `json:"wallMs"`
	CPUMS                int64      `json:"cpuMs"`
	PeakUserMemoryBytes  int64      `json:"peakUserMemoryBytes"`
	PeakTotalMemoryBytes int64      `json:"peakTotalMemoryBytes"`
	PhysicalInputBytes   int64      `json:"physicalInputBytes"`
	PhysicalInputRows    int64      `json:"physicalInputRows"`
	ProcessedInputBytes  int64      `json:"processedInputBytes"`
	ProcessedInputRows   int64      `json:"processedInputRows"`
	OutputBytes          int64      `json:"outputBytes"`
	OutputRows           int64      `json:"outputRows"`
	WrittenBytes         int64      `json:"writtenBytes"`
	WrittenRows          int64      `json:"writtenRows"`
	CompletedSplits      int64      `json:"completedSplits"`
	ErrorCode            string     `json:"errorCode"`
	ErrorType            string     `json:"errorType"`
	ErrorMessage         string     `json:"errorMessage"`
	Plan                 string     `json:"plan"`
	JSONPlan             string     `json:"jsonPlan"`
	InputsJSON           string     `json:"inputsJson"`
	ResourceGroup        string     `json:"resourceGroup"`
	ServerVersion        string     `json:"serverVersion"`
	Environment          string     `json:"environment"`
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
	QueryID             string     `json:"queryId"`
	CreateTime          time.Time  `json:"createTime"`
	ExecutionStartTime  *time.Time `json:"executionStartTime"`
	UserName            string     `json:"userName"`
	Source              string     `json:"source"`
	Catalog             string     `json:"catalog"`
	QueryState          string     `json:"queryState"`
	QueryType           string     `json:"queryType"`
	QueryPreview        string     `json:"queryPreview"`
	WallMS              int64      `json:"wallMs"`
	CPUMS               int64      `json:"cpuMs"`
	PhysicalInputBytes  int64      `json:"physicalInputBytes"`
	PeakUserMemoryBytes int64      `json:"peakUserMemoryBytes"`
	OutputRows          int64      `json:"outputRows"`
	ProcessedInputRows  int64      `json:"processedInputRows"`
	ErrorCode           string     `json:"errorCode"`
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
