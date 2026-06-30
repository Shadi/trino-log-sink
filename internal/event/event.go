// Package event models Trino's QueryCompletedEvent as delivered by the built-in
// HTTP event listener and decodes it tolerantly. Optional fields are pointers
// because Jackson omits them when empty; unknown fields are ignored by the JSON
// decoder so the model survives Trino version drift.
package event

import (
	"errors"
	"fmt"
	"time"
)

type Event struct {
	Metadata    Metadata     `json:"metadata"`
	Statistics  Statistics   `json:"statistics"`
	Context     Context      `json:"context"`
	IOMetadata  IOMetadata   `json:"ioMetadata"`
	FailureInfo *FailureInfo `json:"failureInfo"`

	CreateTime         *time.Time `json:"createTime"`
	ExecutionStartTime *time.Time `json:"executionStartTime"`
	EndTime            *time.Time `json:"endTime"`
}

type Metadata struct {
	QueryID       string  `json:"queryId"`
	TransactionID *string `json:"transactionId"`
	Query         string  `json:"query"`
	UpdateType    *string `json:"updateType"`
	PreparedQuery *string `json:"preparedQuery"`
	QueryState    string  `json:"queryState"`
	URI           string  `json:"uri"`
	Plan          *string `json:"plan"`
	JSONPlan      *string `json:"jsonPlan"`
}

type Statistics struct {
	CPUTime       Duration `json:"cpuTime"`
	WallTime      Duration `json:"wallTime"`
	QueuedTime    Duration `json:"queuedTime"`
	AnalysisTime  Duration `json:"analysisTime"`
	PlanningTime  Duration `json:"planningTime"`
	ExecutionTime Duration `json:"executionTime"`

	PeakUserMemoryBytes int64 `json:"peakUserMemoryBytes"`
	PeakTaskTotalMemory int64 `json:"peakTaskTotalMemory"`
	PhysicalInputBytes  int64 `json:"physicalInputBytes"`
	PhysicalInputRows   int64 `json:"physicalInputRows"`
	ProcessedInputBytes int64 `json:"processedInputBytes"`
	ProcessedInputRows  int64 `json:"processedInputRows"`
	OutputBytes         int64 `json:"outputBytes"`
	OutputRows          int64 `json:"outputRows"`
	WrittenBytes        int64 `json:"writtenBytes"`
	WrittenRows         int64 `json:"writtenRows"`
	CompletedSplits     int64 `json:"completedSplits"`
}

type Context struct {
	User            string   `json:"user"`
	Principal       *string  `json:"principal"`
	Source          *string  `json:"source"`
	Catalog         *string  `json:"catalog"`
	Schema          *string  `json:"schema"`
	ClientTags      []string `json:"clientTags"`
	ResourceGroupID []string `json:"resourceGroupId"`
	QueryType       *string  `json:"queryType"`
	ServerVersion   string   `json:"serverVersion"`
	Environment     string   `json:"environment"`
}

type IOMetadata struct {
	Inputs []InputMetadata `json:"inputs"`
}

type InputMetadata struct {
	CatalogName        string        `json:"catalogName"`
	Schema             string        `json:"schema"`
	Table              string        `json:"table"`
	Columns            []InputColumn `json:"columns"`
	PhysicalInputBytes *int64        `json:"physicalInputBytes"`
	PhysicalInputRows  *int64        `json:"physicalInputRows"`
}

type InputColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type FailureInfo struct {
	ErrorCode      ErrorCode `json:"errorCode"`
	FailureType    *string   `json:"failureType"`
	FailureMessage *string   `json:"failureMessage"`
}

type ErrorCode struct {
	Code int    `json:"code"`
	Name string `json:"name"`
	Type string `json:"type"`
}

var ErrInvalidEvent = errors.New("invalid query completed event")

func (e *Event) Validate() error {
	if e.Metadata.QueryID == "" {
		return fmt.Errorf("missing metadata.queryId: %w", ErrInvalidEvent)
	}
	if e.CreateTime == nil {
		return fmt.Errorf("missing createTime: %w", ErrInvalidEvent)
	}
	return nil
}

func (e *Event) SourceValue() string {
	if e.Context.Source == nil {
		return ""
	}
	return *e.Context.Source
}
