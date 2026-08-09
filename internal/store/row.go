package store

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/Shadi/trino-log-sink/internal/config"
	"github.com/Shadi/trino-log-sink/internal/event"
)

func RowFromEvent(e *event.Event) Row {
	r := Row{
		QueryID:              e.Metadata.QueryID,
		QueryState:           e.Metadata.QueryState,
		QueryType:            deref(e.Context.QueryType),
		UserName:             e.Context.User,
		Source:               e.SourceValue(),
		Principal:            deref(e.Context.Principal),
		ClientTags:           strings.Join(e.Context.ClientTags, ","),
		Catalog:              deref(e.Context.Catalog),
		SchemaName:           deref(e.Context.Schema),
		QueryText:            e.Metadata.Query,
		UpdateType:           deref(e.Metadata.UpdateType),
		QueuedMS:             e.Statistics.QueuedTime.Millis(),
		AnalysisMS:           e.Statistics.AnalysisTime.Millis(),
		PlanningMS:           e.Statistics.PlanningTime.Millis(),
		ExecutionMS:          e.Statistics.ExecutionTime.Millis(),
		WallMS:               e.Statistics.WallTime.Millis(),
		CPUMS:                e.Statistics.CPUTime.Millis(),
		PeakUserMemoryBytes:  e.Statistics.PeakUserMemoryBytes,
		PeakTotalMemoryBytes: e.Statistics.PeakTaskTotalMemory,
		PhysicalInputBytes:   e.Statistics.PhysicalInputBytes,
		PhysicalInputRows:    e.Statistics.PhysicalInputRows,
		ProcessedInputBytes:  e.Statistics.ProcessedInputBytes,
		ProcessedInputRows:   e.Statistics.ProcessedInputRows,
		OutputBytes:          e.Statistics.OutputBytes,
		OutputRows:           e.Statistics.OutputRows,
		WrittenBytes:         e.Statistics.WrittenBytes,
		WrittenRows:          e.Statistics.WrittenRows,
		CompletedSplits:      e.Statistics.CompletedSplits,
		Plan:                 deref(e.Metadata.Plan),
		JSONPlan:             deref(e.Metadata.JSONPlan),
		ResourceGroup:        strings.Join(e.Context.ResourceGroupID, "."),
		ServerVersion:        e.Context.ServerVersion,
		Environment:          e.Context.Environment,
	}

	if e.CreateTime != nil {
		r.CreateTime = *e.CreateTime
	}
	r.ExecutionStartTime = e.ExecutionStartTime
	r.EndTime = e.EndTime

	if f := e.FailureInfo; f != nil {
		r.ErrorCode = f.ErrorCode.Name
		r.ErrorType = f.ErrorCode.Type
		if r.ErrorType == "" {
			r.ErrorType = deref(f.FailureType)
		}
		r.ErrorMessage = deref(f.FailureMessage)
	}

	if len(e.IOMetadata.Inputs) > 0 {
		if b, err := json.Marshal(e.IOMetadata.Inputs); err == nil {
			r.InputsJSON = string(b)
		}
	}

	return r
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

type PlanPolicy struct {
	Capture string
	MinWall time.Duration
}

func (r Row) keepsPlans(p PlanPolicy) bool {
	switch p.Capture {
	case config.PlanCaptureNone:
		return false
	case config.PlanCaptureSlowOrFailed:
		return r.ErrorCode != "" || r.WallMS >= p.MinWall.Milliseconds()
	default:
		return true
	}
}

func (r Row) ApplyPlanPolicy(p PlanPolicy) Row {
	if !r.keepsPlans(p) {
		r.Plan = ""
		r.JSONPlan = ""
	}
	return r
}

func (r Row) WithPreview(maxBytes int) Row {
	preview := strings.Join(strings.Fields(r.QueryText), " ")
	if maxBytes > 0 && len(preview) > maxBytes {
		preview = preview[:runeSafeCut(preview, maxBytes)]
	}
	r.QueryPreview = preview
	return r
}
