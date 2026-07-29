package store

import (
	"strings"
	"testing"
	"time"

	"github.com/Shadi/trino-log-sink/internal/event"
)

func ptr[T any](v T) *T { return &v }

func TestRowFromEventSuccess(t *testing.T) {
	ct := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	est := ct.Add(50 * time.Millisecond)
	et := ct.Add(1200 * time.Millisecond)

	e := &event.Event{
		Metadata: event.Metadata{
			QueryID:    "20260630_120000_00001_abcde",
			Query:      "SELECT 1",
			QueryState: "FINISHED",
			UpdateType: ptr("INSERT"),
			Plan:       ptr("- Output"),
			JSONPlan:   ptr(`{"id":"0"}`),
		},
		Statistics: event.Statistics{
			CPUTime:             durationOf(t, "PT0.5S"),
			WallTime:            durationOf(t, "PT1.2S"),
			QueuedTime:          durationOf(t, "PT0.05S"),
			PeakUserMemoryBytes: 5242880,
			PeakTaskTotalMemory: 8388608,
			PhysicalInputBytes:  1024000,
			PhysicalInputRows:   5000,
			OutputRows:          10,
			CompletedSplits:     7,
		},
		Context: event.Context{
			User:            "alice",
			Source:          ptr("jdbc"),
			Catalog:         ptr("gravitino"),
			Schema:          ptr("observability"),
			ClientTags:      []string{"team:data", "env:prod"},
			ResourceGroupID: []string{"global", "adhoc"},
			QueryType:       ptr("SELECT"),
			ServerVersion:   "478",
			Environment:     "production",
		},
		IOMetadata: event.IOMetadata{
			Inputs: []event.InputMetadata{
				{CatalogName: "gravitino", Schema: "obs", Table: "t", PhysicalInputBytes: ptr(int64(1024000))},
			},
		},
		CreateTime:         &ct,
		ExecutionStartTime: &est,
		EndTime:            &et,
	}

	r := RowFromEvent(e)

	if r.QueryID != e.Metadata.QueryID || r.QueryState != "FINISHED" || r.QueryType != "SELECT" {
		t.Errorf("identity mapping wrong: %+v", r)
	}
	if r.CPUMS != 500 || r.WallMS != 1200 || r.QueuedMS != 50 {
		t.Errorf("durations wrong: cpu=%d wall=%d queued=%d", r.CPUMS, r.WallMS, r.QueuedMS)
	}
	if r.ClientTags != "team:data,env:prod" {
		t.Errorf("client tags join wrong: %q", r.ClientTags)
	}
	if r.ResourceGroup != "global.adhoc" {
		t.Errorf("resource group join wrong: %q", r.ResourceGroup)
	}
	if r.PeakTotalMemoryBytes != 8388608 || r.PhysicalInputBytes != 1024000 || r.CompletedSplits != 7 {
		t.Errorf("numeric stats wrong: %+v", r)
	}
	if !strings.Contains(r.InputsJSON, `"table":"t"`) {
		t.Errorf("inputs json missing table: %q", r.InputsJSON)
	}
	if r.ExecutionStartTime == nil || !r.ExecutionStartTime.Equal(est) {
		t.Errorf("execution start time wrong: %v", r.ExecutionStartTime)
	}
	if r.ErrorCode != "" || r.ErrorMessage != "" {
		t.Errorf("successful query should have no error fields: %+v", r)
	}
}

func TestRowFromEventFailure(t *testing.T) {
	ct := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	e := &event.Event{
		Metadata:   event.Metadata{QueryID: "q1", QueryState: "FAILED"},
		Context:    event.Context{User: "bob"},
		CreateTime: &ct,
		FailureInfo: &event.FailureInfo{
			ErrorCode:      event.ErrorCode{Code: 1, Name: "SYNTAX_ERROR", Type: "USER_ERROR"},
			FailureMessage: ptr("line 1:1: mismatched input"),
		},
	}
	r := RowFromEvent(e)
	if r.ErrorCode != "SYNTAX_ERROR" || r.ErrorType != "USER_ERROR" {
		t.Errorf("error code/type wrong: %q %q", r.ErrorCode, r.ErrorType)
	}
	if !strings.Contains(r.ErrorMessage, "mismatched input") {
		t.Errorf("error message wrong: %q", r.ErrorMessage)
	}
}

func TestRowFromEventEmptyOptionalsAreZeroValues(t *testing.T) {
	ct := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	e := &event.Event{
		Metadata:   event.Metadata{QueryID: "q2", QueryState: "FINISHED"},
		Context:    event.Context{User: "carol"},
		CreateTime: &ct,
	}
	r := RowFromEvent(e)
	if r.QueryType != "" || r.Catalog != "" || r.Principal != "" || r.ClientTags != "" {
		t.Errorf("absent optionals should be empty strings: %+v", r)
	}
	if r.InputsJSON != "" {
		t.Errorf("no inputs should yield empty inputs_json, got %q", r.InputsJSON)
	}
	if r.ExecutionStartTime != nil || r.EndTime != nil {
		t.Errorf("absent timestamps should be nil")
	}
}

func durationOf(t *testing.T, iso string) event.Duration {
	t.Helper()
	var d event.Duration
	if err := d.UnmarshalJSON([]byte(`"` + iso + `"`)); err != nil {
		t.Fatalf("durationOf(%q): %v", iso, err)
	}
	return d
}
