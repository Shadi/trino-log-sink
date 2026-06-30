package event

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func decodeFixture(t *testing.T, name string) *Event {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", name)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	return &e
}

func TestDecodeSuccessFixture(t *testing.T) {
	e := decodeFixture(t, "query_completed_event.json")

	if err := e.Validate(); err != nil {
		t.Fatalf("fixture should be valid: %v", err)
	}
	if e.Metadata.QueryID != "20240115_103000_00042_a1b2c" {
		t.Errorf("queryId = %q", e.Metadata.QueryID)
	}
	if e.Statistics.CPUTime.Millis() != 12430 {
		t.Errorf("cpuTime ms = %d, want 12430", e.Statistics.CPUTime.Millis())
	}
	if e.Statistics.WallTime.Millis() != 3870 {
		t.Errorf("wallTime ms = %d, want 3870", e.Statistics.WallTime.Millis())
	}
	if e.SourceValue() != "bi-tool" {
		t.Errorf("source = %q, want bi-tool", e.SourceValue())
	}
	if len(e.IOMetadata.Inputs) != 2 || e.IOMetadata.Inputs[0].CatalogName != "lakehouse" {
		t.Errorf("inputs not parsed: %+v", e.IOMetadata.Inputs)
	}
	if e.IOMetadata.Inputs[0].PhysicalInputBytes == nil || *e.IOMetadata.Inputs[0].PhysicalInputBytes != 1006632960 {
		t.Errorf("input bytes not parsed: %+v", e.IOMetadata.Inputs[0].PhysicalInputBytes)
	}
	if e.CreateTime == nil || e.CreateTime.Year() != 2024 {
		t.Errorf("createTime not parsed: %v", e.CreateTime)
	}
	if e.FailureInfo != nil {
		t.Errorf("successful query should have no failureInfo")
	}
}

func TestValidateWrapsSentinel(t *testing.T) {
	e := &Event{}
	err := e.Validate()
	if err == nil {
		t.Fatal("empty event should be invalid")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("Validate error should wrap ErrInvalidEvent, got %v", err)
	}
}

func TestDecodeFailedFixture(t *testing.T) {
	e := decodeFixture(t, "query_failed_event.json")
	if e.FailureInfo == nil {
		t.Fatal("failed fixture should have failureInfo")
	}
	if e.FailureInfo.ErrorCode.Name != "TABLE_NOT_FOUND" || e.FailureInfo.ErrorCode.Type != "USER_ERROR" {
		t.Errorf("errorCode parsed wrong: %+v", e.FailureInfo.ErrorCode)
	}
	if e.Metadata.QueryState != "FAILED" {
		t.Errorf("queryState = %q", e.Metadata.QueryState)
	}
}
