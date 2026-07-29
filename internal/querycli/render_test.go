package querycli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Shadi/trino-log-sink/internal/event"
	"github.com/Shadi/trino-log-sink/internal/store"
)

func TestPrintList(t *testing.T) {
	env := &listEnvelope{
		Queries: []store.QuerySummary{
			{QueryID: "q-1", UserName: "alice", QueryState: "FINISHED", QueryType: "SELECT", CPUMS: 59929133, WallMS: 789429, OutputRows: 846303721},
			{QueryID: "q-2", UserName: "bob", QueryState: "FAILED", QueryType: "INSERT"},
		},
		Count: 2, Offset: 0, HasNext: true,
	}
	var b bytes.Buffer
	printList(&b, env)
	out := b.String()
	for _, want := range []string{"ID", "STATE", "q-1", "alice", "FAILED", "16h38m", "846,303,721", "more results", "--offset 2"} {
		if !strings.Contains(out, want) {
			t.Errorf("printList output missing %q\n%s", want, out)
		}
	}
}

func TestPrintListEmpty(t *testing.T) {
	var b bytes.Buffer
	printList(&b, &listEnvelope{})
	if !strings.Contains(b.String(), "no queries") {
		t.Errorf("want 'no queries', got %q", b.String())
	}
}

func TestPrintDetail(t *testing.T) {
	bytesIn := int64(1048576)
	rowsIn := int64(113313755)
	d := &queryDetail{
		Row: store.Row{
			QueryID: "q-detail", QueryState: "FAILED", ErrorCode: "ADMINISTRATIVELY_KILLED", ErrorType: "USER_ERROR",
			UserName: "trino", Source: "dbt", QueryType: "INSERT",
			CPUMS: 59929133, WallMS: 789429, PeakUserMemoryBytes: 314417094561,
			PhysicalInputBytes: 16658425623, PhysicalInputRows: 323955562,
			ErrorMessage: "Query killed by\nresource group", Plan: "- Output[1]",
		},
		Inputs: []event.InputMetadata{
			{CatalogName: "gravitino", Schema: "dbt_walaa", Table: "fct_balance_transactions", PhysicalInputBytes: &bytesIn, PhysicalInputRows: &rowsIn},
		},
	}
	var b bytes.Buffer
	printDetail(&b, d)
	out := b.String()
	for _, want := range []string{"q-detail", "ADMINISTRATIVELY_KILLED", "16h38m", "292.8GiB", "fct_balance_transactions", "113,313,755", "Query killed by resource group", "query plan q-detail"} {
		if !strings.Contains(out, want) {
			t.Errorf("printDetail output missing %q\n%s", want, out)
		}
	}
}

func TestPrintDetailOmitsPlanBody(t *testing.T) {
	d := &queryDetail{Row: store.Row{QueryID: "q", Plan: "SECRET_PLAN_BODY_XYZ", JSONPlan: "{json}"}}
	var b bytes.Buffer
	printDetail(&b, d)
	if strings.Contains(b.String(), "SECRET_PLAN_BODY_XYZ") {
		t.Errorf("plan body must not appear in get output:\n%s", b.String())
	}
}

func TestGetProjectionClearsPlan(t *testing.T) {
	d := &queryDetail{Row: store.Row{QueryID: "q", Plan: "big", JSONPlan: "big"}}
	p := getProjection(d)
	if p.Plan != "" || p.JSONPlan != "" {
		t.Errorf("projection should clear plan bodies, got Plan=%q JSONPlan=%q", p.Plan, p.JSONPlan)
	}
	raw, _ := json.Marshal(p)
	if strings.Contains(string(raw), "big") {
		t.Errorf("json projection leaked plan body: %s", raw)
	}
	if !strings.Contains(string(raw), `"queryId":"q"`) {
		t.Errorf("json projection missing queryId: %s", raw)
	}
}

func TestPrintHotspots(t *testing.T) {
	var b bytes.Buffer
	printHotspots(&b, []hotspot{{Operator: "Window[user_key]", CPUPct: 86.5, CPU: "14.40h", Rows: 846303721}})
	out := b.String()
	for _, want := range []string{"CPU%", "OPERATOR", "86.5%", "14.40h", "846,303,721", "Window[user_key]", "--raw"} {
		if !strings.Contains(out, want) {
			t.Errorf("printHotspots missing %q\n%s", want, out)
		}
	}
}

func TestPrintHotspotsEmpty(t *testing.T) {
	var b bytes.Buffer
	printHotspots(&b, nil)
	if !strings.Contains(b.String(), "no per-operator CPU stats") {
		t.Errorf("want empty notice, got %q", b.String())
	}
}
