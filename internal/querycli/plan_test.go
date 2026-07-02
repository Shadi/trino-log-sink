package querycli

import (
	"strings"
	"testing"
)

const samplePlan = `Fragment 1 [HASH]
    CPU: 2.59m, Scheduled: 3.68m, Input: 24721792 rows
    └─ Window[partitionBy = user_key]
       │   Layout: [user_key:bigint]
       │   CPU: 14.40h (86.48%), Scheduled: 20h, Output: 846303721 rows (342GB)
       └─ LeftJoin[("user_key" = "customer_key")]
          │   CPU: 1.81m (3.26%), Scheduled: 2.5m, Output: 1346579962 rows (1GB)
    Output[columnNames = [rows]]
    │   CPU: 0.00ns (0.00%), Scheduled: 0.00ns, Output: 1 row (9B)
`

func TestParseHotspotsRanksByCPU(t *testing.T) {
	hs := parseHotspots(samplePlan)
	if len(hs) != 3 {
		t.Fatalf("got %d hotspots, want 3: %+v", len(hs), hs)
	}
	if hs[0].CPUPct != 86.48 {
		t.Errorf("top CPUPct = %v, want 86.48", hs[0].CPUPct)
	}
	if hs[0].CPU != "14.40h" {
		t.Errorf("top CPU = %q, want 14.40h", hs[0].CPU)
	}
	if hs[0].Rows != 846303721 {
		t.Errorf("top Rows = %d, want 846303721", hs[0].Rows)
	}
	if !strings.Contains(hs[0].Operator, "Window") {
		t.Errorf("top Operator = %q, want it to mention Window", hs[0].Operator)
	}
	if hs[1].CPUPct != 3.26 || !strings.Contains(hs[1].Operator, "LeftJoin") {
		t.Errorf("second = %+v, want ~3.26 LeftJoin", hs[1])
	}
	if hs[2].CPUPct != 0.0 {
		t.Errorf("third CPUPct = %v, want 0", hs[2].CPUPct)
	}
}

func TestParseHotspotsEmpty(t *testing.T) {
	hs := parseHotspots("")
	if hs == nil {
		t.Fatal("parseHotspots must return a non-nil slice so -o json emits [] not null")
	}
	if len(hs) != 0 {
		t.Fatalf("expected no hotspots, got %+v", hs)
	}
	if hs := parseHotspots("no stats here\njust text"); len(hs) != 0 {
		t.Fatalf("expected no hotspots, got %+v", hs)
	}
}

func TestParseHotspotsKeepsCommaInOperator(t *testing.T) {
	plan := "└─ Aggregate(FINAL)[user_id, order_date]\n   CPU: 5s (10.0%), Output: 3 rows"
	hs := parseHotspots(plan)
	if len(hs) != 1 {
		t.Fatalf("got %d hotspots, want 1: %+v", len(hs), hs)
	}
	if !strings.Contains(hs[0].Operator, "order_date") {
		t.Errorf("operator label dropped the comma-separated part: %q", hs[0].Operator)
	}
}

func TestParseCommaInt(t *testing.T) {
	if got := parseCommaInt("1,346,579,962"); got != 1346579962 {
		t.Errorf("got %d", got)
	}
	if got := parseCommaInt("42"); got != 42 {
		t.Errorf("got %d", got)
	}
}
