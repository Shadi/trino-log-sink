package querycli

import (
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
)

type hotspot struct {
	Operator string  `json:"operator"`
	CPUPct   float64 `json:"cpuPct"`
	CPU      string  `json:"cpu"`
	Rows     int64   `json:"rows"`
}

var (
	reCPUPct   = regexp.MustCompile(`CPU:\s*([0-9.]+\s*[a-zµ]+)\s*\(([0-9.]+)%\)`)
	reRows     = regexp.MustCompile(`Output:\s*([0-9,]+)\s+rows?`)
	reOpBranch = regexp.MustCompile(`[├└]─\s*(.+)$`)
	reOpTop    = regexp.MustCompile(`^\s*([A-Z][A-Za-z]+\[.*)$`)
)

func runPlan(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("query plan", flag.ContinueOnError)
	common := registerCommon(fs)
	top := fs.Int("top", 10, "number of hotspots to show (0 = all)")
	raw := fs.Bool("raw", false, "print the full text plan instead of a hotspot summary")
	id, err := parseWithID(fs, args)
	if err != nil {
		return err
	}

	cl, err := newClient(common)
	if err != nil {
		return err
	}

	d, err := cl.getQuery(id)
	if err != nil {
		return err
	}

	if *raw {
		if d.Plan == "" {
			fmt.Fprintln(os.Stderr, "(no text plan recorded for this query)")
			return nil
		}
		fmt.Fprintln(out, d.Plan)
		return nil
	}

	hs := parseHotspots(d.Plan)
	if *top > 0 && len(hs) > *top {
		hs = hs[:*top]
	}
	return render(out, common.output, hs, func(w io.Writer) { printHotspots(w, hs) })
}

func parseHotspots(plan string) []hotspot {
	out := []hotspot{}
	lastOp := ""
	for ln := range strings.SplitSeq(plan, "\n") {
		if m := reOpBranch.FindStringSubmatch(ln); m != nil {
			lastOp = opName(m[1])
			continue
		}
		if m := reOpTop.FindStringSubmatch(ln); m != nil {
			lastOp = opName(m[1])
			continue
		}
		if m := reCPUPct.FindStringSubmatch(ln); m != nil {
			pct, _ := strconv.ParseFloat(m[2], 64)
			h := hotspot{Operator: lastOp, CPUPct: pct, CPU: normSpace(m[1])}
			if rm := reRows.FindStringSubmatch(ln); rm != nil {
				h.Rows = parseCommaInt(rm[1])
			}
			out = append(out, h)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CPUPct > out[j].CPUPct })
	return out
}

func printHotspots(out io.Writer, hs []hotspot) {
	if len(hs) == 0 {
		fmt.Fprintln(out, "no per-operator CPU stats found in plan (the query may not have executed)")
		return
	}
	w := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "CPU%\tCPU\tROWS\tOPERATOR")
	for _, h := range hs {
		fmt.Fprintf(w, "%.1f%%\t%s\t%s\t%s\n", h.CPUPct, h.CPU, humanCount(h.Rows), h.Operator)
	}
	w.Flush()
	fmt.Fprintln(out, "\n(use --raw for the full text plan)")
}

func opName(s string) string {
	return trunc(normSpace(s), 60)
}

func normSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func parseCommaInt(s string) int64 {
	n, _ := strconv.ParseInt(strings.ReplaceAll(s, ",", ""), 10, 64)
	return n
}
