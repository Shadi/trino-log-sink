package querycli

import (
	"flag"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/Shadi/trino-query-log-sink/internal/event"
	"github.com/Shadi/trino-query-log-sink/internal/store"
)

type queryDetail struct {
	store.Row
	Inputs []event.InputMetadata `json:"inputs"`
}

func runGet(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("query get", flag.ContinueOnError)
	common := registerCommon(fs)
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
	return render(out, common.output, getProjection(&d), func(w io.Writer) { printDetail(w, &d) })
}

func getProjection(d *queryDetail) queryDetail {
	cp := *d
	cp.Plan = ""
	cp.JSONPlan = ""
	return cp
}

func printDetail(out io.Writer, d *queryDetail) {
	r := &d.Row
	fmt.Fprintf(out, "Query    %s\n", r.QueryID)
	state := r.QueryState
	if r.ErrorCode != "" {
		state += fmt.Sprintf("  (%s / %s)", r.ErrorCode, r.ErrorType)
	}
	fmt.Fprintf(out, "State    %s\n", state)
	fmt.Fprintf(out, "User     %s    Source %s    Type %s\n", r.UserName, r.Source, r.QueryType)
	fmt.Fprintf(out, "Timings  queued %s  planning %s  exec %s  wall %s\n",
		humanMS(r.QueuedMS), humanMS(r.PlanningMS), humanMS(r.ExecutionMS), humanMS(r.WallMS))
	fmt.Fprintf(out, "CPU      %s    Peak mem %s\n", humanMS(r.CPUMS), humanBytes(r.PeakUserMemoryBytes))
	fmt.Fprintf(out, "IO       in %s / %s rows    out %s rows    written %s rows\n",
		humanBytes(r.PhysicalInputBytes), humanCount(r.PhysicalInputRows),
		humanCount(r.OutputRows), humanCount(r.WrittenRows))
	if r.ErrorMessage != "" {
		fmt.Fprintf(out, "Error    %s\n", trunc(oneLine(r.ErrorMessage), 200))
	}

	if len(d.Inputs) > 0 {
		fmt.Fprintln(out, "Inputs:")
		w := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
		fmt.Fprintln(w, "  TABLE\tROWS\tBYTES")
		for _, in := range d.Inputs {
			fmt.Fprintf(w, "  %s.%s.%s\t%s\t%s\n", in.CatalogName, in.Schema, in.Table,
				humanCountPtr(in.PhysicalInputRows), humanBytesPtr(in.PhysicalInputBytes))
		}
		w.Flush()
	}

	if r.Plan != "" || r.JSONPlan != "" {
		fmt.Fprintf(out, "\nPlan     %s text / %s json — run `query plan %s` for CPU hotspots\n",
			humanBytes(int64(len(r.Plan))), humanBytes(int64(len(r.JSONPlan))), r.QueryID)
	}
}
