package querycli

import (
	"flag"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/Shadi/trino-log-sink/internal/store"
)

type listEnvelope struct {
	Queries []store.QuerySummary `json:"queries"`
	Count   int                  `json:"count"`
	Limit   int                  `json:"limit"`
	Offset  int                  `json:"offset"`
	HasNext bool                 `json:"hasNext"`
}

func runList(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("query list", flag.ContinueOnError)
	common := registerCommon(fs)
	rng := fs.String("range", "24h", "time range: 1h|6h|24h|7d|30d")
	user := fs.String("user", "", "filter by user")
	catalog := fs.String("catalog", "", "filter by catalog")
	state := fs.String("state", "", "filter by state: FINISHED|FAILED|CANCELED")
	sortKey := fs.String("sort", "start", "sort by: start|wall|cpu|bytes|mem|rows")
	desc := fs.Bool("desc", true, "sort descending (use --desc=false for ascending)")
	limit := fs.Int("limit", 20, "max rows to return (1-500)")
	offset := fs.Int("offset", 0, "row offset for paging")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument(s): %s", strings.Join(fs.Args(), " "))
	}
	if *limit < 1 {
		*limit = 1
	}
	if *limit > 500 {
		*limit = 500
	}
	if *offset < 0 {
		*offset = 0
	}

	cl, err := newClient(common)
	if err != nil {
		return err
	}

	q := url.Values{}
	q.Set("range", *rng)
	if *user != "" {
		q.Set("user", *user)
	}
	if *catalog != "" {
		q.Set("catalog", *catalog)
	}
	if *state != "" {
		q.Set("state", *state)
	}
	q.Set("sort", *sortKey)
	if *desc {
		q.Set("dir", "desc")
	} else {
		q.Set("dir", "asc")
	}
	q.Set("limit", strconv.Itoa(*limit))
	q.Set("offset", strconv.Itoa(*offset))

	var env listEnvelope
	if err := cl.getJSON("/api/v1/queries", q, &env); err != nil {
		return err
	}
	return render(out, common.output, env, func(w io.Writer) { printList(w, &env) })
}

func printList(out io.Writer, env *listEnvelope) {
	if len(env.Queries) == 0 {
		fmt.Fprintln(out, "no queries")
		return
	}
	w := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTATE\tTYPE\tUSER\tCPU\tWALL\tROWS")
	for _, s := range env.Queries {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			s.QueryID, s.QueryState, s.QueryType, trunc(s.UserName, 20),
			humanMS(s.CPUMS), humanMS(s.WallMS), humanCount(s.OutputRows))
	}
	w.Flush()
	if env.HasNext {
		fmt.Fprintf(out, "\n… more results (next: --offset %d)\n", env.Offset+len(env.Queries))
	}
}
