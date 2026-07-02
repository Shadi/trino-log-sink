// Package querycli implements the `query` client subcommands (list, get, plan)
// that read a running service's JSON API (/api/v1) over HTTP. It is a read-only
// client: it decodes into the store/event types but never invokes the
// ingest/store/server logic and opens no Trino or cache connections, so it needs
// only a --url, not the service's environment configuration.
package querycli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

const (
	exitOK       = 0
	exitError    = 1
	exitNotFound = 4
)

var (
	errNotFound = errors.New("query not found")
	errUsage    = errors.New("usage")
)

// Main runs a `query` subcommand and returns a process exit code. Results are
// written to out; errors and flag usage go to stderr.
func Main(args []string, out io.Writer) int {
	if len(args) == 0 {
		usage()
		fmt.Fprintln(os.Stderr, "query: missing subcommand")
		return exitError
	}

	sub, rest := args[0], args[1:]
	var err error
	switch sub {
	case "list":
		err = runList(rest, out)
	case "get":
		err = runGet(rest, out)
	case "plan":
		err = runPlan(rest, out)
	case "-h", "--help", "help":
		usage()
		return exitOK
	default:
		usage()
		fmt.Fprintf(os.Stderr, "query: unknown subcommand %q\n", sub)
		return exitError
	}

	switch {
	case err == nil:
		return exitOK
	case errors.Is(err, flag.ErrHelp):
		return exitOK
	case errors.Is(err, errUsage):
		return exitError
	case errors.Is(err, errNotFound):
		fmt.Fprintln(os.Stderr, "error:", err)
		return exitNotFound
	default:
		fmt.Fprintln(os.Stderr, "error:", err)
		return exitError
	}
}

func parseFlags(fs *flag.FlagSet, args []string) error {
	err := fs.Parse(args)
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return err
	}
	return errUsage
}

func parseWithID(fs *flag.FlagSet, args []string) (string, error) {
	var id string
	rem := args
	for {
		if err := parseFlags(fs, rem); err != nil {
			return "", err
		}
		if fs.NArg() == 0 {
			break
		}
		if id != "" {
			return "", fmt.Errorf("unexpected argument: %s", fs.Arg(0))
		}
		id = fs.Arg(0)
		rem = fs.Args()[1:]
	}
	if id == "" {
		return "", fmt.Errorf("missing <queryId> argument")
	}
	return id, nil
}

func usage() {
	fmt.Fprint(os.Stderr, `trino-query-log-sink query <subcommand> [flags]

  list             list query summaries
  get <id>         show one query's details (no plan body)
  plan <id>        show a query's top CPU operators (--raw for the full plan)

Common flags:
  --url URL          base server URL (required), e.g. http://localhost:8090
  -H, --header K:V   extra request header (repeatable)
  --token-file PATH  file containing a token (sent as "Authorization: Bearer <token>")
  --token-header H   override Authorization: Bearer — send the --token-file token raw in header H
  --timeout D        HTTP timeout (default 30s)
  --insecure         skip TLS certificate verification
  -o, --output FMT   output format: table|json (default table)

Run 'query <subcommand> -h' for a subcommand's own flags. Examples:
  trino-query-log-sink query list --url http://localhost:8090 --sort cpu --limit 5
  trino-query-log-sink query get  20260630_151601_01073_mhbfn --url http://localhost:8090
  trino-query-log-sink query plan 20260630_151601_01073_mhbfn --url http://localhost:8090 --top 5
`)
}
