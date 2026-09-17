package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Shadi/trino-log-sink/internal/config"
)

const (
	defaultClickHouseTestImage = "docker.io/clickhouse/clickhouse-server:26.8"
	clickHouseTestUser         = "test"
	clickHouseTestPassword     = "test"
	clickHouseTestLabel        = "trino-log-sink-test=1"
	clickHouseStartTimeout     = 90 * time.Second
	clickHousePullTimeout      = 10 * time.Minute
	podmanCommandTimeout       = 60 * time.Second
)

var errPodmanUnavailable = errors.New("podman not found on PATH; set CLICKHOUSE_TEST_ADDR to use an existing server")

func podman(timeout time.Duration, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "podman", args...).CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("podman %s: %v: %s", args[0], err, out)
	}
	return out, nil
}

func reapStaleContainers() {
	out, err := podman(podmanCommandTimeout, "ps", "-aq", "--filter", "label="+clickHouseTestLabel)
	if err != nil {
		return
	}
	for _, id := range strings.Fields(string(out)) {
		_, _ = podman(podmanCommandTimeout, "rm", "-f", id)
	}
}

type clickHouseServer struct {
	addr, user, password string
	stop                 func()
	err                  error
}

var (
	chServerOnce sync.Once
	chServer     clickHouseServer
	chDBCounter  atomic.Int64
)

func TestMain(m *testing.M) {
	code := m.Run()
	if chServer.stop != nil {
		chServer.stop()
	}
	os.Exit(code)
}

func startClickHouse() clickHouseServer {
	if addr := os.Getenv("CLICKHOUSE_TEST_ADDR"); addr != "" {
		user := os.Getenv("CLICKHOUSE_TEST_USER")
		if user == "" {
			user = "default"
		}
		return clickHouseServer{addr: addr, user: user, password: os.Getenv("CLICKHOUSE_TEST_PASSWORD")}
	}
	if _, err := exec.LookPath("podman"); err != nil {
		return clickHouseServer{err: errPodmanUnavailable}
	}
	image := os.Getenv("CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = defaultClickHouseTestImage
	}
	reapStaleContainers()
	if _, err := podman(clickHousePullTimeout, "pull", "-q", image); err != nil {
		return clickHouseServer{err: err}
	}
	out, err := podman(podmanCommandTimeout, "run", "-d", "--rm", "--label", clickHouseTestLabel,
		"-p", "127.0.0.1::9000",
		"-e", "CLICKHOUSE_USER="+clickHouseTestUser, "-e", "CLICKHOUSE_PASSWORD="+clickHouseTestPassword,
		image)
	if err != nil {
		return clickHouseServer{err: err}
	}
	id := strings.TrimSpace(string(out))
	stop := func() { _, _ = podman(podmanCommandTimeout, "rm", "-f", id) }

	addr, err := podmanMappedPort(id)
	if err != nil {
		stop()
		return clickHouseServer{err: err}
	}
	srv := clickHouseServer{addr: addr, user: clickHouseTestUser, password: clickHouseTestPassword, stop: stop}
	if err := waitForClickHouse(srv); err != nil {
		logs, _ := podman(podmanCommandTimeout, "logs", "--tail", "30", id)
		stop()
		return clickHouseServer{err: fmt.Errorf("%w\ncontainer logs:\n%s", err, logs)}
	}
	return srv
}

func podmanMappedPort(id string) (string, error) {
	out, err := podman(podmanCommandTimeout, "port", id, "9000/tcp")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "127.0.0.1:") || strings.HasPrefix(line, "0.0.0.0:") {
			return "127.0.0.1:" + line[strings.LastIndex(line, ":")+1:], nil
		}
	}
	return "", fmt.Errorf("podman port: no IPv4 mapping in %q", out)
}

func waitForClickHouse(srv clickHouseServer) error {
	deadline := time.Now().Add(clickHouseStartTimeout)
	var last error
	for time.Now().Before(deadline) {
		s, err := NewClickHouse(config.ClickHouse{
			Addr: srv.addr, Database: "default", Table: "probe", User: srv.user, Password: srv.password,
			DialTimeout: 2 * time.Second, QueryTimeout: 5 * time.Second, MaxBatchBytes: 1 << 20,
		})
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		last = s.Validate(ctx)
		cancel()
		_ = s.Close()
		if last == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("clickhouse at %s not ready after %s: %w", srv.addr, clickHouseStartTimeout, last)
}

func newTestClickHouse(t *testing.T) *ClickHouseStore {
	t.Helper()
	if testing.Short() && os.Getenv("CLICKHOUSE_TEST_REQUIRED") == "" {
		t.Skip("skipping ClickHouse integration test in -short mode")
	}
	chServerOnce.Do(func() { chServer = startClickHouse() })
	if errors.Is(chServer.err, errPodmanUnavailable) && os.Getenv("CLICKHOUSE_TEST_REQUIRED") == "" {
		t.Skip(chServer.err)
	}
	if chServer.err != nil {
		t.Fatal(chServer.err)
	}

	db := fmt.Sprintf("qlog_test_%d_%d", os.Getpid(), chDBCounter.Add(1))
	s, err := NewClickHouse(config.ClickHouse{
		Addr: chServer.addr, Database: db, Table: "trino_query_log", User: chServer.user, Password: chServer.password,
		DialTimeout: 5 * time.Second, QueryTimeout: 30 * time.Second, MaxBatchBytes: 32 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = s.conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+quoteIdent(db))
		_ = s.Close()
	})
	return s
}

func newInitialisedClickHouse(t *testing.T) *ClickHouseStore {
	t.Helper()
	s := newTestClickHouse(t)
	if err := s.Init(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s
}

func integrationRow(i int, createTime time.Time) Row {
	users := []string{"alice", "bob", "carol"}
	states := []string{"FINISHED", "FAILED", "CANCELED"}
	r := Row{
		QueryID:             fmt.Sprintf("%s_%06d_%05d_abcde", createTime.Format("20060102_150405"), i, i),
		QueryState:          states[i%len(states)],
		QueryType:           "SELECT",
		UserName:            users[i%len(users)],
		Source:              "dbt-trino",
		Catalog:             []string{"hive", "iceberg"}[i%2],
		SchemaName:          "obs",
		QueryText:           fmt.Sprintf("SELECT %d FROM t WHERE x = 'it''s'", i),
		QueryPreview:        fmt.Sprintf("SELECT %d FROM t", i),
		CreateTime:          createTime,
		WallMS:              int64((i * 7919) % 1000),
		CPUMS:               int64((i * 104729) % 1000),
		PhysicalInputBytes:  int64((i * 15485863) % 100000),
		PeakUserMemoryBytes: int64((i * 32452843) % 100000),
		OutputRows:          int64((i * 49979687) % 1000),
		ProcessedInputRows:  int64(i),
		ServerVersion:       "480",
		Environment:         "test",
	}
	if i%3 == 1 {
		r.ErrorCode = "GENERIC_INTERNAL_ERROR"
		r.ErrorType = "INTERNAL_ERROR"
		r.ErrorMessage = "boom"
	}
	if i%4 != 0 {
		start := createTime.Add(time.Duration(i) * time.Millisecond)
		end := start.Add(time.Duration(r.WallMS) * time.Millisecond)
		r.ExecutionStartTime = &start
		r.EndTime = &end
	}
	return r
}

func integrationRows(n int, base time.Time) []Row {
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = integrationRow(i, base.Add(time.Duration(i)*time.Second+time.Duration(i)*time.Microsecond))
	}
	return rows
}

func sortValue(q QuerySummary, key SortKey) int64 {
	switch key {
	case SortWall:
		return q.WallMS
	case SortCPU:
		return q.CPUMS
	case SortBytes:
		return q.PhysicalInputBytes
	case SortMemory:
		return q.PeakUserMemoryBytes
	case SortRows:
		return q.OutputRows
	default:
		return q.CreateTime.UnixMicro()
	}
}

func TestClickHouseInitAndValidate(t *testing.T) {
	s := newTestClickHouse(t)
	ctx := t.Context()
	if err := s.Validate(ctx); err != nil {
		t.Fatalf("Validate before init: %v", err)
	}
	if err := s.ValidateTable(ctx); err == nil {
		t.Fatal("ValidateTable must fail before the table exists")
	}
	for range 2 {
		if err := s.Init(ctx); err != nil {
			t.Fatalf("Init must be idempotent: %v", err)
		}
	}
	if err := s.ValidateTable(ctx); err != nil {
		t.Fatalf("ValidateTable after init: %v", err)
	}
}

func TestClickHouseListQueries(t *testing.T) {
	s := newInitialisedClickHouse(t)
	ctx := t.Context()
	base := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	rows := integrationRows(30, base)
	if err := s.InsertBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	window := QueryFilter{Since: base.Add(-time.Hour), Until: base.Add(time.Hour), Limit: 100}

	for _, key := range []SortKey{SortStart, SortWall, SortCPU, SortBytes, SortMemory, SortRows} {
		for _, desc := range []bool{false, true} {
			f := window
			f.Sort, f.Desc = key, desc
			got, err := s.ListQueries(ctx, f)
			if err != nil {
				t.Fatalf("list %s desc=%v: %v", key, desc, err)
			}
			if len(got) != len(rows) {
				t.Fatalf("list %s desc=%v: %d rows, want %d", key, desc, len(got), len(rows))
			}
			ordered := sort.SliceIsSorted(got, func(a, b int) bool {
				va, vb := sortValue(got[a], key), sortValue(got[b], key)
				if va != vb {
					if desc {
						return va > vb
					}
					return va < vb
				}
				if desc {
					return got[a].QueryID > got[b].QueryID
				}
				return got[a].QueryID < got[b].QueryID
			})
			if !ordered {
				t.Errorf("list %s desc=%v not ordered with query_id tiebreak", key, desc)
			}
		}
	}

	page1, err := s.ListQueries(ctx, QueryFilter{Since: window.Since, Until: window.Until, Sort: SortWall, Desc: true, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	page2, err := s.ListQueries(ctx, QueryFilter{Since: window.Since, Until: window.Until, Sort: SortWall, Desc: true, Limit: 5, Offset: 5})
	if err != nil {
		t.Fatal(err)
	}
	all, _ := s.ListQueries(ctx, QueryFilter{Since: window.Since, Until: window.Until, Sort: SortWall, Desc: true, Limit: 10})
	if len(page1) != 5 || len(page2) != 5 || page1[4].QueryID != all[4].QueryID || page2[0].QueryID != all[5].QueryID {
		t.Errorf("offset paging mismatch: page1=%v page2=%v", ids(page1), ids(page2))
	}

	filtered, err := s.ListQueries(ctx, QueryFilter{Since: window.Since, Until: window.Until, User: "alice", Catalog: "hive", State: "FINISHED", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	want := 0
	for _, r := range rows {
		if r.UserName == "alice" && r.Catalog == "hive" && r.QueryState == "FINISHED" {
			want++
		}
	}
	if len(filtered) != want || want == 0 {
		t.Errorf("filtered list: %d rows, want %d", len(filtered), want)
	}
	for _, q := range filtered {
		if q.UserName != "alice" || q.Catalog != "hive" || q.QueryState != "FINISHED" {
			t.Errorf("filter leaked row %+v", q)
		}
	}

	exact := rows[7].CreateTime
	incl, err := s.ListQueries(ctx, QueryFilter{Since: exact, Until: exact.Add(time.Microsecond), Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(incl) != 1 || incl[0].QueryID != rows[7].QueryID {
		t.Errorf("microsecond window must select exactly the boundary row, got %v", ids(incl))
	}
	excl, err := s.ListQueries(ctx, QueryFilter{Since: exact.Add(-time.Microsecond), Until: exact, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(excl) != 0 {
		t.Errorf("until must be exclusive at microsecond precision, got %v", ids(excl))
	}

	none, err := s.ListQueries(ctx, QueryFilter{Since: base.Add(48 * time.Hour), Until: base.Add(72 * time.Hour), Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if none != nil {
		t.Errorf("empty result must be nil, got %v", none)
	}

	summary := all[0]
	var full *Row
	for i := range rows {
		if rows[i].QueryID == summary.QueryID {
			full = &rows[i]
		}
	}
	if full == nil || summary.UserName != full.UserName || summary.QueryPreview != full.QueryPreview ||
		summary.ErrorCode != full.ErrorCode || !summary.CreateTime.Equal(full.CreateTime) {
		t.Errorf("summary columns mismatch: %+v vs %+v", summary, full)
	}
	if (summary.ExecutionStartTime == nil) != (full.ExecutionStartTime == nil) {
		t.Errorf("nullable timestamp mismatch in summary: %+v", summary)
	}
}

func ids(qs []QuerySummary) []string {
	out := make([]string, len(qs))
	for i, q := range qs {
		out[i] = q.QueryID
	}
	return out
}

func TestClickHouseGetQuery(t *testing.T) {
	s := newInitialisedClickHouse(t)
	ctx := t.Context()
	base := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	rows := integrationRows(4, base)
	rows[1].Plan = strings.Repeat("- Output[1] CPU: 1.00s\n", 12_000)
	rows[1].JSONPlan = `{"id":"0","plan":"` + strings.Repeat("x", 300_000) + `"}`
	rows[1].InputsJSON = `[{"catalogName":"hive","schema":"s","table":"t"}]`
	rows[1].ClientTags = "team:data,env:prod"
	rows[1].ExecutionStartTime = ptr(base.Add(1500 * time.Microsecond))
	rows[1].EndTime = ptr(base.Add(2*time.Second + 123456*time.Microsecond))
	rows[0].ExecutionStartTime, rows[0].EndTime = nil, nil
	if err := s.InsertBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetQuery(ctx, rows[1].QueryID)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("row not found")
	}
	want := rows[1]
	if got.Plan != want.Plan || got.JSONPlan != want.JSONPlan || got.InputsJSON != want.InputsJSON ||
		got.QueryText != want.QueryText || got.ClientTags != want.ClientTags || got.ErrorMessage != want.ErrorMessage {
		t.Errorf("large fields did not round-trip")
	}
	if !got.CreateTime.Equal(want.CreateTime) || got.ExecutionStartTime == nil || !got.ExecutionStartTime.Equal(*want.ExecutionStartTime) ||
		got.EndTime == nil || !got.EndTime.Equal(*want.EndTime) {
		t.Errorf("timestamps did not round-trip: %v %v %v", got.CreateTime, got.ExecutionStartTime, got.EndTime)
	}
	if got.WallMS != want.WallMS || got.PeakUserMemoryBytes != want.PeakUserMemoryBytes || got.ProcessedInputRows != want.ProcessedInputRows {
		t.Errorf("metrics did not round-trip: %+v", got)
	}

	nilTimes, err := s.GetQuery(ctx, rows[0].QueryID)
	if err != nil || nilTimes == nil {
		t.Fatalf("row 0: %v %v", nilTimes, err)
	}
	if nilTimes.ExecutionStartTime != nil || nilTimes.EndTime != nil {
		t.Errorf("nil timestamps must stay nil, got %v %v", nilTimes.ExecutionStartTime, nilTimes.EndTime)
	}

	if missing, err := s.GetQuery(ctx, "20260915_100000_99999_zzzzz"); err != nil || missing != nil {
		t.Errorf("miss must be nil, nil: %v %v", missing, err)
	}
	if bad, err := s.GetQuery(ctx, "nonsense"); err != nil || bad != nil {
		t.Errorf("malformed id must be nil, nil: %v %v", bad, err)
	}
}

func TestClickHouseReplaceAndDedup(t *testing.T) {
	s := newInitialisedClickHouse(t)
	ctx := t.Context()
	base := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	v1 := integrationRow(1, base)
	v1.WallMS = 100
	if err := s.InsertBatch(ctx, []Row{v1}); err != nil {
		t.Fatal(err)
	}
	v2 := v1
	v2.WallMS = 200
	v2.QueryState = "FAILED"
	for range 2 {
		if err := s.InsertBatch(ctx, []Row{v2}); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.GetQuery(ctx, v1.QueryID)
	if err != nil || got == nil {
		t.Fatalf("get: %v %v", got, err)
	}
	if got.WallMS != 200 || got.QueryState != "FAILED" {
		t.Errorf("latest write must win, got wall=%d state=%s", got.WallMS, got.QueryState)
	}
	list, err := s.ListQueries(ctx, QueryFilter{Since: base.Add(-time.Hour), Until: base.Add(time.Hour), Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].WallMS != 200 {
		t.Errorf("list must collapse versions to one row with the latest values, got %+v", list)
	}

	rawCount := countRows(t, s, false)
	if rawCount > 2 {
		t.Errorf("identical retried batch must be deduplicated server-side, raw row count = %d", rawCount)
	}
	if final := countRows(t, s, true); final != 1 {
		t.Errorf("FINAL count = %d, want 1", final)
	}
}

func countRows(t *testing.T, s *ClickHouseStore, final bool) uint64 {
	t.Helper()
	stmt := "SELECT count() FROM " + s.table
	if final {
		stmt += " FINAL"
	}
	rows, err := s.conn.Query(t.Context(), stmt)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var n uint64
	if !rows.Next() {
		t.Fatal("count returned no rows")
	}
	if err := rows.Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestClickHouseFilterValuesAreNotInterpolated(t *testing.T) {
	s := newInitialisedClickHouse(t)
	ctx := t.Context()
	base := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	if err := s.InsertBatch(ctx, integrationRows(6, base)); err != nil {
		t.Fatal(err)
	}
	window := QueryFilter{Since: base.Add(-time.Hour), Until: base.Add(time.Hour), Limit: 100}
	control, err := s.ListQueries(ctx, window)
	if err != nil || len(control) != 6 {
		t.Fatalf("control query: %d rows, %v", len(control), err)
	}
	for _, hostile := range []string{`' OR 1=1 --`, `alice\`, `\'`, `{p1:String}`, `alice' OR user_name = 'bob`, "alice\x00", "alice\nbob", "a\tb"} {
		for _, f := range []QueryFilter{
			{Since: window.Since, Until: window.Until, Limit: 100, User: hostile},
			{Since: window.Since, Until: window.Until, Limit: 100, Catalog: hostile},
			{Since: window.Since, Until: window.Until, Limit: 100, State: hostile},
		} {
			got, err := s.ListQueries(ctx, f)
			if err != nil {
				t.Errorf("hostile filter %q errored: %v", hostile, err)
			}
			if len(got) != 0 {
				t.Errorf("hostile filter %q matched %d rows", hostile, len(got))
			}
		}
		if row, err := s.GetQuery(ctx, "20260915_"+hostile); err != nil || row != nil {
			t.Errorf("hostile id %q: %v %v", hostile, row, err)
		}
	}
}

func TestClickHouseAwkwardFilterValuesRoundTrip(t *testing.T) {
	s := newInitialisedClickHouse(t)
	ctx := t.Context()
	base := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	rows := integrationRows(4, base)
	awkward := []string{`DOMAIN\user`, "it's", "tab\there", "new\nline", `back\\slash'quote`}
	for i, v := range awkward {
		rows[i%len(rows)].UserName = v
	}
	rows[3].Catalog = awkward[4]
	if err := s.InsertBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	window := QueryFilter{Since: base.Add(-time.Hour), Until: base.Add(time.Hour), Limit: 100}
	for _, want := range rows {
		f := window
		f.User, f.Catalog = want.UserName, want.Catalog
		got, err := s.ListQueries(ctx, f)
		if err != nil {
			t.Fatalf("filter %q/%q: %v", want.UserName, want.Catalog, err)
		}
		if len(got) != 1 || got[0].QueryID != want.QueryID || got[0].UserName != want.UserName {
			t.Errorf("filter %q/%q returned %v", want.UserName, want.Catalog, ids(got))
		}
	}
}

func TestClickHouseMultiChunkBatch(t *testing.T) {
	s := newInitialisedClickHouse(t)
	s.maxBatchBytes = 8_000
	ctx := t.Context()
	base := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	rows := integrationRows(12, base)
	for i := range rows {
		rows[i].QueryText = strings.Repeat("x", 3_000)
	}
	if n := len(s.SplitBatch(rows)); n < 4 {
		t.Fatalf("test needs a multi-chunk batch, got %d chunk(s)", n)
	}
	if err := s.InsertBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, s, true); got != uint64(len(rows)) {
		t.Errorf("committed %d rows, want %d", got, len(rows))
	}
}

func TestClickHousePrune(t *testing.T) {
	s := newInitialisedClickHouse(t)
	ctx := t.Context()
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	var rows []Row
	for d := -3; d <= 0; d++ {
		rows = append(rows, integrationRow(10+d, day.AddDate(0, 0, d).Add(6*time.Hour)))
	}
	if err := s.InsertBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	remaining := func() []string {
		list, err := s.ListQueries(ctx, QueryFilter{Since: day.AddDate(0, 0, -10), Until: day.AddDate(0, 0, 1), Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		return ids(list)
	}

	if err := s.Prune(ctx, day.AddDate(0, 0, -1)); err != nil {
		t.Fatal(err)
	}
	if got := remaining(); len(got) != 2 || got[0] != rows[2].QueryID || got[1] != rows[3].QueryID {
		t.Errorf("aligned prune kept %v, want the last two days", got)
	}

	if err := s.Prune(ctx, day.Add(-12*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := remaining(); len(got) != 2 {
		t.Errorf("non-aligned cutoff must prune at day granularity, kept %v", got)
	}

	if err := s.Prune(ctx, day.AddDate(0, 0, 5)); err != nil {
		t.Fatal(err)
	}
	if got := remaining(); len(got) != 0 {
		t.Errorf("future cutoff must drop everything, kept %v", got)
	}
	if err := s.Prune(ctx, day.AddDate(0, 0, 5)); err != nil {
		t.Errorf("pruning an empty table must succeed: %v", err)
	}
}

func TestClickHouseMissingTableIsNonRetryable(t *testing.T) {
	s := newTestClickHouse(t)
	if err := s.conn.Exec(t.Context(), s.DatabaseDDL()); err != nil {
		t.Fatal(err)
	}
	err := s.InsertBatch(t.Context(), integrationRows(1, time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)))
	if err == nil {
		t.Fatal("insert into a missing table must fail")
	}
	if !errors.Is(err, ErrNonRetryable) {
		t.Errorf("missing table must be non-retryable, got %v", err)
	}
	var ex *clickhouse.Exception
	if !errors.As(err, &ex) {
		t.Errorf("server exception must stay in the chain: %v", err)
	}
}
