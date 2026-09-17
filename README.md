# trino-query-log-sink

Trino query coordinator shows statistics about queries in the last 10 minutes,
for clusters with high churn rate that is not an enough window to review queries performance,
Trino offers a way to register an [events listener](https://trino.io/docs/current/admin/event-listeners-http.html) that receives the events encoded in json format,
this app can be registered as an event listener to receive and persist these events either on a gcs bucket
in [iceberg](https://trino.io/docs/current/connector/iceberg.html) format (written and read through Trino) or in a
self-hosted [ClickHouse](https://clickhouse.com/) (see [ClickHouse backend](#clickhouse-backend)),
and provide a simple UI to browse the performance and query plans for each event. The UI, JSON API and CLI
behave the same with either backend; `STORE_BACKEND` selects it.

![Trino Query Log dashboard — last-7-days queries with filters, "top by" sorting, and per-query stats](ui_screenshot.png)

The app can run in 2 modes, first as a single app for both events consumption and UI using the `serve` command,
or in two separate apps one for ingestion use `ingest` command and one for ui using `ui` command.

## Subcommands

The single binary has several modes (configuration always comes from the environment):

| Command | Purpose |
|---------|---------|
| `serve` (default) | Run the ingest endpoint **and** the web UI in one process. |
| `ingest` | Run only the ingest endpoint (write path) — for a separate ingest deployment. |
| `ui` | Run only the read UI (read path) — for a separate, independently-scaled deployment. |
| `init` | Apply the schema + table DDL once (needs `CREATE` privileges). |
| `prune` | Delete rows older than the `RETENTION_DAYS` cutoff (run as a CronJob). |
| `optimize` | Compact the last `OPTIMIZE_DAYS` days of data files (CronJob; Trino backend only, a no-op otherwise). |
| `maintain` | Expire snapshots + remove orphan files older than `MAINTAIN_RETENTION` (CronJob; Trino backend only, a no-op otherwise). |
| `ddl` | Print the DDL to stdout. |

`serve` is the simplest single-deployment option; `ingest` + `ui` split the write
and read paths so the UI can scale and restart without touching ingestion. All
three share the same configuration; `/healthz`, `/readyz`, and `/metrics` are
present in every mode, `/ingest` only in `serve`/`ingest`, and the UI routes only
in `serve`/`ui`.

> **Why `optimize` and `maintain`?** (Trino/Iceberg backend only.) Writing one INSERT per flush through Trino
> creates many small Iceberg files and a snapshot per commit, and `prune`'s
> `DELETE` writes delete-files rather than reclaiming space.
> [`optimize`](https://trino.io/docs/current/connector/iceberg.html#optimize)
> rewrites the recent partitions into fewer, bigger files;
> [`expire_snapshots`](https://trino.io/docs/current/connector/iceberg.html#expire-snapshots) +
> [`remove_orphan_files`](https://trino.io/docs/current/connector/iceberg.html#remove-orphan-files)
> (the `maintain` job) then drop the superseded snapshots and delete the files
> nothing references, which is what actually frees storage. Run them as separate
> CronJobs, scheduled after `prune`.
>
> Every live snapshot is serialized into the table's `metadata.json`, which is
> re-read on each `loadTable`, so a long `MAINTAIN_RETENTION` on a
> high-commit-rate table inflates both metadata size and planning time. Keep it
> just above the longest in-flight reader (`TRINO_QUERY_TIMEOUT`) — shortening it
> deletes no rows, it only shrinks time-travel depth.

## Endpoints

- `POST /ingest` — accepts a [`QueryCompletedEvent`](https://trino.io/docs/current/admin/event-listeners-http.html); responds `202` quickly.
- `GET /` — dashboard: filter by time range / user / catalog / state, sort by
  start / wall / CPU / bytes scanned / peak memory / output rows, "top by"
  shortcuts, and a failed-only toggle.
- `GET /query/{queryId}` — full SQL, statistics, plan(s), per-table input bytes,
  and failure details.
- `GET /api/v1/queries` — JSON list of query summaries for programmatic use (CLI, etc.);
  same filters as the dashboard (`range`, `user`, `catalog`, `state`, `sort`, `dir`) plus
  `limit` (default 100, max 500) and `offset` (max 100000). Returns
  `{ "queries": [...], "count", "limit", "offset", "hasNext" }`. Each summary carries
  `queryPreview` — the truncated SQL prefix stored at ingest — not the full `queryText`.
- `GET /api/v1/queries/{queryId}` — JSON detail for one query (all statistics, `plan`,
  `jsonPlan`, and parsed `inputs`); `404` if the id is unknown.
- `GET /healthz` — liveness. `GET /readyz` — reflects the last background
  readiness probe, whose cost is set by `READINESS_MODE` (by default a bare
  `SELECT 1` that never plans against the Iceberg table). `GET /metrics` —
  Prometheus counters (if enabled).

## Configuration

| Variable | Default | Notes |
|----------|---------|-------|
| `LISTEN_ADDR` | `:8080` | HTTP listen address. |
| `STORE_BACKEND` | `trino` | Where rows are stored and read from: `trino` (Iceberg through Trino) or `clickhouse`. `TRINO_*` settings only matter for `trino`, `CLICKHOUSE_*` only for `clickhouse`; `TRINO_SOURCE` is used for event suppression with either. |
| `CLICKHOUSE_ADDR` | *(empty)* | ClickHouse native-protocol address (`host:port`); required when `STORE_BACKEND=clickhouse`. |
| `CLICKHOUSE_DATABASE` | `observability` | Database holding the query-log table; plain identifier (letters, digits, underscore). |
| `CLICKHOUSE_TABLE` | `trino_query_log` | Table name; plain identifier. |
| `CLICKHOUSE_USER` | `default` | ClickHouse user. |
| `CLICKHOUSE_PASSWORD` | *(empty)* | ClickHouse password (set it from a Secret). |
| `CLICKHOUSE_TLS` | `false` | Connect over TLS. A password without TLS is sent in cleartext and logged as a warning at startup. |
| `CLICKHOUSE_TLS_INSECURE` | `false` | Skip server certificate verification; only valid with `CLICKHOUSE_TLS=true`. |
| `CLICKHOUSE_TLS_CA` | *(empty)* | PEM file with the CA that signed the server certificate; only valid with `CLICKHOUSE_TLS=true`. |
| `CLICKHOUSE_DIAL_TIMEOUT` | `5s` | Connection establishment deadline. |
| `CLICKHOUSE_QUERY_TIMEOUT` | `60s` | Per-query deadline (client read timeout and server `max_execution_time`); also bounds each flush. |
| `CLICKHOUSE_MAX_BATCH_BYTES` | `8388608` | Estimated byte budget per INSERT batch; larger flushes are split into several batches. A batch is held in memory twice while it is encoded, so keep this small relative to the pod's memory limit. Minimum 1 MiB. |
| `TRINO_HOST` | `trino-cluster-trino.trino` | Coordinator host. |
| `TRINO_PORT` | `8080` | Coordinator HTTP port. |
| `TRINO_USER` | `trino-query-log` | Trino user. |
| `TRINO_SOURCE` | `trino-query-log` | Session source; events with this source are suppressed. |
| `TRINO_CATALOG` | `gravitino` | Iceberg catalog. |
| `TRINO_SCHEMA` | `observability` | Schema. |
| `TRINO_TABLE` | `trino_query_log` | Table. |
| `ICEBERG_LOCATION` | *(empty)* | Warehouse path (e.g. `gs://bucket/observability`) used by `init`/`ddl`. Trino backend only. |
| `BATCH_SIZE` | `100` | Max rows per flush. |
| `FLUSH_INTERVAL` | `5s` | Max time before a partial batch flushes (manifests set `10m`). |
| `BUFFER_CAPACITY` | `10000` | In-memory queue depth; events are dropped when full. |
| `FLUSH_MAX_RETRIES` | `3` | Retries per failed flush before the batch is dropped. Non-retryable errors (e.g. statement too large, missing table, bad credentials) skip retries and are counted in `flush_non_retryable_total`. |
| `FLUSH_RETRY_BACKOFF` | `1s` | Delay before the first retry; it doubles on every retry. |
| `FLUSH_RETRY_MAX_BACKOFF` | `30s` | Cap on the retry delay. With the defaults a batch is retried for ~7 s; raise `FLUSH_MAX_RETRIES` (e.g. `6` ≈ 1 min) to ride out a store restart. Events keep queueing in the buffer meanwhile. |
| `MAX_STATEMENT_BYTES` | `700000` | Size budget per INSERT statement; each flush is split into chunks under it. Keep headroom below the cluster's `query.max-length` (default 1MB). Trino backend only. |
| `MAX_FIELD_BYTES` | `300000` | Per-field cap applied at ingest to `query_text`, `plan`, `json_plan`, `inputs_json`, and `error_message`; oversized values are truncated with a `[truncated N bytes]` marker. All other string fields are capped at 16KB. |
| `PLAN_CAPTURE` | `slow_or_failed` | Which events keep their `plan`/`json_plan` bodies: `all`, `slow_or_failed` (failed, or wall time ≥ `PLAN_CAPTURE_MIN_WALL`), or `none`. Plans dominate row size, so this is the main lever on written bytes and Iceberg file growth. |
| `PLAN_CAPTURE_MIN_WALL` | `10s` | Wall-time threshold that makes a successful query "slow" enough to keep its plans under `PLAN_CAPTURE=slow_or_failed`. Ignored by the other modes. |
| `QUERY_PREVIEW_BYTES` | `200` | Size cap for the `query_preview` column — a whitespace-collapsed prefix of `query_text` written at ingest. The dashboard and `/api/v1/queries` read the preview, so listing never scans the full SQL. |
| `RETENTION_DAYS` | `7` | Whole UTC days of rows kept *before* today, used by `prune`; the cutoff is midnight-aligned, so `2` keeps today plus the two previous UTC days and `0` keeps today only. Manifests set `2`. |
| `OPTIMIZE_DAYS` | `1` | Trino backend only. Days covered by `optimize`, **including today**: `1` compacts today only, `2` also compacts yesterday. The window starts at UTC midnight. |
| `MAINTAIN_RETENTION` | `7d` | Trino backend only. Snapshot/orphan age threshold for `maintain`. Must be ≥ the catalog's `iceberg.expire-snapshots.min-retention` and `iceberg.remove-orphan-files.min-retention` (both default `7d`) or Trino rejects the procedure. Manifests set `6h` against a catalog lowered to `1h`/`6h`. |
| `READINESS_MODE` | `connection` | What `/readyz` probes: `connection` runs a bare `SELECT 1`; `table` also plans a read of the query-log table, which loads its Iceberg metadata and can take tens of seconds on a large table (on ClickHouse it is cheap, so `table` is the better choice there). |
| `READINESS_INTERVAL` | `30s` | Gap between probes while the service is ready. |
| `READINESS_FAIL_INTERVAL` | `30s` | Gap between probes while it is not ready. |
| `READINESS_TIMEOUT` | `10s` | Per-probe deadline; a probe that exceeds it counts as not ready. |
| `METRICS_ENABLED` | `true` | Expose `/metrics` (Prometheus text). |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error`. |
| `TRINO_PASSWORD` | *(empty)* | Optional [basic-auth password](https://trino.io/docs/current/security/password-file.html). |
| `TRINO_ACCESS_TOKEN` | *(empty)* | Optional [bearer/JWT token](https://trino.io/docs/current/security/jwt.html). |
| `TRINO_SSL` | `false` | Use [HTTPS/TLS](https://trino.io/docs/current/security/tls.html) to the coordinator. |
| `TRINO_QUERY_TIMEOUT` | `60s` | Per-query timeout; also bounds each flush. Trino backend only. |
| `REDIS_ADDR` | *(empty)* | Optional read-through cache backend (`host:6379`). **Empty disables the cache** and the service reads Trino directly. |
| `REDIS_PASSWORD` | *(empty)* | Cache password / Memorystore AUTH string. |
| `REDIS_DB` | `0` | Cache logical database. |
| `REDIS_TLS` | `false` | Connect to the cache over TLS (e.g. Memorystore in-transit encryption). |
| `CACHE_LIST_TTL` | `10m` | Dashboard/list result TTL. |
| `CACHE_QUERY_TTL` | `1h` | Per-query detail TTL (completed queries are immutable). |
| `CACHE_TIMEOUT` | `250ms` | Per-operation cache deadline; bounds added latency when the cache is slow/down. |

## Quick start (local, against a port-forwarded Trino)

```bash
# 1. Reach the in-cluster coordinator.
kubectl -n trino port-forward svc/trino-cluster-trino 8080:8080 &

# 2. Build.
go build -o bin/trino-query-log-sink ./cmd/trino-query-log-sink

# 3. Create the schema + table (needs CREATE privileges).
TRINO_HOST=localhost ICEBERG_LOCATION=gs://your-warehouse/observability \
  ./bin/trino-query-log-sink init

# 4. Run the service.
TRINO_HOST=localhost ./bin/trino-query-log-sink serve

# 5. Post a sample event and open the UI.
scripts/curl-ingest.sh http://localhost:8080
open http://localhost:8080
```

To only see the DDL: `./bin/trino-query-log-sink ddl` (or apply
[`ddl/trino_query_log.sql`](ddl/trino_query_log.sql) by hand).

## Deploy

Manifests in [`deploy/k8s`](deploy/k8s) target namespace `trino`. Edit the image
reference and the `ConfigMap`/`Secret`, then apply the config, deployment, and service:

```bash
kubectl apply -f deploy/k8s/configmap.yaml -f deploy/k8s/secret.yaml
kubectl apply -f deploy/k8s/deployment.yaml -f deploy/k8s/service.yaml
```

The Service is named **`trino-query-log-sink`**; point Trino's
[event listener](https://trino.io/docs/current/admin/event-listeners-http.html) at
`http://trino-query-log-sink.trino:8080/ingest`. Browse the UI with
`kubectl -n trino port-forward svc/trino-query-log-sink 8080:8080`.

## ClickHouse backend

Set `STORE_BACKEND=clickhouse` and `CLICKHOUSE_ADDR` and the sink talks to
ClickHouse directly over the native protocol; Trino is then only the event
source and receives no queries from the sink. Everything above the store is
unchanged: the UI, `/api/v1`, the `query` CLI, `prune`, `/readyz`, `/metrics`.

- **Schema**: `init` creates the database and a `ReplacingMergeTree` table
  partitioned by day and ordered by `(create_time, query_id)`; `ddl` prints it
  ([`ddl/clickhouse_query_log.sql`](ddl/clickhouse_query_log.sql)). The five
  large text columns use ZSTD codecs; an `ingested_at` version column makes the
  latest write for a `(create_time, query_id)` pair win, and reads use `FINAL`,
  so a redelivered or retried event (same id, same create time) never shows up
  twice. Each batch also carries a content-derived `insert_deduplication_token`,
  so an identical retry is dropped by the server.
- **Ingest**: one batch per flush (split when it exceeds
  `CLICKHOUSE_MAX_BATCH_BYTES`), no statement-size limit, so
  `FLUSH_INTERVAL` can be much shorter than with Trino (e.g. `30s`).
  Deterministic server errors (unknown table, type mismatch, access denied, ...)
  are not retried.
- **Retention**: `prune` drops whole day partitions older than the
  `RETENTION_DAYS` cutoff (space is reclaimed immediately). `optimize` and
  `maintain` exit without doing anything; their CronJobs can be removed. If you
  prefer server-side expiry, add a `TTL` to the table yourself and stop running
  `prune`.
- **Readiness**: use `READINESS_MODE=table`; `SELECT 1 FROM ... WHERE 0` is
  free on ClickHouse and, unlike a bare ping, notices a lost database.
- **Grants** (least privilege): the serving user needs `INSERT, SELECT` on the
  table; the `prune` user additionally needs `ALTER` on the table and `SELECT`
  on `system.parts`; `init` needs `CREATE DATABASE` / `CREATE TABLE`. Put
  `CLICKHOUSE_PASSWORD` in the Secret, never in the ConfigMap, and enable
  `CLICKHOUSE_TLS` whenever the connection leaves the namespace.
- **Schema evolution**: any column added later with `ALTER TABLE ... ADD COLUMN`
  must carry a `DEFAULT`, because reads do not `COALESCE` on this backend.
- **Durability**: a single ClickHouse node keeps the data on its own volume, not
  on object storage, and does not fsync freshly inserted parts by default, so a
  node crash can lose the last few seconds and a lost volume loses everything.
  The sink's in-memory buffer does not survive a ClickHouse restart either:
  a flush that exhausts `FLUSH_MAX_RETRIES` is dropped. With day-scale
  observability data that is usually acceptable; if not, raise the retry budget
  and schedule `BACKUP TABLE ... TO S3(...)` against your bucket.
- **Deploying ClickHouse**: [`deploy/k8s/clickhouse.yaml`](deploy/k8s/clickhouse.yaml)
  is a single-node StatefulSet (20 GiB `standard-rwo` volume, non-spot nodes,
  Prometheus on 9363, no Keeper or operator); create the
  `trino-query-log-clickhouse` Secret with `CLICKHOUSE_PASSWORD` first, as the
  file's header shows. The image entrypoint creates the `trino-query-log` user
  and removes the passwordless `default` user. Disk: measured on production
  rows, an ordinary query costs ~0.7 KB compressed and a slow query with plans
  ~17 KB, so today's ~76k events/day need ~65 MB/day, about 2 GB for 30 days;
  the 20 GiB volume leaves room for growth, merges and ClickHouse's own
  `query_log`/`part_log` (the heavier per-second system logs are disabled in
  the ConfigMap).
- **Cutover from Trino**: deploy ClickHouse, run `init`, flip `STORE_BACKEND`
  (and set `READINESS_MODE=table`). From that moment `prune`, `optimize` and
  `maintain` no longer touch the Iceberg table, so it stops being pruned and
  compacted: either drop it, or keep one `prune` CronJob pinned to
  `STORE_BACKEND=trino` until it is empty. History is not migrated, so the
  dashboard starts empty; switching the variable back is the rollback while the
  old table still has data.

## Query CLI

The same binary has a `query` client that reads the JSON API of a **running**
instance and prints terminal-friendly output (it never dumps full query plans into your terminal/context unless asked).
It only talks to the API — no direct Trino connection — so it needs no
Trino/env config.

```bash
# list — recent queries, sorted (start|wall|cpu|bytes|mem|rows)
trino-query-log-sink query list --url http://localhost:8080 --sort cpu --limit 5

# get — one query's stats + input tables (no plan body)
trino-query-log-sink query get <queryId> --url http://localhost:8080

# plan — top CPU operators, parsed from the text plan; --raw for the full plan
trino-query-log-sink query plan <queryId> --url http://localhost:8080 --top 10
```

From the image (the client ships in the same binary):

```bash
docker run --rm ghcr.io/shadi/trino-log-sink:TAG \
  query list --url http://trino-query-log-sink.trino:8080
```

Common flags (all subcommands): `--url` (required), `-H/--header "Key: Value"`
(repeatable), `--token-file PATH` (sent as `Authorization: Bearer <token>`, or use
`--token-header NAME` to send the raw token in a custom header), `--timeout`,
`--insecure`, and `-o/--output table|json`. `list` also takes `--range`, `--user`,
`--catalog`, `--state`, `--sort`, `--desc`, `--limit`, `--offset`. Exit codes:
`0` ok, `1` error, `4` query not found.

### For AI agent

Every command's output is small and bounded by design, so it won't flood the
context window:

- Start with `query list --sort cpu --limit 10` to find the expensive query id.
- `query get <id>` for stats + input tables — it **omits** the query plan.
- `query plan <id>` for the top CPU operators (a few lines), not the whole plan.
- Avoid `--raw` (dumps the full multi-KB plan) unless you truly need it.
- `-o json` stays concise too — the `get` JSON omits the `plan`/`jsonPlan` bodies.
- Branch on exit codes (`0`/`1`/`4`) instead of parsing prose; errors go to stderr.

## Development

```bash
go test ./...          # unit tests (no Trino required) + ClickHouse integration tests
go test -race ./...    # the buffer is concurrency-sensitive
go test -short ./...   # skip the ClickHouse integration tests
go vet ./...
```

The ClickHouse integration tests start a throwaway `clickhouse-server` with
[podman](https://podman.io/) (one container per test run, one database per test)
and skip with a message when `podman` is not on `PATH` or under `-short`.
`CLICKHOUSE_TEST_IMAGE` overrides the image (default
`docker.io/clickhouse/clickhouse-server:26.8`); `CLICKHOUSE_TEST_ADDR` (plus
`CLICKHOUSE_TEST_USER` / `CLICKHOUSE_TEST_PASSWORD`) points them at an existing
server instead, and `CLICKHOUSE_TEST_REQUIRED=1` turns a skip into a failure.
CI runs `go test -short`, so it exercises no containers; run the full suite
locally before changing the ClickHouse store or bumping its driver.
