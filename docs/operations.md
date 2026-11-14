---
title: Operations
section: Operations
order: 100
summary: "Running marco serve: the two listeners, every flag and its env var, the metrics, profiling and tracing, and the console."
---

```hero
diagram:
  caption: "Two listeners. The API listener is public: Graph reads, the docs and the health checks. The admin listener stays private: Admin, /metrics, /debug and the console, behind the token."
  nodes:
    - { id: app, label: appview, at: [0, 0], size: [8, 3] }
    - { id: op, label: operator, sub: "token or proxy login", at: [0, 5.5], size: [8, 3] }
    - { id: api, label: ":7300", sub: "Graph · /docs · /readyz", at: [13, 0], size: [10, 3], tone: blue }
    - { id: adm, label: "127.0.0.1:7301", sub: "Admin · /metrics · /admin", at: [13, 5.5], size: [10, 3], tone: violet }
    - { id: m, label: marco serve, at: [28, 2.75], size: [8, 3], tone: accent }
  edges:
    - "app -> api: reads"
    - "op -> adm"
    - "api.r -> m.l25"
    - "adm.r -> m.l75"
facts:
  - { value: ":7300", label: "--listen", note: "Graph, open reads · docs", tone: blue }
  - { value: ":7301", label: "--admin-listen", note: "loopback · token", tone: violet }
  - { value: "16", label: "token characters at least", note: "`--admin-token-file`", tone: rust }
  - { value: "0", label: trace ratio by default, note: "`--trace-ratio` · `--otlp`", tone: muted }
```

`marco serve` is one process with two listeners. Bind the admin listener to loopback or a private
network, and put the API listener wherever your appview can reach it. The process needs its
snapshot directory on local disk with room for half again its size (checkpoints add patches, and
compaction lets a shard's files reach `--compaction-ratio` of what it holds, plus the one shard it
rewrites), and `--memory` for marco's own cache of the snapshot's pages, which it reads with
`O_DIRECT` and checks against their sums ([Sizing](sizing.md)).

> [!WARNING]
> Run one `marco serve` per snapshot directory. Nothing locks it, so two writable processes on one
> directory (an overlapping rolling restart, say) delete each other's temp files and write the
> same segment numbers. Stop the old process before the new one opens the directory.

## Listeners and auth

| | API (`--listen`) | Admin (`--admin-listen`) |
|---|---|---|
| Graph reads | open, gated on ready | open, always answer |
| Graph writes | token | token or proxy login |
| `marco.v1.Admin` | no | token or proxy login (`GetSession` is open) |
| `/metrics`, `/debug/*` | no | token or proxy login |
| `/admin/checkpoint` (checkpoint export) | no | token or proxy login |
| `/admin` (console) | no | the page is open, its data needs a login |
| `/docs`, `/healthz`, `/readyz` | open | open |
| `/` | the landing page and explorer | redirects to `/admin/` |
| `/resolve?handle=` | with `--handle-resolver` | no |

The API listener's `/` is a landing page with an explorer. It runs the Graph reads against the same
origin and shows each response, its latency and the matching `curl`. With `--handle-resolver` the
explorer takes handles too: `/resolve?handle=` asks that host, caches the answer for an hour (a miss
for 5 minutes) and makes at most 10 lookups a second across all visitors. Without it, `/resolve` is
404 and the explorer takes DIDs.

With `--api-read-only` the API listener has no write routes at all: `Write` and `WriteBatch`
answer 404 whatever the token, so a public API listener can't be written to even if the token
leaks. Jetstream or the firehose keep the graph current, and the admin listener still takes writes.

The token is the contents of `--admin-token-file`, at least 16 characters, sent as
`Authorization: Bearer <token>`. Without a token file or a proxy header, Admin and Graph writes
are off. Scrape `/metrics` with the bearer token too.

Behind a reverse proxy that signs operators in, `--admin-proxy-header` names the header carrying
the login, `--admin-proxy-from` the proxy's own addresses (CIDRs, the header from anywhere else is
ignored), and `--admin-operators` the logins allowed. A proxy login's writes work only from the
console's own page (same-origin), so a link or another site can't use it to change anything.
Reads through the proxy (Admin reads, Graph reads, `/metrics`) also work from curl, on purpose.

When the proxy's address also carries other callers' streams (a service mesh whose proxy delivers
every caller's connections from its own address), the address can't say which caller set the
header. `--proxy-protocol-from` names the addresses whose connections, on both listeners, start
with a PROXY protocol v2 header naming the caller in TLV `0xE0` (a SPIFFE ID, say), and
`--admin-proxy-peers` the callers whose header login is believed. A login from any other caller is
refused with 403, and a connection from those addresses without a good header is closed before
any HTTP is read (`marco_proxy_header_refused_total`). Only the proxy may be able to reach the
listeners from those addresses, or the header proves nothing.

## Flags

Every flag has an environment variable. A flag on the command line wins over the env. The
console's Settings page shows each one's value and where it came from.

| Flag | Env | Default | |
|---|---|---|---|
| `--snapshot DIR` | `MARCO_SNAPSHOT` | required | The snapshot directory. |
| `--listen` | `MARCO_LISTEN` | `0.0.0.0:7300` | The API listener. |
| `--admin-listen` | `MARCO_ADMIN_LISTEN` | `127.0.0.1:7301` | The admin listener. Keep it private. |
| `--ops-listen` | `MARCO_OPS_LISTEN` | off | `/metrics` and `/debug/pprof/{profile,heap}` with no token, for a scraper. Keep it private. |
| `--admin-token-file` | `MARCO_ADMIN_TOKEN_FILE` | | The admin token. |
| `--admin-proxy-header` | `MARCO_ADMIN_PROXY_HEADER` | | A header a proxy sets to the operator's login. |
| `--admin-proxy-from` | `MARCO_ADMIN_PROXY_FROM` | | The proxy's addresses, comma-separated CIDRs. |
| `--admin-operators` | `MARCO_ADMIN_OPERATORS` | | The logins the header may name, comma-separated. |
| `--proxy-protocol-from` | `MARCO_PROXY_PROTOCOL_FROM` | | Addresses (CIDRs) whose connections start with a PROXY v2 header naming the caller. |
| `--admin-proxy-peers` | `MARCO_ADMIN_PROXY_PEERS` | | The callers (TLV `0xE0`) whose header login is believed, comma-separated. |
| `--jetstream URL` | `MARCO_JETSTREAM` | | Follow a Jetstream instance. |
| `--jetstream-v2 URL` | `MARCO_JETSTREAM_V2` | | Follow a Jetstream v2 instance (`subscribeEvents`) by seq. Conflicts with `--jetstream` and `--firehose`. |
| `--firehose URL` | `MARCO_FIREHOSE` | | Follow a relay's `subscribeRepos`. Conflicts with `--jetstream` and `--jetstream-v2`. |
| `--source-retention-hours` | `MARCO_SOURCE_RETENTION_HOURS` | 36 | How far back the Jetstream (v1 or v2) instance replays. An older bulk-built snapshot is refused. |
| `--allow-gap` | `MARCO_ALLOW_GAP` | off | Follow past the retention, or a firehose from a bulk-built snapshot. |
| `--read-only` | `MARCO_READ_ONLY` | off | Refuse every write. Can't follow a source. |
| `--api-read-only` | `MARCO_API_READ_ONLY` | off | Don't route Graph writes on the API listener (404, token or not). The admin listener and the sources still write. |
| `--serve-before-ready` | `MARCO_SERVE_BEFORE_READY` | off | Answer Graph reads before ready. |
| `--segment-interval` | `MARCO_SEGMENT_INTERVAL` | 5 | Seconds between change segments: what a kill -9 can lose. |
| `--checkpoint-ops` | `MARCO_CHECKPOINT_OPS` | 3000000 | Checkpoint once the segments since the last hold this many ops. |
| `--checkpoint-age` | `MARCO_CHECKPOINT_AGE` | 86400 | …or the last is this many seconds old. |
| `--tombstone-days` | `MARCO_TOMBSTONE_DAYS` | 7 | Checkpoints drop tombstones older than this. |
| `--compaction-ratio` | `MARCO_COMPACTION_RATIO` | 1.43 | Compact a shard once its files are this many times what its index still names. 0: only when a space nears its cap. |
| `--compaction-min-garbage-mb` | `MARCO_COMPACTION_MIN_GARBAGE_MB` | 64 | …and it holds at least this much garbage. |
| `--background-io` | `MARCO_BACKGROUND_IO` | `200,5000` | The pace of background I/O, MB a second then operations a second (0: unpaced): compactions, checkpoint writes, verify and list stats. Requests go first. |
| `--merge-ratio` | `MARCO_MERGE_RATIO` | 0.125 | With `--compact-min`, a list's delta merges past this share of its base. |
| `--compact-min` | `MARCO_COMPACT_MIN` | 0 | Merge a list's delta inside the write past this many entries. 0 leaves it to checkpoints. |
| `--memory` | `MARCO_MEMORY` | | Required: the store's budget (`13GiB`, `6G`), its pinned sections, page cache and tables. Pages are read with `O_DIRECT` into the cache, every one checked against its sum; when the budget holds them all, open loads every page. It must hold the pinned sections, the shards' index and heads a second time (a checkpoint reads them again beside the old until its switch), and 2 GiB more, or marco won't start. The graph's deltas and the process live outside it, up to what `--memory-limit` leaves. |
| `--memory-limit` | `MARCO_MEMORY_LIMIT` | the cgroup's `memory.max` | The whole process's limit (`8GiB`, or `none`). What the process holds beside `--memory` (the graph's deltas in both served copies, a checkpoint's replay, requests) past the limit less `--memory` less 512 MiB comes out of the cache, so a day of writes and its checkpoint stay under it. `marco_process_beside_bytes` is what it holds, `marco_store_bytes{part="reserved"}` what the cache gave up for it. |
| `--miss-threads` | `MARCO_MISS_THREADS` | 64 | The threads a request that misses the cache moves to. A request that waits over a second for one gets `resource_exhausted`. |
| `--trace-ratio` | `MARCO_TRACE_RATIO` | 0 | The share of ops that get a span, 0 to 1. |
| `--otlp URL` | `MARCO_OTLP` | | An OTLP/HTTP collector's base URL for spans. |
| `--ui-dir` | `MARCO_UI_DIR` | the embedded build | A built console and docs (`ui/dist`) to serve instead of the ones built into the binary. |
| `--public-url` | `MARCO_PUBLIC_URL` | | The API listener's public origin (`https://marco.example.com`). The docs' and landing page's card tags use it, and the docs' read examples point at it. |
| `--handle-resolver` | `MARCO_HANDLE_RESOLVER` | off | An XRPC host for `com.atproto.identity.resolveHandle` (`https://public.api.bsky.app`), behind `/resolve` for the explorer. |

`marco synth --out DIR [--actors N] [--shards N]` writes a synthetic snapshot with fake DIDs, for
trying the console and the API without data.

## Health

- `/healthz`: `ok` while the process is up.
- `/readyz`: 200 `ready` once every configured source has caught up since start, else 503 with
  why ([the ready gate](durability.md#the-ready-gate)). It also answers 503 `storage failing`
  while 64 or more distinct pages have failed their reads on the device (EIO after retries, or
  short) in the last 60 s: the disk is going. A few bad pages leave it ready; their requests fail
  with `data_loss` or `unavailable` naming the page.

## Metrics

`/metrics` on the admin listener is Prometheus text, with the process's own `process_*` series.
A scraper that can't send the token reads the same text from `--ops-listen`, which serves only
`/metrics`, the pprof GETs and `/healthz`.

| Metric | Labels | |
|---|---|---|
| `marco_rpc_seconds` | `rpc`, `path` | RPC time in the server, request decoded to response encoded; `path` is `fast` (served on its worker) or `pool` (moved to the miss pool) |
| `marco_rpc_errors_total` | `rpc`, `status` | RPCs that failed |
| `marco_proxy_header_refused_total` | | Connections from `--proxy-protocol-from` closed for a missing or bad PROXY v2 header |
| `marco_op_seconds` | `op`, `rel` | API op latency: `has`, `relationships`, `out_list`, `in_list`, `counts`, `intersect`, `filter`, `add`, `delete` |
| `marco_op_results_total` | `op`, `rel` | Items API ops returned |
| `marco_applied_total` | `rel`, `outcome` | Writes applied, by outcome (`added`, `exists`, `removed`…) |
| `marco_ready` | | 1 once ready |
| `marco_reads_before_ready_total` | | Graph reads that came before ready |
| `marco_ingest_lag_seconds` | `source` | Now minus the event's time when its change is applied |
| `marco_ingest_behind_microseconds` | `source` | Now minus the newest applied event's time |
| `marco_ingest_cursor` | `source` | The newest applied cursor (`time_us` or `seq`) |
| `marco_ingest_connected` | `source` | 1 while the stream is open |
| `marco_ingest_events_total` | `source`, `kind` | Events read |
| `marco_ingest_ops_total` | `source`, `op` | Ops applied (`add`, `delete`, `status`) |
| `marco_ingest_skipped_ops_total` | `source`, `reason` | Ops marco couldn't use (`bad_subject`, `bad_list`, `missing_block`…) |
| `marco_ingest_decode_errors_total` | `source`, `reason` | Events that didn't decode |
| `marco_ingest_reconnects_total` | `source`, `reason` | Streams dialled again |
| `marco_ingest_apply_errors_total` | `source` | Sink writes that failed and were retried |
| `marco_ingest_sync_events_total` | `source` | `#sync` events read (a repo reset), which marco doesn't resync |
| `marco_ingest_deduped_events_total` | `source` | Jetstream v2 events at or below the last seq read, dropped (the overlap of a resume) |
| `marco_ingest_cursor_notices_total` | `source`, `name` | Jetstream v2 `#info` cursor notices: `OutdatedCursor` (events may be missing), `FutureCursor` |
| `marco_ingest_cursor_too_old_total` | `source` | Dials a Jetstream v2 instance refused with `CursorTooOld`, which ends the source |
| `marco_ingest_apply_seconds`, `marco_ingest_batch_ops` | `source` | One sink write's time and size |
| `marco_ingest_queued_events` | `source` | Events read and not yet written |
| `marco_segment_write_seconds` | | One change segment: encode, write, fsync, rename |
| `marco_segment_bytes_total`, `marco_segment_errors_total` | | Segment bytes, and failed writes (each retried) |
| `marco_checkpoint_seconds` | | A checkpoint, cut to new copies serving |
| `marco_checkpoint_errors_total` | `stage` | Checkpoint failures, and compactions' (`stage="compaction"`) |
| `marco_store_checkpoint_bytes` | `part` | The last checkpoint's bytes: `patch` (changed lists), `pinned` (the patches' indexes and tables), `base` (new bases) |
| `marco_store_compaction_shards_total`, `marco_store_compaction_bytes_total`, `marco_store_compaction_garbage_bytes_total` | | Shards compacted, the bytes they wrote, the garbage they cleared |
| `marco_store_compaction_seconds` | | One shard's compaction |
| `marco_store_compaction_pending` | | Shards the running compaction has left, 0 when none runs |
| `marco_store_bytes` | `part` | Where the budget is: `budget`, `pinned_dir`, `pinned_index`, `pinned_heads`, `pinned_tables`, `cache_main`, `cache_small`, `retired`, `pinned_frames`, `free`, `borrowed`, `garbage`, `reserved` (given up for what the process holds beside it, `--memory-limit`) |
| `marco_process_beside_bytes`, `marco_store_reserve_short_bytes` | | With a memory limit: what the process holds beside the store (RSS less the store's books), and how much of what it should have given back the store couldn't |
| `marco_store_lookups_total` | `structure`, `result`, `queue` | Page lookups by structure (`out_small`, `out_subj`, `out_rkey`, `out_bysubj`, `in_small`, `in_blocks`), `hit` or `miss`, and the queue a hit was in (`none` for a write's reads, which don't count as uses) |
| `marco_store_request_pages`, `marco_store_request_rounds` | `rpc` | Pages a request read from disk, and the rounds it waited |
| `marco_store_miss_pool_busy`, `marco_store_miss_pool_wait_seconds`, `marco_store_busy_total` | | The miss pool's threads in use, the wait for one, and requests refused after a second |
| `marco_store_io_seconds`, `marco_store_io_bytes_total`, `marco_store_io_queue_depth` | `queue` | Device reads by lane (`request`, `background`): latency per read call, bytes, reads waiting |
| `marco_store_read_errors_total` | `kind` | Failed page reads: `checksum`, `eio`, `short`, and `eio_retried` (a retry fixed it) |
| `marco_store_bad_pages`, `marco_store_failing_pages` | | Pages marked bad, and distinct pages failing on the device in the last 60 s |
| `marco_store_evictions_total`, `marco_store_ghost_hits_total` | `queue` | Frames evicted by queue, and misses the ghost sent straight to main |
| `marco_store_write_through_pages_total` | `result` | Pages checkpoints and compactions put in the cache as they wrote them (`published`), or let go (`skipped`) |
| `marco_store_past_pages_total` | | Pages compactions and list stats read past the cache |
| `marco_delta_entries`, `marco_delta_lists` | `rel`, `dir` | Unmerged delta entries, and lists with a delta |
| `marco_merges_total`, `marco_merge_bytes_total` | `rel`, `dir` | List deltas merged, and the bytes they wrote |
| `marco_merge_seconds` | `dir` | Time to merge one list's delta |
| `marco_override_bytes` | `rel`, `dir` | Bytes of lists merged since the snapshot |
| `marco_tombstones` | `rel` | Deletes waiting for their create |
| `marco_directory_dids` | `part` | DIDs: `base` (snapshot) or `live` |
| `marco_snapshot_edges` | `rel`, `dir` | Records (out) and pairs (in) in the snapshot |
| `marco_snapshot_bytes` | `section` | Snapshot bytes by section |
| `marco_snapshot_seconds` | `what` | Snapshot open and write times |
| `marco_build_info` | `version`, `commit` | 1 |
| `marco_export_leases` | | Checkpoint export leases open |
| `marco_export_bytes_total`, `marco_export_files_total` | | Checkpoint file bytes and file reads the export served |
| `marco_export_hashed_bytes_total` | | Bytes read to sum checkpoint files for a lease (once per file: they never change) |

What to watch:

```promql
marco_ready == 0                                                   # not ready since start
max by (source) (marco_ingest_behind_microseconds) / 1e6 > 30      # a source falling behind
rate(marco_segment_errors_total[5m]) > 0                           # segments not landing
increase(marco_checkpoint_errors_total[1h]) > 0
marco_store_bad_pages > 0                                          # a page needs a rebuild or a restored file
histogram_quantile(0.99, sum by (le, rpc) (rate(marco_rpc_seconds_bucket[5m])))
```

A lag of a few seconds is usually the source's. In the durability tests, two public Jetstream instances
differed by up to 53 s while marco's own share of the lag stayed under 205 ms, so compare against
another instance before blaming marco ([Durability and restarts, measured](p1-results.html)). marco doesn't ship a
Grafana dashboard yet.

## Checkpoint export

Another process can copy the current checkpoint while marco keeps serving, checkpointing and
compacting: seek's builder mirrors it to run PageRank, and it seeds a replica or takes a backup the
same way. All of it is on the admin listener, behind the token.

| Call | What it does |
|---|---|
| `GET /admin/checkpoint?ttl=SECS` | A lease (default 1800 s, at most 6 h): the manifest, its checkpoint number, and every file it names with its bytes and xxh3 |
| `GET /admin/checkpoint?peek=1` | The checkpoint number, build and time only, no lease |
| `GET /admin/checkpoint/files/NAME?lease=ID` | One file, with `Range: bytes=A-` to resume |
| `DELETE /admin/checkpoint?lease=ID` | Ends the lease |

A lease hard-links the files its manifest names into `<snapshot>.export/<lease>/`, so the snapshot's
filesystem has to allow hard links (it's the same one). A compaction or a newer checkpoint can delete
the originals meanwhile and the copy still reads the leased checkpoint. The links go when the lease is
deleted or its ttl passes, and a restart clears them all. Reads and sums go through `--background-io`,
so requests go first. The first lease after a start sums every file once. A file never changes under its
name, so later leases sum only files they haven't seen. Copied files plus the manifest are a snapshot
directory `marco serve` or `marco-load` can open.

`marco-load mirror --url ADMIN --token-file FILE --dir DIR` keeps a local copy in sync. It takes a
lease, downloads only the files it doesn't have (each checked against its xxh3, a partial one resumed),
links them into `DIR/snap-<checkpoint>` and points `DIR/current` at it, then drops files no snapshot
names. The first sync copies the whole snapshot, and later ones copy what the checkpoints since wrote.

## Profiling and tracing

| Route (admin listener) | |
|---|---|
| `GET /debug/pprof/profile?seconds=30` | CPU profile, pprof protobuf |
| `GET /debug/pprof/heap` | jemalloc heap in use, pprof |
| `GET /debug/tracing` | The trace ratio. `POST /debug/tracing?ratio=0.01` sets it. |

```bash local
curl -s -H "authorization: Bearer $(cat token)" '127.0.0.1:7301/debug/pprof/profile?seconds=30' > cpu.pb.gz
go tool pprof -http :8080 cpu.pb.gz
```

Spans go to `--otlp` (an OTLP/HTTP collector's base URL) for the share of ops `--trace-ratio`
picks, 0 by default. Tracing off costs one atomic load an op. At 1% it doesn't show on one core
and costs 2% of all-thread throughput, and at 100% it adds ~1.2 µs an op
([Build and reads, whole network](p0-results.html)). The console's Requests page changes the ratio live.

## The console

`http://127.0.0.1:7301/admin/`, signed in with the token (kept for the tab only) or through the
proxy. `g` then a letter jumps to a section, `⌘K` opens the palette, `?` lists the keys.

| Section | |
|---|---|
| Overview | Health line (ready, sources, reads, writes, durability), edges by relation, writes by outcome, ops, snapshot, memory, live writes |
| Explorer | An actor's or list's out and in lists, a pair's relationships, a record by URI, Intersect and Filter on live data, edge actions |
| Relations | The relations, their config as TOML, list lengths, lists over the by-subject threshold, duplicate records |
| Storage | Shards (clean, dirty, writing · checksums), deltas against their base, merges, tombstones, bytes by section, durability, Checkpoint now |
| Ingest | Sources (cursor, lag, rate, errors, reconnects), ops by outcome, non-TID rkeys |
| Requests | RPC latency and errors, the trace ratio, CPU and heap profiles |
| Settings | The console's own settings, the build, every flag with its source |
