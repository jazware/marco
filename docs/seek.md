---
title: seek
section: seek
order: 1
summary: "seek, the actor typeahead that calls marco: a builder publishes one index artifact every 15 minutes, thin pods map it and follow Jetstream, and an AppView asks them bsky.Service/SearchActorsTypeahead."
---

```hero
diagram:
  caption: "The builder publishes one artifact every 15 minutes; pods map it, follow Jetstream into a small overlay and answer the AppView. marco gives a pod each viewer's follows and blocks, and its checkpoint to the builder's PageRank."
  nodes:
    - { id: app, label: AppView, sub: "SearchActorsTypeahead", at: [0, 0], size: [9, 3] }
    - { id: pod, label: seek serve, sub: "artifact + overlay", at: [13, 0], size: [10, 3], tone: accent }
    - { id: b, label: seek builder, sub: "names · PageRank", at: [13, 5.5], size: [10, 3], tone: violet }
    - { id: m, label: marco, sub: "follows · blocks", at: [27, 0], size: [8, 3], tone: blue }
    - { id: js, label: Jetstream, sub: "profile · #identity · #account", at: [27, 5.5], size: [8, 3], tone: cyan }
  edges:
    - "app.r -> pod.l50: query"
    - "pod.r -> m.l50: Filter"
    - "b.t50 -> pod.b50: artifact"
    - "js.l50 -> b.r50"
    - "m.b50 -> b.r25: checkpoint"
facts:
  - { value: "15", unit: min, label: between artifacts, note: "`--interval-seconds`", tone: violet }
  - { value: "24", unit: h, label: replay window, note: "builds must land within it", tone: rust }
  - { value: ":7310", label: pod API, note: "gRPC · Connect", tone: accent }
  - { value: ":7311", label: ops, note: "/metrics · /debug", tone: muted }
```

seek answers the AppView's actor typeahead: a query prefix, an optional viewer and a limit come in,
and ranked DIDs with scores go out. The AppView hydrates them as it does every other skeleton. One
prefix index covers every actor's handle and display name, ranked by PageRank over the follow graph.
A signed-in viewer sees the accounts they follow first, and nobody who blocks them or whom they
block. The design and its measurements are in [the design doc](seek.html).

## How it runs

| Part | What it does | State |
|---|---|---|
| `seek builder` | Holds every actor's handle claim, display name and status. It follows Jetstream into them, verifies handles, and every 15 minutes builds the artifact and serves it at `/artifact/latest` (uploading it to a bucket when one is configured). On each new marco checkpoint it mirrors the checkpoint (files it already has by name and size are skipped, and each fetched file is checked against its section checksums) and runs PageRank on the mirror. | Its volume: the names state, the artifacts, the mirror and the ranks |
| `seek serve` (a pod) | Downloads the newest artifact, checks it, maps it, replays Jetstream from the artifact's cursor into a small live overlay, and answers queries. A newer artifact is swapped in without a pause. At start it serves the artifact it kept, once that checks ([Starting](#starting)). | Its volume: the artifact it serves, kept for the next start (the overlay is rebuilt from Jetstream) |
| marco | Gives a pod each viewer's follow count, follows and mutuals (once per viewer, then cached) and filters every logged-in page by blocks both ways. It exports its checkpoint to the builder's mirror. | Its own |

The artifact is one file: named sections, each 64-byte aligned with its own xxh3, behind a
checksummed header. It holds the DID table (seek's ids are DID order), each actor's u16 rank,
hidden bit and handle fingerprint, the front-coded keys with their postings, top lists for heavy
prefixes, and a forward table from actor to its keys. It also records the Jetstream cursor it's
consistent with.

**Builds must land more often than Jetstream's replay window.** A pod replays Jetstream from its
artifact's cursor. If that cursor is older than the window (`--replay-window-hours`, 24 by default),
or Jetstream's replay starts past it, the pod refuses to serve stale data: `/readyz` says why,
`seek_refusing_stale` is 1, and it waits for a newer artifact. At one build every 15 minutes the
margin is wide. The alert `SeekArtifactAgingTowardsReplayWindow` fires at 6 hours.

## Running it

```bash local
# the builder: the first state from the archive's export, ranks from marco's checkpoint
seek builder --dir /var/lib/seek-builder --bootstrap bootstrap.jsonl.zst --cursor-us 1794628081312952 \
  --marco-admin http://marco:7301 --marco-token-file /run/secrets/marco-admin-token

# a pod
seek serve --source http://seek-builder:7320/artifact/latest --marco http://marco:7300
```

With Docker, one image runs both:

```bash local
docker run -v seek-builder:/var/lib/seek-builder -p 7320:7320 ghcr.io/jazware/seek \
  builder --dir /var/lib/seek-builder --marco-admin http://marco:7301 --marco-token-file /run/secrets/token
docker run -p 7310:7310 ghcr.io/jazware/seek \
  serve --source http://seek-builder:7320/artifact/latest --marco http://marco:7300
```

The bootstrap is JSON lines (zstd), one per DID:
`{"did", "handle", "verified_us", "name", "profile"}`. `handle` is the DID's current claim,
`verified_us` when that claim was last verified both ways (0 if never), `name` the profile's display
name and `profile` whether it has a profile record. The builder verifies every unverified claim
itself, newest first and then by rank. Only verified handles get handle keys, so an actor with an
unverified claim is findable by its name until its handle checks out.

An AppView calls `bsky.Service/SearchActorsTypeahead` on the pod. That's the reference AppView's
dataplane method with the same package, messages and field numbers, over gRPC (HTTP/2) or Connect.
A `viewer` makes the query logged in. A pod without `--marco` refuses those with
`failed_precondition`, since it can't check blocks.

## Ranking

| Match | Example | Weight |
|---|---|---|
| The handle, typed out in full | `jane.bsky.social` finds `jane.bsky.social` | First, always |
| The start of the handle or the display name | `jane` finds `janefoster.bsky.social` and Jane Foster | 0 |
| The start of a later label or name word | `jane` finds `mary-jane.bsky.social` and Mary Jane Doe | −1 nat |
| Typed with spaces: the start of the handle without them | `jane fos` finds `janefoster.bsky.social` | 0, a later label −1 nat |
| Typed with spaces: each word starts a different word of the name, in any order | `foster jane` and `jane f` find Jane Q. Foster | −1 nat |

A score is the actor's u16 log PageRank (about 4,400 a nat) plus its match's weight. Exact means
the handle itself: each actor's handle fingerprint has to match the typed query, not just its fold.
An exact match stays on the page even when a block would drop it, as the reference AppView does, so
a user can find who they blocked. Signed in, follows sort ahead of everyone (`--follows wall`),
however they matched, and mutuals get +1 nat among them. `--follows N` gives follows a boost of N
nats instead.

The two spaced matches are for a query with a space in it: a handle typed the way it reads, and a
name's words in the order someone remembers them. The query's words are its fold's, two to five,
and the name's are its first five, the ones with keys. Candidates come from the query word with the
fewest postings (its top list when it's heavy, else its range), and the 64 best ranked are checked
against their keys, so two common words find the best-ranked accounts with both, not every one. A
dot or hyphen is a word break, not a space: `jane.fos` doesn't find `janefoster.bsky.social`.

The app's search box sends post-search syntax too, and a query loses it before it's folded, as the
reference typeahead's does. An operator no actor has goes (`from:me`, `lang:ja`, `since:`,
`media:true`, a URL), while `from:`, `to:` and `mentions:` leave the handle or DID they name. A
negated word (`-spam`) goes. Double quotes and a hashtag's `#`s are spaces, so `"jane foster"` is
`jane foster` and `#jane#foster` is too. What's in quotes is never an operator or a negation, and a
query with nothing else left finds no one. An apostrophe is not a quote.

Blocks are marco's `block` records both ways. Moderation-list blocks (`listblock`) aren't covered:
seek reads no listblock relation, so the AppView still filters those at hydration.

## Flags

Every flag has an env var. `seek serve`:

| Flag | Env | Default | |
|---|---|---|---|
| `--listen` | `SEEK_LISTEN` | `0.0.0.0:7310` | The API: `bsky.Service`, `/healthz`, `/readyz` |
| `--ops-listen` | `SEEK_OPS_LISTEN` | `127.0.0.1:7311` | `/metrics`, `/debug/pprof/{profile,heap}`, `/debug/tracing`. Bind it privately |
| `--source` | `SEEK_SOURCE` | | The builder's `http(s)://…/artifact/latest`, `s3://bucket/prefix`, or a file or directory (a file is named as the builder names it, `<version>.seek`) |
| `--data` | `SEEK_DATA` | `/var/lib/seek` | The artifact served, kept across restarts, and the next one at a swap: room for two. Never the source's directory |
| `--poll-seconds` | `SEEK_POLL_SECONDS` | 30 | Between source polls |
| `--marco` | `SEEK_MARCO` | | marco's API listener |
| `--marco-connections` | `SEEK_MARCO_CONNECTIONS` | 4 | h2 connections to marco, calls spread round-robin over them |
| `--marco-timeout-ms` | `SEEK_MARCO_TIMEOUT_MS` | 500 | Each marco call, its retry included. A call that fails in transit (a connection refused or reset, a GOAWAY, a stream reset) or with `unavailable` (a 503 on the way, or a marco that can't serve it now) is tried once more on a fresh connection. They're all reads, so a second try is safe |
| `--viewer-deadline-ms` | `SEEK_VIEWER_DEADLINE_MS` | 2000 | A viewer's follows and mutuals, every page. Past it the query answers without follows first; blocks are still filtered. 0 turns follows first off |
| `--max-follows` | `SEEK_MAX_FOLLOWS` | 5000 | A viewer following more gets no follows first, though blocks are still filtered. One marco Counts call decides, and the cache keeps the verdict. Just past the 95th percentile of accounts that post mentions (Viewer builds, below) |
| `--viewer-cache-mb` | `SEEK_VIEWER_CACHE_MB` | 1024 | The viewer cache's weighed size, about 13 KB a viewer. 1 GiB holds every account that posted a mention in two days, 783 MB of them |
| `--viewer-idle-seconds` | `SEEK_VIEWER_IDLE_SECONDS` | 86400 | A viewer's entry goes this long after its last query, and is read from marco again this long after the last read however often it's used. Jetstream follow events drop it sooner. Keep it short with `--follow-events false` |
| `--marco-lag-ms` | `SEEK_MARCO_LAG_MS` | 5000 | How far marco's follows may trail Jetstream's. A build whose marco read starts within this of a follow event isn't cached, and the entry is dropped again at twice this |
| `--follows` | `SEEK_FOLLOWS` | `wall` | `wall`, or a boost in nats |
| `--replay-window-hours` | `SEEK_REPLAY_WINDOW_HOURS` | 24 | Jetstream's replay window |
| `--max-lag-seconds` | `SEEK_MAX_LAG_SECONDS` | 300 | Not ready while Jetstream is further behind, or silent this long |
| `--follow-events` | `SEEK_FOLLOW_EVENTS` | true | Read follow records to drop a viewer's entry when they follow or unfollow |
| `--jetstream` | `SEEK_JETSTREAM` | `wss://jetstream2.us-east.bsky.network` | Several comma-separated: a socket that goes quiet while behind, with nothing read past the cursor, redials the next one. Once every instance was stuck there, the cursor steps past (1 s, doubling) and the replay's gap is reported |
| `--plc`, `--plc-rate` | `SEEK_PLC`, `SEEK_PLC_RATE` | `https://plc.directory`, 10/s | DID documents for handle verification |
| `--verify-rate`, `--verify-workers` | `SEEK_VERIFY_RATE`, `SEEK_VERIFY_WORKERS` | 10/s, 8 | DNS TXT and HTTPS well-known lookups, to hosts the handles name |
| `--no-verify` | `SEEK_NO_VERIFY` | off | A handle change leaves the actor with no handle until the next artifact |
| `--s3-endpoint`, `--s3-region`, `--s3-credentials` | `SEEK_S3_*` | | For an `s3://` source: a JSON file `{"access_key_id", "secret_access_key"}` |
| `--trace-ratio`, `--otlp` | `SEEK_TRACE_RATIO`, `SEEK_OTLP` | 0 | The share of queries traced, and the collector |
| `--log-format` | `SEEK_LOG_FORMAT` | `json` | `json` or `text`; `SEEK_LOG` filters (`info`) |
| `--shutdown-drain-secs` | `SEEK_SHUTDOWN_DRAIN_SECS` | 10 | On SIGTERM or Ctrl-C, how long `/readyz` answers 503 while the listeners keep serving ([Stopping](#stopping)). 0 stops at once |
| `--shutdown-grace-secs` | `SEEK_SHUTDOWN_GRACE_SECS` | 10 | Then, once the listeners stop accepting, how long the requests in flight have to finish |

`seek builder` also takes `--jetstream`, the verification flags, the S3 flags, the ops flags and the
shutdown flags, and:

| Flag | Env | Default | |
|---|---|---|---|
| `--dir` | `SEEK_BUILDER_DIR` | | The state, artifacts, mirror and ranks |
| `--listen` | `SEEK_LISTEN` | `0.0.0.0:7320` | `/artifact/latest` (an ETag, `x-seek-version`, `x-seek-cursor`), `/healthz`, `/readyz` |
| `--bootstrap`, `--cursor-us` | `SEEK_BOOTSTRAP`, `SEEK_BOOTSTRAP_CURSOR_US` | | The first state when `--dir` has none, and the Jetstream time it's consistent with |
| `--interval-seconds` | `SEEK_INTERVAL_SECONDS` | 900 | Between artifacts |
| `--index` | `SEEK_INDEX` | `profiles` | Who's indexed: anyone with a profile record, or `all` the DIDs it knows |
| `--marco-admin`, `--marco-token-file` | `SEEK_MARCO_ADMIN`, `SEEK_MARCO_TOKEN_FILE` | | On each new marco checkpoint: mirror it into `DIR/mirror`, then PageRank into `DIR/ranks`. The token is marco's export token (`--export-token-file`), which opens the checkpoint export alone. The peek has a 30 s deadline; a mirror past 3 h or a PageRank past 2 h is killed |
| `--marco-load` | `SEEK_MARCO_LOAD` | `marco-load` | The binary that mirrors and ranks |
| `--pagerank-source`, `--pagerank-memory` | `SEEK_PAGERANK_SOURCE`, `SEEK_PAGERANK_MEMORY` | `inset`, `16GiB` | `inset` iterates over the mirror's in sets: on the whole network 33 passes and 69 s cold, 10 passes and 27 s warm from a day-old graph's ranks, ~14 GB peak. `csr-inset` builds an in-edge CSR first: about twice as fast, but ~27 GB peak, so a ~45 GB builder |
| `--ranks`, `--ranks-token-file` | `SEEK_RANKS`, `SEEK_RANKS_TOKEN_FILE` | | Ranks from elsewhere: a ranks file or directory, or a URL |
| `--without-ranks` | | off | Publish without ranks. Otherwise the builder won't publish until it has them, since the ranks carry marco's takedowns |
| `--keep` | `SEEK_KEEP` | 4 | Artifacts kept, locally and in the bucket |
| `--bucket` | `SEEK_BUCKET` | | Also upload each artifact to `s3://bucket/prefix`, with `latest.json` naming the newest |
| `--state-every` | `SEEK_STATE_EVERY` | 4 | Save the names state every this many cycles |
| `--verify-per-cycle`, `--recheck-days` | `SEEK_VERIFY_PER_CYCLE`, `SEEK_RECHECK_DAYS` | 9000, 30 | Handles queued for verification a cycle, and how old a check gets before it's done again |

## Starting

A pod binds its listeners first, then serves the artifact its `--data` kept, if it has one:

1. Downloads cut short (`*.part`) are deleted. So is every artifact when the directory was filled
   from another `--source` (the file `source` there names the one it came from).
2. The newest artifact left is checked as a download is: the header's and every section's xxh3, and
   its meta must name the version in its file name. One that fails is deleted and the next newest
   is tried; any older ones are deleted once one is served.
3. It's mapped and Jetstream is replayed from its cursor, with the same rule as a download: a cursor
   older than `--replay-window-hours`, or a replay that starts past it, and the pod refuses to serve
   until a newer artifact comes.
4. The poll starts with that version as its ETag, so the builder answers 304 until it has a newer
   one, and a newer one swaps in as usual. The one it replaces is deleted once its last query ends.

So a restart is ready once the check, the map and the Jetstream catch-up are done, without waiting
for the builder. With nothing kept (the first start, or every kept artifact refused), the pod waits
for the builder's artifact.

## Health

`/healthz` answers while the process runs. A pod's `/readyz` is 200 once it serves, else 503 with
the reason: no artifact yet; refusing to serve (the artifact's cursor is older than the replay
window, or Jetstream's replay skipped events); replaying Jetstream from the artifact's cursor; or
Jetstream more than `--max-lag-seconds` behind or silent. The builder's `/readyz` is 200 once it has
published and its last cycle succeeded. Both answer 503 `shutting down` from SIGTERM on.

## Stopping

A pod and the builder stop as marco does ([Stopping](operations.md#stopping)). On SIGTERM or
Ctrl-C, `/readyz` turns 503 while every listener keeps serving, queries included, for
`--shutdown-drain-secs` (10 s), and each HTTP/1 answer says `Connection: close`. Then the listeners
stop accepting, HTTP/2 connections (the AppView's) get a GOAWAY, and queries in flight get up to
`--shutdown-grace-secs` (10 s). The process exits without waiting for a build or a state save in
progress: each lands whole by rename or not at all, and the builder starts again from its last saved
state. A pod keeps the artifact it serves on its volume for its next start. One whose artifact
download the builder's stop cut short tries again at its next poll.
Kubernetes' default `terminationGracePeriodSeconds` of 30 s covers the defaults.

## Metrics

Every series exists at 0 from start. Latencies on the keystroke path are histograms sharded per
thread, as marco's op histograms are.

| Metric | Labels | |
|---|---|---|
| `seek_queries_total` | `mode` (`logged_out`, `logged_in`), `result` (`ok`, `empty`, `error`) | Queries answered |
| `seek_query_seconds` | `mode` | Query latency inside seek |
| `seek_query_stage_seconds` | `stage`: `viewer`, `prefix`, `personal`, `global`, `filter` | The viewer's cache entry (marco on a miss), the prefix's ranges (a spaced query's words and joined handle too) and exact handles, the viewer's matching follows, the global fill, marco's block filter |
| `seek_query_results_total` | `mode` | Actors returned |
| `seek_query_errors_total` | `kind`: `bad_request`, `not_ready`, `marco`, `no_marco`, `internal` | Failed queries |
| `seek_queries_in_flight` | | |
| `seek_query_logged_out_fallbacks_total` | | Logged-in queries answered without follows first (marco couldn't give them in time; blocks were still filtered) |
| `seek_query_blocked_total`, `seek_query_exact_kept_total` | | Candidates the block filter dropped, and exact handle matches kept through it |
| `seek_viewer_cache_hits_total`, `seek_viewer_cache_misses_total` | | |
| `seek_viewer_cache_evictions_total` | `cause`: `idle`, `size`, `invalidated` | |
| `seek_viewer_cache_entries`, `seek_viewer_cache_bytes` | | |
| `seek_viewer_cache_rekeys_total` | | Entries moved onto a new artifact's ids, without marco |
| `seek_viewer_capped_total` | | Viewer builds that found more than `--max-follows` follows |
| `seek_viewer_builds_unkept_total` | | Builds used for their query but not cached, as a follow event for the viewer came during or just before marco's read |
| `seek_viewer_build_seconds`, `seek_viewer_keys_seconds`, `seek_viewer_follows` | | A viewer's entry from marco, its keys found in an artifact, and each built viewer's follow count |
| `seek_marco_seconds` | `method`: `filter`, `list_out`, `intersect`, `counts` | |
| `seek_marco_errors_total` | `method`, `kind`: `timeout`, `unavailable`, `other` | Calls that failed, their retry included |
| `seek_marco_retries_total` | `method` | Calls tried a second time on a fresh connection. A slot's connection is replaced at most once a second |
| `seek_artifact_info` | `version` | 1 for the artifact served |
| `seek_artifact_built_timestamp_seconds`, `seek_artifact_cursor_timestamp_seconds`, `seek_artifact_age_seconds` | | |
| `seek_artifact_build_seconds`, `seek_artifact_actors`, `seek_artifact_mapped_bytes` | | |
| `seek_artifact_download_bytes_total`, `seek_artifact_download_seconds` | | |
| `seek_artifact_verify_failures_total`, `seek_artifact_poll_failures_total` | | |
| `seek_artifact_refused_total` | `reason`: `invalid`, `older`, `uncovered`, `gap` | Artifacts not swapped in: bad contents, built before the one served, a cursor before what the pod can replay, or before a Jetstream gap |
| `seek_artifact_swaps_total`, `seek_artifact_swap_seconds`, `seek_artifact_swap_replayed` | | Swaps, their time, and the changes the last one replayed |
| `seek_refusing_stale`, `seek_ready` | | |
| `seek_overlay_actors`, `seek_overlay_keys`, `seek_overlay_bytes`, `seek_ring_entries` | | The live overlay, and the changes kept for the next swap |
| `seek_jetstream_events_total` | `kind`: `profile`, `identity`, `account`, `follow`, `skipped` | |
| `seek_jetstream_lag_seconds`, `seek_jetstream_cursor_timestamp_seconds` | | |
| `seek_jetstream_reconnects_total`, `seek_jetstream_decode_errors_total`, `seek_jetstream_gaps_total`, `seek_jetstream_stuck_total` | | |
| `seek_ingest_panics_total` | | Changes that panicked and were skipped (a bug) |
| `seek_verify_queue` | | Handles waiting |
| `seek_verify_checks_total` | `result`: `ok`, `mismatch`, `error`, `rate_limited`; `method`: `did_doc`, `dns`, `well_known` | |
| `seek_verify_seconds` | `method` | |
| `seek_builder_step_seconds`, `seek_builder_step_last_seconds` | `step`: `ingest`, `mirror`, `pagerank`, `ranks`, `build`, `verify`, `upload`, `state` | |
| `seek_builder_cycles_total` | `result` | |
| `seek_builder_last_success_timestamp_seconds`, `seek_builder_artifact_bytes`, `seek_builder_upload_seconds`, `seek_builder_ranks_age_seconds` | | |
| `seek_builder_actors` | `state`: `known`, `indexed`, `handle_verified`, `handle_unverified`, `hidden` | |
| `seek_pagerank_passes`, `seek_pagerank_l1`, `seek_pagerank_seconds` | | The last run |
| `seek_pagerank_runs_total` | `result`: `ok`, `error`, `timeout`, `unreachable` | PageRank runs, killed past their limit, or not started because marco's checkpoint couldn't be read |
| `seek_mirror_runs_total` | `result`: `ok`, `error`, `timeout` | Mirrors of marco's checkpoint |
| `process_*` | | RSS, CPU, open fds |

## Alerts

Only a pod not serving pages, and only after 15 minutes. Everything else is a ticket.

| Alert | Severity | When |
|---|---|---|
| `SeekNotServing` | page | `seek_ready == 0` for 15 minutes |
| `SeekArtifactAgingTowardsReplayWindow` | ticket | The served artifact's cursor is over 6 hours old |
| `SeekBuilderFailing` | ticket | A failed cycle in the last hour |
| `SeekBuilderStalled` | ticket | No artifact published for an hour |
| `SeekJetstreamLag` | ticket | A pod or the builder over 2 minutes behind for 10 minutes |
| `SeekQueryErrors` | ticket | Over 1% of queries failing for 15 minutes |
| `SeekMarcoErrors` | ticket | marco errors on a method over 0.1/s for 15 minutes |

## Profiling and tracing

The ops listener serves `/debug/pprof/profile?seconds=N` (CPU, pprof) and `/debug/pprof/heap`
(jemalloc's heap, pprof), as marco's does, so the same scraper reads both. `/debug/tracing` reads the
trace ratio, and `POST /debug/tracing?ratio=R` sets it without a restart. At 0 a query takes one
atomic load and allocates no span. A traced query is one span with a child for the viewer, the match
(with the prefix, personal and global times as fields) and the filter. The request header
`x-seek-trace: 1` traces that query whatever the ratio. Spans go out as OTLP/HTTP through a batch
queue of 65,536, so a slow collector drops spans and never holds a query.

## Throughput

```facts
- { value: "~45k", unit: req/s, label: per core logged out, note: "1 to 4 cores, bare metal; 490k on 8 cores with SMT", tone: accent }
- { value: "22", unit: µs, label: CPU per query at peak, note: "the match itself is 7 µs p50, 110 µs p99", tone: blue }
- { value: "2.6", unit: GB, label: RSS at any load, note: "the mapped artifact; load adds nothing", tone: muted }
- { value: "~68k", unit: req/s, label: marco's logged-in ceiling, note: "4 vCPUs, 54 µs a query, ~2,500 viewer builds/s", tone: violet }
```

`seek bench --mode closed` keeps N queries in flight over several h2 connections and steps N up.
`--mode open` offers fixed rates and times each query from when it was due, so a stalled pod can't
hide its queue. Each step samples `process_cpu_seconds_total` and `process_resident_memory_bytes`
from the pod's ops listener, and marco's with `--marco-metrics` (a URL it can read without a
token). The queries are the latency
bench's mix: every prefix of the handles and names of 2,000 actors, half from the top million.

```bash local
seek bench --artifact latest.seek --write-queries queries.txt    # once, where the artifact is
seek bench --mode closed --queries queries.txt --target http://pod:7310 --ops http://pod:7311 \
  --connections 16 --concurrency 1,2,4,8,16,32,64,128,256,512 --step-seconds 15
seek bench --mode open --queries queries.txt --target http://pod:7310 --ops http://pod:7311 \
  --rates 10000,20000,40000 --logged-in --viewers viewers.txt --health http://pod:7310/readyz
```

The output is JSON on stdout and a table on stderr. A sweep stops after a step with over 1% errors
(`--stop-error-ratio`), past `--stop-p99-ms`, when a `--health` URL fails, or when an open-loop
step can't keep up with its rate.

**Logged out, on bare metal.** A pod on a 16-core Ryzen AI Max+ 395 serving the whole network's
artifact (43M actors), pinned with `taskset` to cores on one CCD, with the load generator on the
other CCD:

| Pod CPUs | Peak req/s | Per core | Closed loop, p99 ≤ 1 ms | Pod RSS |
|---|---|---|---|---|
| 1 core | 44,600 | 44,600 | 36,500 (2 in flight) | 2.60 GB |
| 2 cores | 89,900 | 44,900 | 71,900 (32 in flight) | 2.60 GB |
| 4 cores | 179,000 | 44,800 | 148,000 (64 in flight) | 2.61 GB |
| 8 cores | 292,000 | 36,600 | 247,000 (64 in flight) | 2.62 GB |
| 8 cores, 16 threads | 490,000 | 61,200 | 423,000 (128 in flight) | 2.64 GB |

Peaks are at 512 in flight. Throughput keeps climbing past full CPU as deeper queues batch more
queries per h2 frame and syscall, at the cost of latency. CPU per query is about 32 µs at 1 in
flight and 22 µs at the peak on 1 to 4 cores, of which the match is a small part (`seek_query_seconds`: 7 µs p50, 110 µs
p99). The rest is gRPC, h2 and the kernel. It scales linearly to 4 cores, 8 cores give 18% less
per core, and SMT adds 68%.

The latency curve, 2 cores:

| Load | 4 in flight | 32 in flight | 512 in flight | 22,500/s offered | 44,900/s | 71,900/s |
|---|---|---|---|---|---|---|
| req/s | 67,200 | 71,900 | 89,900 | 22,500 | 44,900 | 71,900 |
| p50 | 52 µs | 428 µs | 5.8 ms | 66 µs | 69 µs | 198 µs |
| p99 | 154 µs | 999 µs | 11.9 ms | 186 µs | 6.8 ms | 12.3 ms |

Open loop, p99 stays near 200 µs up to about a quarter of the peak, at least 11,000/s a core, and
then jumps to a few milliseconds (on 16 threads: 206 µs at 49,000/s, then 1.5 to 2.6 ms from
122,000/s to 441,000/s). The generator adds 0.3 ms at p99 and the match 0.1 ms, so the rest is
bursts queueing in the pod's transport. Fewer or more connections, and fewer in flight, don't
change it.

**The 4-vCPU microVM.** The same artifact in a cloud-hypervisor microVM with 4 vCPUs and 8 GB, its
load generated on the host (a 64-core EPYC 7713). Logged-in queries go to marco in its own 4-vCPU VM
on the same host. agent sets each VM's cgroup quota (`cpu.max`) to one CPU per vCPU, and the quota
covers the VMM and its virtio-net thread as well as the vCPUs, so the guest never gets all 4.
Raising the pod's quota to 8 CPUs lets the 4 vCPUs run flat out with the NIC beside them. That's what a pod
sized at 4 vCPUs plus NIC headroom does.

| | Quota | Closed loop, short queue | Open loop, low p99 | Peak |
|---|---|---|---|---|
| Logged out | 4 CPUs | 13,100/s at 4 in flight, p99 0.59 ms | | 74,700/s at 2,048 in flight, p50 26 ms |
| Logged out | 8 CPUs | 13,700/s at 4 in flight, p99 0.54 ms | | 80,400/s at 2,048 in flight, p50 24 ms |
| Logged in | 4 CPUs | 2,200/s at 2 in flight, p99 2.8 ms | 6,000/s, p99 21 ms | 23,900/s at 1,024 in flight; open loop 21,400/s |
| Logged in | 8 CPUs | 4,800/s at 4 in flight, p99 2.4 ms | 7,000/s, p99 2.7 ms | 28,000/s at 2,048 in flight; open loop 22,500/s |

Logged in, every viewer is in seek's viewer cache, so a query costs one marco Filter (500 viewers).
At agent's quota the VM is throttled from 256 in flight logged out (520 to 630 ms of every second at
the top) and from 8 in flight logged in. At 8 CPUs it's never throttled. The logged-out peak is the
same at both quotas within 8%, because seek's own process reaches 3.4 to 3.9 cores either way. The
low-load logged-in p99 of 19 to 26 ms at agent's quota at 2,400 to 6,000/s came with little or no
throttling, so the quota doesn't explain it.

What the whole VM costs next to seek's process, per query:

| | 1 in flight, seek | 1 in flight, VM | Peak, seek | Peak, VM |
|---|---|---|---|---|
| Logged out, 8 CPUs | 112 µs | 272 µs | 48 µs | 53 µs |
| Logged in, 8 CPUs | 282 µs | 751 µs | 134 µs | 154 µs |

At low rates the guest kernel and virtio-net cost 1.5 to 2.5 times seek's own CPU (every query is
its own wakeup and packet). At the peak, deep queues batch them and they add 10 to 15%.

**marco under logged-in load.** To take the pod out of the way, `seek serve` ran on the host
itself, not in a VM, on 8 cores (16 threads) that no VM uses. It sent logged-in queries to marco's API
over loopback, with the same 500 cached viewers, so each query was one Filter in marco:

| seek on the host | Logged-in req/s | p99 | marco CPU (process / VM) | marco VM per query | marco p50 / p99 |
|---|---|---|---|---|---|
| Open loop, 10,700/s | 10,700 | 1.6 ms | 1.3 / 2.4 cores | 223 µs | 48 µs / 0.25 ms |
| Open loop, 34,300/s | 34,300 | 11 ms | 2.5 / 3.6 cores | 102 µs | 45 µs / 1.3 ms |
| One pod, peak | 42,900 | 10 ms (closed, 256 in flight) | 2.7 / 3.5 cores | 80 µs | 95 µs / 3.9 ms |
| Two pods, peak | 67,900 | 20 ms (closed, 256 in flight each) | 3.2 / 3.9 cores | 54 µs | 0.65 ms / 7.3 ms |

**marco tops out at about 68,000 logged-in queries a second** on its 4 vCPUs, 54 µs of VM CPU a
query (47 µs in marco's process). With two pods it's throttled in nearly every 100 ms period (145 to
156 of 150 in a 15 s window), so its cgroup quota is the limit. With one pod it stops at 43,000/s with its VM at 3.5 cores
and never throttled: each vCPU thread at 0.7 to 0.85 of a core, the virtio-net thread at 0.65. That
limit is one h2 connection to marco, not marco: these runs used one connection a pod, and two
reach the quota. A pod opens `--marco-connections` (4) and spreads its calls over them. marco's miss pool stayed idle (`marco_store_miss_pool_busy` 0) and no query
errored. Per query, marco's CPU falls from 316 µs at 1 in flight to 54 µs at the top as deeper
queues batch more calls per h2 frame and wakeup.

**Viewer builds.** A viewer that isn't cached costs marco a Counts call for their follow count,
then a ListOut of their follows (a page per 1,000) and one Intersect page for their mutuals. A viewer
who follows more than `--max-follows` gets neither: the cache keeps a small verdict, and their queries
are filtered by blocks without follows first. The test mix is every account that posted a mention in
the last two days, 62,978 of them, with these follow counts:

| Follows | p50 | p90 | p95 | p99 | p99.9 | max | mean |
|---|---|---|---|---|---|---|---|
| Mention authors | 324 | 2,087 | 4,241 | 23,734 | 79,219 | 591,685 | 1,406 |

| Cap | Viewers over it | Their share of all follows | Mean follows under it |
|---|---|---|---|
| 2,000 | 10.4% | 73% | 421 |
| 5,000 | 4.3% | 60% | 588 |
| 10,000 | 2.4% | 51% | 712 |
| 50,000 | 0.3% | 19% | 1,148 |

The long tail is mostly bots, which don't type into a search box. A cap of 5,000 sits just past the
95th percentile: it drops the 2,705 accounts (4.3%) that hold 60% of the mix's follows, and a build
reads at most five ListOut pages and one Intersect page. Built once each, 16 at a time against
production marco, the mix took 25.6 s (2,460 builds a second) against 97.6 s (645 a second) with
the old 50,000 cap. With every author asking at random against a 256 MB cache, the pod answered
4,540 logged-in queries a second at 256 in flight against 2,560, and no query lost follows first.
With the old cap 23,166 did, because builds ran past marco's timeout and paused.

**The cache entry.** An entry holds the viewer's follows and mutuals as the artifact's u32 ids,
sorted, and the positions of the follows' keys in the artifact's sorted key array. A key starts with
the typed prefix exactly when its position falls inside the prefix's range, so a viewer's matching
follows are a range of their positions, with no key bytes kept. Follows the overlay speaks for (new
accounts and changed names) keep their keys. That's about 21 bytes a follow, where the old entry kept
each follow's DID and folded keys:

| Follows (mention authors under the cap) | Before, weighed | Before, RSS | Now, weighed | Now, RSS |
|---|---|---|---|---|
| 43 (p10 to p20) | 7.6 KB | 11.1 KB | 1.4 KB | 1.6 KB |
| 359 (p50 to p60) | 63 KB | 85 KB | 8.1 KB | 8.6 KB |
| 1,157 (p80 to p90) | 202 KB | 257 KB | 25 KB | 28 KB |
| 4,461 (p99 to p100) | 781 KB | 1.0 MB | 96 KB | 101 KB |
| Mean, 588 | 103 KB | 138 KB | 13.0 KB | 14.6 KB |

The weight is each allocation at its jemalloc size class plus 512 bytes for moka's entry and the key.
RSS grew 12% more than the weight over all 60,264 entries (783 MB weighed, 880 MB RSS). Logged-in
CPU is the same: with 500 cached viewers on bare metal, 296 µs of pod CPU a query before and 298 µs
after at 16 in flight. The match itself is 29 µs either way (`seek_query_stage_seconds`): the global
fill is 0.9 µs slower, as a follow check is now a binary search.

**Artifact swaps.** Ids are per artifact. At a swap the pod maps each old id to the new artifact's
in one merge of the two DID tables, then a sweep moves every cached entry onto the new ids. It keeps
the entry's idle deadline and asks marco nothing. A query that reaches an entry first moves it
itself. A follow neither artifact nor overlay has (an account with no handle or name yet) is kept as
its DID and tried again at each swap. On the whole network's artifact the map (43.0M ids, 172 MB)
took 342 ms. With 60,148 viewers cached (783 MB weighed), the sweep took 8.0 s on one thread, about
1% of a core at one swap every 15 minutes, and the map went when it ended. Of 2,000 of those viewers
asked after the swap, all but 9 hit. Those 9 had followed someone since, so Jetstream had dropped
their entries.

**Hit rate.** Replaying the two days of mention posts in order, each post a session whose first
keystroke finds its viewer cached or builds it (121,508 sessions after the 12,284 from capped
authors, 42 a minute, about 350 distinct viewers in 10 minutes). Half the sessions are a viewer
coming back, so 50.4% is the most a cache can hit:

| Cache | 64 MB | 128 MB | 256 MB | 512 MB | 1 GB | 2 GB |
|---|---|---|---|---|---|---|
| Idle 10 minutes | 22.6% | 22.6% | 22.6% | 22.6% | 22.6% | 22.6% |
| Idle 6 hours | 34.7% | 37.8% | 38.8% | 38.8% | 38.8% | 38.8% |
| Idle 24 hours | 34.7% | 38.3% | 42.7% | 48.0% | 48.0% | 48.0% |
| No idle limit | 34.7% | 38.3% | 42.7% | 49.3% | 50.4% | 50.4% |
| The old entry (same cap), no idle limit | 25.3% | 28.5% | 32.0% | 35.5% | 39.1% | 43.8% |

At 10 minutes idle the cache's size doesn't matter: more than half of returning viewers come back
later. So entries live a day (`--viewer-idle-seconds 86400`). A day-old entry stays right because
Jetstream's follow records drop it:

- A viewer's follow or unfollow drops their entry, and a follow drops its subject's too, since the
  subject's mutuals changed. An unfollow's delete doesn't name the subject, so the subject keeps
  the unfollower as a mutual (a +1 nat among follows) until their entry is read again.
- marco can apply a follow after seek sees it on Jetstream: 99% of marco's events within 1.1 s of
  their time, and at most 2.4 s behind seek's own Jetstream position over a day. A build whose marco
  read starts before the event plus `--marco-lag-ms` (5 s) isn't cached, so a build already reading
  when the event came isn't kept either. The entry is dropped once more at 10 s.
- Whatever an event missed, an entry is read from marco again a day after its last read, however
  often it's used. 1 GiB (`--viewer-cache-mb 1024`)
holds the whole two days of authors (783 MB weighed) with room for more traffic. The pod's 8 GB
has 4.87 GB at a swap with two artifacts mapped. A full cache adds about 1.2 GB of RSS and the id
map 0.17 GB while the sweep runs, so a swap peaks near 6.3 GB. On bare metal, with 783 MB cached
and both artifacts paged in, the swap above peaked at 6.06 GB.

On 2 vCPUs, an artifact swap takes the pod a few seconds of CPU (the download, its check and the swap).
At 21,000/s offered, logged out, one step that overlapped it backed up to a 2.7 s p50. During the
swap RSS reached 4.87 GB, with both artifacts mapped, against the VM's 5 GB.
