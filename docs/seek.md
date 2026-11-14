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
| `seek builder` | Holds every actor's handle claim, display name and status. It follows Jetstream into them, verifies handles, and every 15 minutes builds the artifact and serves it at `/artifact/latest` (uploading it to a bucket when one is configured). On each new marco checkpoint it mirrors the checkpoint and runs PageRank on the mirror. | Its volume: the names state, the artifacts, the mirror and the ranks |
| `seek serve` (a pod) | Downloads the newest artifact, checks it, maps it, replays Jetstream from the artifact's cursor into a small live overlay, and answers queries. A newer artifact is swapped in without a pause. | None: a scratch directory for the mapped artifacts |
| marco | Gives a pod each viewer's follows and mutuals (once per session) and filters every logged-in page by blocks both ways. It exports its checkpoint to the builder's mirror. | Its own |

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

A score is the actor's u16 log PageRank (about 4,400 a nat) plus its match tier's weight. The
folded handle and display name start at 0, a later label or word at −1 nat. An exact handle match
comes first and stays on the page even when a block would drop it, as the reference AppView does,
so a user can find who they blocked. Exact means the handle itself: each actor's handle fingerprint
has to match the typed query, not just its fold. Signed in, follows sort ahead of everyone
(`--follows wall`), and mutuals get +1 nat among them. `--follows N` gives follows a boost of N nats
instead.

Blocks are marco's `block` records both ways. Moderation-list blocks (`listblock`) aren't covered:
marco has no listblock relation, so the AppView still filters those at hydration.

## Flags

Every flag has an env var. `seek serve`:

| Flag | Env | Default | |
|---|---|---|---|
| `--listen` | `SEEK_LISTEN` | `0.0.0.0:7310` | The API: `bsky.Service`, `/healthz`, `/readyz` |
| `--ops-listen` | `SEEK_OPS_LISTEN` | `127.0.0.1:7311` | `/metrics`, `/debug/pprof/{profile,heap}`, `/debug/tracing`. Bind it privately |
| `--source` | `SEEK_SOURCE` | | The builder's `http(s)://…/artifact/latest`, `s3://bucket/prefix`, or a file or directory |
| `--data` | `SEEK_DATA` | `/var/lib/seek` | Where the mapped artifacts live: two at a swap. Never the source's directory |
| `--poll-seconds` | `SEEK_POLL_SECONDS` | 30 | Between source polls |
| `--marco` | `SEEK_MARCO` | | marco's API listener |
| `--marco-timeout-ms` | `SEEK_MARCO_TIMEOUT_MS` | 500 | Each marco call |
| `--viewer-deadline-ms` | `SEEK_VIEWER_DEADLINE_MS` | 2000 | A viewer's follows and mutuals, every page. Past it the query answers without follows first; blocks are still filtered. 0 turns follows first off |
| `--max-follows` | `SEEK_MAX_FOLLOWS` | 50000 | Follows read per viewer, newest first |
| `--viewer-cache-mb` | `SEEK_VIEWER_CACHE_MB` | 256 | The viewer cache's weighed size |
| `--viewer-idle-seconds` | `SEEK_VIEWER_IDLE_SECONDS` | 600 | A viewer's entry goes this long after its last query, or when Jetstream shows them follow or unfollow |
| `--follows` | `SEEK_FOLLOWS` | `wall` | `wall`, or a boost in nats |
| `--replay-window-hours` | `SEEK_REPLAY_WINDOW_HOURS` | 24 | Jetstream's replay window |
| `--max-lag-seconds` | `SEEK_MAX_LAG_SECONDS` | 300 | Not ready while Jetstream is further behind, or silent this long |
| `--follow-events` | `SEEK_FOLLOW_EVENTS` | true | Read follow records to drop a viewer's entry when they follow or unfollow |
| `--jetstream` | `SEEK_JETSTREAM` | `wss://jetstream2.us-east.bsky.network` | |
| `--plc`, `--plc-rate` | `SEEK_PLC`, `SEEK_PLC_RATE` | `https://plc.directory`, 10/s | DID documents for handle verification |
| `--verify-rate`, `--verify-workers` | `SEEK_VERIFY_RATE`, `SEEK_VERIFY_WORKERS` | 10/s, 8 | DNS TXT and HTTPS well-known lookups, to hosts the handles name |
| `--no-verify` | `SEEK_NO_VERIFY` | off | A handle change leaves the actor with no handle until the next artifact |
| `--s3-endpoint`, `--s3-region`, `--s3-credentials` | `SEEK_S3_*` | | For an `s3://` source: a JSON file `{"access_key_id", "secret_access_key"}` |
| `--trace-ratio`, `--otlp` | `SEEK_TRACE_RATIO`, `SEEK_OTLP` | 0 | The share of queries traced, and the collector |
| `--log-format` | `SEEK_LOG_FORMAT` | `json` | `json` or `text`; `SEEK_LOG` filters (`info`) |

`seek builder` also takes `--jetstream`, the verification flags, the S3 flags and the ops flags, and:

| Flag | Env | Default | |
|---|---|---|---|
| `--dir` | `SEEK_BUILDER_DIR` | | The state, artifacts, mirror and ranks |
| `--listen` | `SEEK_LISTEN` | `0.0.0.0:7320` | `/artifact/latest` (an ETag, `x-seek-version`, `x-seek-cursor`), `/healthz`, `/readyz` |
| `--bootstrap`, `--cursor-us` | `SEEK_BOOTSTRAP`, `SEEK_BOOTSTRAP_CURSOR_US` | | The first state when `--dir` has none, and the Jetstream time it's consistent with |
| `--interval-seconds` | `SEEK_INTERVAL_SECONDS` | 900 | Between artifacts |
| `--index` | `SEEK_INDEX` | `profiles` | Who's indexed: anyone with a profile record, or `all` the DIDs it knows |
| `--marco-admin`, `--marco-token-file` | `SEEK_MARCO_ADMIN`, `SEEK_MARCO_TOKEN_FILE` | | On each new marco checkpoint: mirror it into `DIR/mirror`, then PageRank into `DIR/ranks` |
| `--marco-load` | `SEEK_MARCO_LOAD` | `marco-load` | The binary that mirrors and ranks |
| `--pagerank-source`, `--pagerank-memory` | `SEEK_PAGERANK_SOURCE`, `SEEK_PAGERANK_MEMORY` | `csr-inset`, `16GiB` | `csr-inset` peaks near 27 GB on the whole network and takes about 20 s warm; `inset` near 10 GB and slower |
| `--ranks`, `--ranks-token-file` | `SEEK_RANKS`, `SEEK_RANKS_TOKEN_FILE` | | Ranks from elsewhere: a ranks file or directory, or a URL |
| `--without-ranks` | | off | Publish without ranks. Otherwise the builder won't publish until it has them, since the ranks carry marco's takedowns |
| `--keep` | `SEEK_KEEP` | 4 | Artifacts kept, locally and in the bucket |
| `--bucket` | `SEEK_BUCKET` | | Also upload each artifact to `s3://bucket/prefix`, with `latest.json` naming the newest |
| `--state-every` | `SEEK_STATE_EVERY` | 4 | Save the names state every this many cycles |
| `--verify-per-cycle`, `--recheck-days` | `SEEK_VERIFY_PER_CYCLE`, `SEEK_RECHECK_DAYS` | 9000, 30 | Handles queued for verification a cycle, and how old a check gets before it's done again |

## Health

`/healthz` answers while the process runs. A pod's `/readyz` is 200 once it serves, else 503 with
the reason: no artifact yet; refusing to serve (the artifact's cursor is older than the replay
window, or Jetstream's replay skipped events); replaying Jetstream from the artifact's cursor; or
Jetstream more than `--max-lag-seconds` behind or silent. The builder's `/readyz` is 200 once it has
published and its last cycle succeeded.

## Metrics

Every series exists at 0 from start. Latencies on the keystroke path are histograms sharded per
thread, as marco's op histograms are.

| Metric | Labels | |
|---|---|---|
| `seek_queries_total` | `mode` (`logged_out`, `logged_in`), `result` (`ok`, `empty`, `error`) | Queries answered |
| `seek_query_seconds` | `mode` | Query latency inside seek |
| `seek_query_stage_seconds` | `stage`: `viewer`, `prefix`, `personal`, `global`, `filter` | The viewer's cache entry (marco on a miss), the prefix's range and exact handles, the viewer's matching follows, the global fill, marco's block filter |
| `seek_query_results_total` | `mode` | Actors returned |
| `seek_query_errors_total` | `kind`: `bad_request`, `not_ready`, `marco`, `no_marco`, `internal` | Failed queries |
| `seek_queries_in_flight` | | |
| `seek_query_logged_out_fallbacks_total` | | Logged-in queries answered without follows first (marco couldn't give them in time; blocks were still filtered) |
| `seek_query_blocked_total`, `seek_query_exact_kept_total` | | Candidates the block filter dropped, and exact handle matches kept through it |
| `seek_viewer_cache_hits_total`, `seek_viewer_cache_misses_total` | | |
| `seek_viewer_cache_evictions_total` | `cause`: `idle`, `size`, `invalidated` | |
| `seek_viewer_cache_entries`, `seek_viewer_cache_bytes` | | |
| `seek_viewer_build_seconds`, `seek_viewer_keys_seconds`, `seek_viewer_follows` | | A viewer's entry from marco, its keys folded against an artifact, and its follows |
| `seek_marco_seconds` | `method`: `filter`, `list_out`, `intersect` | |
| `seek_marco_errors_total` | `method`, `kind`: `timeout`, `unavailable`, `other` | |
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
| `seek_jetstream_reconnects_total`, `seek_jetstream_decode_errors_total`, `seek_jetstream_gaps_total` | | |
| `seek_ingest_panics_total` | | Changes that panicked and were skipped (a bug) |
| `seek_verify_queue` | | Handles waiting |
| `seek_verify_checks_total` | `result`: `ok`, `mismatch`, `error`, `rate_limited`; `method`: `did_doc`, `dns`, `well_known` | |
| `seek_verify_seconds` | `method` | |
| `seek_builder_step_seconds`, `seek_builder_step_last_seconds` | `step`: `ingest`, `mirror`, `pagerank`, `ranks`, `build`, `verify`, `upload`, `state` | |
| `seek_builder_cycles_total` | `result` | |
| `seek_builder_last_success_timestamp_seconds`, `seek_builder_artifact_bytes`, `seek_builder_upload_seconds`, `seek_builder_ranks_age_seconds` | | |
| `seek_builder_actors` | `state`: `known`, `indexed`, `handle_verified`, `handle_unverified`, `hidden` | |
| `seek_pagerank_passes`, `seek_pagerank_l1`, `seek_pagerank_seconds` | | The last run |
| `seek_pagerank_runs_total` | `result` | |
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
