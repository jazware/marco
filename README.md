# marco

marco is a graph database for atproto actor-to-actor edges: follows, blocks, list items and any
record whose subject is a DID. It holds who follows, blocks or lists whom for every account on the
network, and answers the graph questions an appview asks while it hydrates a page. Does the
viewer follow this author? Which of these 50 accounts block the viewer? Who are the mutuals? Which
of these actors are on a mute list?

It's one binary on local disk. The whole network's 2.97B follows and blocks are a 38.5 GB
snapshot, 13 B an edge, served from a 32-thread box with 61 GiB of RAM at about 1M `Has` calls a
second through the server. After a bulk build it keeps itself current from Jetstream (or a relay's
firehose, or your own pipeline through the write API).

## Highlights

- **Edges are records.** An edge's identity is its actor, relation and rkey, so a repeat create is
  a no-op and a delete by URI removes exactly one record. Relations come from config, so a new
  app's graph records need no code. → [Relations](docs/relations.md)
- **Lists are sets.** A list is a node like an actor, so "who's on it", "which lists is this DID
  on" and "these actors less the list's members" are all set operations.
- **One API.** `marco.v1.Graph` is a ConnectRPC service: gRPC, gRPC-Web and JSON over POST from one
  proto (`proto/marco/v1`). `Has`, `Relationships`, `BlockPairs`, `ListOut`, `ListIn`, `Counts`,
  `Intersect` and `Filter` read, and `Write` and `WriteBatch` write. → [Query API](docs/query-api.md)
- **Several ways to build a snapshot.** `marco-load build` reads build input: zstd RowBinary rows
  per relation, which any exporter can write (a ClickHouse archive export, or a script). If you
  run an appview, `marco-load scan-scylla` reads its follows, blocks and list items tables in
  Scylla with a parallel full scan and builds the snapshot from them: the whole network's follows
  and blocks went from an empty directory to ready in 6.8 minutes, and matched the archive build
  record for record. `marco-load scan-jetstream` replays a Jetstream v2 instance's sealed archive
  of the graph collections, account events included, and stamps the instance's last sealed seq,
  so `marco serve --jetstream-v2` on the same host resumes from it exactly; `scan-hubble` reads
  every repo from a sync-API mirror. → [Quickstart](docs/quickstart.md),
  [Rebuilding from Scylla](docs/scylla.md), [marco-load](crates/marco-load/README.md)
- **Its own cache.** marco reads the snapshot with `O_DIRECT` into a cache of `--memory` bytes,
  checking every page against its sum, and a budget that holds the whole network's snapshot loads
  it in 7.8 s from a cold disk. Writes go to a change segment every 5 s with every source's cursor,
  and a checkpoint folds them into new shard files. → [Durability and restarts](docs/durability.md)
- **An operator console and these docs are built in.** `/admin` on the admin listener shows the
  graph, its relations, ingest, requests, storage and settings, and `/docs` on both listeners serves the pages in `docs/`.
  `/` on the API listener is an explorer that runs the read calls and shows each answer, its latency and its `curl`.
  → [Operations](docs/operations.md)

## Quickstart

You need Rust (the toolchain in `rust-toolchain.toml`), Node and, for the example input, Python 3
and the `zstd` CLI.

```bash
(cd ui && npm ci && npm run build)     # the console and the docs, embedded by the release build
cargo build --release -p marco-load -p marco-server
```

That gives `target/release/marco-load`, the bulk loader, and `target/release/marco`, the server.
Build a snapshot from build input, then serve it and catch up from Jetstream:

```bash
target/release/marco-load build --input input --out snap
openssl rand -hex 16 > token
target/release/marco serve --snapshot snap --memory 3GiB --admin-token-file token \
  --jetstream wss://jetstream2.us-east.bsky.network
curl -s localhost:7300/readyz          # 503 while it catches up, then "ready"
curl -s localhost:7300/marco.v1.Graph/Has -H 'content-type: application/json' \
  -d '{"rel":"follow","from":"did:plc:…","to":"did:plc:…"}'
```

The [Quickstart](docs/quickstart.md) writes a four-account `input` by hand and runs the whole path
in about five minutes. To rebuild from an appview's Scylla instead, start from
[`examples/scylla.toml`](examples/scylla.toml) and [Rebuilding from Scylla](docs/scylla.md):

```bash
target/release/marco-load scan-scylla --config scylla.toml --out /data/scan --snapshot /data/snap
```

Without an appview of your own, `scan-hubble` builds the input from a sync-API mirror's repos
(Hubble by default). Its catch-up limit, and the rate a whole-network sweep needs, are in
[Building from Hubble](crates/marco-load/README.md#building-from-hubble):

```bash
target/release/marco-load scan-hubble --out /data/hubble --snapshot /data/snap --contact you@example.com
```

The image is `ghcr.io/jazware/marco`. Its entrypoint is `marco`, with the snapshot in
`/var/lib/marco`:

```bash
docker run -p 7300:7300 -v $PWD/snap:/var/lib/marco/snap ghcr.io/jazware/marco \
  serve --snapshot /var/lib/marco/snap --jetstream wss://jetstream2.us-east.bsky.network
```

## Docs

The same pages are served at `/docs` by every `marco serve`.

| | |
|---|---|
| [Overview](docs/overview.md) | What marco holds, the shape of a snapshot, what it isn't |
| [Quickstart](docs/quickstart.md) | Build, serve and query a four-account example |
| [Relations](docs/relations.md) | Relations from config, lists and the owner rule |
| [Query API](docs/query-api.md) | Every Graph call, with JSON examples |
| [Writing and ingest](docs/ingest.md) | Jetstream, the firehose and the write API |
| [Durability and restarts](docs/durability.md) | Segments, checkpoints, the ready gate |
| [Rebuilding from Scylla](docs/scylla.md) | `scan-scylla`: an appview's tables to a snapshot |
| [Operations](docs/operations.md) | Listeners, auth, flags, metrics |
| [Sizing and performance](docs/sizing.md) | Disk, memory, read and write rates, rebuild times |
| [seek](docs/seek.md) | The actor typeahead that calls marco: running it, flags, metrics, alerts |
| [`crates/marco-load`](crates/marco-load/README.md) | The build input format and every loader command |

The measured results are standalone pages in `docs/`: the whole-network build and reads
(`p0-results.html`), throughput and latency (`perf.html`), durability (`p1-results.html`), lists
(`lists-results.html`), a read mix from real app traffic (`read-mix.html`) and the Scylla rebuild
(`scylla-results.html`).

## seek

seek is an actor typeahead for an AppView, in this repository because it calls marco. A builder
keeps every actor's handle and display name and publishes one index artifact every 15 minutes, with
PageRank over marco's follows baked in. Pods map the newest artifact, follow Jetstream and answer
the reference AppView's `bsky.Service/SearchActorsTypeahead`. Signed in, the viewer's follows come
first and blocks both ways are dropped. One image runs both:

```bash
docker run -v seek-builder:/var/lib/seek-builder -p 7320:7320 ghcr.io/jazware/seek \
  builder --dir /var/lib/seek-builder --bootstrap /var/lib/seek-builder/bootstrap.jsonl.zst --cursor-us 1794628081312952 \
  --marco-admin http://marco:7301 --marco-token-file /run/secrets/marco-admin-token
docker run -p 7310:7310 ghcr.io/jazware/seek \
  serve --source http://seek-builder:7320/artifact/latest --marco http://marco:7300
```

`docker build --target seek -t seek:local .` builds the image from this repository. [docs/seek.md](docs/seek.md)
has the rest.

## The crates

| Crate | |
|---|---|
| `marco-core` | The engine: the DID directory, out logs, in sets, snapshots, change segments |
| `marco-server` | The server, binary `marco`: the Graph and Admin APIs, the console, the docs |
| `marco-proto` | The API, generated from `proto/` |
| `marco-ingest` | The Jetstream and firehose consumers |
| `marco-load` | The bulk loader: `build`, `scan-scylla`, `fill-scylla`, `diff`, benches |
| `marco-harness` | Source captures, the kill test, ingest lag, Jetstream retention, perf runs |
| `marco-ops` | `/metrics`, pprof CPU and heap profiles, the tracing switch |
| `seek-core` | seek's index: folding, keys, the artifact, the overlay, ranking |
| `seek-api` | seek's API: the dataplane's `bsky.Service/SearchActorsTypeahead` |
| `seek-server` | seek, binary `seek`: the pod (`seek serve`) and the builder (`seek builder`) |
| `seek-proto` | The design's prototype and its measurements (`docs/seek.html`) |

marco builds on [vlatproto](https://github.com/jazware/vlatproto) for CBOR, CIDs, firehose frames
and DIDs.

## Development

```bash
cargo test --workspace                # the Scylla integration tests are #[ignore]d: they need a Scylla
cargo clippy --all-targets -- -D warnings
cd ui && npm run dev:mock             # the console and docs against a mock, on :5730
```

`crates/marco-load/README.md` has how to run the Scylla tests against a local Scylla in Docker.
`just gen` regenerates the API code from `proto/` with pinned tools.

## License

MIT, see [LICENSE](LICENSE).
