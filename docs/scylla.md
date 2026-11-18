---
title: Rebuilding from Scylla
section: marco
order: 7
summary: "scan-scylla reads an appview's follows, blocks and list items tables with a parallel full scan, builds a snapshot, and marco catches up from Jetstream: 6.8 min for the whole network's follows and blocks."
---

```hero
diagram:
  caption: "scan → spill → build → catch-up. The scan writes the same build input any exporter writes, and the build doesn't know where it came from. Catch-up is Jetstream-only."
  nodes:
    - { id: sc, label: Scylla, sub: "follows · blocks · lists", at: [0, 0], size: [8, 3], shape: store, tone: muted }
    - { id: scan, label: scan-scylla, sub: "per-shard ranges", at: [12, 0], size: [9, 3], tone: accent }
    - { id: spill, label: build input, sub: "1,025 files a table", at: [25, 0], size: [9, 3], shape: store, tone: amber }
    - { id: snap, label: snapshot, sub: "time_us = start − 5 m", at: [25, 5.5], size: [9, 3], shape: store, tone: amber }
    - { id: srv, label: marco serve, sub: "--jetstream", at: [12, 5.5], size: [9, 3], tone: accent }
    - { id: js, label: Jetstream, at: [0, 5.5], size: [8, 3], tone: muted }
  edges:
    - "sc -> scan: token ranges"
    - "scan -> spill"
    - "spill -> snap: build"
    - "snap -> srv: O_DIRECT"
    - "js -> srv: since time_us"
facts:
  - { value: "4.4", unit: min, label: to scan 2.97B rows, note: "follows + blocks · 11.2M rows/s · one node", tone: accent }
  - { value: "114", unit: s, label: to build, note: "38.5 GB snapshot", tone: amber }
  - { value: "6.8", unit: min, label: empty directory to ready, note: "scan · build · catch up", tone: cyan }
  - { value: "0", label: records differ, note: "against the archive build", tone: ok }
```

If you run an appview, you already have the graph in Scylla. `marco-load scan-scylla` reads the
follows, blocks and list items tables, writes [build input](quickstart.md#the-input-format), and
with `--snapshot` builds it. Then `marco serve --jetstream` replays what changed since the scan
began. On one ScyllaDB node holding the whole network's follows and blocks (2.97B rows), that took
6.8 minutes from an empty directory to `/readyz` 200, and the snapshot matched the archive build
record for record ([Rebuild from Scylla, measured](scylla-results.html)). List items weren't part of that run, so
every whole-network number on this page is follows and blocks only.

## Run it

```bash
# 1. scan and build; run it again with the same --out if it stops
marco-load --ops 127.0.0.1:9101 --report scan.json scan-scylla \
  --config scylla.toml --out /data/scan --snapshot /data/snap

# 2. serve it and catch up from the snapshot's time; /readyz turns 200 once caught up
marco serve --snapshot /data/snap --memory 13GiB --jetstream wss://jetstream2.us-east.bsky.network
```

```toml
nodes = ["10.0.0.1:9042", "10.0.0.2:9042"]   # contact points; the driver finds the rest
keyspace = "bsky"
# datacenter = "us-east"       # read only this datacenter's replicas, retries included
# username = "marco"           # with password_file
# password_file = "/run/secrets/scylla-password"
# consistency = "local_quorum" # default one (local_one with a datacenter)
page_size = 5000               # rows per page
ranges_per_shard = 8           # ranges per replica shard: the unit of work and resume
in_flight_per_shard = 2        # ranges read at once per replica shard
bypass_cache = true            # SELECT … BYPASS CACHE
compression = "lz4"            # lz4, snappy, none
request_timeout_secs = 60
retries = 8                    # attempts per page before the scan stops (resume continues it)

[[table]]
relation = "follow"            # a relation in the relations config (--relations)
name = "follows"
actor = "actor_did"            # the record's repo DID (default actor_did)
rkey = "rkey"                  # the record key (default rkey)
subject = "target_did"         # the subject DID (default target_did)

[[table]]
relation = "block"
name = "blocks"

[[table]]
relation = "listitem"          # a list relation: the table needs a list column
name = "list_items"
subject = "subject"
list = "list"                  # the column with the list's AT URI (default list)

# listblock and modlist aren't defaults: these two need them in --relations

[[table]]
relation = "listblock"         # subjects are list URIs: the subject column holds them
name = "listblocks"
subject = "list"

[[table]]
relation = "modlist"           # `when` reads the record: the table names its record column
name = "lists"
record = "raw"                 # the record's JSON (blob or text); no subject or list column
```

The scan needs `SELECT` on the tables, `system_schema` and `system.tablets`. Column and table
names that aren't plain lower-case identifiers are quoted for you.

`--snapshot` builds with the defaults: 64 shards and a by-subject index on lists of 4,096 records
or more. For anything else, scan with `--keep-spill` (or without `--snapshot`) and run
`marco-load build --input /data/scan --relations /data/scan/relations.toml --shards N` yourself.

`examples/scylla.toml` is this config with every table's columns spelled out.

## Try it on one machine

You can run the whole path against a throwaway Scylla in Docker. `fill-scylla` loads build input
into tables as current state, so the [Quickstart](quickstart.md)'s four-account `input` works:

```bash
docker run -d --name scylla scylladb/scylla --smp 2 --memory 2G --developer-mode 1 --overprovisioned 1
ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' scylla)
sed -e "s/^nodes = .*/nodes = [\"$ip:9042\"]/" -e 's/^keyspace = .*/keyspace = "marco"/' \
  examples/scylla.toml > scylla.toml

# 1. the input into Scylla: makes the keyspace and the three tables, keyed ((actor_did), rkey)
target/release/marco-load fill-scylla --config scylla.toml --input input --create actor-rkey

# 2. scan it back and build, then compare with the direct build
target/release/marco-load scan-scylla --config scylla.toml --out scan --snapshot snap-scylla
target/release/marco-load build --input input --out snap
target/release/marco-load diff --a snap --b snap-scylla

# 3. serve the rebuilt snapshot as in the quickstart
target/release/marco serve --snapshot snap-scylla --memory 3GiB --jetstream wss://jetstream2.us-east.bsky.network
```

`diff` compares the two snapshots DID by DID and reports any record that differs.
`docker rm -f scylla` removes the container and its data.

## What it assumes about the tables

- One row per record: the record's repo DID, its rkey and its subject DID, as text. A list
  relation's table also has the list's AT URI (the `list` column), and only a list relation's
  table may have one. For a relation whose subjects are records, the subject column holds the
  record's AT URI ([Records as subjects](relations.md#records-as-subjects)).
- A relation with `when` or a `$self` subject needs a table that holds each record
  ([Tables that hold the record](#tables-that-hold-the-record)), and the config is refused without
  one.
- The partition key comes from the schema, and the token bounds use all of its columns, so any
  partition key works.
- The snapshot holds exactly the relations the config's tables name. A relation left out isn't in
  the snapshot at all, and since marco follows Jetstream only for the snapshot's relations, it
  never fills. So name the list items table too, or the rebuilt marco has no lists.
- A `listitem` table gives the snapshot `listitem` and its companion `listitem.owner`: a list's
  members, an owner's items and the lists a DID is on. An item whose list is in another repo is
  dropped, as on the live path, and counted as `cross_repo_rows` in the report's table
  ([the owner rule](relations.md#the-owner-rule)).
- Rows whose repo isn't a DID, or whose subject isn't one the relation takes, are counted and
  skipped. The whole-network run had 13
  (3 follows, 10 blocks). A row with a null column is skipped and counted as `null_rows`.

### Tables that hold the record

Some relations can't be read from columns. `modlist` keeps a list only while its record's
`purpose` is a mod list, and a table that stores each list's record as JSON has no purpose column
to read. Name that column as the table's `record`, and the scan reads `actor`, `rkey` and
the record, puts the record's URI together (`at://<actor>/<collection>/<rkey>`) and runs it
through the relation's rule, as the live path does with a Jetstream record. The rule reads the
subject (and a list relation's list) from the record by the relation's own paths, so a record
table names no `subject` or `list` column. A relation with `when` or a `$self` subject needs one.

| A row whose record | |
|---|---|
| is an edge | written as a create, its subject the record's own URI under `$self` |
| fails `when` (a curation or reference list) | dropped, counted as `unmatched_rows` |
| names a list in another repo | dropped, counted as `cross_repo_rows` |
| doesn't parse as a JSON object, or isn't an edge (a bad subject, a bad rkey) | dropped, counted as `bad_rows` |
| is null | dropped, counted as `null_rows` |

The record column is a blob or text, and the scan refuses any other type before it reads a row.
`fill-scylla` writes column tables only, since build input holds no records. The scan keeps the
edges a rule gave, not the records, so it resumes only under the rules it began with: change a
record table's relation in `--relations` (its `when`, say) and it asks for `--fresh`.

### The primary key

| Key | What the snapshot holds |
|---|---|
| `((actor_did), rkey)` | Exactly the table's records: each record is its own row. |
| `((actor_did), target_did)`, rkey a plain column | One row per pair. An actor that follows someone twice (two records, about 0.14% of follows) has one, and marco keeps that one's rkey. A later Jetstream delete of the other finds nothing, and a delete of the kept one drops the pair while a record still exists in the repo. |

The scan warns when the key leaves out the rkey column.

## How it reads

For each table the scan asks the cluster who owns what. Tablet tables list each tablet's replicas
and shards in `system.tablets`. Vnode tables give a replica per vnode, and Scylla's sharding
function says which shard owns each token. The scan cuts ranges so that one replica shard owns
each one whole, about `ranges_per_shard` (8) per shard, and routes every query straight to that
shard:

```sql
SELECT token(actor_did), actor_did, rkey, target_did FROM ks.follows
 WHERE token(actor_did) > ? AND token(actor_did) <= ? BYPASS CACHE
```

It keeps `in_flight_per_shard` (2) ranges going per shard, at consistency ONE unless set, over
LZ4-compressed connections, `page_size` (5,000) rows a page. `BYPASS CACHE` keeps a scan of every
row from evicting the cluster's working set. A page that fails is retried on another replica. With
replication factor 3, ranges spread over all three replicas.

Token order isn't DID order, so rows are bucketed by the repo DID's first two characters as they
arrive, 1,025 files per table, which is the order the build needs.

## Memory and disk

| | |
|---|---|
| Client memory | `--memory-gb` (4) caps what all ranges in flight buffer, by lowering `--chunk-rows` (262,144). 20 nodes of 64 shards is 2,560 ranges at once, about 5,000 rows a chunk in 4 GB. A partition is held whole until it ends, so a very long list adds its size on top. The budget counts the chunks only. The whole-network run peaked at 9.0 GB RSS at 2 ranges in flight per shard and 11.5 GB at 8, both under the default 4 GB. |
| Scan output | About the rows compressed with zstd: 66.4 GB for the whole network's follows and blocks, 22 B a row, in `--out`. A list item's row also carries its list's URI (~70 B raw), and the tests' synthetic items took 27 B a row. Put it on local NVMe. |
| Build spill | The build's own in-pass spill, about 23.7 GB for the whole network, goes beside the snapshot in `<snapshot>.spill`. scan-scylla has no `--tmp` to move it. |
| Peak | About 128 GB at once: the scan output, the build spill and the snapshot being written. |
| Open files | One per bucket per table (1,025 each), plus a connection per shard per node. The scan raises its soft limit to the hard one and stops with a message if that's still too few. |
| Snapshot | 38.5 GB, built from the scan output in 114 s on 32 threads. `--snapshot` deletes the scan output after (`--keep-spill` keeps it). |

## How long it takes

| Step | One 16-shard node, whole network |
|---|---|
| Scan | 4.2-4.4 min, 11.2M rows/s, 861 MB/s of row data |
| Build | 114 s on the whole box (153 s sharing it with Scylla) |
| Open the snapshot | 234 ms |
| Jetstream catch-up to `/readyz` | 3 s (11.8 min of Jetstream: 18,696 adds, 4,449 deletes) |
| Total | 6.8 min |

Scylla's CPU was the limit: its reactors were 88% busy at the default 2 ranges in flight per
shard, while the client used 2.9 of its 16 threads and the NVMe read 484 MB/s. One range per shard
left shards idle (8.8M rows/s), and 4 or 8 bought 5% at most. Each Scylla shard served ~0.7M rows/s,
so a real cluster scans that many times faster per shard it has, until the client or the network
fills. One 32-thread client would carry ~110M rows/s by the same ratio.

If the scan is slow, look at the cluster's per-shard CPU first. Raise `in_flight_per_shard` if
shards idle, lower it if the cluster serves traffic and its latency suffers. More ranges in flight
also take more client memory (11.5 GB RSS at 8 against 9.0 GB at 2).

## Resume

A checkpoint every `--checkpoint-secs` (10) fsyncs the bucket files and records each file's
length and each range's position (its last whole partition) in `scan-state.json`. If the scan
stops, from a crash, a lost node or a page that failed `retries` (8) times, run the same command
again. It cuts the files back to the checkpoint and goes on from there, losing at most the seconds
since the last checkpoint. `--fresh` starts over. With `datacenter` set, the scan never fails over
to another datacenter: it stops instead, and resumes when run again.

The resume was tested by SIGKILLing scans at random until one finished (71 kills over 10 rounds):
every row in the spill exactly once, and the snapshot equal to the direct build.

## Catching up

```timeline
caption: "The scan isn't one moment. Rows read early can change before it ends, so the snapshot's time is the scan's start less a margin, and the catch-up replays everything since. Not to scale."
scale: 60
lanes:
  - { id: sc, label: scan-scylla, tone: accent }
  - { id: js, label: Jetstream, sub: replay, tone: muted }
spans:
  - { lane: sc, from: 1, to: 6, label: scan + build }
  - { lane: js, from: 0, to: 7.5, label: "replayed from time_us", tone: blue }
marks:
  - { at: 1, label: scan starts, tone: muted }
ticks:
  - { at: 0, label: "start − 300 s" }
  - { at: 7.5, label: ready }
```

The scan records when it began and writes `time_us` into `cursors.json` `--cursor-margin-secs`
(300) before that. `marco serve --jetstream` replays everything since. Replay is idempotent by
record URI and deletes leave tombstones by time, so a record created during the scan is added if
the scan missed it, and a record deleted during the scan is removed even if the scan read it.

Catch-up is Jetstream-only. A relay's `subscribeRepos` resumes only by `seq`, so `--firehose` on
a snapshot with only a `time_us` refuses to start, since it would begin at the live tail and miss
everything in between (`--allow-gap` overrides). Once caught up, the snapshot has a Jetstream
cursor of its own.

The source has to still hold that time when the server starts. The scan warns when its pace heads
past `--retention-hours` (36), the build refuses a time with under 2 h to spare, and the server
refuses one past `--source-retention-hours` (36) or one its Jetstream instance has dropped.
Measure your instance's retention with `marco-harness retention --jetstream URL`. The public
`jetstream2.us-east` kept 36.6 h when measured.

## For production

The margin only covers what changed after the snapshot's time. Two things it can't fix stay wrong
until the next rebuild:

- The table lags the network. If the indexer writing it runs minutes behind, a row it hadn't
  written when the scan read its range is neither scanned nor replayed. Set
  `--cursor-margin-secs` above the indexer's worst lag.
- A replica missed writes. At consistency ONE the scan reads one replica, and one that dropped
  writes hands back an old row set. Read at `consistency = "local_quorum"`, or repair first.

Also measure the source's retention and pass it as `--retention-hours` and
`marco serve --source-retention-hours`, and start the server soon after the build, since the
window closes while the snapshot waits. `crates/marco-load/README.md` has every flag and the
integration tests (`fill-scylla`, `diff`, the kill test).
