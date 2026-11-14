# marco-load

The bulk loader: it builds a marco snapshot from a directory of exported
records, without going through the server's write path. It also scans
ScyllaDB tables, a sync-API host's repos or a Jetstream v2 instance's
archive into that directory, and carries the measurement commands from the
P0 and P1 runs.

```text
marco-load build        --input DIR --out SNAP           build input -> snapshot
marco-load scan-scylla  --config TOML --out DIR [--snapshot SNAP]
                                                         Scylla tables -> build input (-> snapshot)
marco-load fill-scylla  --config TOML --input DIR        build input -> Scylla tables (tests, benchmarks)
marco-load scan-hubble  --out DIR [--host URL] [--snapshot SNAP]
                                                         a sync host's repos -> build input (-> snapshot)
marco-load scan-jetstream --out DIR [--host URL] [--snapshot SNAP] --api-key-file FILE
                                                         a Jetstream v2 archive -> build input (-> snapshot)
marco-load diff         --a SNAP --b SNAP                two snapshots, record by record
marco-load verify       --snapshot SNAP                  checksums, every list decoded, both directions agree
marco-load open | bench | replay                         the P0 measurements
marco-load bench-lists  --snapshot SNAP                  list sizes, lookups, set ops, listitem writes
marco-load pagerank     --snapshot SNAP --out-dir DIR    PageRank over the follows -> a ranks file
```

`--ops 127.0.0.1:PORT` serves `/metrics` and `/debug/pprof/*` while any of
them runs. `--report FILE` writes the JSON report that also goes to stdout.

## The input format

`marco-load build` reads one directory. Any source becomes a marco snapshot
by writing it, so this is the contract every exporter writes to. The
ClickHouse archive export, `scan-scylla`, `scan-hubble` and `scan-jetstream` are such exporters.

```text
DIR/
  cursors.json              optional: source -> cursor the records are current to
  accounts.rb.zst           optional: account events (statuses, deletions, resets)
  relations.toml            optional, not read by the build: scan-scylla's note of what it holds
  follow/                   one directory per relation (its name in the relations config)
    p-22.rb.zst             did:plc repos whose identifier starts "22"
    p-23.rb.zst
    …
    p-zz.rb.zst
    z-other.rb.zst          every other repo DID (did:web, and malformed did:plc)
  block/
    …
```

**Rows.** A file is a zstd stream: one frame, or several concatenated.
Decompressed, it is rows back to back, in ClickHouse's RowBinary encoding:

| Column    | Type   | Encoding                                  |
|-----------|--------|-------------------------------------------|
| `repo`    | String | LEB128 length, then UTF-8 bytes           |
| `rkey`    | String | the same                                  |
| `subject` | String | the same; empty for a delete              |
| `list`    | String | only in a list relation's files: the list's AT URI; empty for a delete |
| `time_us` | Int64  | 8 bytes little-endian: when the op happened |
| `op`      | UInt8  | 1 create, 2 update, 3 delete              |

Follow and block files have no `list` column: their rows are (repo, rkey,
subject, time_us, op). A list relation (one whose config names a `list`
path, like `listitem`) names a list as well as a subject, so its rows are
(repo, rkey, subject, list, time_us, op), and a delete row's subject and
list are both empty.

**Ops, not state.** The build keeps the latest op of each (repo, rkey) by
`time_us`, and a delete wins a tie. A key whose latest op is a delete is
left out. So an exporter can hand over a raw op log, creates and deletes
mixed, in any order. An exporter that has current state, like
`scan-scylla`, writes creates with `time_us` 0.

**Chunks.** These rules are what the build relies on:

1. Every row of one repo is in one file of its relation. The build turns a
   file into out lists, a repo split over two files would get two lists.
2. Within a relation, file names sort (byte order) in the DIDs' order, and
   the repos of two files don't interleave. The build places lists in id
   order and refuses files that break it. Ids are did:plc first, sorted by
   identifier, then every other DID, sorted. The `p-<prefix>` names do this
   for the base32 alphabet `234567abcdefghijklmnopqrstuvwxyz`, which sorts
   the same as bytes. A prefix of one character or of two both work, and
   `z-other` comes last. A did:plc counts as one only if it is exactly 24
   characters of that alphabet, the rest go in `z-other`.
3. Inside a file, rows may come in any order.
4. A file is read whole into one thread's memory, about 60 bytes a row
   while it's reduced. Two-character prefixes keep the whole network's
   follows (2.7B records) at about 3M rows a file.

Rows whose repo or subject isn't a DID are counted (`bad_repo`,
`bad_subject` in the report) and skipped.

**Relations.** The build reads one directory per relation in its
relations config (`--relations`, default follow, block and listitem), and a
missing directory is an error. An input that holds only some relations is built
with a relations config naming just those.

**Accounts.** An exporter that sees account events writes them to
`accounts.rb.zst`, a zstd stream of rows (`did` String, `time_us` Int64,
`event` UInt8): 0 active, 1 deleted, 2 takendown, 3 reset. Each DID gets
the status of its latest event of 0 to 2 by `time_us` (on a tie the higher
event wins): deleted or takendown is kept in the snapshot, hidden as the
live path hides it, and active clears. A deletion also drops every record
of that repo whose latest op is at or before it, as `#account` deleted does
live. A reset drops the repo's records whose latest op is before it: a
repo whose records were replaced wholesale, re-listed after it. Rows may
come in any order. The build report's `account_dropped` counts the records
they dropped, and `accounts` the rows and the statuses kept. Without the
file no actor has a status.

**cursors.json.** A JSON object, source name to cursor string, copied into
the snapshot's manifest. An exact source name (`jetstream:<host>`,
`jetstreamv2:<host>`) resumes that source. The key `time_us` is the time, in microseconds, the records
are current to, for an exporter that can't know which Jetstream instance
the server will follow: Jetstream cursors are event times, so `marco serve
--jetstream` starts a new source there. Once the source has a cursor of its
own, `time_us` is dropped. Catching up is Jetstream-only: a relay's
subscribeRepos resumes only by seq, so `marco serve --firehose` on a
snapshot with only `time_us` refuses to start (it would begin at the live
tail and miss everything in between) unless given `--allow-gap`.

A time the source no longer holds would leave a gap no replay fills. The
build refuses a time cursor that leaves under 2 hours before
`--retention-hours` (36). When `marco serve` starts the source, it refuses
a time past `--source-retention-hours` (36), and asks the instance too: if
Jetstream's first event from that time comes more than 10 minutes after it,
the instance has dropped it. `--allow-gap`
overrides either. Retention differs by instance: measure yours with
`marco-harness retention --jetstream URL`. The public `jetstream2.us-east`
kept 36.6 h when measured.

## Building from ScyllaDB

`scan-scylla` reads every row of the tables in a config with a parallel
full-table scan and writes the input format. With `--snapshot` it then runs
the build and deletes its output.

```text
  Scylla                   scan-scylla                       build                marco serve
  ┌───────────────┐   token ranges, each   ┌───────────────┐   ┌──────────┐   ┌────────────────────┐
  │ follows       │── routed to the shard ─▶ DIR/follow/   │──▶│ snapshot │──▶│ --jetstream: start  │
  │ blocks        │   that owns it, paged   │ DIR/block/    │   │ + cursor │   │ at scan start − 5 m │
  │ list_items    │   BYPASS CACHE, CL ONE  │ DIR/listitem/ │   └──────────┘   └────────────────────┘
  └───────────────┘                         │ cursors.json  │
                                             └───────────────┘
```

**How it reads.** For each table it asks the cluster who owns what.
Tablet tables list each tablet's replicas and shards in `system.tablets`.
Vnode tables give a replica per vnode, and Scylla's sharding function says
which shard owns each token. Ranges are cut so that one replica shard owns
each one whole, about `ranges_per_shard` (8) per shard, and each query is
routed straight to that shard:

```sql
SELECT token(actor_did), actor_did, rkey, target_did FROM ks.follows
 WHERE token(actor_did) > ? AND token(actor_did) <= ? BYPASS CACHE
```

It keeps `in_flight_per_shard` (2) ranges going per shard, at consistency
ONE unless set, over LZ4-compressed connections, `page_size` (5000) rows a
page. A page that fails is retried on another replica, and the range goes
back to its owner shard after.
`BYPASS CACHE` keeps a scan of every row from evicting the cluster's working
set. With replication factor 3, ranges spread over all three replicas.

**Relations.** The snapshot holds exactly the relations the config's
tables name, in that order. A relation left out of the scan is absent from
the snapshot, and Jetstream never fills it: the server follows only the
snapshot's relations. `--relations` is where their definitions come
from. The scan writes them to `DIR/relations.toml`, so a build of a kept
scan is `marco-load build --input DIR --relations DIR/relations.toml`.

**List items.** A list relation's table (listitem) has a `list` column on
top of actor, rkey and subject: the list's AT URI. The scan reads it into
the `list` column of the input's rows, and the snapshot gets the relation
and its companion, `listitem.owner`: a list's members, an actor's items and
the lists a DID is on. marco keeps an item only on a list in the item's own
repo, as the live path does, so the scan drops an item whose list URI names
another repo, counts it (`cross_repo_rows` in the report's table) and says
so at the end. A row with a null column is skipped and counted as
`null_rows`, for any table.

**Where rows go.** Token order isn't DID order, so rows are bucketed by repo
DID prefix as they arrive. Each range buffers `--chunk-rows` (262144) rows,
then at the next partition boundary compresses them into a frame per bucket
and appends the frames. A big cluster has many ranges in flight, two per
shard, so `--memory-gb` (4) caps what they buffer in all and lowers the
chunk to fit: 20 nodes of 64 shards is 2,560 ranges reading at once, about
5,000 rows a chunk in 4 GB. Smaller chunks make smaller frames, which
compress a little worse. A partition is held whole until it ends, so an
actor with a very long list adds its size on top for as long as it's
read. The scan keeps one file per bucket per table open (1,025 each), plus
a connection per shard per node, and raises its soft open-file limit to the
hard one; it stops with a message if that's still too few. The output
takes about as much disk as the rows compressed with zstd: the whole
network's follows and blocks (2.97B rows) took 66 GB, 22 B a row
(docs/scylla-results.html). A list item's row adds its list's URI, about
70 bytes before compression. zstd takes most of it back, since an actor's
items name a few lists and their URIs start with the actor's DID. The
test's synthetic items (`tests/scylla.rs`, 73k rows) took 27 B a row,
against 22 B for the network's follows.

**Resume.** A checkpoint every `--checkpoint-secs` (10) fsyncs the bucket
files and records each file's length and each range's position (its last
whole partition) in `scan-state.json`. If the scan stops, from a crash, a
lost node or a page that failed `retries` (8) times, run the same command
again. It cuts the files back to the checkpoint and goes on from there. A
crash loses at most the seconds since the last checkpoint. `--fresh` starts
over.

**Catching up.** The scan isn't a snapshot of one moment. Rows read early
can change before it ends. It records when it began, and `cursors.json`
gets `time_us` `--cursor-margin-secs` (300) before that. `marco serve
--jetstream URL` then replays everything since (catch-up is Jetstream-only).
Replay is idempotent by record URI, and deletes leave tombstones by time.
So a record created during the scan is added if the scan missed it, and a
record deleted during the scan is removed even if the scan read it. The
server is ready once it has caught up. The source has to still hold that
time when the server starts: the scan warns when its pace heads past
`--retention-hours`, the build refuses a time with under 2 h to spare, and
the server refuses one past the source's retention.

**What the margin assumes.** Replay covers what changed after the snapshot's
time. It can't fix rows the table was missing at scan time for another
reason, and those stay wrong for good:

- The table lags the network. If the indexer writing it runs minutes
  behind, a row it hadn't written when the scan read its range is neither
  scanned nor replayed. Set `--cursor-margin-secs` above the indexer's
  worst lag.
- A replica missed writes. At consistency ONE the scan reads one replica,
  and one that dropped writes (a hint lost, a node down past the hint
  window) hands back an old row set. For a production rebuild, read at
  `local_quorum`, or run a repair first.

### The config

```toml
nodes = ["10.0.0.1:9042", "10.0.0.2:9042"]   # contact points; the driver finds the rest
keyspace = "bsky"
# datacenter = "us-east"     # read only this datacenter's replicas, retries included
# username = "marco"         # with password_file
# password_file = "/run/secrets/scylla-password"
# consistency = "local_quorum" # default one (local_one with a datacenter); see "What the margin assumes"
page_size = 5000              # rows per page
ranges_per_shard = 8          # ranges per replica shard: the unit of work and resume
in_flight_per_shard = 2       # ranges read at once per replica shard
bypass_cache = true           # SELECT … BYPASS CACHE
compression = "lz4"           # lz4, snappy, none
request_timeout_secs = 60
retries = 8                   # attempts per page before the scan stops (resume continues it)

[[table]]
relation = "follow"           # a relation in the relations config (--relations)
name = "follows"
actor = "actor_did"           # the column with the record's repo DID (default actor_did)
rkey = "rkey"                 # the record key (default rkey)
subject = "target_did"        # the subject DID (default target_did)

[[table]]
relation = "block"
name = "blocks"

[[table]]
relation = "listitem"         # a list relation: the table needs a list column
name = "list_items"
subject = "subject"
list = "list"                 # the column with the list's AT URI (default list)
```

Column and table names that aren't plain lower-case identifiers are quoted
for you. The partition key comes from the schema, and the token bounds use
all of its columns, so any partition key works. The scan needs `SELECT` on
the tables, `system_schema` and `system.tablets`. A `list` column is only
for a list relation's table, and a list relation's table always has one.

**Primary keys.** With `((actor_did), rkey)`, each record is its own row
and the snapshot holds exactly the table's records. With `((actor_did),
target_did)` and rkey a plain column, an actor that follows someone twice
(two follow records, about 0.12% of them) has one row. The snapshot keeps
the one rkey, so a later Jetstream delete of the other one finds nothing,
and a delete of the kept one drops the pair while a record still exists in
the repo. The scan warns when the key leaves out the rkey column.

### The flags

| Flag | Default | |
|---|---|---|
| `--config FILE` | | the TOML above |
| `--out DIR` | | the output (the build's input); a scan already there resumes |
| `--snapshot SNAP` | | build here when the scan is whole, then delete `--out` |
| `--keep-spill` | off | with `--snapshot`, keep `--out` |
| `--relations FILE` | built-in | where the tables' relations are defined; the snapshot holds only those |
| `--fresh` | off | discard the scan in `--out` and start over |
| `--nodes A,B` | config | override the contact points |
| `--ranges-per-shard N` | config | |
| `--in-flight-per-shard N` | config | |
| `--page-size N` | config | |
| `--cursor-margin-secs S` | 300 | the snapshot's time is the scan's start less this: above the table's lag behind the network |
| `--retention-hours H` | 36 | the source's replay window: warn, and refuse the build with under 2 h to spare |
| `--allow-gap` | off | build even past the retention |
| `--chunk-rows N` | 262144 | rows a range buffers before committing, at most |
| `--memory-gb G` | 4 | what all ranges in flight may buffer; lowers the chunk to fit |
| `--checkpoint-secs S` | 10 | what a crash rescans |
| `--zstd-level N` | 1 | |
| `--progress-secs S` | 5 | |

Progress goes to stderr every few seconds: rows and rows/s, MB/s of row
data, ranges done and reading, the share of the ring done, spill bytes,
retries and an ETA. With `--ops`, `/metrics` has
`marco_load_scan_rows_total`, `marco_load_scan_bytes_total`,
`marco_load_scan_pages_total`, `marco_load_scan_retries_total{kind}`,
`marco_load_scan_ranges{state}` and `marco_load_scan_spill_bytes`. The JSON
report at the end has the plan (mode, primary key, ranges per table), rows,
bytes, spill size, retries, the client's CPU, and the cursor.

### Against a cluster

```sh
# 1. scan and build; run it again with the same --out if it stops
marco-load --ops 127.0.0.1:9101 --report scan.json scan-scylla \
  --config scylla.toml --out /data/scan --snapshot /data/snap

# 2. serve it and catch up from the snapshot's time; /readyz turns 200 once caught up
marco serve --snapshot /data/snap --memory 13GiB --jetstream wss://jetstream.example.com
```

For production:

- Read at `consistency = "local_quorum"`, or repair the tables first.
- Set `--cursor-margin-secs` above the worst lag of whatever writes the
  tables.
- Measure the source's retention (`marco-harness retention`) and pass it as
  `--retention-hours` and `marco serve --source-retention-hours`.
- Start the server soon after the build: the window closes while the
  snapshot waits.

Put `--out` on local NVMe with room for the compressed rows. The client
needs a core per few shards being read. If the scan is slow, look at the
cluster's per-shard CPU first: `in_flight_per_shard` 2 keeps every shard
busy with one range while the other waits on the network. Raise it if
shards idle, lower it if the cluster serves traffic and its latency suffers.
With `datacenter` set, every query goes to that datacenter's replicas,
retried pages included, and never fails over to another: the scan stops
instead, and resumes when run again.

### Testing it

`fill-scylla` loads build input into the config's tables as current state:
`--create actor-rkey|actor-subject` makes the keyspace and tables
(`--tablets false` for vnodes, `--replication-factor N`), and `--from`/`--to`
limit it to a range of file names, which is a set of whole actors. A list
table gets its `list` column and is keyed `((actor), rkey)` either way. It
loads every list item, the cross-repo ones too, as a real table would hold
them. `--relations` says which relations are list relations, as for the scan. `diff`
compares two snapshots by DID, so a snapshot built from Scylla can be
checked against one built straight from the same input.

```sh
docker run -d --name scylla --network host scylladb/scylla --smp 4 --memory 4G --developer-mode 1 \
  --listen-address 127.0.0.1 --rpc-address 127.0.0.1
MARCO_SCYLLA=127.0.0.1:9042 cargo test -p marco-load --test scylla --test scylla_kill -- --ignored
```

`scylla` writes synthetic ops, builds them directly, fills Scylla three
ways (tablets, vnodes, and a `((actor), subject)` key), scans each back and
diffs the snapshots, then aborts a scan after a checkpoint and resumes it.
Its list items are spread over a few lists per owner, with items moved
between lists, a list of 7,500 members and items naming another repo's
list; each rebuilt snapshot must answer every list's members, every
owner's items and the lists a DID is on as the direct build does, and drop
exactly the cross-repo items.
`scylla_kill` SIGKILLs scans at random moments until one finishes, and
checks the raw spill holds every row exactly once (the build would hide a
row committed twice). `MARCO_SCYLLA_OPS` sets their size,
`MARCO_KILL_ROUNDS` the kill test's rounds.

## Building from Hubble

`scan-hubble` fetches every repo a sync-API host lists and writes the input
format, so a snapshot can be built with nothing but a public mirror. The
default host is Hubble (`https://hubble.microcosm.blue`, run by microcosm),
which mirrors every atproto repo. Any host that serves
`com.atproto.sync.listRepos` and `com.atproto.sync.getRepo` works with
`--host`: a PDS for its own accounts, or a relay that serves repos. With
`--snapshot` it then runs the build and deletes its output.

```text
  sync host                  scan-hubble                          build               marco serve
  ┌──────────────┐  listRepos pages,    ┌───────────────┐   ┌──────────┐   ┌────────────────────┐
  │ listRepos    │── then getRepo per ──▶ DIR/follow/   │──▶│ snapshot │──▶│ --jetstream: start  │
  │ getRepo (CAR)│   active repo,       │ DIR/block/    │   │ + cursor │   │ at scan start − 10 m│
  └──────────────┘   ≤ --rate a second  │ DIR/listitem/ │   └──────────┘   └────────────────────┘
                                        │ cursors.json  │
                                        └───────────────┘
```

**How it reads.** It pages `listRepos` (`--page-size`, 1000) and fetches
each active repo's CAR. The commit names the MST root, and the walk decodes
only the nodes whose key range can hold a relation's collection, so a
repo's posts and likes are never decoded. Each record of a relation's
collection becomes a row, with its subject (and list, for a list
relation) read from the relation's paths. The rows are current state:
creates with `time_us` 0. Every relation in `--relations` is scanned. A
list item naming another repo's list is dropped and counted (`cross_repo`),
as marco never keeps one. The scan checks that the commit names the DID it
asked for and that each kept record hashes to its CID. It doesn't verify
commit signatures: the host is trusted to serve the repos it mirrors.

**Politeness.** `--rate` (5) caps the requests a second to the host:
`getRepo`, `listRepos` and retries all count. At most `--concurrency` (4)
repos download at once. A 429 or 503 pauses every request for the host's
`Retry-After` (or atproto's `RateLimit-Reset`), 10 s without either and
15 min at most. Other failures (5xx, timeouts, dropped connections) back off
from 1 s to 60 s, `--retries` (8) attempts a request. A request that runs out
of attempts stops the scan, and running it again resumes it. The User-Agent
is `marco-load/<version> (+https://github.com/jazware/marco; contact <--contact>)`:
set `--contact` so the host's operator can reach you. Hubble is someone
else's service, so ask its operator before raising `--rate`.

**What's skipped.** The report counts every repo it didn't read, by reason
(`skipped_repos`):

| Reason | |
|---|---|
| `inactive:<status>` | `listRepos` lists it as inactive (deactivated, takendown, …): not fetched |
| `gone:<error>` | `getRepo` answered 4xx: `RepoNotFound`, `RepoTakendown`, … |
| `bad_car:<why>` | an unreadable CAR, a commit naming another DID, a missing or malformed MST node |
| `too_big` | a CAR over `--max-car-mb` (1024) |

and every record of a relation it left out (`skipped_records`):
`no_subject`, `no_list`, `cross_repo`, `bad_list`, `bad_record` (not
DAG-CBOR), `missing_block`, `bad_block` (its bytes don't match its CID). A
subject that isn't a DID is left to the build, which counts it as
`bad_subject`.

**Resume.** Rows go in the same files as `scan-scylla`'s, bucketed by repo
DID prefix. A `listRepos` page is the unit of commit: its repos are fetched
in parallel but committed in listing order. The commit appends the page's
rows, records the next cursor, fsyncs and writes `hubble-state.json`. If
the scan stops, run the same command again: it cuts the files back to the
state and lists on from the cursor. A crash refetches at most the page in
flight. `--fresh` starts over. The state names the host, the relations,
`--dids` and `--limit`, and a run with other ones is refused.

**Catching up, and its limit.** The scan isn't a snapshot of one moment:
repos read early change before it ends. `cursors.json` gets `time_us`
`--cursor-margin-secs` (600) before the scan began, above the mirror's own
lag behind the network, and `marco serve --jetstream URL` replays
everything since. Replay is idempotent by record URI, so a record created
during the scan is added and one deleted during it is removed.

That replay has to still be inside the Jetstream instance's retention when
`marco serve` starts: 36 h on the public instances (`jetstream2.us-east`
kept 36.6 h). The build refuses a cursor with under 2 h of it left, and the
server refuses one past `--source-retention-hours`. So a full sweep has to
finish in about 33.5 h: 36 h less the 2 h to start the server, the 10 min
margin and the ~15 min a whole-network build takes. The network has about
41M repos with records, which makes about 340 repos a second, sustained,
and each `getRepo` downloads the whole repo, posts and likes included (a
long-lived account's is ~30 MB). At the default 5 a second a sweep takes
about 95 days: far too slow for a live catch-up. Whether a host serves
340 a second is its operator's call, not the scan's. Hubble lists repos in
DID order, so the scan can tell how far through `did:plc` it is: once its
pace is clear it warns if it will end past the retention, with the rate
it would need. A sweep that ends past it builds only with `--allow-gap`,
and that snapshot misses every change between its cursor and the oldest
event Jetstream still holds, for good.

**Samples.** `--limit N` lists only the first N repos, and `--dids A,B`
fetches just those. Their snapshots hold a few accounts, not the network:
they're for trying the path end to end.

### The flags

| Flag | Default | |
|---|---|---|
| `--host URL` | `https://hubble.microcosm.blue` | the sync-API host |
| `--out DIR` | | the output (the build's input); a scan already there resumes |
| `--snapshot SNAP` | | build here when the scan is whole, then delete `--out` |
| `--keep-spill` | off | with `--snapshot`, keep `--out` |
| `--relations FILE` | built-in | the relations to scan; the snapshot holds those |
| `--fresh` | off | discard the scan in `--out` and start over |
| `--rate N` | 5 | requests a second to the host, retries included |
| `--concurrency N` | 4 | repos downloading and parsing at once |
| `--page-size N` | 1000 | repos per `listRepos` page: the unit of resume |
| `--contact TEXT` | | a URL or email for the host's operator, in the User-Agent |
| `--retries N` | 8 | attempts a request before the scan stops |
| `--timeout-secs S` | 300 | per request, body included |
| `--max-car-mb N` | 1024 | a bigger repo is skipped as `too_big` |
| `--dids A,B` | | fetch only these repos |
| `--limit N` | | list only the first N repos |
| `--cursor-margin-secs S` | 600 | the snapshot's time is the scan's start less this |
| `--retention-hours H` | 36 | the source's replay window: warn, and refuse the build with under 2 h to spare |
| `--allow-gap` | off | build even past the retention |
| `--zstd-level N` | 1 | |
| `--progress-secs S` | 10 | |

Progress goes to stderr: repos and repos/s, MB/s, pages, records, repos
skipped, retries, throttles, and with a listing in DID order the share of
`did:plc` done and an ETA. With `--ops`, `/metrics` has
`marco_load_hubble_requests_total{method,status}`,
`marco_load_hubble_bytes_total{method}`,
`marco_load_hubble_repos_total{outcome}` and
`marco_load_hubble_records_total{relation}`. The JSON report at the end has
the counts above, bytes downloaded, the cursor, and this run's requests,
retries and throttles.

```sh
# a few repos end to end
marco-load scan-hubble --out /data/sample --snapshot /data/sample-snap --limit 100 --contact you@example.com

# every repo; run it again with the same --out if it stops
marco-load --ops 127.0.0.1:9101 --report scan.json scan-hubble \
  --out /data/hubble --snapshot /data/snap --contact you@example.com
```

### Testing it

`tests/hubble.rs` serves `listRepos` and `getRepo` from an in-test HTTP
server, over CARs it builds: follows, blocks and list items among posts and
likes, plus inactive repos, `RepoNotFound`, a cut-off CAR, a CAR of another
repo, a 429 with `Retry-After` and a 500. It aborts a scan after two pages,
resumes it, builds, and checks that the snapshot holds exactly the records
it should (`diff` against a direct build of them, and every actor's
follows), that committed pages weren't fetched again, the skip counts, the
cursor, the User-Agent and the rate cap. A live check against Hubble reads
the first `listRepos` page of three and one repo by DID, at 2 requests a
second:

```sh
cargo test -p marco-load --test hubble -- --ignored live
```

`MARCO_SYNC_HOST` and `MARCO_LIVE_DID` point it elsewhere. Never point a
test at a whole host.

## Building from the Jetstream archive

`scan-jetstream` replays a Jetstream v2 instance's sealed archive of the
relations' collections and writes the input format, so a snapshot holds the
current state of the whole history the instance archived. With `--snapshot`
it then runs the build and deletes its output. The snapshot's cursor is the
instance's own seq, so `marco serve --jetstream-v2` on the same host picks up
exactly where the archive ended.

```text
  Jetstream v2                  scan-jetstream                     build               marco serve
  ┌─────────────────┐  plan pinned at    ┌───────────────┐   ┌──────────┐   ┌──────────────────────────┐
  │ planSnapshot    │── sealedTipSeq S,──▶ DIR/follow/   │──▶│ snapshot │──▶│ --jetstream-v2: cursor=S, │
  │ getSegment      │   whole segments   │ DIR/block/    │   │ + seq S  │   │ replays S.. inclusive,    │
  │ getBlock        │   or block ranges  │ DIR/listitem/ │   └──────────┘   │ drops seq ≤ S             │
  └─────────────────┘   ≤ --rate a sec   │ accounts      │                  └──────────────────────────┘
                                         │ cursors.json  │
                                         └───────────────┘
```

**How it reads.** It asks `planSnapshot` for the relations' collections
and the `commit`, `account` and `sync` kinds, from `--after-seq` (0, the
whole archive), and pins the first page's `sealedTipSeq` as S. Later pages
ask for `afterSeq` = the last page's `plannedThroughSeq` and `beforeSeq` =
S, until `plannedThroughSeq` reaches S. Segments sealed during the sweep
are above S and aren't chased: the live stream replays them. Each unit is
downloaded by its mode: a whole segment (`getSegment`), whose xxh3 over
its header and footer must match the header's checksum, or the listed
blocks one by one (`getBlock`), each a zstd frame whose content checksum
zstd checks. A unit that fails its checksum is fetched again, up to
`--retries`. The plan over-sends (whole blocks, whole segments, a segment
straddling the cursor), so every row is filtered exactly: its seq has to be
above the cursor and at or below S, and a commit has to be in one of the
relations' collections. A segment rewritten by the instance's compaction
since the plan named it (a new checksum) is taken as it is now and counted
(`generation_changed`): compaction only drops rows a later delete
superseded.

**What a row becomes.** The rows are ops, in the format above, with
`time_us` the event's time (what the live stream sends as `time`). Rows
come straight from the archive's own fold rules:

| Archive row | Build input |
|---|---|
| create, update, a `#sync`'s replacement create | the record's subject (and list, for a list relation): create or update |
| a create or update whose record isn't an edge (no subject, a subject that isn't a DID, a list in another repo) | a delete: the key ends empty, as it does live (`skipped` counts the reason) |
| delete | a delete |
| `#account` deleted, takendown, active | an `accounts.rb.zst` row: 1, 2, 0 |
| `#account` deactivated, suspended, throttled, desynchronized | nothing: the account comes back as it was |
| `#sync` | an `accounts.rb.zst` reset (3) |
| `#identity`, another collection | nothing |

**Account statuses.** They do what `#account` does on the live path: a
deleted account's records before the deletion are gone and the account is
hidden, a takedown hides the account and keeps its records, and active
clears either. The build applies them from `accounts.rb.zst` (see
"Accounts" above) and stores the statuses in the snapshot, so a takedown
from years ago is still a takedown when the server starts. An `active` is
written only for a DID that had a status earlier in the scan, since one
that clears nothing changes nothing. What the build can't carry is the
tombstones a live deletion leaves: an op older than the deletion that
arrives later from another source (a backfill) is taken. Events after S
come from the live stream with the live rules.

**`#sync`.** The live path counts a `#sync` and keeps the repo's edges as
they were. The archive is different: Jetstream stores a `#sync` row
followed by the repo's records as the resync found them, and its
compaction treats the row as a reset of the repo. The scan does the same
(a reset, then the replacement creates), so a repo's edges end where
Jetstream's own fold of the archive ends, whether or not compaction has
rewritten that segment yet.

**The API key.** The archive needs `Authorization: Bearer <key>` on
`planSnapshot`, `getSegment` and `getBlock`. The key comes from a file,
`--api-key-file` or the file `JETSTREAM_API_KEY_FILE` names, so it's never
in argv or in a child's environment. It's never printed, and it goes only
to the instance's own origin: a download it redirects to a CDN is fetched
without it. The host has to be `https://` (plain `http://` only on
loopback, for tests). A 401 or 403 stops the scan: the key was refused.

**Politeness.** `--rate` (10) caps the requests a second to the archive,
retries included, and at most `--concurrency` (4) downloads run at once. A
429 or 503 pauses every request for its `Retry-After` (10 s without one, 15
min at most). Other failures back off from 1 s to 60 s, `--retries` (8)
attempts a request; one that runs out stops the scan, and running it again
resumes. The User-Agent is `marco-load/<version>
(+https://github.com/jazware/marco; contact <--contact>)`. A whole segment
is held in memory while it's decoded (256 MB typically, 1 GiB at most), so
`--concurrency` is also the memory: about 1 GB at 4.

**Resume.** Rows go in the same bucket files as the other scans', and
account events in `accounts.rb.zst`. A plan unit is the unit of commit:
units are fetched in parallel but committed in plan order, and a commit
appends the unit's rows, moves the seq cursor past it, fsyncs and writes
`jetstream-state.json`. If the scan stops, run the same command again: it
cuts the files back to the state, plans again from the cursor up to the
same S, and goes on. A crash refetches at most the units in flight.
`--fresh` starts over. The state names the host, the relations and the
seq window, and a run with other ones is refused.

**The cursor, and the handoff.** When the sweep reaches S, `cursors.json`
gets `jetstreamv2:<host>` = S, the name `marco serve --jetstream-v2
wss://<host>` keeps that source's cursor under. The server then treats it
as its own durable cursor: it dials `cursor=S`, the instance replays from
S inclusive, which covers the segments sealed during the sweep and the
active one, and marco drops every event at or below S, so the seam
applies each event once (docs/ingest.md, "Following a Jetstream v2
instance"). A seq means something only on the instance that assigned it,
so serve the snapshot from the same host: another host is another source,
and it starts at its live tail.

**Catching up, and its limit.** The instance's live stream replays about
36 h, and a seq below its floor is refused (`CursorTooOld`: that source
ends, and marco stays not ready). A whole-archive sweep can take longer
than that, so when a pass ends with S older than `--catch-up-secs` (3600),
the scan plans again from S to the new sealed tip and sweeps that, until a
pass ends with a recent pin or nothing new is sealed. Each pass covers
only what was sealed while the last one ran, so they shrink fast. The scan
warns if S leaves under 2 h of `--retention-hours` (36) to start the
server. `--before-seq` makes a fixed slice and turns the passes off.

**How long a sweep takes.** Measured against `jetstream.us-east` on
2026-11-11: the plan for follows, blocks and list items to sealed seq
27,081,788,050 was one page of 7,639 whole segments and 290 block units
(150,281 blocks). A segment was 258 MB on average (8 downloaded), a block
0.3 MB (596), so a sweep downloads about 2.0 TB. Downloads redirect to a
CDN. The first three segments came at 63 MB/s with two at once, then the
archive answered 429 with a `Retry-After` of 71 to 131 s, and five more
took 511 s: 2.6 MB/s, about 100 s a segment, sustained. Block units ran at
the `--rate` of 5 requests a second without a 429 (1.5 MB/s). At those
rates a sweep is about 9 days of segments, with the blocks inside it, and
catch-up passes for what was sealed meanwhile; without the archive's limit
it would be about 9 hours. Decoding is cheap: about 8 CPU seconds a GB,
under 1 GB of memory at `--concurrency 2`. The spill took 22 bytes an op.

**Slices and measurements.** `--after-seq N` starts after seq N and
`--before-seq M` stops at M: the snapshot holds only what happened in
between, for trying the path. `--max-units N` stops after N units this run
and leaves the scan resumable, and `--plan-only` pages the whole plan
without downloading anything and counts its units and blocks.

### The flags

| Flag | Default | |
|---|---|---|
| `--host URL` | `https://jetstream.us-east.bsky.network` | the Jetstream v2 instance |
| `--out DIR` | | the output (the build's input); a scan already there resumes |
| `--snapshot SNAP` | | build here when the scan is whole, then delete `--out` |
| `--keep-spill` | off | with `--snapshot`, keep `--out` |
| `--relations FILE` | built-in | the relations to scan; the snapshot holds those |
| `--fresh` | off | discard the scan in `--out` and start over |
| `--api-key-file FILE` | `$JETSTREAM_API_KEY_FILE` | the archive's API key, in a file |
| `--rate N` | 10 | requests a second to the archive, retries included |
| `--concurrency N` | 4 | downloads at once (each whole segment is held in memory) |
| `--contact TEXT` | | a URL or email for the instance's operator, in the User-Agent |
| `--retries N` | 8 | attempts a request, or a unit that fails its checksum |
| `--timeout-secs S` | 600 | per request, body included |
| `--after-seq N` | 0 | start after this seq |
| `--before-seq N` | | stop at this seq instead of the sealed tip (no catch-up passes) |
| `--catch-up-secs S` | 3600 | another pass while the pin is older than this |
| `--max-units N` | | stop after this many units this run |
| `--plan-only` | off | count the plan's units and blocks, download nothing |
| `--retention-hours H` | 36 | the live stream's replay window: warn with under 2 h to spare |
| `--zstd-level N` | 1 | |
| `--progress-secs S` | 10 | |

Progress goes to stderr: the cursor against S, units, events and events/s,
MB/s, retries, throttles and an ETA. With `--ops`, `/metrics` has
`marco_load_jetstream_requests_total{method,status}`,
`marco_load_jetstream_bytes_total{method}`,
`marco_load_jetstream_units_total{mode}` and
`marco_load_jetstream_ops_total{kind}`. The JSON report at the end has
the pin, the cursor, units by mode, bytes downloaded, rows decoded, events
and ops by relation, account events, skips, refetches, and this run's
rates under `run`.

```sh
# what a sweep would download
marco-load scan-jetstream --out /data/js --api-key-file ~/.jetstream.key --plan-only

# the whole archive of the graph collections; run it again with the same --out if it stops
marco-load --ops 127.0.0.1:9101 --report scan.json scan-jetstream \
  --out /data/js --snapshot /data/snap --api-key-file ~/.jetstream.key --contact you@example.com
marco serve --snapshot /data/snap --memory 13GiB --jetstream-v2 wss://jetstream.us-east.bsky.network
```

### Testing it

`tests/jetstream.rs` serves a fake archive over the segments in
`testdata/jsarchive`, which Jetstream's own segment package wrote (`go run
.` in `testdata/jsarchive/gen` remakes them, and `archive.json` beside them
lists every event and block). The fake plans the way the real planner
does: units by density, whole segments or coalesced block ranges, a few
per page and cut at a unit's end. It seals a segment after the first plan,
answers one plan with a 429, corrupts a segment's footer and a block once
each, and redirects one download to another origin, which checks that the
key doesn't follow. A scan aborts after two units, resumes, builds, and is
diffed against a direct build of the archive's events folded in the test
(deletes winning, the account deletions and takedowns, the `#sync`), with
the statuses and the cursor checked. Other tests cover a slice that
straddles a unit at both ends, a pass to the new tip, a refused key and
`--plan-only`.

## PageRank

`marco-load pagerank` ranks every actor in a snapshot by PageRank over its
follows and writes a ranks file, which seek bakes into its typeahead index.
Damping 0.85, a uniform teleport, dangling mass spread uniformly, one edge
per (follower, subject), self-follows and taken-down or deleted accounts
left out. What one follower passes along one edge is capped at
`--max-pass` average ranks (default 1000, the excess to the teleport), so a
sign-up hub followed by most of the network doesn't hand its rank to the
few accounts it follows. It iterates until the L1 change is below `--tol`
(1e-5), with sources renumbered by out-degree so a pass's random reads share
cache lines.

```sh
# daily, next to a running marco: link its current checkpoint, warm-start from the last run
marco-load pagerank --snapshot /data/snap --link /data/pr-link --out-dir /data/ranks
```

`--out-dir` warm-starts from `DIR/latest.ranks` (unless `--cold` or
`--warm FILE`), writes `DIR/ranks-<UTC time>.ranks`, points `latest.ranks`
at it and keeps the newest `--keep` (8). `--link DIR` hard-links the
checkpoint's files into `DIR` (the same filesystem) and reads that, so a
checkpoint or compaction on the live directory can't pull a file away
mid-run. `--source inset` skips the in-edge CSR: about 10 GB peak instead of
25 GB on the whole network, and each pass about six times slower.
`--source csr-out --before-us T` keeps only follow records whose TID is
older than `T`: the graph as it was, for evaluations.

On the whole network (47M ids, 3.03B follows, 32 threads) a cold run takes
34 passes and 38 s, a run warm from the day before 15 passes and 22 s.

### The ranks file

Little-endian, one row per actor in the snapshot's directory, keyed by DID:

| Part | Bytes | |
|---|---|---|
| header | 128 | `mrcranks`, version 1, rows, did:plc rows, created ms, the snapshot's build and checkpoint, iterations, converged, damping, max pass, final L1, and the u16 scale's ln low and high |
| DIDs | 15 per did:plc, then (u16 length, bytes) per other DID | did:plc packed as marco packs it, ascending, then the rest by string |
| rank | 4 a row | f32, summing to 1 |
| rank16 | 2 a row | ln(rank) scaled between the header's low (0) and high (65535) |
| followers | 4 a row | distinct followers that pass rank: the in-degree a recency signal diffs |
| flags | 1 a row | bit 0: hidden (taken down or deleted) |
| trailer | 16 | xxh3-64 of everything before it, then `mrcranks` |
