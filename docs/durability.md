---
title: Durability and restarts
section: marco
order: 6
summary: "No write-ahead log. Change segments every 5 s carry the sources' cursors, rare checkpoints fold them into the snapshot, and the sources replay the rest."
---

```hero
timeline:
  caption: "A kill -9 and a restart. The writes since the last segment are lost, the restart replays the segments after the snapshot's cut, and the source resumes from the newest segment's cursor. Not to scale."
  scale: 52
  lanes:
    - { id: w, label: writer, sub: applies ops, tone: accent }
    - { id: d, label: disk, sub: "segments · snapshot", tone: amber }
    - { id: s, label: Jetstream, sub: replays, tone: muted }
  spans:
    - { lane: w, from: 0, to: 4.2, label: serving }
    - { lane: d, from: 0.2, to: 1, label: seg }
    - { lane: d, from: 2.2, to: 3, label: seg }
    - { lane: w, from: 6, to: 8, label: replay, tone: amber }
    - { lane: s, from: 8, to: 9.6, label: catch up, tone: blue }
    - { lane: w, from: 9.8, to: 12, label: ready, tone: solid }
  events:
    - { lane: w, at: 4.8, label: kill -9, tone: danger }

  marks:
    - { at: 3, label: last cursor on disk, tone: amber }
  ticks:
    - { at: 0, label: "0" }
    - { at: 3, label: "≤5 s lost" }
    - { at: 9.8, label: "~0.5 s later" }
facts:
  - { value: "5", unit: s, label: between segments, note: "`--segment-interval` · what a kill -9 loses", tone: amber }
  - { value: "3M", unit: ops, label: or a day per checkpoint, note: "`--checkpoint-ops` · `--checkpoint-age`", tone: accent }
  - { value: "0", label: diffs after 94 kill -9s, note: "489,121 keys compared", tone: ok }
```

marco keeps no write-ahead log. Every source it reads can replay from a cursor, and replaying an
ordered suffix is idempotent because edges are keyed by URI. So a log would be a second copy of
data upstream already keeps. What marco does write is a change segment every few seconds with each
source's cursor, and a checkpoint now and then that folds the segments into the snapshot. A crash
loses the writes since the last segment that landed, normally at most 5 s, and the sources hand
those back.

The cost is that a restart needs its source reachable and still holding the cursor (Jetstream
keeps about 36 h), and an API writer has to replay from its own cursor
([Writing and ingest](ingest.md#cursors)). marco doesn't check the first part on a restart: a
marco down for longer than the source's retention resumes anyway and misses what the source
dropped ([the warning](ingest.md#the-built-in-consumer)). Rebuild after an outage that
long.

## Change segments

```steps
- title: Every `--segment-interval` (5 s), if anything was written
  body: "The writer cuts the ops applied since the last cut and hands them to the segment thread with every source's cursor at that cut."
- title: Written whole
  body: "Temp file, fsync, rename, so a segment is there or it isn't and there's no torn tail. A segment that fails to write is retried until it lands, and the console shows the error meanwhile."
- title: Off the write path
  body: "Writes keep applying while a segment is written: ~5 ms a segment and ~109 B an op on the whole-network run."
```

The first cursor a source ever sets, and the operator's `SetActorStatus`, don't wait for the
timer: their segment is written before they answer. A clean stop (SIGTERM or Ctrl-C) drains the
listeners first ([Stopping](operations.md#stopping)), then gives the consumer up to 10 s to hand over
what it read and writes a last segment, so it loses nothing.

> [!WARNING]
> While segment writes fail (a full disk, I/O errors), marco keeps applying and acknowledging
> writes, holds them in memory and retries the segment. A kill in that window loses everything
> since the last segment that landed, which can be far more than 5 s. Alert on
> `marco_segment_errors_total` and fix the disk before restarting.

Only one `marco serve` may use a snapshot directory. Nothing locks it, so two writable processes
on one directory (say, an overlapping rolling restart) delete each other's temp files and write
the same segment numbers. Stop the old process before starting the new one.

## Checkpoints

A checkpoint folds every segment since the last into new shard files at one cut of the graph. It
starts when the segments since the last one hold `--checkpoint-ops` (3,000,000) ops, which is what
a restart would replay in about 10 s, or when the last is `--checkpoint-age` (86,400 s) old and
something has been written since. The console's Storage page and `Admin.Checkpoint` start one by
hand.

```steps
- title: Cut
  body: "A thread of its own opens the snapshot and replays the segments up to a new cut."
- title: Write a patch per changed shard
  body: "A patch holds the lists that changed, after the pages the shard's files already hold, and the shard's index and side tables whole. A long list keeps every block whose records didn't change, so an append writes its tail. Unchanged lists stay where they are. A shard with no file yet gets a base. Every checkpoint takes a new id, so its files never land on a name the committed manifest uses."
- title: Commit
  body: "Rename the new `manifest.json` into place. A crash anywhere before that leaves the old snapshot whole."
- title: Switch
  body: "Open the new snapshot, replay the segments written since the cut, and swap it in. Reads and writes never wait on the checkpoint."
```

Old versions of changed lists stay in the shard's files as garbage. Once a shard's files hold
1.43× what its index still names and at least 64 MiB of garbage (`--compaction-ratio`,
`--compaction-min-garbage-mb`), a compaction rewrites its base and patches as one base with the same
lists, paced by `--background-io` (200 MB/s), one shard at a time after each checkpoint. Its
manifest rename is its commit, and the served copies swap the shard in place. A checkpoint that
comes due gives it up, the shard it was on included, and a crash leaves either the old files or the
new base, never a mix. With a `--memory` that holds the whole snapshot, old versions stay cached
too, in pages that still hold live lists as well as in dead ones. So compaction also takes the
shards a rewrite would shrink the most, until the next checkpoint's patches fit in what's free and
what's left to win back is under 1 GiB: close to every shard a day.

On the whole network, a day of the archive's follows, blocks and list items (2.75M ops) made a
checkpoint of 25.5 s under a read load, with a `--memory` that holds the snapshot, and 37.3 s with
one that holds about a third of it. At today's write rate that's about once a day. Compacting a shard (about 0.75 GB) took 2.8 s unpaced.

## Restart

| Step | Whole network, `--memory` holds the snapshot | `--memory` holds about a quarter of it |
|---|---|---|
| Open: read and check the footers, page sums and pinned sections, then load every page when `--memory` holds them | 7.8 s from a cold disk | 8-11 s (pinned sections only; the rest is read on demand) |
| Replay the segments after the manifest's cut | 4.46M ops in 10.3 s (432k ops/s) | 2.19M ops in ~50 s (~36k ops/s) |
| Each source resumes from the newest segment's cursor | caught up in under a second | not measured apart |
| Ready | every expected source under 1 s behind | |

Replay is most of a restart with more than a day of segments (4.46M ops, where a day of follows
and blocks is ~2.4-2.7M). With the snapshot resident, replay runs from memory. When `--memory`
holds only part of it (12 GiB for a 44 GB snapshot in the second column), open loads only what's
pinned and the cache starts cold, so each replayed op that touches a list not yet read waits on
a disk read: replay ran 12 times slower, and 22 h of segments made a 62 s open. Checkpoints keep
the segments short, and a checkpoint just before a planned restart (`POST /admin/checkpoint`,
[Checkpoint export](operations.md#checkpoint-export)) leaves only the minutes since to replay.
Every page open reads is checked against its sum, and so is every page read after it.

## The ready gate

```diagram
caption: "`/readyz` answers 503 with why until every configured source has caught up since start, then 200 for good, unless the engine's reads are failing across the disk. Graph reads on the API listener follow the sources."
nodes:
  - { id: start, label: start, sub: open + replay, at: [0, 0], size: [8, 3], tone: muted }
  - { id: catch, label: catching up, sub: "503 · reads unavailable", at: [12, 0], size: [11, 3], tone: amber }
  - { id: ready, label: ready, sub: "200 · marco_ready 1", at: [33, 0], size: [10, 3], tone: solid }
edges:
  - "start -> catch"
  - "catch -> ready: sources < 1 s behind"
```

- A source counts as caught up once it reports a lag under 1 s. Lag is now, by marco's clock,
  minus the newest applied event's time. So a source that's itself behind keeps a restarted marco
  unready: in the durability tests one public Jetstream instance had a p99 of 49.9 s. If that happens, follow
  another instance, or run with `--serve-before-ready` and watch `marco_ingest_lag_seconds`.
- Before ready, Graph reads on the API listener answer `unavailable` ("catching up:
  jetstream:…"). `--serve-before-ready` answers them anyway. Either way they're counted in
  `marco_reads_before_ready_total`.
- Ready is sticky. Once every source has caught up after a start, a later lag is the source's
  problem, so marco stays ready and the lag shows in `marco_ingest_lag_seconds`.
- A marco with no source configured is ready at once. For an API-only deployment that means
  readers see right away that a kill -9 rolled back the last few seconds of writes, until your
  pipeline replays them.
- `/healthz` answers `ok` whenever the process is up. Use `/readyz` for a load balancer.

## kill -9

The durability tests killed marco 94 times mid-ingest against the whole-network snapshot, at random, just
after a segment, 30 ms before one, and in the middle of checkpoints, then compared every key the
capture touched against the expected state:

| Run | Kills | Keys compared | Differ |
|---|---|---|---|
| Jetstream, forced checkpoints | 40 | 224,437 | 0 |
| Jetstream, automatic checkpoints | 30 | 224,437 | 0 |
| Firehose, automatic checkpoints | 24 | 40,247 | 0 |

Checkpoints cut short by a kill left the old snapshot whole, and the next start replayed from
it. Details, timelines and the method: [Durability and restarts, measured](p1-results.html).

If the writer or segment thread panics, the process aborts. A writer that keeps answering writes
no segment will hold would be worse than a restart.
