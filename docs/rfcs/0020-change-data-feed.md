# RFC 0020: Change data feed

- **Date:** 2026-07-13

## Summary

DuckLake's change data feed — `ducklake_table_changes`,
`ducklake_table_insertions`, `ducklake_table_deletions` — is computed
entirely by DuckLake's own scan machinery over the catalog tables: it
resolves the requested bounds against `ducklake_snapshot`, then
reconstructs each snapshot's inserted and deleted rows from data files,
delete files, and inlined data by their begin/end snapshots. This RFC
decides that moraine adds **no change-feed logic**: no new keyspace, no new
C ABI surface, no shim code. The feed works iff the projections moraine
already serves are faithful, so the design's substance is naming the
invariants the feed depends on — snapshot attribution served verbatim
(begin/end snapshots and `partial_max` on both file tables), rowid
provenance, and begin/end history including flush backdating — and pinning
the feed live in the e2e suite.
This is the same posture RFC 0013 takes for pruning and the roadmap takes
for time travel: moraine serves rows, DuckLake computes.

## Goals

- **Zero added machinery.** The change data feed introduces no key kind, no
  projection, no C ABI entry point, and no core API. Everything it reads is
  already served.
- **Invariants named, not assumed.** Each property the feed depends on is
  stated here and traceable to the code that maintains it, so a future
  change that would break the feed trips over a written constraint.
- **Validated live.** The feed is exercised end to end — real DuckDB, the
  full `ducklake:moraine:` chain — across inline and flushed data, deletes,
  updates, and ranges crossing a flush, before the roadmap item is checked
  off.

Non-goals:

- **A moraine-native change-feed API.** The core's `ChangeSet` parses
  `changes_made` for conflict detection only; exposing a public Rust
  change-feed surface is not on the v0.1 path (RFC 0003 owns the public API
  shape if that ever changes).
- **The `changes_made` grammar itself.** The wire grammar and its role in
  conflict detection are RFC 0004's; this RFC consumes the stored string,
  it does not re-specify it.
- **Snapshot expiry interactions.** Expiring a snapshot inside a queried
  range makes the feed unanswerable for that range; how expiry behaves is
  RFC 0007's concern, and the feed inherits whatever DuckLake's own error
  is for an expired snapshot.

## Background

DuckLake exposes the change data feed as catalog table functions:

```sql
FROM ducklake_table_changes('lake', 'main', 't', 2, 4);
FROM ducklake_table_insertions('lake', 'main', 't', 2, 4);
FROM ducklake_table_deletions('lake', 'main', 't', 2, 4);
```

The bounds are inclusive and each may be a `BIGINT` snapshot version or a
`TIMESTAMPTZ`, resolved against `ducklake_snapshot`. `table_changes` is a
table macro over the other two: insertions and deletions outer-joined on
`(snapshot_id, rowid)`, yielding the table's columns plus `snapshot_id`,
`rowid`, and `change_type` (`insert`, `delete`, and — when one snapshot
deletes and re-inserts the same rowid, as `UPDATE` does —
`update_preimage` / `update_postimage`). The two scans are the ordinary
DuckLake table scan in a changes mode, driven by attribution columns, not
by `changes_made`:

- **Insertions** select the table's data files with `begin_snapshot` in
  range — or `partial_max` reaching back into it for a flushed file
  carrying rows from several snapshots — plus inlined rows by
  `begin_snapshot`; a row's reported `snapshot_id` is its file's (or its
  own) `begin_snapshot`, filtered per row through the file's snapshot
  columns when `partial_max` is set.
- **Deletions** select delete files with `begin_snapshot` in range (joined
  against each file's *previous* delete file to subtract already-deleted
  rows), data files fully deleted in range by `end_snapshot`, and inlined
  rows ended in range by `end_snapshot`.

moraine already serves every table and column those scans touch,
row-faithfully and with history: `ducklake_data_file` /
`ducklake_delete_file` carry begin/end snapshots, `row_id_start`,
`mapping_id`, and `partial_max`; the dynamic `ducklake_inlined_data_<t>_<v>`
/ `ducklake_inlined_delete_<t>` tables serve inlined rows with per-row
begin/end and `row_id` (RFC 0005); and `ducklake_snapshot` serves the
version/timestamp resolution.

## Design

There is no new design surface. The change data feed is served by the
projections that exist; what follows are the invariants it depends on and
where each one lives.

### Snapshot attribution is served verbatim

The feed's attribution rides `begin_snapshot` / `end_snapshot` /
`partial_max` on `ducklake_data_file` and `ducklake_delete_file`, and
per-row `begin_snapshot` / `end_snapshot` on inlined data. All of them are
DuckLake's own staged writes, stored and served back unaltered; moraine
allocates or rewrites none of them. `ducklake_snapshot` resolves the
requested bounds (`snapshot_id` for version bounds, `snapshot_time` for
timestamp bounds) and is served the same way.

`changes_made`, notably, is **not** consulted by the feed — it serves
conflict detection (RFC 0004) and stays a verbatim conduit there, but no
change-feed invariant attaches to it. A future DuckLake that changes its
grammar cannot break the feed against moraine.

### Rowid provenance is served verbatim

`change_type` pairing and row identity ride rowids: `row_id_start` on
`ducklake_data_file`, the `row_id` column in inlined data and inlined
deletes, and delete-file row references. moraine stores and serves all of
them verbatim from DuckLake's staged writes; it allocates rowids on no path
of its own.

### History and flush backdating

The feed reads *past* state, so it inherits the time-travel invariant:
every file, delete file, and inlined row is served current-and-history with
its begin/end snapshots. Flush is the one place this is subtle — flushing
inlined data must not surface as fresh inserts in a later range, and a
range crossing a flush must still attribute rows to the snapshots that
originally inserted them. moraine already backdates flushed files to their
rows' original begin snapshots, and a flushed file collecting rows from
several snapshots carries the `partial_max` DuckLake wrote so the scan can
filter per row; the feed is a second consumer of that behavior (time
travel was the first) and pins it independently.

### Errors

Nothing to add. Out-of-range bounds, tables that do not exist in the range,
and malformed arguments are all raised by DuckLake from its own SQL;
moraine's projections just answer the underlying queries.

### Test obligations

All live, in the e2e suite over the full `ducklake:moraine:` chain:

- **Insert attribution.** Inserts across several snapshots; `table_changes`
  over sub-ranges attributes each row to its minting snapshot with
  `change_type = 'insert'` and stable rowids.
- **Deletes.** A `DELETE` surfaces as `change_type = 'delete'` carrying the
  deleted rows' values and rowids; `table_deletions` agrees.
- **Updates.** An `UPDATE` surfaces as an `update_preimage` /
  `update_postimage` pair sharing a rowid within one snapshot.
- **Flush crossing.** A range spanning `ducklake_flush_inlined_data`
  reports the same changes before and after the flush — flush itself
  contributes no rows to the feed.
- **Function agreement.** `table_insertions` and `table_deletions` return
  exactly the corresponding subsets of `table_changes`.
- **Timestamp bounds.** The `TIMESTAMPTZ` overload resolves bounds through
  served snapshot times and agrees with the version form.

Any gap these surface is fixed as a serving-fidelity bug (failing test
first), not by adding feed machinery.

## Alternatives considered

- **moraine materializes the feed** (a computed projection or table
  function returning change rows). Rejected: there is no `ducklake_*`
  catalog table for it to serve — the feed is derived, not stored — so this
  would duplicate DuckLake's reader logic inside moraine, breaking the
  established division (moraine serves rows, DuckLake computes) and
  creating a second implementation to keep bug-compatible.
- **Strict `changes_made` validation at commit** (core parses the staged
  string through `ChangeSet` and rejects unknown entries). Rejected: the
  conflict-detection parser deliberately passes unknown kinds through
  (classifying them conservatively), because a newer DuckLake may write
  entries this binary does not model; strictness would turn forward
  compatibility into commit failures and buys the feed nothing — the feed
  never reads the string.
- **A core public change-feed API** (`Catalog::snapshot_changes(from, to)`
  returning parsed change sets). Rejected for v0.1: no consumer exists —
  the only reader is DuckLake, which wants rows, not structs — and the
  public API shape is expensive to reverse (RFC 0003). The internal
  `ChangeSet` stays internal.
