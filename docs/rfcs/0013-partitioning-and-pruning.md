# RFC 0013: Partitioning and pruning

- **Date:** 2026-07-09

## Summary

Places DuckLake's partitioning tables in the moraine keyspace and fixes
moraine's stance on partition pruning. Three DuckLake tables are in scope:
`ducklake_partition_info` (a table's partition **spec**),
`ducklake_partition_column` (the **columns and transforms** of a spec), and
`ducklake_file_partition_value` (a data file's partition **values**). The spec
already has a home — the `partition` kind of [RFC 0002](0002-slatedb-key-encoding.md);
this RFC confirms its lifecycle, embeds partition columns per RFC 0002's
convention, and embeds per-file partition values into the `file` record so a
table's files and their partition values are one contiguous scan. On pruning it
mirrors RFC 0002 exactly: moraine stores specs, transforms, and values
**verbatim** and serves them efficiently; DuckLake's planner does the pruning.
Server-side partition pruning is deferred, and is the same open question as
RFC 0002's stats-pruning pushdown.

## Goals

- Every partitioning table has a defined home, validated against real DuckLake
  SQL in the e2e suite.
- A planner's core question — "scan the files of table T with their partition
  values" — is one contiguous `table_id`-first `current` range, no join, no second
  subspace.
- Partition specs evolve over a table's life (set, change, clear) and time
  travel reconstructs the spec-in-force at any snapshot for free.
- A data file keeps the spec it was written under, so files under different
  specs coexist correctly.

Non-goals:

- Server-side partition pruning — deferred (see Open questions), the same
  posture RFC 0002 takes on stats pruning.
- Evaluating transforms. moraine stores transform definitions verbatim and
  never applies them (RFC 0006 row-faithfulness).
- Compaction planning across partitions — [RFC 0008](0008-compaction.md) owns
  the rewrite; this RFC only states what partition state a rewrite must carry.
- Hidden/implicit partitioning schemes beyond DuckLake's spec.

## Background

DuckLake partitions a table by a **partition spec**: an ordered list of
partition columns, each pairing a source column with a **transform** (identity,
`year`/`month`/`day`/`hour`, `bucket(N)`, `truncate`). A written data file
records the partition **values** it falls under — one value per partition
column of the spec it was written against. A table can be repartitioned: the
spec is set, later changed, or cleared, and files written under older specs
remain valid. DuckLake models this with `ducklake_partition_info` (the spec,
temporally versioned like every catalog entity), `ducklake_partition_column`
(spec columns), and `ducklake_file_partition_value` (per-file values).

RFC 0002 already reserves the `partition` kind for the spec and states the
embedding convention this RFC applies. RFC 0002 also fixes the pruning stance
this RFC extends from stats to partitions: min/max are stored verbatim, the
comparison belongs to DuckLake, and any future server-side pruning must be
type-aware rather than a naive lexicographic compare.

## Design

### Placement

| DuckLake table | Home | Rationale |
|---|---|---|
| `ducklake_partition_info` | `partition` kind (RFC 0002), `(table_id, partition_id)`, in `current`/`history` | The spec has an independent lifecycle — a table repartitions over time — so it earns its own kind and is temporally versioned. |
| `ducklake_partition_column` | **Embedded** in the `partition` value | Pure child of the spec, no independent lifecycle: columns + transforms live and die with their spec (RFC 0002 convention). |
| `ducklake_file_partition_value` | **Embedded** in the `file` value | Per-data-file, 1:N (one value per partition column), no independent lifecycle: values live and die with their data file (RFC 0002 convention). |

The load-bearing choice is embedding `file_partition_value` in the `file`
record rather than giving it its own kind or subspace. A file's partition
values travel with its `current`/`history` record. Because per-table collections are
keyed `table_id`-first (RFC 0002), "scan the files of table T with their
partition values" is exactly one contiguous `current` range — no join against a
second subspace, no scatter. That is the shape a planner needs, and it costs no
new keyspace.

### The partition spec record

The `partition` value carries the spec's `begin_snapshot`, its ordered
partition columns, and for each column the source column reference and the
transform. Partition columns reference their source column **by field id**
(`column_id`), never by name — see schema evolution below. Transforms are
stored **verbatim** as DuckLake defines them (the transform name and its
parameter, e.g. `bucket(16)` or `truncate(10)`); moraine never parses or
applies them.

### Spec evolution

Partitioning is set, changed, or cleared over a table's life. Each is a
**schema-changing commit** that bumps `schema_version` and conflicts at
table level ([RFC 0004](0004-commit-protocol.md)). The spec is versioned
temporally like any entity:

- **Set / change** — end the current spec's `current` version into `history` with
  `end_snapshot` (if one existed) and insert the new spec into `current`, in the
  same `WriteBatch` (RFC 0002 atomicity invariant).
- **Clear** — end the current spec's `current` version into `history`; the table has
  no live spec afterward.

A data file records, in its `file` value, the `partition_id` of the spec it was
**written under**. Files written under different specs therefore coexist
unambiguously: each names its own spec. Reconstructing the spec-in-force at
snapshot `S` is the ordinary `current`+`history` temporal filter (RFC 0002) — no
special path. Time travel gets the historical spec, and each file's values, for
free.

### Pruning — moraine does not prune, DuckLake does

This is the spine, and it is RFC 0002's stats stance applied to partitions.

moraine **serves** partition specs, transforms, and per-file partition values —
faithfully and efficiently, available inside the single per-table file scan —
and **does not evaluate** them. DuckLake/DuckDB's planner reads the spec, reads
each candidate file's partition values (in the same scan), applies the
transforms it already understands, and prunes. moraine returns rows; the
planner decides which files to read.

A future **server-side partition-pruning pushdown** is deferred, and it is the
**same open question** as RFC 0002's stats-pruning pushdown and RFC 0006's
pushdown surface — moraine serves filter/projection pushdown where it maps
cleanly onto the key layout, and partition pruning is not such a case today. If
it is ever added it must be both **transform-aware** and **type-aware**:
correctly pruning a `bucket(16)` or `year(...)` partition means reproducing
DuckLake's transform semantics, and comparing a value means DuckLake's typed
comparison, never a lexicographic compare over stored strings. Doing it wrong
silently drops correct rows — the exact failure RFC 0002 warns against. Until
there is e2e evidence that a real DuckLake access pattern demands it, moraine
stays row-faithful.

### Interaction with schema evolution (RFC 0012)

Partition columns reference source columns by **field id** (`column_id`), and
[RFC 0012](0012-schema-evolution-and-versioning.md)'s field-id stability is
exactly what keeps a spec valid across column renames and reorders: the spec
names field 7, and field 7 remains field 7 whatever it is called or wherever it
sits. This is why keying by name would be wrong (see Alternatives).

Whether a column that a live spec partitions on **may be dropped or altered** is
a DuckLake rule, not moraine's. moraine follows DuckLake row-faithfully: it
does not invent its own validation that could drift from DuckLake's. The
validation boundary sits in DuckLake's planner/executor, which issues (or
refuses to issue) the commit; moraine records whatever committed state results.
Where DuckLake relies on catalog constraints to enforce this, those are the
constraints RFC 0006 discusses enforcing at the catalog layer.

### Interaction with compaction (RFC 0008)

Compaction rewrites data files and must preserve or recompute the correct
partition values for the rewritten files; RFC 0008 owns that. moraine stores
whatever partition values the rewrite produces, embedded in the new `file`
records, and ends the compacted-away files' `current` versions into `history` as
usual. If moraine ever drives compaction planning, partition boundaries
constrain which files may merge — a merge must not cross partitions whose values
differ under the governing spec. That constraint is noted here and specified in
RFC 0008; this RFC only fixes that the partition state a rewrite needs is
already present in the per-table file scan.

### Property-test and e2e obligations

Per RFC 0001, `store`-layer encoding ships with proptest coverage, and the
partitioning tables extend it:

- **Spec + values round-trip.** A partition spec (columns + transforms) and a
  file's partition values encode and decode verbatim, byte-for-byte against
  what DuckLake wrote — transforms included, unparsed.
- **Evolution time-travels.** Set → change → clear a table's partitioning
  across snapshots; time travel to each snapshot reconstructs the
  spec-in-force, and every file still reports the spec it was written under and
  the values under it.
- **Access pattern captured in e2e.** DuckLake's own partition-pruning queries
  are captured in the e2e suite, both to validate the mapping against real
  DuckLake SQL and to settle the file-partition-value placement (below) against
  observed reads.

## Open questions

- **Server-side partition-pruning pushdown.** Deferred. The same open question
  as RFC 0002's stats-pruning pushdown and RFC 0006's pushdown surface. If ever
  built, it must be transform-aware **and** type-aware, never a naive compare.
- **Embedded vs. own kind for `file_partition_value`.** This is the same
  access-pattern question as RFC 0002's `fstat` file-major-vs-column-major
  ordering: embedding keeps a file's values in the same contiguous per-file
  scan (the write unit and the per-file predicate unit), whereas a value-major
  layout would favor "all files' values for one partition column." Settled
  against the captured DuckLake partition queries in the e2e suite before the
  collection grows large enough that reversing it means a migration. Embedded
  stands until then.
- **Drop/alter of a partitioned column.** Where exactly DuckLake draws the line,
  and whether moraine must enforce any part of it at the catalog-constraint
  layer (RFC 0006) or purely follows DuckLake's committed state.
- **Hidden/implicit partitioning.** Whether DuckLake grows partitioning schemes
  beyond the explicit spec (e.g. derived or hidden partitions) that would need
  their own representation.
- **Compaction across partition boundaries.** The precise merge-eligibility
  rule, owned by RFC 0008.

## Alternatives considered

- **A separate `pval` subspace/kind for `file_partition_value`.** Rejected: the
  values have no independent lifecycle, and a separate kind would split "the
  files of table T and their partition values" into a join across two ranges.
  Embedding keeps them in one contiguous `table_id`-first scan (RFC 0002
  convention) — the shape the planner reads.
- **moraine evaluating transforms and pruning server-side now.** Rejected for
  the same reason RFC 0002 rejects server-side stats pruning: row-faithfulness,
  and correctness would demand full transform-awareness plus type-aware
  comparison. A wrong compare silently drops rows. Deferred to an open question,
  gated on e2e evidence.
- **Keying partition columns by name instead of field id.** Rejected: it breaks
  under column rename and reorder. Field-id references (RFC 0012) keep a spec
  valid across schema evolution.
- **A single global partition spec per table with no history.** Rejected: it
  defeats repartitioning and time travel. Files written under an earlier spec
  would have nowhere to point, and reconstructing a past catalog would report
  the wrong partitioning. The temporal `current`/`history` spec is what makes both
  correct.
