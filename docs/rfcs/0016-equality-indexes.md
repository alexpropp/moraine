# RFC 0016: Equality indexes

- **Date:** 2026-07-10
- **Revised:** 2026-07-19 — staged (multi-commit) builds designed in;
  previously deferred to Open questions.
- **Revised:** 2026-07-21 — rewrite-file row-id resolution designed in
  (Rewrite files); previously an implementation follow-up behind a typed
  refusal.

## Summary

Adds a moraine-native **equality index**: a catalog object (`create_index` /
`drop_index` on the [RFC 0003](0003-public-api-shape.md) verb surface) whose
entries live in a new `idx` subspace and serve two reads — **row location**
(key values → row ids and the files/chunks that hold them) and **uniqueness
enforcement** at commit. DuckLake v1.0 models no indexes, so this is a native
feature: real inside moraine, invisible to every DuckLake catalog scan.
Entries are **live-only** (no temporal versioning) and point at **row ids**,
which DuckLake preserves across flush, update-rewrites, and compaction — data
movement never touches the index. Uniqueness rides SlateDB's write-write
conflict detection: a unique entry's store key *is* the key value, so racing
commits inserting the same value collide mechanically and the loser resolves
to a typed `Constraint` under [RFC 0004](0004-commit-protocol.md)'s ordinary
retry. Rows moraine holds are indexed automatically; externally written
Parquet is covered by **writer-supplied entries** at registration (embedding
API) or by moraine's **scoped read** of the registered file (extension path).
Builds too large for one commit run **staged**: the definition lands in a
`building` state, backfill streams in bounded batches while writers maintain
entries from day one, and a final commit flips the index `ready` (Staged
builds).

The store is one index; the ways in are two. The **embedding (verb) API**
creates and maintains indexes directly — the bulk of this RFC. The
**extension path** reaches the same `idx` storage from DuckDB SQL through
registered moraine functions, and covers DuckLake-written Parquet by the
scoped read, which also enforces uniqueness over SQL writes.
DuckLake-native `CREATE INDEX` waits on DuckLake itself, which owns the
user-table binder and refuses index DDL before moraine is consulted
(Extension path).

## Goals

- An index covers **all live rows** of its table — inlined, flushed, and
  externally registered — or refuses the operation that would breach that;
  uniqueness that silently under-covers is worse than none.
- Index maintenance rides the commit it belongs to: entries land in the same
  single `WriteBatch` (RFC 0002 atomicity invariant), and a unique violation
  aborts the commit that caused it, never a later one.
- Flush, compaction, and update rewrites — every operation that moves a row
  without changing its identity — cost the index **zero writes**.
- Concurrent appends to one indexed table keep RFC 0004's append-append
  benignity unless they actually collide on a unique key value.
- Reads are consistent: a lookup and the snapshot it serves reflect one cut
  of the store (RFC 0009 pinned handles).
- Stores that never create an index are byte-identical to stores built
  before this RFC, and remain readable by older binaries.

Non-goals:

- **Range scans.** The contract is equality only. Canonical encodings are
  chosen order-compatible where that is free (see Encoding), so a future
  range contract is an upgrade, not a rewrite — but nothing here promises
  order.
- **Serving DuckLake's planner transparently.** DuckLake owns the user-table
  binder and optimizer, so index-routed pushdown waits on DuckLake (Extension
  path, Future directions). The extension path integrates through an explicit
  surface — registered functions and the scoped read — never `ducklake_*`
  changes or the planner.
- **Approximate structures** (bloom filters, zone maps) — they cannot carry
  uniqueness; RFC 0013's pruning stance already covers the skipping use case.

## Background

DuckLake v1.0 has no index tables, so an index cannot be smuggled in as
another row-faithful `ducklake_*` mapping — it is moraine inventing a
capability, and the honest shape for that is a native feature stored where no
catalog scan (current or time-traveling) can see it.

Three established facts carry most of this design:

- **Row ids are stable identity.** DuckLake allocates row ids per table
  (`next_row_id` in `tstat`, RFC 0004) and preserves them across inline
  flush, UPDATE's delete-and-rewrite, and compaction — row lineage. Every
  data file records its row-id range, and RFC 0005's inlined chunks carry
  `row_id_start`/`row_count`. A row id therefore resolves to its current
  location(s) from the materialized catalog snapshot alone.
- **moraine does not read Parquet on the scan path** (RFC 0006 non-goal) —
  merge-on-read, lineage, and pushdown are DuckLake's. On the embedding API
  it sees row contents at exactly two moments: `inline_insert` and flush
  (moraine writes that Parquet itself); entries for external rows are
  writer-supplied, like file column stats. The extension path adds one
  bounded exception: projecting the indexed columns of a freshly registered
  file (Extension path) — a raw-value projection with no merge, not the scan
  path the non-goal guards.
- **Write-write conflict detection is the store's only race primitive**
  (RFC 0004): no read-write detection, no key CAS. A design that needs
  "detect that someone else inserted this value" must arrange for the race
  to be a write-write collision on one key.

## Design

### The index definition — a catalog entity

A new `index` kind in `current`/`history`, keyed `(table_id, index_id)` with
`index_id` allocated from the global `next_catalog_id`. The value carries the
index name, the **ordered list of indexed columns referenced by field id**
(never by name — the RFC 0012/0013 rule, so renames are free), the unique
flag, and `begin_snapshot`. Staged builds add a state field and a build
cursor to the same value (Staged builds); a definition without a state field
is `ready`, so single-commit creates are unchanged on disk. The *definition*
is temporally versioned like any entity — time travel reconstructs which
indexes existed at snapshot `S` — even though entries (below) are not.

Verbs on `Transaction`:

| Verb | Effect |
|---|---|
| `create_index(table, name, columns, unique)` | Insert the definition; build entries for every live row (see Coverage) in the same commit. |
| `drop_index(index)` | End the definition into `history`. Entries are orphaned and reclaimed lazily (see Reclamation). |

`CatalogSnapshot` gains `indexes_of(table)`; domain types gain `IndexId` and
`IndexInfo`.

**Commit classification.** Index DDL is recorded in `changes_made` as
`altered_table:<table_id>` — DuckLake's parser throws on unknown kinds
(RFC 0004), so the entry must use vocabulary it parses. `altered_table` is
also *correct*, not just parseable: an alter truly conflicts with concurrent
inserts, deletes, and other alters, in both directions — exactly what index
DDL needs. A `create_index` racing a `register_data_file` must not win
mechanically (its backfill was staged against the old file set), so the race
aborts as `CommitConflict` and the caller re-drives with fresh backfill. For
coherence with DuckLake's own classification (every alter bumps), index DDL
sets `schema_changed`; the cost is one spurious schema-cache refresh per
index DDL.

**Column DDL on indexed columns.** `drop_column` and `alter_column` (type
change) on a column a live index references fail with `Constraint` — the
canonical encoding is type-bound, and a silent cascade would discard an
object the host created deliberately. Drop the index first. `rename_column`
is unaffected (field-id references).

### The `idx` subspace and entry keys

A new subspace, one leading discriminant byte appended to the `Key` enum —
a SlateDB segment of its own (RFC 0002), so entry churn from a hot indexed
table compacts independently of the metadata subspaces, the same isolation
`inline` gets. Golden vectors pin the discriminant like every other kind.

| Kind | Key components | Value |
|---|---|---|
| `idx/unique` | `index_id, canon(key values)` | `row_id` |
| `idx/multi` | `index_id, canon(key values), row_id` | (empty) |

Two shapes, one deliberate asymmetry:

- **Unique entries key on the value alone.** The store key *is* the claim of
  uniqueness. Two commits inserting the same value — the race no read-side
  check can close, since SlateDB detects write-write conflicts only — write
  the *same key* and collide in the store's own conflict detection. The loser
  retries per RFC 0004, re-runs its closure, sees the winner's entry, and
  returns `Constraint`. Uniqueness under concurrency is correct by
  construction, with zero new race machinery.
- **Non-unique entries append the row id**, so rows sharing a value occupy
  distinct keys: concurrent appends of different rows write disjoint entry
  keys and stay **benign** under the append-append refinement — indexing a
  table does not serialize its writers. A lookup for `v` is the ascending
  prefix scan `idx/multi/{index_id}/{canon(v)}/` (RFC 0002 forward-only).

`index_id`-first keying makes each index one contiguous range — the unit
lookups scan and drop orphans (Reclamation).

### Canonical value encoding

Entry keys embed column values — a deliberate, bounded exception to
RFC 0002's "no strings in keys" rule, which exists so *entity* keys stay
rename-stable; an index key's whole job is to be the value. The contract is
**canonical bytes for equality**, per DuckLake column type, pinned by golden
vectors and proptest roundtrips like every other store codec:

- Integers: fixed-width big-endian, sign bit flipped; widths normalized per
  the column's type. Order-compatible, though order is not contracted.
- Strings/blobs: the UTF-8/raw bytes, `storekey`-escaped so component
  boundaries stay unambiguous in composite keys.
- Floats: IEEE bits with `-0.0` normalized to `+0.0` and all NaNs collapsed
  to one quiet-NaN pattern — matching DuckDB's total-order equality, where
  `NaN = NaN` holds.
- Temporal types: their underlying integer representation.
- Composite indexes concatenate component encodings in declared column
  order; `storekey` tuple framing keeps `("ab","c")` distinct from
  `("a","bc")`.

**NULL semantics follow SQL:** a row with NULL in any indexed column gets
**no entry** — a unique index admits any number of such rows, and an
equality lookup on NULL has no index answer (typed error; NULL scans were
never point lookups).

**Oversized values are refused.** Indexed values beyond a fixed cap fail
with `Constraint` at insert/registration — huge keys degrade the whole
segment, and equality over megabyte values is not this feature's job.
Hash-overflow is rejected for v1 (Open questions records the threshold).

### Coverage — who writes entries, and when

Coverage is total over live rows, maintained at the three moments rows
enter, and the two moments they die:

| Moment | Who has the rows | Entry source |
|---|---|---|
| `inline_insert` | moraine (rows arrive through the API) | computed by moraine, staged in the same batch |
| flush (RFC 0005) | moraine (it writes the Parquet) | **nothing to do** — entries point at row ids, which flush preserves |
| `register_data_file` | the writer | **writer-supplied**: `(row ordinal, key values)` pairs alongside the file, mapped to row ids at commit (`row_id_start + ordinal`) — or `(row_id, key values)` for rewrite files that preserve row ids — like the file column stats the writer already computes |
| `inline_delete` of a store-resident row | moraine (the chunk is in the store) | moraine recomputes the key values from the chunk and stages the entry delete |
| `register_delete_file`, or `inline_delete` against flushed rows | the writer (it read the rows to produce positions) | **writer-supplied**: `(row_id, key values)` per deleted row; moraine deletes the named entries |

The embedding-path rule generalizing the last two: any operation that kills
rows moraine cannot read from the store must name their indexed key values —
entries are keyed by value, and moraine cannot derive a value from a row id
it cannot dereference. (The extension path lifts that premise by reading the
file.)

**Registration without entries is refused.** A `register_data_file` or
`register_delete_file` on an indexed table that omits the required entries
fails with `Constraint` — a silently under-covered unique index is a lie
(mark-stale is recorded in Alternatives).

**DuckLake registrations read instead of refusing.** The refusal above is the
embedding-API contract: the writer computes entries as it computes stats. A
DuckLake staged registration carries no entries by construction, so on any
indexed table moraine derives them by the scoped read (Extension path) —
however the index was created. Reject-vs-read splits by who writes, not by
which API made the index.

An UPDATE on the verb path (expire + register preserving row ids) composes:
the delete side removes old entries, the insert side adds new — same commit,
same batch, uniqueness checked against the post-image.

**Backfill at `create_index`.** Rows moraine holds (inlined, any schema
version) are backfilled by scanning the chunks. Rows in external files need
writer-supplied backfill entries passed to `create_index` — except through
`moraine_create_index`, which has no writer and backfills by scoped-reading
**every live data file** of the table, so its build cost is the table's
indexed columns, and the one-commit bound bites hardest exactly there. The
whole build is one commit, one batch; uniqueness is validated over the
assembled entry set before staging, and a duplicate aborts the create. A
backfill that exceeds the store's batch bound fails typed before staging
anything, and the caller re-drives as a staged build (Staged builds).

### Uniqueness enforcement

At commit, for each new unique entry the committer point-gets the entry key
through the transaction handle (RFC 0004 step 1's read discipline): present
with a **different** row id → `Constraint`; present with the **same** row id
→ no-op (a re-derived entry for a rewrite file, Extension path); absent →
stage the put. Duplicates *within* the commit are checked in memory. The race window between the get and a concurrent commit's
identical put is closed by the key collision described above — that is the
load-bearing property of keying unique entries on the value alone.

Because entries are live-only, "present" means "a live row holds this
value": deletes remove entries in the same batch that kills the row, so
delete-then-reinsert behaves as SQL expects, within one commit or across
commits.

The `Constraint` here is a verb-path error (embedding API) — no DuckLake
wire contract applies to its text.

### What data movement costs the index: nothing

The entry payload is a row id, not a location. Flush re-homes rows from
chunks to Parquet; compaction and UPDATE rewrites re-home them across files;
row ids survive all three. Resolution to a current location happens at read
time against the materialized snapshot: chunks and files declare row-id
ranges, so the lookup finds the holder by interval — in memory, at catalog
scale. No maintenance operation rewrites an entry. This is why live-only
entries plus row-id payloads is the whole design: every alternative payload
(file id, chunk key) turns flush and compaction into index rewrites
proportional to moved rows.

### Lookups

One accessor family on `CatalogSnapshot`, served under the snapshot's pinned
read handle so the lookup and the catalog it points into are one consistent
cut:

- `index_lookup(table, index, key_values) -> Vec<RowLocation>` — point-get
  (unique) or prefix scan (non-unique) in `idx`, then resolve each row id
  against the snapshot's chunk and file ranges. A `RowLocation` names the
  row id and its holder: an inlined chunk, or candidate data file(s) whose
  live row-id range contains the id. The consumer applies delete files as
  any DuckLake scan does — moraine returns candidates, not adjudicated rows.
  The extension path surfaces this accessor as `moraine_index_lookup`.

Lookups are **head-only**: entries are live-only, so `snapshot_at(S)` fails
with a typed error and time travel falls back to what it always was — a scan
problem. The hot path (current head) gets the index; the rare path pays
nothing to keep it honest.

### Staged builds — multi-commit backfill

A backfill too large for one `WriteBatch` cannot be one commit, and the
bounded-commit rule (Reclamation's posture) is not negotiable. The staged
protocol splits the build into a `building`→`ready` lifecycle built from the
same three primitives as everything else here: writer-maintained coverage,
the value-keyed collision, and bounded batches. No new race machinery.

**The lifecycle.** `create_index(…, staged)` commits the definition in
`building` state — `altered_table`, `schema_changed`, exactly like the
single-commit create. From that commit forward the **full Coverage contract
applies to every writer**: inserts stage entries, deletes stage entry
removals, entry-less registrations are refused, DuckLake writes are covered
by the scoped read — a building index is maintained as if it were real,
because by the time it flips `ready` it must already have been. Unique
entries are value-keyed from the first commit; there is no second key form
to convert later. What `building` withholds is the *outward* surface:
`index_lookup` fails typed (`IndexBuilding`) — the coverage Goal is kept by
refusal, the same way it is kept everywhere else — and a unique collision
does not fail the writer (below).

**Backfill batches.** The builder covers exactly the rows live at the
definition snapshot `S₀`; everything after `S₀` is writer-covered by the
paragraph above, so the two sets meet with no gap. It walks the table's
`S₀` chunk and file list in row-id order, deriving entries per the Coverage
table (chunks by scanning, external files writer-supplied or scoped-read),
and commits one bounded batch at a time. Each batch commit atomically
advances a **cursor** persisted in the definition value — the last covered
(file, row-id) watermark — so a crashed build resumes from its cursor, and
re-derived entries land as idempotent puts (multi keys include the row id;
unique puts hit the same-row-id no-op arm). Two builders racing the same
build both write the definition key and collide write-write — the cursor
serializes them mechanically. Batches are classified
`inserted_into_table:<table_id>`: parseable vocabulary, and the correct
semantics — benign with concurrent appends (a build must not serialize the
table's writers), conflicting with alters (a schema change mid-build must
re-drive the batch; column DDL on indexed columns stays refused outright).

**The delete race.** A row live at `S₀` can die before its batch lands, and
a stale entry for a dead row is corruption — for a unique index it
manufactures false `Constraint`s. Two mechanisms close it, split by tense:

- *Past deletes* (committed before the batch's snapshot): the batch excludes
  them. The cursor carries the last snapshot scanned for deletes; each batch
  enumerates the delete bookkeeping registered since — inline deletes name
  row ids directly; delete files name positions, resolved to row ids by the
  same rule the scoped read uses (embedded row-id column, else
  `row_id_start + ordinal`). Cost is proportional to deletes-during-build,
  never table size.
- *Concurrent deletes* (racing the batch): the killing commit stages entry
  removals for the building index (Coverage, in force since `S₀`) — a
  tombstone on the same entry key the batch is putting. Same key, two
  writers: the store's write-write detection fires, the loser re-runs, and
  the batch's re-run sees the newer bookkeeping and excludes the row. The
  collision that gives uniqueness its guarantee gives the build its
  correctness.

**Duplicates poison the build, not the writer.** During `building` the
entry set is incomplete, so an absent point-get proves nothing and
enforcement cannot be offered — but violations can still be *detected*,
because unique keys are value-keyed from day one. Whenever a unique put
finds an entry for a **different row id** — a backfill batch discovering a
pre-existing duplicate, or a writer inserting a value another live row
holds — the commit stages a terminal `poisoned` flag on the definition and
skips the put; the writer's own rows land normally. SQL semantics for a
concurrent build: the data's duplicate fails the *index*, not the insert. A
poisoned build stops at the next cursor step: the driver ends the
definition into `history` (ordinary drop, Reclamation sweeps the range) and
surfaces `Constraint` to the `create_index` caller. Poisoning writes the
definition key, so it conflicts with cursor advances — both re-run; the
flag is terminal so every interleaving converges.

**The ready flip.** When the cursor reaches the end of the `S₀` set and the
definition is not poisoned, one final commit flips `building`→`ready` —
`altered_table`, `schema_changed`, like the create: the flip changes what
readers may do, so it must conflict with in-flight writes, whose re-runs
then see `ready` and apply full enforcement (`Constraint` instead of
poison). From the flip forward the index is indistinguishable from a
single-commit build: same keys, same coverage, same guarantees. The
before/after entry ranges are byte-identical to what a from-scratch build
over the same live rows would produce — that equivalence is a test
obligation.

**Driving the build.** The embedding API returns from `create_index(staged)`
once the definition commits; the host advances the build with a
`build_index_step(index)` verb — one bounded batch per call, returning the
cursor position or terminal state — and loops at its own pace. The
extension path's `moraine_create_index(…, staged := true)` drives the loop
internally in the same autonomous-commit style as the other DDL functions;
`moraine_indexes` exposes state and cursor for progress. `drop_index` on a
building index is an ordinary drop: the builder's next step re-runs against
the ended definition and stops.

**Format.** Staged builds stamp **format 3** at the first staged
`create_index` — lazily, like format 2. A format-2 binary would ignore the
unknown state field, see a `building` definition as a ready index, and
serve lookups and enforcement from an under-covered entry set — precisely
the silent corruption the gate exists to refuse. Single-commit-only stores
stay format 2; completing or dropping the build does not downgrade the
stamp, per the existing posture.

### Extension path: SQL surface and the scoped read

moraine is DuckLake's metadata catalog, not the catalog that owns user tables
(RFC 0006). `CREATE INDEX` and `PRIMARY KEY`/`UNIQUE` on a DuckLake table die
in DuckLake's own binder (`NotImplementedException`) before moraine is
consulted, so DuckLake-native syntax is not moraine's to offer. The extension
path instead ships its own surface over the same native index and covers
DuckLake's writes itself.

**The SQL surface.** DuckDB's stable C API registers table functions (only
catalog registration forces the C++ shim, RFC 0006), so the extension exposes
the native index without touching DuckLake's binder:

| Function | Effect |
|---|---|
| `CALL moraine_create_index('lake.t', 'name', columns := ['c'], unique := b, staged := b)` | insert the definition, backfill live rows (Coverage; staged drives the multi-commit protocol, Staged builds) |
| `CALL moraine_drop_index('lake.t', 'name')` | end the definition (Reclamation) |
| `moraine_index_lookup('lake.t', 'name', v)` | table function: row ids and holders for value `v` |
| `moraine_indexes('lake.t')` | table function: index introspection |

Each resolves the store handle from the attached catalog and the `table_id`
from `ducklake_table`, then drives the same core verbs as the embedding API.
The DDL functions commit autonomously — their own moraine commit, outside any
enclosing DuckDB transaction — and race concurrent DuckLake commits through
the ordinary `altered_table` conflict. Non-native syntax, explicit reads
(below), but real without a DuckLake change.

**Coverage: intercept inline, read bulk.** Small inserts DuckLake stages
inline; their values cross the ABI as an Arrow body (RFC 0005), and moraine
derives entries from them as embedding `inline_insert` does. Bulk data
DuckLake writes to Parquet directly — those values never cross moraine's
boundary, so there is no "before" to intercept — and the committer instead
**reads the just-registered file**, projecting only the indexed columns, the
row positions, and the row-id column when the file carries one. Entry row ids
follow DuckLake's own resolution rule (source-verified in its reader): the
file's embedded row-id column if present — rewrite files from UPDATE and
compaction preserve old ids there — else `row_id_start + ordinal`. Deletes
are symmetric: a staged `register_delete_file` names positions; moraine reads
those positions' indexed values and row ids from the target file to name the
entries to remove. Indexed columns are located in the file by field id
through the column-mapping rules (RFC 0018). The prohibition on reading
Parquet guards the *scan* path — merge-on-read, lineage, pushdown — not this
raw-value projection.

A value is indexable only if its Parquet form and its inline Arrow form
derive the *same* canonical bytes, so the two write paths collide as they
must. That holds for the scalar types, strings, blobs, `UUID` (a 16-byte
blob on both paths, once the inline body is Arrow-encoded losslessly), and
the temporal types by their underlying integer count. It fails for a 128-bit
integer: DuckDB writes `HUGEINT` to Parquet as a *lossy* `double`, so its
data-file and inline forms disagree and distinct values could collide —
`CREATE INDEX` on one is refused rather than built silently wrong.

Two placement consequences. The Parquet projection and the inline Arrow-body
decode are **core** capabilities (`arrow` and `parquet` enter `moraine`) —
entry derivation is catalog-domain logic, not shim marshalling (RFC 0001).
And the core needs an object-store handle for **data** files: they live under
`DATA_PATH`, not the catalog store. That path is fixed when the lake is
created and does not reach the metadata attach on its own (DuckLake keeps its
`DATA_PATH` and forwards only `META_`-prefixed options), so it is supplied
once at creation via `META_DATA_PATH` and **recorded** in the global
`data_path` metadata option at bootstrap — beside `encrypted`. From then on
the recorded value is authoritative: the core serves it back as DuckLake's
`ducklake_metadata` `data_path` row (so a re-attach need not repeat it, and
DuckLake refuses a `DATA_PATH` that disagrees), and the shim resolves the
maintenance store from it, reusing the catalog store's secret. A re-attach
that supplies a conflicting `META_DATA_PATH` is refused rather than silently
honoured. Data-file paths resolve against that store.

**Re-derivation is idempotent.** A registered file is not always new rows:
rewrite files carry rows that already have entries. Multi entries re-derive
as idempotent puts (the key includes the row id), and the unique check's
same-row-id no-op arm (Uniqueness enforcement) covers the rest — so DuckLake
compaction and UPDATE do not abort against their own existing entries.

**Uniqueness on the SQL write path.** Enforcement hinges on one thing: the
value-keyed put lands in the same atomic commit that adds the row (Uniqueness
enforcement). Both paths do that — inline from the Arrow body, bulk from the
scoped read — so the value's provenance is irrelevant, *provided both derive
the same canonical bytes for a given value*. That holds because the shim
encodes the inline Arrow body losslessly, matching how DuckDB writes Parquet
— a `UUID`, for one, is a 16-byte blob on both paths, not a blob in a data
file and a string inline — so a value inserted inline and the same value
written to a file collide as they must. Two further guarantees are
load-bearing:

- **A unique index's scoped read is synchronous at commit.** Deferring it
  (the non-unique option in Open questions) would let a duplicate commit
  unchecked.
- **A failed read aborts the commit.** If the registered file cannot be read,
  the commit fails with a store error; the check is never skipped.

The `Constraint` message must avoid DuckLake's four retry substrings
(`"conflict"`, `"concurrent"`, `"unique"`, `"primary key"` — RFC 0006): a
unique violation is permanent, and retryable-looking text would spin
DuckLake's `RunCommitLoop` before surfacing. A concurrent collision is the
opposite case — a genuine `CommitConflict` containing `"conflict"`; DuckLake
retries (repeating the scoped read), the re-read finds the winner's entry,
and the permanent `Constraint` surfaces then. On a bulk violation the Parquet
DuckLake already wrote is left orphaned for ordinary cleanup — space, not
correctness.

**Reads are explicit.** DuckLake owns the planner, so no optimizer routing.
The extension path reads through `moraine_index_lookup`; the caller joins
back to the table, whose scan DuckLake adjudicates against delete files. v1
scope: creation, uniqueness enforcement, explicit lookup. Pushdown waits on a
DuckLake change (below).

**Future directions — noted, and ruled out of the main design: both require
DuckLake to move first.** (a) *DuckLake grows index metadata.* That catalog
state would land in moraine like every other `ducklake_*` mapping, with
maintenance arriving as writer-supplied entries (the Coverage contract,
DuckLake as the writer); the protobuf definition value reserves a
`ducklake_index_id` field for mapping such an index onto the same `idx`
range, and that reservation is the entire commitment made here. (b) *A
DuckLake binder patch* — native `CREATE INDEX`/`PRIMARY KEY`, entry
maintenance delegated to the metadata catalog, index-served pushdown; an
upstream change moraine would own. Neither is designed further in this RFC.

Read-only attaches over an indexed store are unaffected.

### Rewrite files: row-id resolution

The scoped read's "embedded row-id column if present" rule (Coverage) is
made concrete here. Source-verified in DuckLake: UPDATE (always),
`rewrite_data_files` (always), and `merge_adjacent_files` (only when the
merged files' row-id ranges are not adjacent) append a trailing
`_ducklake_internal_row_id` BIGINT column to the files they write, tagged
with DuckDB's reserved Parquet **field id 2147483540**
(`MultiFileReader::ROW_ID_FIELD_ID`); its catalog row carries
`row_id_start` NULL. `merge_adjacent_files` and inline-data flush also
append `_ducklake_internal_snapshot_id` (field id 2147483539), which
index maintenance ignores. DuckLake's reader resolves a row's id as: the
field-id column when the file carries one, else `row_id_start + position`,
else a read error. Moraine's scoped read applies the identical rule.

**Column presence wins over `row_id_start` — the precedence is
load-bearing, not a tiebreak.** A flushed inline-data file carries embedded
row ids *and* a non-NULL `row_id_start` (DuckLake records the file's
minimum embedded id there), and its ids may hold gaps: inline rows deleted
before the flush are materialized out, but the survivors keep their
original ids. Dense derivation against such a file writes entries under
row ids the rows do not have — silent index corruption, no error raised.
Any resolution that consults `row_id_start` first is therefore wrong by
construction; the fallback order is fixed as column, then range, then
refusal.

**Resolution lives inside the scoped read.** The reader already fetches the
file's footer; discovering the field-id column there costs nothing and
keeps the precedence rule in one place instead of at every call site.
Callers state intent, two modes:

- **DuckLake-parity** — registration, delete-side maintenance, and index
  backfill. The caller passes the catalog row's `row_id_start` verbatim
  (`Option`); the reader prefers the field-id column, falls back to the
  dense range, and refuses (`Corruption` — the catalog row and the file
  disagree) when neither exists. A NULL or negative embedded id is
  likewise `Corruption`.
- **Ordinal** — the extension-path registration helper, whose contract is
  positions for `register_data_file` to re-map onto a freshly allocated
  dense range. In this mode a file carrying the field-id column is
  **refused**: registering a rewrite file under new dense ids would fork
  its identity — readers honour the embedded column, the catalog would
  claim the dense range, and the index would follow the wrong one.

**The delete side filters by position, not by pre-computed row id.** A
delete file names positions within its target; converting them to row ids
before reading the target bakes in the dense assumption. Instead the
killed positions ride verbatim to the target's scoped read, which returns
entries in file order — an entry is removed when its ordinal is named by
a delete file, or its resolved row id is named by an inline file-delete
(those carry row ids directly). One rule serves dense and per-row-id
targets alike.

**Maintenance stays derivation, never removal.** Registering a rewrite
file only re-derives entries that exist (compaction: the idempotent puts
and the unique same-row-id no-op arm, Uniqueness enforcement) or adds
entries for changed values (UPDATE: the paired delete file removes the
old-value entries in the same commit). No register-side path stages an
entry delete, so a rewrite cannot drop a surviving row's entry — the
property that makes re-derivation safe to run unconditionally.

Indexed columns keep their positional location (Coverage): rewrite and
flush outputs are written under the table's current schema with the
internal columns trailing, so table-column positions are undisturbed.

**Embedding-API boundary.** The verb surface has no way to register a
per-row-id file — `register_data_file` allocates dense ranges, which is
why its entries name rows by ordinal: the ids do not exist until the
commit allocates them. Deletion never has that problem — the target is
registered and its ids are known facts — so `register_delete_file`
removals name rows **by row id against every target**, exactly the
Coverage table's `(row_id, key values)` contract: `row_id_start +
ordinal` for a dense target, where moraine checks the id lies inside the
target's range (the same strength as an ordinal bounds check), and the
embedded id for a per-row-id target, taken verbatim and trusted exactly
as the entry's values are — the writer read the target to learn its
positions and values, and the row-id column sits in the same file. One
shape, one staging path, no per-target rules. What stays out is a
rewrite-*registration* verb: the embedding API cannot express a file
that preserves row ids, indexed or not — that is absent compaction
surface (RFC 0008 keeps compaction DuckLake-driven), not index
maintenance, and would be its own RFC.

### Format gate

An older moraine writer committing to an indexed table would maintain no
entries — silent index corruption. The gate is `sys/format`, bumped
**lazily**: the first `create_index` writes format 2 in the same commit, and
older binaries refuse to open a format-2 store (RFC 0002 bootstrap
validation). Index-free stores stay format 1, byte-identical to today,
compatible in both directions. Format 2 is format 1 plus the `idx` subspace
and `index` kind — no migration, no rewrite; dropping the last index does not
downgrade the stamp. Staged builds bump once more, to format 3 (Staged
builds), under the same lazy posture. The `idx` discriminant leaves the
segment extractor ("first byte") untouched, so existing segments and the
RFC 0011 crash matrix are unaffected.

### Reclamation

`drop_index` (and `drop_table`, which ends the table's indexes with it)
orphans the entry range. Entries are invisible the moment the definition
ends, so reclamation is pure space hygiene: a bounded background sweep
deletes `idx/{index_id}` in batches, riding RFC 0007's maintenance posture —
never inside the dropping commit, whose batch must stay bounded. A SlateDB
range-delete would collapse the sweep into one call (Open questions).

### Test obligations

Per RFC 0001 — store codecs get proptests, protocol claims get integration
tests against real SlateDB on in-memory `object_store`:

- **Encoding roundtrips + goldens.** `decode(encode(k)) == k` for both entry
  kinds; golden vectors pin the `idx` discriminant and the canonical
  encoding of every indexable type, including the float normalizations, NULL
  skip, and composite framing (`("ab","c") ≠ ("a","bc")`).
- **Unique race.** Two concurrent commits inserting the same unique value:
  exactly one lands; the other returns `Constraint` after its closure re-run
  — never two entries, never a lost insert.
- **Benign distinct-value appends.** Two concurrent appends of different
  values to one indexed table both land via the append-append path.
- **Movement invariance.** Insert inlined rows → flush → compact: lookups
  return the same rows throughout; the `idx` range is byte-identical before
  and after flush and compaction.
- **Delete coverage.** Store-resident delete (self-sufficient) and
  writer-supplied delete both remove entries; delete-then-reinsert of a
  unique value succeeds; a `register_delete_file` omitting entries on an
  indexed table is refused.
- **Registration contract.** Entry-less `register_data_file` on an indexed
  table → `Constraint`; with entries → covered lookups; ordinal→row-id
  mapping lands entries above the winner's `next_row_id` under concurrent
  append retry.
- **DDL interactions.** `create_index` racing `register_data_file` on one
  table → `CommitConflict` for one side; `drop_column`/type-change on an
  indexed column refused; rename unaffected; backfill validates uniqueness
  (duplicate in existing rows aborts the create).
- **Format gate.** First `create_index` stamps format 2 atomically with the
  definition; a format-1-only binary (simulated via the version check)
  refuses the store; index-free stores remain format 1. First *staged*
  create stamps format 3; a format-2-only binary refuses it;
  single-commit-only stores remain format 2.
- **Staged lifecycle.** A staged build over a table too large for one batch,
  under concurrent inserts, deletes, and updates: lookups fail typed while
  `building`; after the flip, the `idx` range is byte-identical to a
  from-scratch single-commit build over the same live rows.
- **Staged delete races.** A row deleted between `S₀` and its batch is
  excluded via the delete bookkeeping; a row deleted *concurrently* with its
  batch collides on the entry key, the batch re-runs, and the entry is
  absent after both commits — for multi and unique kinds.
- **Staged duplicate poisoning.** A pre-existing duplicate discovered
  cross-batch poisons the build: the create surfaces `Constraint`, the
  definition ends, entries are reclaimed. A writer inserting a duplicate
  during `building` lands its rows, and the build poisons instead of the
  writer failing.
- **Staged resume and racing builders.** A builder killed mid-build resumes
  from the persisted cursor with idempotent re-puts; two builders advancing
  one build serialize on the definition key.
- **Ready-flip visibility.** A write in flight across the flip re-runs
  (altered-table conflict) and commits under full enforcement — a duplicate
  in that write gets `Constraint`, not poison.
- **Head-only lookups.** `index_lookup` on `snapshot_at(S)` fails typed;
  on a pinned current snapshot it reflects that snapshot's cut even as
  newer commits land.
- **Scoped data-file read.** A staged registration on an indexed table
  derives entries for exactly the indexed columns; a subsequent lookup
  returns the file's rows; the read touches no non-indexed column. Golden
  file fixtures pin the reader against each indexable type. A staged
  `register_delete_file` removes exactly the named positions' entries.
- **Rewrite idempotence.** A rewrite file carrying a row-id column derives
  entries under the preserved ids; DuckLake compaction over an indexed
  table (unique included) leaves the `idx` range byte-identical and never
  aborts on the rows' own existing entries. An UPDATE-shaped commit
  (delete file plus per-row-id file with changed values) removes the
  old-value entries and lands the new-value entries in one commit; an
  unchanged indexed value survives its same-commit delete-then-re-add.
- **Row-id precedence.** A file carrying both the field-id column and a
  non-NULL `row_id_start`, with gaps in its embedded ids (the flushed
  shape), derives entries under the embedded ids — a golden fixture pins
  the precedence. A per-row-id catalog row over a file lacking the column
  fails `Corruption`; ordinal mode refuses a file that carries it.
- **Rewrite delete side.** A delete file targeting a per-row-id file
  removes exactly the named positions' entries; an inline file-delete
  against one removes exactly the named row's. Backfill over a table
  holding per-row-id files derives their entries under embedded ids.
  Embedding: `register_delete_file` removals name rows by id against any
  target — verbatim against a per-row-id file, range-checked against a
  dense one; an id outside a dense target's range refuses with
  `Constraint`.
- **SQL-path uniqueness.** A staged registration whose file duplicates an
  existing unique value aborts with `Constraint`, message free of the four
  retry substrings — no retry storm; a non-duplicate registration lands and
  the next colliding one aborts.
- **Function surface.** The four registered functions resolve the store
  handle and `table_id` from an attached catalog and drive the same core
  verbs as the embedding API; `moraine_create_index` backfills existing
  external files via the scoped read; a lookup joins back to the table under
  merge-on-read.
- **E2E.** DuckLake flows over non-indexed tables are unaffected end to end;
  a `CALL moraine_create_index` then a bulk `INSERT` that duplicates a
  unique value fails the `INSERT` without a retry storm (message-text
  contract), and one that does not duplicate lands and is found by
  `moraine_index_lookup`. Over an indexed table: `UPDATE`, then
  `rewrite_data_files`, then `merge_adjacent_files` (one adjacent merge,
  one not — dense and per-row-id outputs) all commit; lookups stay
  correct after each; a `DELETE` against the rewritten file removes its
  entries; `moraine_create_index` on a table already holding rewrite
  files backfills them.

## Open questions

- **Oversized-value cap.** The refusal threshold for indexed value size
  (strawman: 1 KiB per composite key). Hash-overflow schemes are the
  recorded escape if a real workload needs large indexed values.
- **Backfill batch sizing.** The staged build's batch bound and pacing
  knobs — and whether delete-file position→row-id resolution during the
  exclusion scan (Staged builds) is cheap enough inline or wants a small
  per-file cache for files that received deletes mid-build.
- **Reclamation mechanics.** Whether the pinned SlateDB exposes (or grows) a
  range-delete usable for orphaned entry ranges, versus the batched sweep;
  and the sweep's scheduling knobs.
- **Transparent pushdown.** Whether to carry the DuckLake binder patch
  (Extension path, Future directions) that accepts `CREATE INDEX`/`PRIMARY
  KEY` and routes equality pushdown to the index. Nothing in the layout
  precludes it; nothing here promises it.
- **Deferred maintenance under SQL writes.** The same-commit scoped read adds
  latency proportional to the registered file's indexed columns. A deferred
  (post-commit) mode could shed it for non-unique indexes at the cost of an
  under-coverage window. Unique indexes cannot defer: enforcement *is* the
  commit.
- **Range upgrade.** If equality proves insufficient, contracting order for
  the already-order-compatible encodings — which would reopen the type-aware
  comparison questions RFC 0002 warns about, deliberately dodged here.

## Alternatives considered

- **Temporally versioned entries (`begin`/`end`, current/history-style).**
  Buys index-accelerated time travel at the cost of read-modify-move on every
  row death, entry history joining RFC 0007's GC surface, and a uniqueness
  check that filters dead versions instead of point-getting. A permanent
  hot-path tax to accelerate the path the keyspace treats as rare. Rejected.
- **Derive-at-attach hybrid** (persist external-file entries only, rebuild
  inline entries at attach). Attach cost grows with data, every lookup spans
  two structures, and the uniqueness check stops being a point-get. Rejected.
- **A reverse map (`index_id, row_id → key values`)** to make row-id-only
  deletes self-sufficient. Doubles entry storage and write amplification to
  spare deleting writers values they already read. Rejected; the
  writer-supplied contract keeps delete parity with registration.
- **Location payloads (file id / chunk key) instead of row ids.** One step
  shorter lookups; flush and compaction become index rewrites proportional
  to moved rows. Row lineage is the stabler identity. Rejected.
- **Hashed fixed-width entry keys.** Uniform key size, no cap — but
  collisions need verify-against-payload machinery and the order-compatible
  upgrade path dies. Rejected for v1; recorded as the oversized-value escape.
- **Mark-stale on the extension write path** (a DuckLake write flips the
  index to a `stale` flag). A stale unique index enforces nothing and says so
  only to callers who ask — silent degradation of the exact guarantee the
  feature exists to give. Rejected for the scoped read, which keeps DuckLake
  flows working *and* the index honest; stale-mode survives only as the
  deferred-non-unique open question, where correctness is not at stake.
- **Refusing the extension write path outright** (this RFC's first stance).
  Reading a registered file's indexed columns is a bounded, merge-free
  projection — not the scan path the non-goal guards — so blanket refusal
  traded a real capability for a boundary that did not need defending.
  Superseded by the scoped read (Extension path).
- **Modeling the index as a pseudo-DuckLake table** (an invented
  `ducklake_index` served row-faithfully). Invents non-spec surface real
  DuckLake would never read and future versions could collide with.
  Native-and-invisible is the right shape. Rejected.
- **Uniqueness by scan at commit** (no persistent index; check by scanning
  the table's rows). O(data) per commit against the index's one point-get.
  The scoped read stays bounded to the one registered file precisely
  because the index covers the rest; dropping the index turns that into a
  whole-table scan. Rejected.
- **Staged builds in multi form, converted at ready** (build unique entries
  as `(value, row_id)` keys — faithful under duplicates — then validate and
  rewrite to value-only keys at the flip). Represents duplicates without
  poisoning, but the conversion is an O(entries) rewrite that itself needs
  staging, and the window where writers must maintain both key forms is a
  second protocol. Value-keyed-from-day-one plus the poison flag gets the
  same outcome with the machinery already on the page. Rejected.
- **Blocking table writes for the build's duration.** Turns the build into
  one logical commit and deletes every race — by serializing a table for
  hours precisely when it is largest. The append-append Goal exists to
  forbid this shape. Rejected.
- **Stale entries adjudicated at read** (skip the delete-race machinery;
  let lookups filter dead rows like a scan does). Works for row location —
  candidates are already adjudicated by the consumer — but a unique index
  cannot carry stale entries: a dead row's entry manufactures false
  `Constraint`s at commit, where there is no adjudication step. Rejected.

## Prior art: HelixDB on SlateDB

HelixDB (an OLTP graph-vector database) runs on the same substrate — stock
SlateDB over object storage — making its index surface the closest available
comparison. The engine is closed; what follows is read from its public Rust
SDK (`helix-db` 2.0.6: `IndexSpec`, `SourcePredicate`) and its SlateDB fork,
so the *contract* is observed and the on-disk key layout is not.

**Near-stock SlateDB was enough.** `HelixDB/slatedb` carries a handful of
commits over upstream (reader snapshots, a multi-get batching point-gets) —
no index primitives: no range-delete, no merge operator. The whole index
family, uniqueness included, rides the same `get` / `WriteBatch` /
prefix-scan surface this RFC assumes. A production index engine living
without range-delete answers Reclamation's open question: the batched sweep
is a legitimate permanent design, not a workaround awaiting a SlateDB
feature.

**A wider taxonomy, the same equality core.** `IndexSpec` spans equality,
range (with a physical `Asc`/`Desc` direction), vector, and text. Only
equality carries a `unique` flag — matching the decision here to key
uniqueness on the value alone — and its "uniqueness for supported non-null
values" doc confirms the NULL-exempt semantics, arrived at independently.

**Range is equality plus committed order.** `RangeIndexDirection` bakes the
sort direction into the stored key, not applied at read time — exactly the
upgrade the Encoding section reserves: order-compatible bytes plus a
committed order contract plus a direction bit. Shipped that way, it is
evidence the deferral here is an upgrade path, not a rewrite.

**The pushdown boundary is drawn in the same place.** HelixDB splits a
restricted `SourcePredicate` (`Eq`/`Neq`/`Gt`/`Between`/`StartsWith`/`And`/
`Or`, used at source selection) from a general `Predicate` run as a scan-time
filter: the index serves source selection, everything else filters during the
scan. This RFC draws the same line one notch tighter — no range, so no
`Between`/`StartsWith` on the fast path.

**Where it diverges — and why.** HelixDB manages indexes as runtime
control-plane steps (`create_index_if_not_exists`) against a server that
holds and authored every row; entries are the engine's private business.
moraine does not own the write path — a separate writer produces the Parquet
— and that one fact drives both coverage models: the embedding writer
supplies entries alongside the file it wrote (Coverage); DuckLake supplies
nothing, so moraine derives them by the scoped read. HelixDB never
meets the register-with-entries problem. The divergence is a property of the
embedding, not a different answer to the same question.

**Why equality-only, when HelixDB shows the substrate carries vector and
text too.** HelixDB models both over the same ordered KV surface: its HNSW
persists vectors under `(id, level)` keys and the proximity graph as
adjacency keyed `(source, level, sink)`; its BM25 is an inverted index plus
length/frequency tables. (Layouts read from its earlier LMDB engine; the
modeling ports, since both are ordered KV stores with prefix scans.) So
SlateDB could store these indexes; this RFC excludes them because of what
they cost to *read*. An equality lookup is one point-get or one bounded
prefix scan — fast even on a cold cache. An HNSW search is dozens of
*dependent* point-gets down a graph, and on an LSM over object storage every
hop that misses the block cache is an object-storage round trip: fast only
while the whole graph stays cache-resident, seconds when it does not.
Equality (and the range upgrade) fits RFC 0009's read model; vector search
does not, and that is the entire reason it is out. The boundary is read
cost, not storability.
