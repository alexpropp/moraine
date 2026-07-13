# RFC 0017: Equality indexes

- **Date:** 2026-07-10

## Summary

Adds a moraine-native **equality index**: a catalog object (`create_index` /
`drop_index` on the [RFC 0003](0003-public-api-shape.md) verb surface) whose
entries live in a new `idx` subspace and serve two reads — **row location**
(given key values, find the row ids and the files/chunks that hold them) and
**uniqueness enforcement** at commit time. DuckLake v1.0 models no indexes, so
this is a native feature in the [RFC 0016](0016-recent-row-archive.md) mold:
real inside moraine, invisible to every DuckLake catalog scan. The RFC's
weight is on the mechanism. Entries are **live-only** (no temporal
versioning) and point at **row ids**, which DuckLake preserves across flush,
update-rewrites, and compaction — so data movement never touches the index.
Uniqueness rides SlateDB's write-write conflict detection: a unique entry's
store key *is* the key value, so two racing commits inserting the same value
collide mechanically and the loser resolves to a typed `Constraint` under
[RFC 0004](0004-commit-protocol.md)'s ordinary retry. Coverage spans inlined
and external rows: rows moraine holds are indexed automatically; rows in
externally written Parquet are indexed by **writer-supplied entries** at
registration, the same division of labor as file column stats.

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
- **Serving DuckLake's planner.** No `ducklake_*` surface changes; the
  extension path's stance is refusal, not integration (see Extension path).
- **Multi-commit index builds.** v1 creates an index in one commit; a staged
  building→ready protocol for backfills too large for one batch is deferred
  (Open questions).
- **Approximate structures** (bloom filters, zone maps) — they cannot carry
  uniqueness; RFC 0013's pruning stance already covers the skipping use case.

## Background

DuckLake v1.0 has no index tables and its catalog contract knows nothing of
indexes, so an index cannot be smuggled in as another row-faithful
`ducklake_*` mapping — it is moraine inventing a capability, and the honest
shape for that is the one RFC 0016 established: a native feature served by
the embedding API, stored where no catalog scan (current or time-traveling)
can see it.

Three established facts carry most of this design:

- **Row ids are stable identity.** DuckLake allocates row ids per table
  (`next_row_id` in `tstat`, RFC 0004) and preserves them across inline
  flush, UPDATE's delete-and-rewrite, and compaction — row lineage. Every
  data file records its row-id range, and RFC 0005's inlined chunks carry
  `row_id_start`/`row_count`. A row id therefore resolves to its current
  location(s) from the materialized catalog snapshot alone.
- **moraine never reads Parquet** (RFC 0006 non-goal) — data files *and*
  delete files. It sees row contents at exactly two moments: `inline_insert`
  (rows arrive through the API) and flush (moraine itself writes the
  Parquet). Everything else is a file it will never open, so index entries
  for external rows must be supplied by whoever has the rows in hand — the
  writer — exactly as file column stats already are.
- **Write-write conflict detection is the store's only race primitive**
  (RFC 0004): no read-write detection, no key CAS. A design that needs
  "detect that someone else inserted this value" must arrange for the race
  to be a write-write collision on one key.

## Design

### The index definition — a catalog entity

A new `index` kind in `cur`/`hist`, keyed `(table_id, index_id)` with
`index_id` allocated from the global `next_catalog_id`. The value carries the
index name, the **ordered list of indexed columns referenced by field id**
(never by name — the RFC 0012/0013 rule, so renames are free), the unique
flag, and `begin_snapshot`. The *definition* is temporally versioned like any
entity — time travel reconstructs which indexes existed at snapshot `S` —
even though entries (below) are not.

Verbs on `Transaction`:

| Verb | Effect |
|---|---|
| `create_index(table, name, columns, unique)` | Insert the definition; build entries for every live row (see Coverage) in the same commit. |
| `drop_index(index)` | End the definition into `hist`. Entries are orphaned and reclaimed lazily (see Reclamation). |

`CatalogSnapshot` gains `indexes_of(table)`; domain types gain `IndexId` and
`IndexInfo`.

**Commit classification.** Index DDL is a table-grain mutation recorded in
`changes_made` as `altered_table:<table_id>` — DuckLake's grammar has no
index kind, and its parser throws on unknown kinds (RFC 0004), so the entry
must be spelled in vocabulary DuckLake parses. `altered_table` is also the
*correct* classification, not just a parseable one: an alter is a true
conflict against concurrent inserts, deletes, and other alters on the table,
in both directions — exactly what index DDL needs. A `create_index` racing a
`register_data_file` must not win mechanically: the create's backfill was
staged against the old file set, so the race aborts to the caller as
`CommitConflict`, who re-drives with fresh backfill. For coherence with the
emitted grammar (in DuckLake's own classification every alter bumps), index
DDL sets the `schema_changed` flag; the cost is one spurious DuckDB
schema-cache refresh per index DDL, which is rare by nature.

**Column DDL on indexed columns.** `drop_column` and `alter_column` (type
change) on a column a live index references fail with `Constraint` — the
canonical encoding is type-bound, and a silent cascade would discard an
object the host created deliberately. The host drops the index first.
`rename_column` is unaffected (field-id references).

### The `idx` subspace and entry keys

A new subspace, one leading discriminant byte appended to the `Key` enum —
which makes it a SlateDB segment of its own (RFC 0002), so entry churn from
a hot indexed table compacts independently of the metadata subspaces, the
same isolation `inline` gets. Golden vectors pin the discriminant as they
pin every other kind.

| Kind | Key components | Value |
|---|---|---|
| `idx/unique` | `index_id, canon(key values)` | `row_id` |
| `idx/multi` | `index_id, canon(key values), row_id` | (empty) |

Two shapes, one deliberate asymmetry:

- **Unique entries key on the value alone.** The store key *is* the claim of
  uniqueness. Two commits inserting the same key value — the race no
  read-side check can close, because SlateDB detects write-write conflicts
  only — write the *same key* and collide in the store's own conflict
  detection. The loser retries per RFC 0004, re-runs its closure, now sees
  the winner's entry, and returns `Constraint`. Uniqueness under concurrency
  is correct by construction, with zero new race machinery.
- **Non-unique entries append the row id**, so distinct rows sharing a value
  occupy distinct keys. Concurrent appends of different rows to one indexed
  table therefore write disjoint entry keys and remain **benign** under the
  append-append refinement — indexing a table does not serialize its
  writers. A lookup for value `v` is the prefix scan
  `idx/multi/{index_id}/{canon(v)}/`, ascending, per RFC 0002's
  forward-only rule.

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
with `Constraint` at insert/registration time — huge keys degrade the whole
segment, and equality over megabyte values is not this feature's job.
Hashing oversized values into fixed-width keys is rejected for v1 (Open
questions records the threshold choice).

### Coverage — who writes entries, and when

Coverage is total over live rows, maintained at the three moments rows
enter, and the two moments they die:

| Moment | Who has the rows | Entry source |
|---|---|---|
| `inline_insert` | moraine (rows arrive through the API) | computed by moraine, staged in the same batch |
| flush (RFC 0005) | moraine (it writes the Parquet) | **nothing to do** — entries point at row ids, which flush preserves |
| `register_data_file` | the writer | **writer-supplied**: `(row ordinal, key values)` pairs alongside the file, mapped to row ids at commit (`row_id_start + ordinal`), like the file column stats the writer already computes |
| `inline_delete` of a store-resident row | moraine (the chunk is in the store) | moraine recomputes the key values from the chunk and stages the entry delete |
| `register_delete_file`, or `inline_delete` against flushed rows | the writer (it read the rows to produce positions) | **writer-supplied**: `(row_id, key values)` per deleted row; moraine deletes the named entries |

The rule generalizing the last two: **any operation that kills rows moraine
cannot read from the store must name their indexed key values** — entries
are keyed by value, and moraine cannot derive a value from a row id it
cannot dereference. Deletes of store-resident rows stay self-sufficient.

**Registration without entries is refused.** A `register_data_file` or
`register_delete_file` against an indexed table that omits the required
entries fails with `Constraint`. This is the reject arm of reject-vs-degrade,
chosen because a silently under-covered unique index is a lie; a
mark-index-stale degrade mode is recorded in Alternatives.

An UPDATE on the verb path (expire + register preserving row ids) composes:
the delete side removes the old entries (writer-supplied values), the insert
side adds the new — same commit, same batch, uniqueness checked against the
post-image.

**Backfill at `create_index`.** Rows moraine holds (inlined, any schema
version) are backfilled by scanning the chunks. Rows in external files need
writer-supplied backfill entries passed to `create_index`; the whole build is
one commit, one batch, so backfill size is bounded by what the host accepts
in one batch. Uniqueness is validated over the assembled entry set before
staging; a duplicate aborts the create. Tables too large for one-batch
backfill wait for the staged-build protocol (Open questions).

### Uniqueness enforcement

At commit, for each new unique entry the committer point-gets the entry key
through the transaction handle (RFC 0004 step 1's read discipline): present
→ `Constraint`, absent → stage the put. Duplicates *within* the commit are
checked in memory. The race window between the get and a concurrent commit's
identical put is closed by the key collision described above — that is the
load-bearing property of keying unique entries on the value alone.

Because entries are live-only, "present" means "a live row holds this
value": deletes remove entries in the same batch that kills the row, so
delete-then-reinsert of a value inside one commit or across commits behaves
as SQL expects.

The `Constraint` error here is a verb-path error (embedding API) — no
DuckLake wire contract applies to its text.

### What data movement costs the index: nothing

The entry payload is a row id, not a location. Flush re-homes rows from
chunks to Parquet; compaction and UPDATE rewrites re-home them across files;
row ids survive all three by DuckLake's own lineage rules. Resolution from
row id to current location happens at read time against the materialized
snapshot: chunks and files both declare row-id ranges, so the lookup path
(next section) finds the holder by interval — in memory, at catalog scale.
No maintenance operation rewrites an entry. This is why live-only entries
plus row-id payloads is the whole design: every alternative payload
(file id, chunk key) turns flush and compaction into index rewrites
proportional to moved rows.

### Lookups

One accessor family on `CatalogSnapshot`, served under the snapshot's pinned
read handle (the RFC 0016 pattern — the snapshot's in-memory accessors stay
I/O-free; this family, like `recent_rows`, reads the store through the pin,
so the lookup and the catalog it points into are one consistent cut):

- `index_lookup(table, index, key_values) -> Vec<RowLocation>` — point-get
  (unique) or prefix scan (non-unique) in `idx`, then resolve each row id
  against the snapshot's chunk and file ranges. A `RowLocation` names the
  row id and its holder: an inlined chunk (the row is store-resident and can
  be materialized via the RFC 0016 machinery) or candidate data file(s) —
  files whose live row-id range contains the id. The consumer applies delete
  files as any DuckLake scan does; moraine does not read them, so it returns
  candidates, not adjudicated rows.

Lookups are **head-only**: entries are live-only, so `snapshot_at(S)` has no
index to consult and the accessor fails there with a typed error. Time
travel falls back to what it always was — a scan problem. This is the
deliberate trade of the live-only model: the hot path (current head) gets
the index; the rare path pays nothing to keep it honest.

### Extension path: refusal

DuckLake authors staged-row commits and will never supply index entries, so
a staged-row commit that touches an indexed table is **refused** with a
typed error before staging. Indexes are an embedding-API feature per table:
creating one commits that table to entry-supplying writers. Two deliberate
details:

- The refusal's message text must avoid DuckLake's four retry substrings
  (`"conflict"`, `"concurrent"`, `"unique"`, `"primary key"` — RFC 0006):
  this is a permanent condition, and text that pattern-matches retryable
  would put DuckLake's `RunCommitLoop` into a futile bounded retry before
  surfacing.
- Read-only DuckDB attaches over an indexed store are unaffected — refusal
  gates the *write* path only. (Whether a read-only scan could someday
  consult the index for filter pushdown is an open question, not a promise.)

### Format gate

An older moraine writer committing to an indexed table would maintain no
entries — silent index corruption, the worst failure mode available. The
gate is `sys/format`, bumped **lazily**: creating the *first* index in a
store writes format 2 in the same commit; older binaries refuse to open a
format-2 store (RFC 0002 bootstrap validation, already enforced from format
v1). Stores that never create an index stay format 1, byte-identical to
today and fully compatible in both directions. Format 2 is format 1 plus the
`idx` subspace and `index` kind — no migration, no rewrite; dropping the
last index does not downgrade the stamp. Appending the `idx` discriminant
does not disturb the segment extractor (it remains "first byte"), so
existing segments and the RFC 0011 crash matrix are untouched.

### Reclamation

`drop_index` (and `drop_table`, which ends the table's index definitions
with it) orphans the index's entry range. Entries are invisible to every
read path the moment the definition ends, so reclamation is pure space
hygiene: a bounded background sweep deletes the `idx/{index_id}` range in
batches, riding the same maintenance posture as RFC 0007's expiry — never
inside the dropping commit, whose batch must stay bounded. If SlateDB grows
a range-delete primitive, the sweep collapses into one call (Open
questions).

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
  refuses the store; index-free stores remain format 1.
- **Head-only lookups.** `index_lookup` on `snapshot_at(S)` fails typed;
  on a pinned current snapshot it reflects that snapshot's cut even as
  newer commits land.
- **E2E.** DuckLake flows over non-indexed tables are unaffected end to end;
  a staged-row write to an indexed table surfaces DuckLake's error without a
  retry storm (message-text contract).

## Open questions

- **Oversized-value cap.** The refusal threshold for indexed value size
  (strawman: 1 KiB per composite key). Hash-overflow schemes are the
  recorded escape if a real workload needs large indexed values.
- **Staged build protocol.** A building→ready lifecycle for backfills too
  large for one batch: definition lands in `building` state, entries stream
  in follow-up commits, uniqueness validated before `ready` flips. The
  definition value is protobuf, so adding the state field later is additive.
- **Reclamation mechanics.** Whether the pinned SlateDB exposes (or grows) a
  range-delete usable for orphaned entry ranges, versus the batched sweep;
  and the sweep's scheduling knobs.
- **Read-only pushdown.** Whether a read-only DuckDB attach should ever
  serve filter pushdown from an index (RFC 0006's pushdown open question,
  extended). Nothing in the layout precludes it; nothing here promises it.
- **Range upgrade.** If equality proves insufficient, contracting order for
  the already-order-compatible encodings — which would reopen the type-aware
  comparison questions RFC 0002 warns about, deliberately dodged here.

## Alternatives considered

- **Temporally versioned entries (`begin`/`end`, cur/hist-style).** Buys
  index-accelerated time travel at the cost of read-modify-move on every row
  death, entry history joining RFC 0007's GC surface, and a uniqueness check
  that must filter dead versions instead of point-getting. Pays a permanent
  tax on the hot path to accelerate the path the whole keyspace design
  treats as rare. Rejected.
- **Derive-at-attach hybrid** (persist external-file entries only, rebuild
  inline entries from chunks at attach). Minimal stored state, but attach
  cost grows with data, every lookup spans two structures, and the
  uniqueness check stops being a point-get. Rejected.
- **A reverse map (`index_id, row_id → key values`)** to make row-id-only
  deletes self-sufficient. Doubles entry storage and write amplification on
  every insert to spare deleting writers from supplying values they already
  read. The writer-supplied contract keeps delete parity with the
  registration contract instead. Rejected.
- **Location payloads (file id / chunk key) instead of row ids.** Makes
  lookups one step shorter and flush/compaction into index rewrites
  proportional to moved rows. Row lineage is the stabler identity. Rejected.
- **Hashed fixed-width entry keys.** Uniform key size and no cap, but
  collisions need verify-against-payload machinery, and the order-compatible
  upgrade path dies. Rejected for v1; recorded as the oversized-value
  escape.
- **Mark-stale instead of refuse** (extension-path writes and entry-less
  registrations flip the index to a `stale` flag). Keeps DuckLake flows
  working on indexed tables, but a stale unique index enforces nothing and
  says so only to callers who ask — silent degradation of the exact
  guarantee the feature exists to give. Refusal is honest. Rejected, and
  revisitable only alongside the staged-build protocol (rebuild = re-cover).
- **Modeling the index as a pseudo-DuckLake table** (an invented
  `ducklake_index` served row-faithfully). Invents non-spec DuckLake surface
  that real DuckLake would never read and future DuckLake versions could
  collide with. Native-and-invisible is the RFC 0016 precedent for exactly
  this situation. Rejected.
- **Uniqueness by scan at commit** (no persistent index; check by scanning
  the table's rows). O(data) per commit and impossible for external Parquet
  moraine cannot read. Rejected.

## Prior art: HelixDB on SlateDB

HelixDB (an OLTP graph-vector database) runs on the same substrate this store
does — stock SlateDB over object storage — which makes its index surface the
closest available comparison. The engine is closed; what follows is read from
its public Rust SDK (`helix-db` 2.0.6: the `IndexSpec` and `SourcePredicate`
types) and its SlateDB fork, not the storage code — so the *contract* is
observed and the on-disk *key layout* is not.

**Stock SlateDB was enough.** `HelixDB/slatedb` is a zero-commit mirror of
upstream: no engine-specific primitives added. HelixDB built its whole index
family, uniqueness included, on the same `get` / `WriteBatch` / prefix-scan
surface this RFC assumes — no range-delete, no merge operator. That is direct
evidence for the Reclamation open question: a batched sweep is the expected
path, not a stopgap waiting on a SlateDB feature that a peer already proved
unnecessary.

**A wider taxonomy, the same equality core.** HelixDB's `IndexSpec` spans
equality, range (with a physical `Asc`/`Desc` direction), vector, and text,
each on both nodes and edges; vector and text carry a multitenant
`tenant_property` partition. Only equality carries a `unique` flag, and only on
nodes — matching this RFC's decision to make uniqueness an equality-index
property whose entry keys on the value alone. Its doc ("uniqueness for
supported non-null values") confirms the NULL-exempt semantics chosen here,
arrived at independently.

**Range is equality plus committed order.** `RangeIndexDirection` is documented
as "physical ordering for range-index storage" — the sort direction is baked
into the stored key, not applied at read time. That is exactly the upgrade the
Encoding section reserves: order-compatible canonical bytes, plus a committed
order contract, plus a direction bit. HelixDB having shipped it that way is
evidence the deferral here is a genuine upgrade path, not a rewrite.

**The pushdown boundary is drawn in the same place.** HelixDB splits a
restricted `SourcePredicate` (the index-friendly subset used at source
selection: `Eq`/`Neq`/`Gt`/`Between`/`StartsWith`/`And`/`Or`) from a general
`Predicate` (arbitrary comparisons, run as a scan-time filter). An index serves
source selection; everything else filters during the scan. This RFC's
equality-only contract with `.where_` fallback is the same line drawn one notch
tighter — no range, so no `Between`/`StartsWith` on the fast path.

**Where it diverges — and why.** HelixDB manages indexes as runtime
control-plane steps (`create_index_if_not_exists`) against a server that holds
the whole graph; entries are the engine's private business. This store owns no
copy of externally written Parquet and cannot read data files, so index DDL is
a commit verb whose entries ride the same `WriteBatch` as the mutation, and
external coverage is a writer-supplied contract (Coverage). HelixDB owns all
its data, so it never meets the register-with-entries problem the Coverage
section exists to solve. The divergence is a property of the embedding, not a
different answer to the same question.

**Vector and text indexes are also just KV entries — with a cost this one
does not pay.** HelixDB's `IndexSpec` also spans vector and text, and both are
modeled as compound-key records over the same ordered KV surface, no engine
primitive beyond `get`/scan/batch. Its from-scratch HNSW persists vectors under
a `(id, level)` key and the proximity graph as a separate adjacency subspace
keyed `(source, level, sink)`; a search is a greedy graph walk expressed as a
chain of small dependent point-gets. Its from-scratch BM25 is an inverted index
(`term -> postings`) plus length/frequency tables, each its own KV database.
(These layouts are read from HelixDB's earlier LMDB-backed engine; the v2 code
on SlateDB is closed, but the modeling ports directly because both are ordered
KV stores with prefix scans.) The lesson for this RFC's equality-only contract
is not that these are hard to *model* — they are the same key-plus-value
discipline the `idx` subspace already uses — but that they change the *read
profile*: an equality lookup is one point-get or one bounded prefix scan, cache
size irrelevant; an HNSW walk is dozens of dependent point-gets down a graph,
where on an LSM over object storage every hop that misses the block cache
becomes an object-storage round trip. Equality (and the deferred range upgrade)
stays inside the read model RFC 0009's caching already serves; vector search
would import a working-set-resident-or-slow tax the whole live-only, point-get
design was shaped to avoid. That the substrate would carry them is exactly why
the boundary is drawn by read cost, not by what SlateDB can store.
