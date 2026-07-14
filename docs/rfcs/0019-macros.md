# RFC 0019: Scalar and table macros

- **Date:** 2026-07-13

## Summary

DuckLake persists `CREATE MACRO` (scalar and table, with parameters,
defaults, and arity overloads) across three catalog tables:
`ducklake_macro` (the versioned identity row), `ducklake_macro_impl` (one
row per overload), and `ducklake_macro_parameters` (one row per parameter
per overload). moraine models all three as **one versioned `macro` kind**
keyed by `macro_id` — an exact sibling of `view` — with the impl and
parameter rows embedded in the value. Creation folds the child-table
inserts under their parent row in the staged batch; `DROP MACRO` is the
generic end-snapshot update; `CREATE OR REPLACE` is upstream's own
drop-plus-create under a fresh `macro_id`, so no new lifecycle machinery
exists. The verb API gains `create_macro`/`drop_macro`, and conflict
classification learns the four macro tokens DuckLake writes into
`changes_made`.

## Goals

- **The full DuckLake macro SQL surface**, verified live: scalar and table
  macros, multiple arity overloads per macro, typed and untyped parameters,
  parameter defaults, `CREATE OR REPLACE`, `DROP`, `DROP SCHEMA CASCADE`,
  and time travel to a snapshot where a since-dropped macro was live.
- **Row-faithful storage**: every column of the three tables maps to
  exactly one stored field; moraine never parses or interprets the macro
  body SQL.
- **The ordering obligation met by construction.** DuckLake reconstructs
  macros with correlated `LIST({...})` subqueries that have **no
  `ORDER BY`** — it consumes impl rows in `impl_id` order and parameter
  rows in `column_id` order as served. Embedding preserves insertion order,
  so the projections satisfy this without sort logic.
- **Verb-path parity with views** (RFC 0003): macros are genuine catalog
  DDL, so the embedding API can express them.

Non-goals:

- **Validating macro semantics.** Body SQL, parameter types, and default
  values are stored and served verbatim; parsing them back into functions
  is DuckLake's reader's job.
- **Physical deletion of orphaned impl/parameter rows.** Upstream sweeps
  them during snapshot expiry (a referential check against live
  `ducklake_macro` rows). moraine's embedding makes the sweep implicit —
  children go when their record goes — which is RFC 0007's concern.

## Background: the DuckLake contract

Verified against the tracked DuckLake sources (`target/ducklake-src`).

- **Writes.** `CREATE MACRO` inserts one `ducklake_macro(schema_id,
  macro_id, macro_name, begin_snapshot, end_snapshot=NULL)` row, one
  `ducklake_macro_impl(macro_id, impl_id, dialect, sql, type)` row per
  overload, and one `ducklake_macro_parameters(macro_id, impl_id,
  column_id, parameter_name, parameter_type, default_value,
  default_value_type)` row per parameter — positional VALUES, table
  definition order, one staged batch.
- **Id allocation.** `macro_id` draws from the snapshot's
  `next_catalog_id`, the counter shared with schemas/tables/views
  (`ducklake_transaction_state.cpp:450`). `impl_id` is the 0-based position
  of the overload within the statement; `column_id` is the 0-based position
  of the parameter within its overload. Neither child id is global.
- **Literals.** `type` is `'scalar'` or `'table'`, uniform across one
  macro's impls — the reader takes `implementations.front().type` for the
  whole macro. `dialect` is always `'duckdb'`. `sql` is DuckDB's canonical
  re-rendering of the body (expression or `SELECT` statement), not the
  user's original text. An untyped parameter stores `parameter_type =
  'unknown'`; an absent default stores `default_value = NULL` with
  `default_value_type = 'unknown'`.
- **Lifecycle.** `DROP MACRO` (and `DROP SCHEMA CASCADE`, which end-stamps
  each contained macro) issues only
  `UPDATE ducklake_macro SET end_snapshot = <id> WHERE end_snapshot IS NULL
  AND macro_id IN (...)`. Impl and parameter rows are never updated and are
  not touched at drop — upstream leaves them orphaned until snapshot expiry
  sweeps rows whose `macro_id` no longer exists. `CREATE OR REPLACE` is a
  drop plus a create under a **new** `macro_id` (impl ids restart at 0).
- **Reads.** Macros load inside the per-snapshot catalog query: visibility
  is filtered on `ducklake_macro.begin/end_snapshot` only (time travel
  applies), and each visible macro pulls its impls and parameters through
  correlated `LIST({...})` subqueries with no `ORDER BY` — served row order
  *is* the reconstruction order. Ended macros' impl rows must therefore
  remain servable for as long as any snapshot can see the macro.
- **`changes_made` tokens:** `created_scalar_macro:"schema"."name"`,
  `created_table_macro:"schema"."name"`, `dropped_scalar_macro:<id>`,
  `dropped_table_macro:<id>`.

## Design

### Keyspace and value

One new versioned kind, keyed like `view` by its DuckLake-allocated id
(RFC 0002's map gains its row; `history` mirroring applies):

| Kind | Key components | DuckLake table(s) |
|---|---|---|
| `macro` | `macro_id` | `ducklake_macro` (+ `ducklake_macro_impl`, `ducklake_macro_parameters` embedded) |

```proto
MacroValue          { macro_id, begin_snapshot, optional end_snapshot,
                      schema_id, macro_name, repeated MacroImplementation }
MacroImplementation { impl_id, dialect, sql, macro_type,
                      repeated MacroParameter }
MacroParameter      { column_id, parameter_name, parameter_type,
                      optional default_value, default_value_type }
```

`macro_type` stays per-implementation because that is where the table
carries it; uniformity across one macro's impls is validated at write
(load-bearing for the reader), but the literal set is not — a future
DuckLake macro type must not be rejected here. Impl and parameter rows
embed because they have no independent lifecycle: never updated, dropped
only with (or, upstream, after) their macro.

The embedded children ride the version lifecycle for free: ending a macro
mirrors the whole record — children included — into `history`, so the
impl/parameter projections keep serving rows for macros that time travel
can still see, which is exactly the upstream orphans-until-expiry
semantics without the orphans.

### Staged write: fold and lifecycle

`ducklake_macro_impl` and `ducklake_macro_parameters` are fold-only staged
tables: a pre-pass drains their insert rows into groups keyed by
`macro_id` (parameters sub-grouped by `impl_id`); each `ducklake_macro`
insert consumes its group into one `MacroValue`. Validated, not trusted:

- a macro insert with zero impl rows (invisible to its own reader);
- impl or parameter groups left unconsumed (orphans);
- non-contiguous 0-based `impl_id`s, or non-contiguous 0-based
  `column_id`s within an impl (upstream assigns positions; the reader
  consumes positionally, so gaps or duplicates would misalign
  reconstruction);
- mixed `macro_type` across one macro's impls.

`DROP MACRO` needs nothing new: the shim's generic
`UPDATE ... SET end_snapshot` path ends the record, and the diff engine
mirrors it to `history`. Replace arrives as that update plus a fresh insert
in the same batch, already ordered ends-before-inserts.

### Verbs and change tracking

The verb API (RFC 0003's table) gains:

- `create_macro(schema_id, name, implementations) -> MacroId` — allocates
  from the catalog id counter; the name must be free **among live macros in
  the schema only** (DuckDB catalogs functions separately from relations,
  so macros do not join the table/view namespace check);
- `drop_macro(macro_id)` — ends the live version.

`DROP SCHEMA` on the verb path keeps requiring an empty schema for
relations and now macros alike; the cascade is DuckLake's (it end-stamps
each macro explicitly in the same commit, which the staged path already
expresses).

Operations and `ChangeSet` learn `CreateMacro`/`DropMacro` and the four
tokens above, classified as schema-changing like views. Today those tokens
parse as *unknown*, which is safe but maximally conservative in conflict
classification; recognition makes a concurrent macro create/drop classify
precisely instead of conflicting with everything.

### Reads

Three dump projections: `ducklake_macro` (one row per record, current and
history), `ducklake_macro_impl` and `ducklake_macro_parameters` (flat-maps
of the embedded children of every record, history included — see above).
The shim's always-empty stand-ins are replaced; the `ducklake_macro` spec
row gains the write kind, end-key (`macro_id`), and end-snapshot column;
the two child tables are insert-only.

### Test obligations

- **Store:** proptest roundtrip for `MacroValue`; golden key vectors for
  the `macro` kind in `current` and `history`.
- **Verbs (integration):** create a scalar macro with overloads and a
  defaulted parameter and a table macro; drop; recreate under a new id;
  time travel resolves `macro_by_id`/`macro_by_name` at each snapshot;
  name collisions rejected among live macros only.
- **Staged:** happy-path fold; one test per constraint above; drop ends the
  record with children intact in `history`.
- **e2e (`ducklake_load.rs`):** `CREATE MACRO` with two overloads and a
  default parameter plus a table macro, called through DuckLake SQL;
  `CREATE OR REPLACE` re-binds; `DROP MACRO` removes it; a time-travel
  attach at the pre-drop snapshot calls the old macro; row-faithful
  `ducklake_macro*` projections verified through a standalone `moraine:`
  attach, impl/parameter rows in served order.

## Alternatives considered

- **Separate `macro_impl`/`macro_parameters` kinds** keyed
  `(macro_id, impl_id[, column_id])`. Rejected: no independent lifecycle
  (RFC 0002's embed criterion), three kinds' worth of keys/codecs/goldens,
  and the read side must reassemble exactly the nesting the embed already
  stores — plus the orphaned-children state upstream tolerates would become
  representable in the keyspace instead of impossible.
- **A macro-level `type` field** (collapse the per-impl column). Rejected:
  not row-faithful — the served `ducklake_macro_impl.type` column would be
  synthesized rather than stored, and a future upstream that varies type
  per impl would be unrepresentable.
- **Reusing the `view` kind with a discriminator** (both are "SQL text
  entities"). Rejected: different tables, different columns, different
  namespaces, different child structure; the shared machinery (versioning,
  end-snapshot updates, diff) is already generic, so the merge would save
  nothing and cost a discriminator in every view read.
- **An `alter_macro` verb.** Rejected: DuckLake models none; replacement
  is drop + create under a new id, and inventing an in-place alteration
  would create version transitions upstream can never produce.
