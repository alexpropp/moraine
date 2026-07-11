# moraine-duckdb

The DuckDB extension surface for [moraine](../moraine). Three layers:
`cpp/` (a C++ shim linking DuckDB's internal C++ API), `src/` (the Rust C-ABI
core, sync↔async bridge), and `vendor/` (the pinned DuckDB header). The shim
carries no DuckLake domain logic — see the crate root docs and RFC 0006.

**Status:** read-only catalog attach with table scans. The cdylib registers
a `duckdb::StorageExtension` under attach type `moraine`
(`ATTACH '<path>' AS m (TYPE moraine)`, standalone attach — `ducklake:`
chaining is a later integration question); schema/table/view listing works
through the C ABI, `SELECT`/`DESCRIBE` on a table scans its live data files
through DuckDB's own `read_parquet` (see "Table scans" below), and every
write path (plus querying a view's definition) raises
`NotImplementedException` (later slices). This document records every build
shape discovery and later tasks pinned, verified against the real DuckDB
v1.5.4 source and a real CLI `LOAD`/`ATTACH`/`SELECT` — not against memory
or documentation.

## Pin

| What | Pinned at |
|---|---|
| DuckDB | **v1.5.4** (git hash `08e34c447b`, codename Variegata) |
| Header source | `libduckdb-src.zip` release asset (the single-file C++ amalgamation), vendored at `vendor/duckdb-v1.5.4/duckdb.hpp` |
| C++ standard | C++17 |
| DuckDB CLI (for `LOAD` testing) | downloaded from the GitHub release, cached under `target/duckdb-cli/` (never committed) |

## Where the header comes from

DuckDB publishes four assets per release relevant here (checked against the
`v1.5.4` GitHub release):

- `libduckdb-src.zip` — the amalgamation: one `duckdb.hpp` (2.0 MB) + one
  `duckdb.cpp` (25 MB) that together *are* DuckDB's C++ internals, plus the
  stable C API headers (`duckdb.h`, `duckdb_extension.h`). This is the
  "vendored header bundle" option and is what we use.
- `libduckdb-{platform}.zip` — prebuilt dylib/staticlib + `duckdb.h` only (no
  C++ internals header) — not usable for `StorageExtension`-style extensions.
- `duckdb_cli-{platform}.zip` — the CLI binary, used only for `LOAD` testing.
- `static-libs-{platform}.zip` — prebuilt static libs, C API only.

We vendor **only `duckdb.hpp`** (not `duckdb.cpp`) under
`vendor/duckdb-v1.5.4/duckdb.hpp` (1.9 MiB on disk). We do **not** compile
`duckdb.cpp` and do **not** link against any DuckDB library — see "Why no
DuckDB library is linked" below. This keeps the build fast (a few seconds)
and avoids the ~15-minute-plus full-DuckDB-from-source path the plan flagged
as an escalation trigger.

**Amalgamation gaps, vendored by hand (Task 4).** `duckdb.hpp` forward-declares
several internal classes it never fully defines — the amalgamation process
only concatenates headers reachable from `duckdb.hpp`'s own top-level
includes, and these live one hop further out. Each was fetched verbatim
(byte-for-byte, only the file's own `#include` line dropped) from the DuckDB
git source at the `v1.5.4` tag and placed alongside `duckdb.hpp` under
`vendor/duckdb-v1.5.4/`, per-file provenance comment at the top of each:

| File | Why it's needed |
|---|---|
| `storage_extension.hpp` | `StorageExtension::Register` — the attach-type registration entry point |
| `database_size.hpp` | `Catalog::GetDatabaseSize`'s pure-virtual return type |
| `create_schema_info.hpp` | Constructing a `SchemaCatalogEntry` |
| `create_table_info.hpp` | Constructing a `TableCatalogEntry` (columns, constraints) |
| `create_view_info.hpp` | Constructing a `ViewCatalogEntry` (name, SQL text) |
| `table_catalog_entry.hpp` | The table entry base class itself |
| `view_catalog_entry.hpp` | The view entry base class itself |
| `thread.hpp` | `thread_id`, named by `ViewCatalogEntry`'s own (unused by us) private `atomic<thread_id>` member — needed for the class to have a complete, ABI-matching layout |
| `not_null_constraint.hpp` | `NotNullConstraint`, attached to the table entry for each catalog column with `nulls_allowed = false` so `DESCRIBE`'s `null` column reflects the catalog's own flag |

Each file is self-contained against types already in `duckdb.hpp` (verified by
grepping the vendored header before adding each one — never trusted from
memory). Kept byte-for-byte identical to upstream rather than trimmed to only
the methods the shim calls: DuckDB's own compiled `duckdb.cpp` (inside the
host process) provides the concrete bodies for every non-pure virtual these
classes declare, resolved at `dlopen` time the same way
`StorageExtension::Register` is; trimming a declaration would silently swap
in `CreateInfo`'s throwing default instead of the host's real implementation
for that vtable slot, without any build-time signal. One class explicitly
*not* vendored: DuckDB's SQL `Parser` (needed to bind a view's defining query)
is not reachable from the amalgamation and pulls in the full parser/tokenizer
— a large, non-self-contained transitive chain, the plan's stated escalation
trigger. Views therefore catalog (name/schema listing, `duckdb_views()`) but
their query is left unparsed; `MoraineViewEntry::GetQuery`/`BindView`
override the base's `return *query;` (which would null-deref) to throw
`NotImplementedException` instead, so `SELECT`ing through a view or
`DESCRIBE`ing it fails loudly rather than crashing — table scans have the
same shape and are Task 5's explicit scope, not a regression here.

## Table scans (Task 5)

`MoraineTableEntry::GetScanFunction` (`cpp/catalog.cpp`) resolves the
table's live data files through `moraine_snapshot_data_files_of` and hands
DuckDB's engine a `TableFunction` (`MoraineScanFunction`, `cpp/scan.hpp` /
`cpp/scan.cpp`) plus fully-populated bind data — no further binding call
happens (confirmed by reading the real
`Binder::Bind(BaseTableRef&)`/`bind_basetableref.cpp`: `table.GetScanFunction`
already returns complete `(TableFunction, bind_data)`, and the caller's
`return_types`/`table_names` come from `table.GetColumns()`, not from
calling the returned function's own `bind` again).

**Path resolution.** The listing ABI carries each data file's `path` and a
`path_is_relative` flag, but moraine's schema/table `path` fields (see
`crates/moraine/src/transaction/verbs.rs`) are never surfaced over the ABI
this slice — only names are. Pragmatic slice rule, implemented in
`MoraineTableEntry::GetScanFunction`:

- `path_is_relative == true`: the file path resolves against
  `<attach_path>/<schema_name>/<table_name>/` (string concatenation, no
  filesystem access — `JoinPath` in `cpp/catalog.cpp`).
- `path_is_relative == false`: `path` is used verbatim.

Verified live against both cases (see "`SELECT` proof" below): a data file
registered with a relative path under the table's directory, and a second
data file on the *same* table registered with an absolute path elsewhere on
disk, both show up in the scan.

**Why scans delegate through SQL text, not a direct `read_parquet` bind.**
The obvious approach — look up the catalog-registered `parquet_scan`/
`read_parquet` `TableFunction` (via `Catalog::GetSystemCatalog(context)
.GetEntry(...)`) and call its own `.bind` callback directly — needs a
`duckdb::TableFunctionBindInput`, whose only constructor takes a
`const duckdb::TableFunctionRef &`. `TableFunctionRef` is a SQL parser AST
node (`duckdb/parser/tableref/table_function_ref.hpp`); constructing (and
later destroying) one requires complete `ParsedExpression`/`SelectStatement`
types, i.e. the same full parser/tokenizer/query-node tree that Task 4
already declined to vendor for view query binding — the plan's own "large,
non-self-contained transitive chain" escalation trigger. `duckdb::Binder`
and `duckdb::ClientContext` are also never fully defined in the amalgamated
`duckdb.hpp` (only forward-declared throughout), which independently rules
out every other internal binding route (e.g. DuckDB's C++ "Relation API",
`Connection::TableFunction`/`Relation::Bind`, needs a `shared_ptr
<ClientContext>` that cannot be obtained from a bare `ClientContext&`
without calling a member function on an incomplete type).

Instead, `MoraineScanInitGlobal` (called once per query *execution*, via
`TableFunction::init_global` — not once per bind/`DESCRIBE`) opens a fresh,
short-lived `duckdb::Connection` to the same `duckdb::DatabaseInstance`
(obtained from `Catalog::GetDatabase()` at `GetScanFunction` time, threaded
through the bind data as a raw pointer) and starts
`SELECT * FROM read_parquet([...])` over the resolved paths as a
*streaming* SQL query — `Connection::SendQuery` with its default
`ALLOW_STREAMING`, never `Query()`, which would materialize the entire
table in memory inside one uninterruptible callback before the first row
left the scan. `duckdb::Connection`/`duckdb::QueryResult` are DuckDB's
public, fully-defined C++ embedding API — no further vendoring needed —
and parse/bind/execute entirely inside the host's own compiled code, so
this shim never touches a parser type itself. `MoraineScanFunctionImpl`
(`TableFunction::function`) then pulls one chunk from the nested query per
call (`.Fetch()`, guarded by the column-count check below) and references
it into DuckDB's output chunk (`DataChunk::Reference`) — a zero-copy
handoff, with the fetched chunk kept alive in the global state for the
reference's lifetime. Pulling per call preserves the outer engine's
per-chunk scheduling cadence: a long scan yields back to DuckDB between
chunks instead of running to completion inside `init_global`. An empty
file list never reaches `read_parquet` at all (which errors on zero
files): `MoraineScanInitGlobal` leaves the nested `Connection`/result
unset, and the function callback reports zero rows unconditionally.

**Recorded upgrade path.** Other storage backends do not use nested SQL:
DuckLake calls the scan function's `.bind` directly with a
default-constructed `TableFunctionRef` (trivial at runtime, but the type
must be complete to compile), and the RDBMS scanners construct their own
`FunctionData` with native readers. Both routes need DuckDB's full
source-tree headers rather than this crate's amalgamated single header.
When the DuckLake-integration slice forces deeper internals anyway,
switch the build to full-tree headers and replace this scan body with
the DuckLake-style direct bind — which also unlocks view query binding.

**Column-count guard (memory safety, enforced).** Before every
`DataChunk::Reference`, the streamed chunk's `ColumnCount()` is checked
against the catalog schema's column count (carried in the bind data) and a
clean `InvalidInputException` naming the table and both counts is thrown on
mismatch. This is not optional hygiene: `DataChunk::Reference`'s own
column-count guard is a debug-only `D_ASSERT`, so a Parquet file with
*more* columns than the catalog declares would be an out-of-bounds vector
write in release builds without it.

**Known limitations, deferred (matches the plan's explicit "no pushdown"
scope for this slice):**

- **Per-column type/order enforcement deferred.** The catalog's mapped
  column types (Task 4's `MapColumnType`) are authoritative for
  `DESCRIBE`; the nested `read_parquet` query's own result schema drives
  the actual data handed back (positionally, via `DataChunk::Reference`)
  with no per-column type or order check against the catalog schema —
  only the column-*count* guard above is enforced. A Parquet file with the
  right column count but mismatched types/order will scan incorrectly (or
  fail deeper in the engine) rather than error cleanly at the scan
  boundary.
- **No snapshot pinning of file paths.** File paths resolve at bind time
  and are read at execution time with nothing holding the files alive in
  between; until GC/expiry lands there is no reclamation, but once there
  is, a file removed in that window surfaces as a generic DuckDB IO error
  from `read_parquet`, not a moraine-classified one.
- **Interruption takes effect between chunks only.** The outer query's
  interrupt is not wired into the nested connection; each per-chunk
  `Fetch()` on the nested streaming query runs to that chunk's completion
  before control returns to the outer engine (where the interrupt is
  honored). Chunk-granular latency, not full-scan latency — but not
  intra-chunk either. Separately, the C++ shim does not yet call
  `moraine_interrupt` anywhere, so this scaffolding seam is unused today:
  Ctrl-C during a blocked snapshot read is not wired up yet.
- **No projection or filter pushdown; the scan advertises
  `projection_pushdown = false` (the `TableFunction` default).** This has a
  concrete, observed consequence: a bare `SELECT count(*) FROM m.s.t` (zero
  real columns referenced) fails with `Not implemented Error: Virtual
  columns require projection pushdown` — DuckDB's optimizer tries a
  zero-column "virtual column" scan for pure `COUNT(*)`, which only
  functions that advertise `projection_pushdown = true` can serve. Any
  query referencing at least one real column (`SELECT count(*), sum(x)
  FROM ...`, `SELECT count(id) FROM ...`, `SELECT * FROM ...`) is
  unaffected and works normally — verified live below.
- **No delete-file filtering** — a scanned table reflects registered data
  files only, matching the plan's stated slice boundary.

## Extension entry-point contract (v1.5.4, C++ ABI)

Verified by reading `ExtensionHelper::LoadExternalExtensionInternal` and
`ExtensionHelper::ParseExtensionMetaData` in the amalgamated `duckdb.cpp`
(not trusted from memory):

- The loader `dlopen()`s the file (`RTLD_NOW | RTLD_LOCAL`) and calls
  `dlsym` for **`<filebase>_duckdb_cpp_init`**, where `filebase` is the
  artifact's filename with **every** `.`-suffix stripped (`FileSystem::
  ExtractBaseName` splits on `.` and takes the first component) — so the
  artifact must be named with exactly one dot, e.g. `moraine_duckdb.duckdb_extension`
  → entry symbol `moraine_duckdb_duckdb_cpp_init`.
- The symbol's signature is `void(duckdb::ExtensionLoader &)`
  (`typedef void (*ext_init_fun_t)(ExtensionLoader &);`). It must call
  nothing that throws past the loader without being an intentional init
  failure; `loader.FinalizeLoad()` runs automatically after the function
  returns.
- A file loaded via `LOAD '<path>'` (full path) **must** end in literally
  `.duckdb_extension` or the loader rejects it before even opening it.
- **512-byte metadata footer**, appended to the end of the file
  (`ParsedExtensionMetaData::FOOTER_SIZE = 512`,
  `SIGNATURE_SIZE = 256`). Byte layout, ascending offset from
  `file_size - 512` (source-derived from `ParseExtensionMetaData`'s reversed
  8×32-byte field read, cross-checked against DuckDB's own
  `scripts/append_metadata.cmake`):

  | Offset (within footer) | Size | Content |
  |---|---|---|
  | `[0, 96)` | 96 B | reserved, zero |
  | `[96, 128)` | 32 B | ABI type: `"CPP"` |
  | `[128, 160)` | 32 B | extension's own version string (free-form) |
  | `[160, 192)` | 32 B | DuckDB version this was built for: `"v1.5.4"` (must equal `DUCKDB_VERSION` exactly for the CPP ABI — checked byte-for-byte) |
  | `[192, 224)` | 32 B | target platform, e.g. `"osx_arm64"` (from `DuckDB::Platform()`, empirically confirmed via `PRAGMA platform;`) |
  | `[224, 256)` | 32 B | magic value `"4"` (zero-padded; `ParsedExtensionMetaData::EXPECTED_MAGIC_VALUE`) |
  | `[256, 512)` | 256 B | signature (crypto). All-zero is fine when unsigned extensions are allowed — this region is only read when `allow_unsigned_extensions = false`. |

  Each 32-byte field is UTF-8, NUL-padded on the right. The WASM-only custom
  section header that DuckDB's build tooling additionally prepends
  (`append_metadata.cmake`'s `duckdb_signature` LEB128-length wrapper) is
  irrelevant for native loading — `ParseExtensionMetaData(FileHandle&)` reads
  only the trailing 512 bytes regardless of what precedes them, so it's
  omitted here.

- **`allow_unsigned_extensions`**: the metadata-footer version/platform
  fields and the crypto signature are only enforced when this setting is
  `false` (the default). It can only be set at process startup, **not** at
  runtime (`SET allow_unsigned_extensions=true` after the database is
  running fails with `Cannot change allow_unsigned_extensions setting while
  database is running`) — the CLI must be started with `duckdb -unsigned`.
  Even with unsigned extensions allowed, a version/platform mismatch still
  throws *unless* `allow_extensions_metadata_mismatch` is also set — we
  don't rely on that escape valve since our footer values are exact.

## How the C++ shim compiles (`build.rs`)

Uses the `cc` crate (`cc::Build`), `cpp(true)`, `-std=c++17`, two include
paths (`vendor/duckdb-v1.5.4`, `cpp`), five source files
(`cpp/extension.cpp`, `cpp/storage_extension.cpp`, `cpp/catalog.cpp`,
`cpp/scan.cpp`, `cpp/transaction_manager.cpp`), producing one static archive
linked into this crate's `cdylib`. Only `extension.cpp`'s entry symbol needs the
force-load/exported-symbol treatment below — the other three are pulled in
normally because `extension.cpp` now calls into
`moraine_duckdb::RegisterMoraineStorageExtension` (a real reference, not an
unreferenced entry point), and the C ABI functions those files call
(`moraine_attach`, `moraine_snapshot`, …) are ordinary same-binary Rust
symbols resolved by the normal link step, not `dynamic_lookup`.

**Why no DuckDB library is linked.** A loadable extension is `dlopen`'d into
a process (the DuckDB CLI, or any embedding application) that has already
resolved every DuckDB symbol — the extension doesn't need its own copy.
Confirmed empirically: the shim compiles and links into a `.dylib`
with **zero** DuckDB library dependencies (`otool -L` shows only system
libs), using the macOS linker flag `-undefined dynamic_lookup` (passed as
two separate driver args, `-undefined` then `dynamic_lookup`) to defer
resolution of DuckDB symbols to load time. This is the same technique
DuckDB's own build tooling uses for loadable extensions. On Linux no
equivalent flag is needed: ELF `-shared` links tolerate undefined symbols by
default.

**Two link-time gotchas found empirically** (both required capturing the
real linker invocation via a `cc`-wrapper shim logging its own `argv`, since
`cargo build`'s default output hides it):

1. Nothing in the Rust crate *calls* the C++ entry point, so the linker
   drops the whole static-archive member as unreferenced before it even gets
   to symbol visibility. Fix: `-Wl,-force_load,<path-to-archive>` forces the
   whole archive in regardless of references.
2. Rust's own cdylib link step passes `-Wl,-dead_strip` *and* an
   auto-generated `-Wl,-exported_symbols_list <file>` containing only
   symbols rustc knows about (`#[no_mangle]` Rust items) — which strips the
   C++ symbol again even after `-force_load` included it. Fix:
   `-Wl,-exported_symbol,_moraine_duckdb_duckdb_cpp_init` (note the leading
   underscore, Mach-O's C symbol-mangling convention) adds to that list
   rather than replacing it.

The flags above are macOS (`ld64`) spellings. `build.rs` branches on
`CARGO_CFG_TARGET_OS` (the *target* platform — `cfg!(target_os)` in a build
script would describe the host, which is wrong under cross-compilation) and
wires the GNU ld equivalents for Linux: `-Wl,--whole-archive,<archive>,
--no-whole-archive` for gotcha 1 and
`-Wl,--export-dynamic-symbol=moraine_duckdb_duckdb_cpp_init` (GNU ld >=
2.35, also honored by lld; no leading underscore — ELF does not decorate C
symbols) for gotcha 2, since Rust restricts the ELF cdylib's
dynamic-symbol list via a version script the same way. **The Linux branch
is written to GNU ld semantics but untested** — no Linux machine in this
discovery loop — until the CI `LOAD` assertion lands in Task 7. Any other
target OS gets a `cargo:warning` at build time stating extension linkage
is unverified there.

## Obtaining a DuckDB v1.5.4 CLI for testing

Downloaded directly from the GitHub release, no build required:

```
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-osx-arm64.zip   # this machine
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-osx-universal.zip
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-linux-amd64.zip
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-linux-arm64.zip
# (+ windows-amd64/arm64, and -musl variants for linux)
```

Cached under `target/duckdb-cli/` (gitignored, never committed — matches the
plan's "large downloads go under `target/`" constraint). The header source
(`libduckdb-src.zip`) is downloaded the same way for extracting `duckdb.hpp`.

## `LOAD` proof

Built via `cargo build -p moraine-duckdb --release`, then packaged into a
`.duckdb_extension` file (rename + append the 512-byte footer described
above — Task 7 should implement this footer-append step in `xtask`, in Rust,
once the real catalog registration lands; it was a one-off script during
discovery, not committed):

```
$ target/duckdb-cli/cli/duckdb -unsigned \
    -c "LOAD '/…/target/duckdb-cli/artifact/moraine_duckdb.duckdb_extension';" \
    -c "SELECT 'moraine_duckdb loaded' AS status;"
┌───────────────────────┐
│        status         │
│        varchar        │
├───────────────────────┤
│ moraine_duckdb loaded │
└───────────────────────┘
$ echo $?
0
```

For contrast, the same command against a nonexistent file exits 1 with a
loud `IO Error`, and against the real artifact *without* `-unsigned` exits 1
with `"...signature is either missing or invalid and unsigned extensions are
disabled..."` — confirming the clean run above is a genuine, non-silent
success and that the unsigned-extension gate is real.

## `ATTACH` proof (Task 4)

Seeded a store through the `moraine` API directly (one schema `s`, tables
`orders` (two columns, `BIGINT`/`DOUBLE`) and `empty` (no data files), one
view `orders_v`), then, against the same packaged artifact as above:

```
$ target/duckdb-cli/cli/duckdb -unsigned \
    -c "LOAD '/…/target/duckdb-cli/artifact/moraine_duckdb.duckdb_extension';" \
    -c "ATTACH '/tmp/moraine-attach-smoke' AS m (TYPE moraine);" \
    -c "SELECT database_name FROM duckdb_databases();"
┌────────────────┐
│ database_name  │
│    varchar     │
├────────────────┤
│ m               │
│ memory          │
│ system          │
│ temp            │
└────────────────┘
$ echo $?
0

$ target/duckdb-cli/cli/duckdb -unsigned \
    -c "LOAD '/…/target/duckdb-cli/artifact/moraine_duckdb.duckdb_extension';" \
    -c "ATTACH '/tmp/moraine-attach-smoke' AS m (TYPE moraine);" \
    -c "SELECT table_name FROM duckdb_tables() WHERE database_name='m';"
┌────────────┐
│ table_name │
│  varchar   │
├────────────┤
│ empty      │
│ orders     │
└────────────┘
```

Also verified beyond the required proof: `SELECT * FROM duckdb_views()`
lists `orders_v` with its textually composed definition (`CREATE VIEW
orders_v AS select * from orders;` — `MoraineViewEntry::ToSQL` assembles it
from the listing ABI's strings, since the base class's implementation
stringifies the parsed query, which is null here and would crash), and
`duckdb_columns()` shows `orders`'s columns with their mapped DuckDB types
(`id BIGINT`, `amount DOUBLE`) — confirming the listing ABI, schema
enumeration, and scalar type mapping all work end to end without a scan.
`DESCRIBE`/`SELECT` on a table now scan (Task 5, see below); on a view
(which DuckDB resolves through a TABLE_ENTRY-typed lookup — the schema
entry's lookup falls back to the view map for table-typed lookups the way
standard DuckDB catalogs do) both still fail with `moraine: querying a
view's definition is not supported yet` — the documented, intentional
view-binding boundary (no SQL parser vendored), not a bug. Neither path
ever reports a bogus "does not exist" or crashes.

## `SELECT` proof (Task 5)

Seeded a store through the `moraine` API directly (schema `s`, table
`orders` with two columns `id BIGINT`/`amount DOUBLE` and a relative-path
data file `s/orders/data.parquet`, table `empty` with no data files), wrote
the Parquet file itself with the DuckDB CLI's own `COPY ... TO` (not the
`moraine` API — the file's *bytes* are DuckDB's, only its *registration* is
moraine's), then, against the same packaged artifact as above:

```
$ duckdb -c "COPY (SELECT i::BIGINT AS id, (i * 1.5)::DOUBLE AS amount
              FROM range(5) t(i)) TO '<store>/s/orders/data.parquet' (FORMAT PARQUET);"
$ target/duckdb-cli/cli/duckdb -unsigned \
    -c "LOAD '/…/moraine_duckdb.duckdb_extension';" \
    -c "ATTACH '<store>' AS m (TYPE moraine);" \
    -c "DESCRIBE m.s.orders;"
┌───────────────┐
│    orders     │
│ id     bigint │
│ amount double │
└───────────────┘
$ … -c "SELECT count(*), sum(amount) FROM m.s.orders;"
┌──────────────┬─────────────┐
│ count_star() │ sum(amount) │
│            5 │        15.0 │
└──────────────┴─────────────┘
$ … -c "SELECT * FROM m.s.orders ORDER BY id;"
   id | amount
    0 |    0.0
    1 |    1.5
    2 |    3.0
    3 |    4.5
    4 |    6.0
$ echo $?
0
```

Empty-table scan (no data files registered, per the required "empty table
scans return 0 rows with correct columns" proof):

```
$ … -c "DESCRIBE m.s.empty;"           # id bigint
$ … -c "SELECT * FROM m.s.empty;"      # 0 rows, column `id` present
$ … -c "SELECT count(id) FROM m.s.empty;"
0
```

Absolute-path resolution (the `path_is_relative == false` half of the
rule): registered a *second* data file on the same `orders` table with an
absolute path to a Parquet file written elsewhere on disk
(`/tmp/moraine-task5-abs/abs.parquet`, 3 rows, `id` 100–102):

```
$ … -c "SELECT count(*), sum(amount) FROM m.s.orders;"
┌──────────────┬─────────────┐
│ count_star() │ sum(amount) │
│            8 │        21.0 │
└──────────────┴─────────────┘
```

8 = 5 (relative-path file) + 3 (absolute-path file); 21.0 = 15.0 + 6.0 —
both files scanned and unioned correctly, confirming both branches of the
path-resolution rule above.

The `count(*)` limitation documented above was also confirmed live: a bare
`SELECT count(*) FROM m.s.orders;` (or `m.s.empty`) fails with `Not
implemented Error: Virtual columns require projection pushdown`, while
`count(id)` (any real column) on the same tables succeeds and returns the
correct count (`5`/`8` and `0` respectively).
