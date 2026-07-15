# moraine-duckdb

The DuckDB extension surface for [moraine](../moraine). Three layers:
`cpp/` (a C++ shim linking DuckDB's internal C++ API), `src/` (the Rust C-ABI
core, sync↔async bridge), and DuckDB's own `src/include/` header tree
(fetched and cached by `build.rs` under `target/duckdb-src/`, not committed
— see "Where the headers come from" below). The shim carries no DuckLake
domain logic — see the crate root docs and RFC 0006.

The cdylib registers a `duckdb::StorageExtension` under attach type
`moraine`, reachable two ways:

- **Primary path — `ATTACH 'ducklake:moraine:<store>' AS lake (DATA_PATH
  '<data-path>')`.** DuckLake nests `ATTACH 'moraine:<store>'` as its
  metadata connection (see "The `moraine:` prefix" below); every
  `ducklake_*` table the store models is synthesized in the catalog's
  `main` schema, and the writable ones accept DuckLake's own
  `INSERT`/`UPDATE`/`DELETE` as staged row mutations committed atomically.
  User-table data is read through **DuckLake's own reader**, not this
  crate's scan.
- **Secondary path — `ATTACH '<path>' AS m (TYPE moraine)`, or the bare
  `moraine:<path>` prefix.** Schema/table/view listing and `DESCRIBE` work
  through the C ABI; every `ducklake_*` projection is queryable directly,
  for independent verification of what DuckLake wrote. A `SELECT` against a
  real user table binds normally but raises `InvalidInputException` at
  execution time, redirecting to the `ducklake:moraine:` attach — table
  data is served only through DuckLake, never through the standalone
  attach.

DDL issued directly against a user schema/table (outside DuckLake's own
`ducklake_*` writes), plus querying a view's definition, raises
`NotImplementedException`.

## Pin

The pin's single source is the repo-root `DUCKDB_VERSION` file, read by
both `build.rs` (headers + amalgamation) and xtask (CLI download +
artifact footer); CI cache keys hash it. Bumping the pin means editing
that file — plus the hand-maintained references to the version string in
this README and `compile_flags.txt`.

| What | Pinned at |
|---|---|
| DuckDB | **v1.5.4** (git hash `08e34c447b`, codename Variegata) |
| Header source | `src/include/` from the `duckdb/duckdb` git tag `v1.5.4`, sparse-checked-out by `build.rs` and cached at `target/duckdb-src/v1.5.4/src/include/` (never committed) |
| Library source | the `libduckdb-src.zip` amalgamation from the same release, compiled once per target by `build.rs` into a static archive cached at `target/duckdb-src/v1.5.4-lib/<target>/` (never committed) — see "Why DuckDB is statically linked" |
| C++ standard | C++17 |
| DuckDB CLI (for `LOAD` testing) | downloaded from the GitHub release, cached under `target/duckdb-cli/` (never committed) |
| DuckLake extension | `INSTALL ducklake` against the pinned CLI — see "Obtaining the DuckLake extension" below |

## Installing a released build

Release assets are unsigned: DuckDB verifies extension signatures only
against its own core/community keys, so a self-distributed build must be
loaded with signature checks off.

1. Download the asset for your platform and the pinned DuckDB release
   from the GitHub release, e.g. `moraine_duckdb-v1.5.4-osx_arm64.zip`.
2. Unzip. The archive holds `moraine_duckdb.duckdb_extension`; the
   filename is load-bearing (DuckDB derives the entry symbol from it).
3. Load with signature checks off:

   ```sh
   duckdb -unsigned -c "LOAD './moraine_duckdb.duckdb_extension';"
   ```

The same artifact tree is produced locally by `cargo xtask package`
(`dist/<duckdb_version>/<platform>/`).

## Where the headers come from

This crate compiles against DuckDB's **full `src/include/` source tree**,
not the amalgamated `duckdb.hpp`. The amalgamation cannot express DuckDB's
SQL parser types (`TableFunctionRef` and anything built on it, e.g.
`TableFunctionBindInput`) — the parser's headers form a separate,
non-amalgamated tree — so constructs that need them (calling a registered
`TableFunction`'s `.bind` directly, the way DuckLake and the built-in RDBMS
scanners do; binding a view's defining query) cannot compile against it. In
the full tree every internal header exists as its own file at its own real
path (`duckdb/parser/tableref/table_function_ref.hpp`,
`duckdb/storage/storage_extension.hpp`, etc.), so nothing is
forward-declared-only and no hand-vendoring is needed.

**Acquisition: git sparse checkout, not the release tarball.** The tagged
source (`https://github.com/duckdb/duckdb/archive/refs/tags/v1.5.4.tar.gz`)
is 101 MB compressed; `src/include/` alone is 8.8 MB across 1,555 files and
the shim needs nothing outside it (every header reachable from `cpp/*.cpp`'s
includes resolves inside `src/include/duckdb/...`, with no `third_party/` on
the include path). So `build.rs` fetches only that subtree, via a blobless
partial clone plus a sparse checkout:

```
git clone --filter=blob:none --no-checkout --depth 1 --branch v1.5.4 \
    https://github.com/duckdb/duckdb.git target/duckdb-src/v1.5.4
git -C target/duckdb-src/v1.5.4 sparse-checkout set src/include
git -C target/duckdb-src/v1.5.4 checkout v1.5.4
```

Cost: ~3.8 MB of git objects + 8.8 MB checked out, under 3 seconds.
`build.rs`'s `ensure_duckdb_headers` runs this once and skips to the cached
path on every later build. The cache sanity check is
`duckdb/storage/storage_extension.hpp` existing under the checked-out
`src/include/` — a file only the full tree has (the amalgamation asset also
ships a file named `duckdb.hpp`, so checking for that name alone would
accept an amalgamation-shaped cache and fail confusingly at compile time; an
unrecognized cache shape is wiped and re-fetched). If `git` can't reach
GitHub, the build fails with an offline-friendly message naming what to
pre-populate: the `src/include/` directory of a `duckdb/duckdb` checkout at
tag `v1.5.4` — **not** `libduckdb-src.zip`, which is the single-file
amalgamation used separately as the statically linked library source (see
"Why DuckDB is statically linked"). Nothing under `target/` is committed.

**CI network surface.** Because the header fetch happens in `build.rs`,
*every* CI job that compiles `moraine-duckdb` — clippy, test, doc, e2e —
needs `git` and network access to github.com whenever `target/duckdb-src/`
is cold or was pruned from the job's cache. Noted as a comment on the
affected jobs in `.github/workflows/ci.yml`.

The only source change the full tree required over the old vendored
amalgamation was that `cpp/catalog.hpp` / `cpp/transaction_manager.hpp`
`#include` headers by their real `duckdb/...` subpaths (e.g.
`#include "duckdb/storage/storage_extension.hpp"`). Views still can't bind
their defining query: `duckdb::Parser` (needed to turn a view's SQL text
back into an AST) is present in the full tree but unused here.
`MoraineViewEntry::GetQuery`/`BindView` override the base's `return *query;`
(which would null-deref, since no parsed query is stored) to throw
`NotImplementedException` — an intentional boundary.

## Extension entry-point contract (v1.5.4, C++ ABI)

Derived from `ExtensionHelper::LoadExternalExtensionInternal` and
`ExtensionHelper::ParseExtensionMetaData` in the amalgamated `duckdb.cpp`:

- The loader `dlopen()`s the file (`RTLD_NOW | RTLD_LOCAL`) and calls
  `dlsym` for **`<filebase>_duckdb_cpp_init`**, where `filebase` is the
  artifact's filename with **every** `.`-suffix stripped (`FileSystem::
  ExtractBaseName` splits on `.` and takes the first component) — so the
  artifact must be named with exactly one dot, e.g.
  `moraine_duckdb.duckdb_extension` → entry symbol
  `moraine_duckdb_duckdb_cpp_init`.
- The symbol's signature is `void(duckdb::ExtensionLoader &)`
  (`typedef void (*ext_init_fun_t)(ExtensionLoader &);`). It must call
  nothing that throws past the loader without being an intentional init
  failure; `loader.FinalizeLoad()` runs automatically after the function
  returns.
- The symbol must sit in the shared object's **dynamic** symbol table, and
  on ELF rustc's cdylib link emits a version script binding every symbol it
  doesn't own to `local` — no additive linker flag
  (`--export-dynamic-symbol`, `--dynamic-list`, `--undefined`) overrides
  that wildcard, and a second `--version-script` is rejected outright. So
  the entry point is defined in **Rust** (`src/entrypoint.rs`,
  `#[no_mangle]`, exported by rustc on every platform) and forwards the
  `ExtensionLoader *` to the C++ shim's registration function
  (`moraine_duckdb_register` in `cpp/extension.cpp`), which stays
  unexported. A C++-side export works on macOS only because ld64's
  `-exported_symbol` *adds* to the export list — that asymmetry keeps the
  local gate green while every Linux CI load fails at `dlsym`.
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

`build.rs`'s `ensure_duckdb_headers` runs first (fetching/caching
`target/duckdb-src/v1.5.4/src/include/` per "Where the headers come from"
above), then `ensure_duckdb_static_archive` (below), then compiles with
the `cc` crate (`cc::Build`), `cpp(true)`, `-std=c++17`, two include paths
(the fetched `src/include/`, and `cpp`), every `.cpp` source file under
`cpp/` (`extension.cpp`, `storage_extension.cpp`, `catalog.cpp`,
`metadata_tables.cpp`, `scan.cpp`, `staged_write.cpp`,
`transaction_manager.cpp`), producing one static archive linked into this
crate's `cdylib` alongside the DuckDB archive. The C ABI functions the
shim calls (`moraine_attach`, `moraine_snapshot`, …) are ordinary
same-binary Rust symbols resolved by the normal link step.

The shim's own compile stays a few seconds; the one-time DuckDB
amalgamation compile below is the slow step, paid once per machine per
target and cached under `target/` thereafter.

**Why DuckDB is statically linked.** A loadable extension is `dlopen`'d
into a host process (the DuckDB CLI, or any embedding application), and
the shim's ~185 undefined `duckdb::` symbols resolve only if that host
*exports* its C++ internals. The macOS release CLI does (23k+ `duckdb::`
symbols in its export table) — but the stock Linux release CLI is
statically linked without `-rdynamic`: its `.dynsym` carries ~450 entries,
none of them DuckDB's own classes, and no `libduckdb.so` ships in the
release zip. An extension that defers DuckDB symbol resolution to `dlopen`
time (the earlier `-undefined dynamic_lookup` approach) can therefore never
load into the stock Linux CLI — it dies with `undefined symbol:
_ZTVN6duckdb18SchemaCatalogEntryE`. Official DuckDB C++ extensions carry
their own copy of DuckDB's internals (`httpfs.duckdb_extension` for
linux_amd64 v1.5.4 has **zero** undefined `duckdb::` symbols and ~11k
defined ones in 21 MB), so this crate does the same: `build.rs` downloads
the pinned release's `libduckdb-src.zip` (the single-file *library*
amalgamation: `duckdb.cpp` + `duckdb.hpp`), compiles `duckdb.cpp` once per
target triple with fixed flags (`-O1`, `NDEBUG` to match the release CLI,
no debug info — profile-independent, so debug and release builds share it),
and caches the archive at `target/duckdb-src/v1.5.4-lib/<target>/`. `-O1`
rather than `-O2` because gcc at `-O2` on this one giant translation unit
peaks past a 16 GB CI runner's memory alongside parallel rustc jobs (exit
143 in every compiling CI job); optimization level does not affect the ABI.
Note the split: the *headers* the shim compiles against come from the full
`src/include/` tree (the amalgamation's single header cannot express the
parser types the shim needs); the amalgamation supplies only the linked
*definitions*. Same pinned version on both sides, so the symbols agree.

Objects cross the extension↔host boundary by pointer (DuckDB hands the shim
an `ExtensionLoader &`, catalog entries, etc.): host objects carry host
vtables, the extension's carry its own, and both are layouts of the same
pinned version compiled for the same platform — the version pin is what
makes the mix sound.

**Link-time constraints:**

1. Nothing in the Rust crate *calls* most of the C++ shim (only the entry
   point's forward target), so the linker would drop unreferenced archive
   members. Fix: `-Wl,-force_load,<shim-archive>` (ld64) /
   `-Wl,--whole-archive,<shim-archive>,--no-whole-archive` (GNU ld)
   forces the whole shim archive in. The DuckDB archive is deliberately
   *not* force-loaded — it is one giant member that the shim's references
   pull in lazily.
2. Each archive must appear on the link line exactly once. `cc`'s
   `compile()` normally emits `cargo:rustc-link-lib=static=…`, adding a
   second, *lazy* mention of the same archive ahead of the force-load one.
   lld resolves cross-member references (e.g. `catalog.o` calling
   `MoraineScanFunction` in `scan.o`) by fetching members from the lazy
   mention while it is still force-loading the other, defining every
   cross-referenced symbol twice — a hard error. ld64 tolerates the double
   mention, so this only bit on Linux, and only once the shim grew beyond
   one `.cpp` file. Fix: `cargo_metadata(false)` on the `cc::Build`, plus
   linking the C++ standard library by hand (`-lc++`/`-lstdc++`), which
   that metadata had been contributing.
3. The extension entry point cannot be exported from C++ on ELF (see
   "Extension entry-point contract" above) — the entry point lives in
   `src/entrypoint.rs` as `#[no_mangle]` Rust, which rustc exports on every
   platform, so no export-related linker flag exists on either OS.

`build.rs` branches on `CARGO_CFG_TARGET_OS` (the *target* platform —
`cfg!(target_os)` in a build script would describe the host, which is
wrong under cross-compilation); the macOS and Linux branches are both
exercised by the e2e suite. Any other target OS gets a `cargo:warning` at
build time stating extension linkage is unverified there.

## Obtaining a DuckDB v1.5.4 CLI for testing

Downloaded directly from the GitHub release, no build required:

```
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-osx-arm64.zip   # this machine
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-osx-universal.zip
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-linux-amd64.zip
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-linux-arm64.zip
# (+ windows-amd64/arm64, and -musl variants for linux)
```

Cached under `target/duckdb-cli/` (gitignored, never committed). The header
source is fetched separately, from a git tag rather than a release asset —
see "Where the headers come from" above.

## Obtaining the DuckLake extension

`INSTALL ducklake` against the pinned `v1.5.4` CLI deterministically
resolves and installs DuckLake — no version pin of our own is needed beyond
the DuckDB version:

```
$ target/duckdb-cli/cli/duckdb \
    -c "INSTALL ducklake;" -c "LOAD ducklake;" \
    -c "SELECT extension_name, extension_version, install_mode, installed_from \
        FROM duckdb_extensions() WHERE extension_name='ducklake';"
┌────────────────┬────────────────────┬──────────────┬────────────────┐
│ extension_name │ extension_version  │ install_mode │ installed_from │
├────────────────┼────────────────────┼──────────────┼────────────────┤
│ ducklake       │ d318a545           │ REPOSITORY   │ core           │
└────────────────┴────────────────────┴──────────────┴────────────────┘
```

`extension_version` is DuckLake's own short git commit hash, resolved from
DuckDB v1.5.4's own build-time pin
(`.github/config/extensions/ducklake.cmake` in the `duckdb/duckdb` source
tree names `GIT_URL https://github.com/duckdb/ducklake` at
`GIT_TAG d318a545571d7d46eb751fa2aa5f6f4389285d3c`) — `INSTALL ducklake`
against this exact CLI build always resolves to this exact commit,
deterministically, from DuckDB's `core` extension repository
(`installed_from: core`, not the community repository).

**Caching under `target/`, not the CLI's default `~/.duckdb/extensions/`.**
`INSTALL`'s default cache is the user's home directory, outside this
repo's `target/` convention. Redirect it with a `SET` run before
`INSTALL`/`LOAD`:

```
$ duckdb -c "SET extension_directory='target/duckdb-extensions';" \
         -c "INSTALL ducklake;" -c "LOAD ducklake;" \
         -c "SELECT install_path FROM duckdb_extensions() WHERE extension_name='ducklake';"
┌──────────────────────────────────────────────────────────────────┐
│                            install_path                          │
├──────────────────────────────────────────────────────────────────┤
│ target/duckdb-extensions/v1.5.4/osx_arm64/ducklake.duckdb_extension │
└──────────────────────────────────────────────────────────────────┘
```

`xtask e2e` runs `SET extension_directory=...` + `INSTALL ducklake` +
`LOAD ducklake` for real on every invocation, against
`crates/moraine-duckdb/tests/ducklake_load.rs`.

## Serving as DuckLake's metadata catalog

DuckLake drives moraine as its own metadata catalog by nesting an
`ATTACH 'moraine:<path>' ...` inside `ATTACH 'ducklake:moraine:<path>' AS
lake (DATA_PATH ...)`. The facts that attach chain depends on are pinned
against the DuckLake source at commit
`d318a545571d7d46eb751fa2aa5f6f4389285d3c`.

### The `moraine:` prefix

No shim code parses it. DuckDB's own core does, unconditionally, for any
top-level `ATTACH '<prefix>:<path>' AS <name>` where no explicit `TYPE` is
given (`src/execution/operator/schema/physical_attach.cpp`):

```cpp
if (options.db_type.empty()) {
    DBPathAndType::ExtractExtensionPrefix(path, options.db_type);
}
```

`ExtractExtensionPrefix` (`src/main/database_path_and_type.cpp`) takes
everything before the first `:` (rejecting `<2`-character prefixes, so
Windows drive letters like `C:` are never misread, and rejecting a `://`
suffix, so URLs are never misread), lowercases it, and hands the
*stripped* remainder on as `info.path`. `AttachDatabase` then looks up a
`StorageExtension` registered under that exact name — which is `"moraine"`,
the name `RegisterMoraineStorageExtension` (`cpp/storage_extension.cpp`)
registers for the `TYPE moraine` form. So `moraine:<path>` and `<path>` +
`TYPE moraine` converge on the identical `MoraineCatalog::Attach` call with
an identical, already-stripped `info.path` — no code change needed.

DuckLake's own `DuckLakeAttach` (`src/storage/ducklake_storage.cpp`)
constructs the nested path: `options.metadata_path = info.path` (the literal
string after `ducklake:` is stripped by the *same* mechanism one level up),
and `options.metadata_database = "__ducklake_metadata_" + name`.
`DuckLakeInitializer::Initialize` then issues `ATTACH OR REPLACE
{METADATA_PATH} AS {METADATA_CATALOG_NAME_IDENTIFIER}` — i.e. literally
`ATTACH OR REPLACE 'moraine:<path>' AS __ducklake_metadata_lake` — through
the same top-level-statement machinery, so the prefix dispatch fires again,
unmodified.

The schema DuckLake queries is `main` — `duckdb::Catalog`'s base-class
default, which `MoraineCatalog` never overrides, and the schema bootstrap
mints from snapshot 0. So every moraine store is DuckLake-attachable from
birth, with synthesized `ducklake_*` tables and any user tables sharing the
same catalog and `main` schema namespace.

### Pinned `ducklake_*` table shapes

Every column list is transcribed verbatim from
`DuckLakeMetadataManager::InitializeDuckLake`'s bootstrap SQL text in the
pinned DuckLake source (`src/storage/ducklake_metadata_manager.cpp`).
`not null` marks only columns DuckLake itself declares `NOT NULL`/`PRIMARY
KEY`. `moraine-duckdb`'s C++ source is the single source of truth
(`cpp/metadata_tables.cpp`'s `MetadataTableSpecsImpl`); this table is a
human-readable mirror of it.

| Table | Fed from | Notes |
|---|---|---|
| `ducklake_snapshot` | `moraine_dump_snapshots` | shares one dump call with `ducklake_snapshot_changes` (the store models them as one merged record) |
| `ducklake_snapshot_changes` | `moraine_dump_snapshots` | see above |
| `ducklake_schema` | `moraine_dump_schemas` | current + history rows |
| `ducklake_table` | `moraine_dump_tables` | current + history rows; `table_id` has no `PRIMARY KEY` in DuckLake's own schema, left nullable to match |
| `ducklake_view` | `moraine_dump_views` | current + history rows |
| `ducklake_column` | `moraine_dump_columns` | `column_type` is translated, not passed through verbatim — see below |
| `ducklake_data_file` | `moraine_dump_data_files` | widest row (16 real columns) |
| `ducklake_delete_file` | `moraine_dump_delete_files` | |
| `ducklake_table_stats` | `moraine_dump_table_stats` | unversioned |
| `ducklake_table_column_stats` | `moraine_dump_table_column_stats` | unversioned |
| `ducklake_file_column_stats` | `moraine_dump_file_column_stats` | unversioned |
| `ducklake_metadata` | synthesized in C++, no ABI call | see below |
| `ducklake_schema_versions` | `moraine_dump_schema_versions` (`ProvideSchemaVersions`) | one row per `(table_id, schema_version)` transition |
| `ducklake_inlined_data_tables` | `moraine_inline_registered_tables` (`ProvideInlinedDataTables`) | every `(table_id, schema_version)` with a recorded `inline/schema`; see "Data inlining" below — writable only as a no-op (`kVoidInsertable`), since `CreateInlineDataTable` already stages the registration this table's own `INSERT` would double-register |
| `ducklake_partition_info` | `moraine_dump_partition_info` | current + history rows; partition columns embed in the spec's record |
| `ducklake_partition_column` | `moraine_dump_partition_columns` | flattened from the spec records' embedded columns |
| `ducklake_file_partition_value` | `moraine_dump_file_partition_values` | flattened from the data-file records' embedded values |
| `ducklake_sort_info` | `moraine_dump_sort_info` | current + history rows; expressions embed in the spec's record |
| `ducklake_sort_expression` | `moraine_dump_sort_expressions` | flattened from the spec records' embedded expressions |
| `ducklake_tag` | `moraine_dump_tags` | one row per embedded entry of the object's container record, ended entries included (each carries its own begin/end) |
| `ducklake_column_tag` | `moraine_dump_column_tags` | flattened from each column's latest record — a column version transition carries entries forward, so only that record's set is emitted |
| `ducklake_files_scheduled_for_deletion` | `moraine_dump_scheduled_deletions` | the physical-deletion schedule (`current/gcfile`, keyed by the scheduled file's id); written by expiry/compaction, drained by `ducklake_cleanup_old_files` |
| `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters`, `ducklake_file_variant_stats`, `ducklake_column_mapping`, `ducklake_name_mapping` | always empty | store models none of these kinds — a missing table is a bind-time error even for a query that would return zero rows (see below) |

Every table DuckLake's own schema defines is served, either with real data
or as an always-empty stand-in; none are left unbound. This is required:
`DuckLakeMetadataManager::BuildCatalogForSnapshot`, the query DuckLake's own
attach/snapshot-load always runs, joins and correlated-subqueries
`ducklake_tag`, `ducklake_column_tag`, `ducklake_inlined_data_tables`,
`ducklake_macro(_impl/_parameters)`, and `ducklake_partition_info`/`_column`
unconditionally while resolving basic table/view/schema info — a *missing*
table is a bind-time `Catalog Error` even though the query would otherwise
return zero rows for it. So absent store-modeled data means an empty table
(`ProvideEmpty` in `cpp/metadata_tables.cpp`), never an absent SQL table.

The exists-probe `SELECT NULL FROM ducklake_metadata LIMIT 1` references
zero real columns; DuckDB's optimizer only takes that "virtual column" scan
shape for a table function that advertises `projection_pushdown = true`
(otherwise `Not implemented Error: Virtual columns require projection
pushdown` fires before the scan callback runs). `MetadataScanTableFunction`
(`cpp/metadata_tables.cpp`) sets the flag and carries
`TableFunctionInitInput`'s `column_ids` through to the `.function` callback,
which projects exactly those columns out of the already-materialized row
set.

### `ducklake_column.column_type`: two type vocabularies, one stored string

Moraine's catalog stores column types as DuckDB SQL syntax (`"BIGINT"`,
`"DOUBLE"`, ...) — what `ColumnDef::column_type` carries, and what
`MapColumnType` (used for the standalone attach's `DESCRIBE`) parses.
DuckLake's `ducklake_column.column_type`, read back through its own
`DuckLakeTypes::FromString` (`src/common/ducklake_types.cpp`), accepts a
*different*, lowercase vocabulary instead (`"int64"`, `"float64"`,
`"timestamptz"`, ...); serving the stored string verbatim throws `Invalid
Input Error: Failed to parse DuckLake type - unsupported type 'BIGINT'`. One
translation point (`DuckLakeColumnType` in `cpp/metadata_tables.cpp`)
reparses the stored SQL string through `MapColumnType`, then names the
resulting `LogicalTypeId` DuckLake's way — never two independently
maintained type tables. Every type `MapColumnType` supports maps exactly,
except `DECIMAL`'s width/scale suffix (DuckLake's own `ToStringBaseType`
returns the bare `"decimal"`; precision/scale plumbing is unexercised).

### `ducklake_metadata` synthesis

Pinned from `DuckLakeInitializer::LoadExistingDuckLake`
(`src/storage/ducklake_initializer.cpp`) — the keys it reads after the
exists-probe (`SELECT NULL FROM ducklake_metadata LIMIT 1`) succeeds:

| Key | Served value | Why |
|---|---|---|
| `version` | `"1.0"` | compared against `"1.0"` exactly; anything else triggers migration logic (`MigrateV01`/`V02`/...) never wired up — the schema served is already 1.0-shaped |
| `encrypted` | `"false"` | read unconditionally, sets `DuckLakeEncryption`; moraine has no encryption support |
| `created_by` | `"moraine"` | never read back by DuckLake's own init path; served anyway since DuckLake itself writes it at bootstrap and it costs nothing |
| `data_path` | **not served** | `LoadExistingDuckLake` only acts on this key if the row exists; moraine has no store-level source of truth for a lake-wide data path to serve faithfully, so the row is omitted — the ATTACH statement's own `DATA_PATH` option is left as the sole authority |

All rows are global (`scope`/`scope_id` `NULL`) — no schema/table-scoped
DuckLake settings exist to serve.

### Data inlining

`ducklake_metadata` also serves `data_inlining_row_limit = "10"`
(DuckLake's compiled default), turning inlining on catalog-wide. With it
on, DuckLake dynamically creates and drives per-table physical tables in
the metadata catalog instead of writing fixed `ducklake_*` rows for small
inserts; `cpp/inline_tables.cpp` recognizes two dynamic name families —
`ducklake_inlined_data_<table_id>_<schema_version>` (columns `row_id`,
`begin_snapshot`, `end_snapshot`, the table's user columns) and
`ducklake_inlined_delete_<table_id>` (columns `file_id`, `row_id`,
`begin_snapshot`) — and routes `CREATE`/`INSERT`/`UPDATE`/`DELETE`/
`SELECT` against them into the `inline/*` keyspace over the same
staged-row commit path the fixed tables ride, instead of materializing
real tables. See `docs/rfcs/0005-data-inlining.md`'s "Extension surface
(as implemented)" for the exact operation → keyspace mapping.

Chunk bodies (`inline/schema`, `inline/insert`) are Arrow IPC. DuckDB's C++
has no IPC serializer, so the work splits along the Arrow C Data
Interface: `inline_tables.cpp` converts a `DataChunk`'s user columns to
`ArrowArray`/`ArrowSchema` with DuckDB's `ArrowConverter` and hands them
to the Rust bridge (`src/arrow_ipc.rs`), which serializes them to IPC with
`arrow-rs`. Decode reverses it — Rust rebuilds the C Data Interface
structs from the IPC bytes and the shim feeds them to DuckDB's own
record-batch importer (`ArrowTableFunction::ArrowToDuckDB`). Because DuckDB
owns both export and import, the encoding is exactly as type-faithful as
DuckDB's Arrow support, nulls and nested types included.

Two DuckDB-internal contracts the import path depends on, both silent on
violation: `ArrowToDuckDB` reads `output.size()` as the row count to
convert, so the output `DataChunk`'s cardinality must be set *before* the
call; and the per-column `ColumnArrowToDuckDB` does not apply a column's
validity itself — its caller must run `SetValidityMask` first, or every
null silently reads back as a default value. `inline/insert` carries the
record-batch body only (no schema message), decoded against the version's
`inline/schema` schema-only stream so the schema is not re-serialized per
chunk; `inline/schema` also reconstructs a looked-up table's columns.
