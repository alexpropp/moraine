# moraine-duckdb

The DuckDB extension surface for [moraine](../moraine). Two layers:
`cpp/` (a C++ shim linking DuckDB's internal C++ API) and `src/` (the Rust
C-ABI core, sync↔async bridge). The shim carries no DuckLake domain logic —
see the crate root docs and RFC 0006.

The extension registers a `duckdb::StorageExtension` under attach type
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

**Creating or writing an S3 lake requires `READ_WRITE`.** DuckDB opens any
attach whose path starts with a remote prefix (`s3://`, `gcs://`, `azure://`,
…) read-only by default, and a read-only attach cannot create a catalog. To
create or write a lake whose moraine catalog lives on S3, add `READ_WRITE`:

```sql
CREATE SECRET s (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-west-2');
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE);
```

Local and `memory://` stores default to read-write and need no flag. A
read-only attach of an uninitialized store fails with an error that names this
fix.

**`CACHE_DIR` — on-disk block cache for S3 catalogs.** Each query reads the
catalog metadata from the store; on S3 that is network latency every time, and
the default in-memory cache is lost on each new process. Point SlateDB's disk
cache at a local directory so warm blocks survive restarts and repeat queries
skip the GETs — `META_CACHE_DIR` through the DuckLake attach (or `CACHE_DIR`
directly on a standalone `moraine:` attach):

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE, META_CACHE_DIR '/var/cache/moraine');
```

Unset, only the in-memory cache applies; redundant for local/`memory://` stores.

## How it is built

moraine-duckdb builds through DuckDB's own extension toolchain
(`extension-ci-tools`) — the same one the community-extensions repository
uses. The Rust crate is compiled as a **static library** (`crate-type =
["staticlib"]`) exporting the C ABI declared in `cpp/moraine_abi.h`; CMake
compiles the C++ shim, links that static library into it, and statically
links DuckDB — producing the loadable `moraine.duckdb_extension`. The
toolchain also writes the extension's metadata footer and, in the community
pipeline, signs the result. None of the extension↔DuckDB linking lives in
this crate; there is no `build.rs`.

Two git submodules pin the build:

- `duckdb/` — DuckDB source at tag **v1.5.4**: the shim compiles against its
  full `src/include/` tree and links its static library.
- `extension-ci-tools/` — the toolchain (Make + CMake helpers) at the
  matching **v1.5.4**.

The moraine Rust static library is bridged into CMake with
[corrosion](https://github.com/corrosion-rs/corrosion) (see the repo-root
`CMakeLists.txt` and `extension_config.cmake`). Build locally with:

```sh
make release GEN=ninja   # needs ninja + a Rust toolchain
```

The loadable lands at `build/release/extension/moraine/moraine.duckdb_extension`
(gitignored). `cargo xtask e2e` builds it that way and drives it through a
real DuckDB CLI plus a real `INSTALL ducklake`.

The DuckDB pin has one source per side: the `duckdb` submodule ref (the
build) and the `duckdb_pin()` constant in `xtask/src/duckdb.rs` (the CLI
download). Bumping the pin means moving both submodules to the new tag and
updating that constant.

| What | Pinned at |
|---|---|
| DuckDB | **v1.5.4** (git hash `08e34c447b`, codename Variegata) |
| Toolchain | `duckdb/extension-ci-tools` tag **v1.5.4** |
| C++ standard | C++17 |
| DuckDB CLI (for `LOAD` testing) | downloaded from the GitHub release, cached under `target/duckdb-cli/` (never committed) |
| DuckLake extension | `INSTALL ducklake` against the pinned CLI — see "Obtaining the DuckLake extension" below |

## Installing

Once moraine is published to the DuckDB community-extensions repository,
installing verifies DuckDB's own signature — no flags needed:

```sql
INSTALL moraine FROM community;
LOAD moraine;
```

Until then — or to load a locally built artifact — load the unsigned
loadable directly with signature checks off. The CLI must be *started* with
`-unsigned`; the setting cannot be changed on a running database:

```sh
duckdb -unsigned -c "LOAD './build/release/extension/moraine/moraine.duckdb_extension';"
```

The loadable's base filename (`moraine`) is load-bearing: DuckDB derives the
entry symbol (`moraine_duckdb_cpp_init`, defined in
`cpp/moraine_extension.cpp`) from the filename before the first `.`. A
version/platform mismatch against the running DuckDB is rejected even when
unsigned.

## Obtaining a DuckDB v1.5.4 CLI for testing

Downloaded directly from the GitHub release, no build required:

```
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-osx-arm64.zip   # this machine
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-osx-universal.zip
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-linux-amd64.zip
https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-linux-arm64.zip
# (+ windows-amd64/arm64, and -musl variants for linux)
```

Cached under `target/duckdb-cli/` (gitignored, never committed). The CLI is
downloaded from a release asset; the DuckDB *source* the extension builds
against comes from the `duckdb` submodule, not this download.

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
except `DECIMAL`'s width/scale suffix and `JSON`. `JSON` is a `VARCHAR`
carrying a `"JSON"` alias, so it is matched on the alias in both directions
(`MapColumnType` maps `"json"` to `LogicalType::JSON()`; `DuckLakeColumnType`
names an aliased-JSON type `"json"` before its `LogicalTypeId` would collapse
to `"varchar"`) — mirroring DuckLake's own `DuckLakeTypes` handling.

The supported scalars track DuckLake's full vocabulary: every signed/unsigned
integer width including `int128`/`uint128`, `float32`/`float64`, `decimal`,
`varchar`/`blob`/`boolean`/`uuid`, the temporal types
(`date`, `time`, `time_ns`, `timetz`, `timestamp`, `timestamp_s`/`_ms`/`_ns`,
`timestamptz`, `interval`), `json`, and `geometry` (a distinct `LogicalTypeId`
needing the `spatial` extension at runtime for values, not for the type or its
Arrow encoding). `variant` is **not** supported: moraine serializes inline data
through Arrow, and DuckDB's Arrow format has no VARIANT representation, so a
`VARIANT` column is rejected at creation with an actionable error (unlike
GEOMETRY, whose Arrow support `spatial` registers).

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
