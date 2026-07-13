# moraine-duckdb

The DuckDB extension surface for [moraine](../moraine). Three layers:
`cpp/` (a C++ shim linking DuckDB's internal C++ API), `src/` (the Rust C-ABI
core, sync↔async bridge), and DuckDB's own `src/include/` header tree
(fetched and cached by `build.rs` under `target/duckdb-src/`, not committed
— see "Where the headers come from" below). The shim carries no DuckLake
domain logic — see the crate root docs and RFC 0006.

**Status:** DuckLake's own metadata catalog, with a metadata-only standalone
attach alongside it. The cdylib registers a `duckdb::StorageExtension`
under attach type `moraine`, reachable two ways:

- **Primary path — `ATTACH 'ducklake:moraine:<store>' AS lake (DATA_PATH
  '<data-path>')`.** DuckLake nests `ATTACH 'moraine:<store>'` as its
  metadata connection (see "The `moraine:` prefix" below); every
  `ducklake_*` table the store models is synthesized in the catalog's
  `main` schema (see "Serving as DuckLake's metadata catalog" below), and
  the writable ones accept DuckLake's own `INSERT`/`UPDATE`/`DELETE` as
  staged row mutations committed atomically (see "The staged-row write
  path" below). Real user-table data is read through **DuckLake's own
  reader**, not this crate's scan.
- **Secondary path — `ATTACH '<path>' AS m (TYPE moraine)`, or the bare
  `moraine:<path>` prefix.** Schema/table/view listing and `DESCRIBE` work
  through the C ABI; every `ducklake_*` projection is queryable directly,
  for independent verification of what DuckLake wrote. A `SELECT` against
  a real user table binds normally but raises `InvalidInputException` at
  execution time, redirecting to the `ducklake:moraine:` attach — see
  "User-table data is served only through DuckLake" below.

DDL issued directly against a user schema/table (outside DuckLake's own
`ducklake_*` writes), plus querying a view's definition, still raises
`NotImplementedException` (later slices). This document records every
build shape discovery and later tasks pinned, verified against the real
DuckDB v1.5.4 source, the real pinned DuckLake source, and a real CLI
`LOAD`/`ATTACH`/`SELECT` — not against memory or documentation.

## Pin

| What | Pinned at |
|---|---|
| DuckDB | **v1.5.4** (git hash `08e34c447b`, codename Variegata) |
| Header source | `src/include/` from the `duckdb/duckdb` git tag `v1.5.4`, sparse-checked-out by `build.rs` and cached at `target/duckdb-src/v1.5.4/src/include/` (never committed) |
| C++ standard | C++17 |
| DuckDB CLI (for `LOAD` testing) | downloaded from the GitHub release, cached under `target/duckdb-cli/` (never committed) |
| DuckLake extension (for the coming e2e) | `INSTALL ducklake` against the pinned CLI — see "Obtaining the DuckLake extension" below |

## Where the headers come from

**Build-model switch.** Earlier this crate vendored a single amalgamated
`duckdb.hpp` (one 1.9 MiB file assembled by DuckDB's own release tooling
from every header reachable through `duckdb.hpp`'s own top-level includes)
plus 10 additional headers fetched by hand for classes the amalgamation
left forward-declared only (`StorageExtension`, `TableCatalogEntry`,
`ViewCatalogEntry`, …). That approach hit a hard wall: DuckDB's SQL parser
types (`TableFunctionRef`, and anything built on it, e.g.
`TableFunctionBindInput`) are never reachable from the amalgamation at
all — the parser's own headers form a separate, non-amalgamated tree — so
constructs that need them (calling a registered `TableFunction`'s `.bind`
directly, the way DuckLake and the built-in RDBMS scanners do; binding a
view's defining query) could not be made to compile no matter how many
individual headers were hand-vendored around the gap.

The fix is DuckDB's **full `src/include/` source tree** instead of the
amalgamation: every internal header exists there as its own file, at its
own real path (`duckdb/parser/tableref/table_function_ref.hpp`,
`duckdb/storage/storage_extension.hpp`, etc.), so nothing is forward-declared-only
and no hand-vendoring is needed for anything the shim's `#include`s reach.

**Acquisition: git sparse checkout, not the release tarball.** DuckDB's
tagged source (`https://github.com/duckdb/duckdb/archive/refs/tags/v1.5.4.tar.gz`)
is **101 MB** compressed — building all of DuckDB from it is the plan's
explicit escalation trigger, and even just downloading it every time this
crate builds cold would be wasteful, since `src/include/` alone is 8.8 MB
across 1,555 files and **the shim needs nothing outside it**: every header
reachable from `cpp/*.cpp`'s `#include`s, transitively, resolves inside
`src/include/duckdb/...` — verified empirically (see "How the C++ shim
compiles" below) by compiling the exact `cpp/*.cpp` set against `-I
src/include` alone, with **no `third_party/` on the include path**, before
wiring this into `build.rs`. So `build.rs` fetches only that subtree, via a
blobless partial clone plus a sparse checkout of `src/include/`:

```
git clone --filter=blob:none --no-checkout --depth 1 --branch v1.5.4 \
    https://github.com/duckdb/duckdb.git target/duckdb-src/v1.5.4
git -C target/duckdb-src/v1.5.4 sparse-checkout set src/include
git -C target/duckdb-src/v1.5.4 checkout v1.5.4
```

Measured cost: ~3.8 MB of git objects (`.git/`) + 8.8 MB checked out,
under 3 seconds total, versus the 101 MB/multi-minute full-tarball
alternative. `build.rs`'s `ensure_duckdb_headers` runs this once and skips
straight to the cached path on every later build, exactly like `xtask`'s
own `target/duckdb-cli/` CLI cache. The cache sanity check is
`duckdb/storage/storage_extension.hpp` existing under the checked-out
`src/include/` — deliberately a file only the full tree has (the
amalgamation asset also ships a file named `duckdb.hpp`, so checking for
that name alone would accept an amalgamation-shaped cache here and then
fail confusingly at compile time; an unrecognized cache shape is wiped
and re-fetched instead). If `git` can't reach GitHub, the build fails
with an offline-friendly message naming exactly what to pre-populate:
the `src/include/` directory of a `duckdb/duckdb` checkout at tag
`v1.5.4` — **not** `libduckdb-src.zip`, which is the single-file
amalgamation, not this tree. Nothing under `target/` is committed —
matches the project's rule that large, reproducible downloads live under
`target/`, never in git.

**CI network surface.** Because the header fetch happens in `build.rs`,
*every* CI job that compiles `moraine-duckdb` — clippy, test, doc, e2e,
not just e2e — needs `git` and network access to github.com whenever
`target/duckdb-src/` is cold or was pruned from the job's cache. Noted as
a comment on the affected jobs in `.github/workflows/ci.yml`; an explicit
cache step for `target/duckdb-src/` is a future improvement.

**Why not vendor `src/include/` into the repo instead?** The plan allows
either — vendor-with-provenance if the tree is "reasonably sized," or
download-and-cache under `target/` if it's large — and directs recording
the decision. 1,555 individual header files (versus the amalgamation's
~10) is a large jump in files-tracked-by-git for comparatively little
benefit: unlike the CLI binary (which genuinely can't be vendored as
source), this tree is deterministically reproducible from a public tag in
under 3 seconds, so there's nothing a git-committed copy buys over the
`target/`-cached copy except permanently inflating clone size (repeated on
every future DuckDB version bump, since old vendored trees would need
either deletion-and-replacement per bump or accumulate). The CLI download
already established the "large + reproducible → `target/`, gitignored"
precedent in this crate; the header tree follows the same rule.

**Amalgamation gaps that no longer need hand-vendoring.** The 10
extra headers this crate used to fetch by hand and commit under
`vendor/duckdb-v1.5.4/` (`storage_extension.hpp`, `database_size.hpp`,
`create_schema_info.hpp`, `create_table_info.hpp`, `create_view_info.hpp`,
`not_null_constraint.hpp`, `table_catalog_entry.hpp`, `thread.hpp`,
`view_catalog_entry.hpp`, plus `duckdb.hpp` itself) are now just files at
their real paths inside the fetched tree; `vendor/` has been deleted
entirely and `cpp/catalog.hpp` / `cpp/transaction_manager.hpp` now
`#include` them by their real `duckdb/...` subpaths (e.g.
`#include "duckdb/storage/storage_extension.hpp"`) instead of the old bare
filenames. This was the **only** source change required — every other line
of `cpp/*.cpp` / `cpp/*.hpp` compiles unchanged against the full tree.

Views still can't bind their defining query: `duckdb::Parser` (needed to
turn a view's SQL text back into an AST) pulls in the tokenizer/grammar
machinery, which is present in the full tree but was never exercised by
this crate and stays out of scope this task (compiling it is possible now;
using it is future work). `MoraineViewEntry::GetQuery`/`BindView` still
override the base's `return *query;` (which would null-deref, since no
parsed query is ever stored) to throw `NotImplementedException` — the
same intentional boundary as before, not a regression from the header
switch.

## User-table data is served only through DuckLake (standalone scan retired)

Table *data* is served only through DuckLake — never through this
standalone attach. DuckLake owns delete-file merge-on-read, row lineage,
and pushdown; a second independent reader here would silently return
stale/deleted rows once merge-on-read exists. `MoraineTableEntry::GetScanFunction`
(`cpp/catalog.cpp`) still binds unconditionally — populating only the
qualified table name, the attach's store path, and the catalog entry
itself (for `DESCRIBE`/`EXPLAIN`'s NOT NULL lookups) — so `DESCRIBE`,
`EXPLAIN`, and any other bind-only plan consumer keep working. The bound
`TableFunction`'s `init_global` (`MoraineScanInitGlobal`, `cpp/scan.cpp`),
called once per query *execution* rather than once per bind, unconditionally
throws `InvalidInputException` naming the table and the `ducklake:moraine:`
attach form to use instead:

```
moraine: table "s"."t" data is served only through DuckLake, not the standalone attach —
attach the lake with ATTACH 'ducklake:moraine:<store>' AS lake (DATA_PATH '<data-path>')
and query it as lake.s.t
```

`<store>` is the real attach path (`MoraineCatalog::GetDBPath()`); `<data-path>`
stays a placeholder — this shim has no store-level source of truth for a
lake-wide `DATA_PATH` to fill in (see "`ducklake_metadata` synthesis"
below). The message deliberately avoids DuckLake's own retry substrings
(`"conflict"`, `"unique"`, `"primary key"`, `"concurrent"` — see the C ABI
error mapping section) since this is not a benign race to retry.

The now-dead scan machinery this replaced — a nested `duckdb::Connection`
streaming `read_parquet` over resolved data-file paths, the relative/
absolute path-resolution rule, the per-chunk `DataChunk::Reference`
handoff, and the column-count memory-safety guard it needed — is deleted,
not archived; `crates/moraine-duckdb/tests/duckdb_load.rs`'s
`attach_lists_and_scans_through_real_duckdb` asserts the redirect error
instead of a real scan, and real data-scan assertions (read, `COUNT`
pushdown, time travel) live in `tests/ducklake_load.rs`'s DuckLake
round-trip test — see "Serving as DuckLake's metadata catalog" below.

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

`build.rs`'s `ensure_duckdb_headers` runs first (fetching/caching
`target/duckdb-src/v1.5.4/src/include/` per "Where the headers come from"
above), then compiles with the `cc` crate (`cc::Build`), `cpp(true)`,
`-std=c++17`, two include paths (the fetched `src/include/`, and `cpp`),
every `.cpp` source file under `cpp/` (`extension.cpp`,
`storage_extension.cpp`, `catalog.cpp`, `metadata_tables.cpp`, `scan.cpp`,
`staged_write.cpp`, `transaction_manager.cpp`), producing one static
archive linked into this crate's `cdylib`. Only `extension.cpp`'s entry
symbol needs the force-load/exported-symbol treatment below — the other
files are pulled in normally because `extension.cpp` calls into
`moraine_duckdb::RegisterMoraineStorageExtension` (a real reference, not an
unreferenced entry point), and the C ABI functions those files call
(`moraine_attach`, `moraine_snapshot`, …) are ordinary same-binary Rust
symbols resolved by the normal link step, not `dynamic_lookup`.

**Total build time against the fetched full tree stayed a few seconds**
(all five `.cpp` files together, cold cache excluded: well under 15
seconds; the plan's escalation trigger was a *compiled-from-source*
DuckDB taking 15+ minutes, which never applies here since no DuckDB
library is compiled — see "Why no DuckDB library is linked" below).

**Linker flags: unchanged, still needed.** The header-source switch alone
doesn't change what symbols the shim resolves at compile time versus what
the host process resolves at `dlopen` time — the gotchas below are about
the *linker's* view of the archive and Rust's cdylib export list, neither
of which depends on where the headers came from. Re-verified empirically
after the switch (see "How the C++ shim compiles" build output and the
`LOAD` proof below): both the force-load/whole-archive step and the
explicit exported-symbol step are still required, unmodified.

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

**Three link-time gotchas found empirically** (the first two required
capturing the real linker invocation via a `cc`-wrapper shim logging its own
`argv`, since `cargo build`'s default output hides it; the third surfaced as
duplicate-symbol errors from lld on Linux CI):

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
3. The archive must appear on the link line exactly once. `cc`'s
   `compile()` normally emits `cargo:rustc-link-lib=static=…`, adding a
   second, *lazy* mention of the same archive ahead of the force-load one.
   lld resolves cross-member references (e.g. `catalog.o` calling
   `MoraineScanFunction` in `scan.o`) by fetching members from the lazy
   mention while it is still force-loading the other, defining every
   cross-referenced symbol twice — a hard error. ld64 tolerates the double
   mention, so this only bit on Linux, and only once the shim grew beyond
   one `.cpp` file (a single member has no cross-member references to
   fetch). Fix: `cargo_metadata(false)` on the `cc::Build`, plus linking
   the C++ standard library by hand (`-lc++`/`-lstdc++`), which that
   metadata had been contributing.

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
plan's "large downloads go under `target/`" constraint). The header source is
fetched separately, from a git tag rather than a release asset — see "Where
the headers come from" above.

## Obtaining the DuckLake extension (for the coming e2e)

`INSTALL ducklake` against the pinned `v1.5.4` CLI, run once at any point
in this discovery, deterministically resolves and installs DuckLake — no
version pin of our own is needed beyond the DuckDB version, and none
drifted from the RFC 0006 pin:

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
(`installed_from: core`, not the community repository). No separate
"which DuckLake version" decision exists to make for this DuckDB pin.

**Version-drift check against RFC 0006's `v1.5-variegata` pin.** RFC 0006
records DuckLake at branch `v1.5-variegata` @ `c23aca43` (a later commit,
observed at RFC-writing time). Confirmed via the DuckLake repository's own
compare API that `d318a545` (what `INSTALL ducklake` actually fetches) is
an ancestor of `v1.5-variegata`'s current tip — same branch line, an
earlier point on it. This is expected, not a bug: DuckDB v1.5.4 was built
against whatever `v1.5-variegata` commit existed when v1.5.4 was released
(2026-06-11-ish), and DuckLake development continued on that branch
afterward; `INSTALL ducklake` for a fixed DuckDB version always resolves
to that frozen point, never the branch's current tip. Drift only matters
if a *newer* DuckLake feature the branch tip has (but `d318a545` doesn't)
turns out to be load-bearing for a later task — not something to guess at
here; flagged for whoever hits it.

**Caching under `target/`, not the CLI's default `~/.duckdb/extensions/`.**
`INSTALL`'s default cache is the user's home directory, outside this
repo's `target/` convention and outside CI's usual workspace-scoped cache
boundaries. Redirect it with a `SET` run before `INSTALL`/`LOAD`, verified
live:

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

Wired into `xtask e2e` (the `moraine:`-prefix and `ducklake:moraine:`
sections below): `SET extension_directory=...` + `INSTALL ducklake` +
`LOAD ducklake` now run for real, every `e2e` invocation, against
`crates/moraine-duckdb/tests/ducklake_load.rs`.

## `LOAD` proof

Built via `cargo build -p moraine-duckdb --release`, then packaged into a
`.duckdb_extension` file (rename + append the 512-byte footer described
above — `xtask`'s `package_extension` does this in Rust, see
`xtask/src/main.rs`):

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
`DESCRIBE` on a table binds and works; `SELECT` on a table binds but
redirects to the DuckLake attach at execution time (see "User-table data
is served only through DuckLake" below); on a view
(which DuckDB resolves through a TABLE_ENTRY-typed lookup — the schema
entry's lookup falls back to the view map for table-typed lookups the way
standard DuckDB catalogs do) both still fail with `moraine: querying a
view's definition is not supported yet` — the documented, intentional
view-binding boundary (no SQL parser vendored), not a bug. Neither path
ever reports a bogus "does not exist" or crashes.

## `SELECT` redirect proof (Task 6)

Seeded a store through the `moraine` API directly (schema `s`, table `t`
with a registered data file, table `empty` with none), then, against the
same packaged artifact as above:

```
$ target/duckdb-cli/cli/duckdb -unsigned \
    -c "LOAD '/…/moraine_duckdb.duckdb_extension';" \
    -c "ATTACH '<store>' AS m (TYPE moraine);" \
    -c "DESCRIBE m.s.t;"
┌───────────────┐
│       t       │
│ id     bigint │
│ amount double │
└───────────────┘
$ … -c "SELECT * FROM m.s.t;"
Invalid Input Error: moraine: table "s.t" data is served only through DuckLake, not the standalone attach —
attach the lake with ATTACH 'ducklake:moraine:<store>' AS lake (DATA_PATH '<data-path>')
and query it as lake.s.t
$ echo $?
1
```

(the error above is the literal text captured live, one line wrapped for
this document; `<store>` is the real attach path, e.g.
`/tmp/moraine-duckdb-load-attach-.../`).

`DESCRIBE` (a bind-only consumer) still works — confirming `GetScanFunction`
keeps binding unconditionally — while the actual scan's `init_global`
raises before any row would be produced, on both a table with a registered
data file and an empty one (`m.s.empty`). Encoded as
`attach_lists_and_scans_through_real_duckdb` in
`crates/moraine-duckdb/tests/duckdb_load.rs`, asserting the table name, the
`ducklake:moraine:` attach form, the real store path, and the absence of
DuckLake's own retry substrings all appear in the error text.

## Serving as DuckLake's metadata catalog (ducklake-integration slice, Task 4)

DuckLake drives moraine as its own metadata catalog by nesting an
`ATTACH 'moraine:<path>' ...` inside `ATTACH 'ducklake:moraine:<path>' AS
lake (DATA_PATH ...)`. This section pins every fact that attach chain
depends on, each verified against the pinned DuckLake source (commit
`d318a545571d7d46eb751fa2aa5f6f4389285d3c`, checked out read-only under
`target/ducklake-src/` for this research — not committed, not a build
dependency) and a real live `ATTACH`/`SELECT`, not assumed.

### The `moraine:` prefix

No shim code parses it. DuckDB's own core does, unconditionally, for any
top-level `ATTACH '<prefix>:<path>' AS <name>` where no explicit `TYPE` is
given — read directly from the pinned DuckDB v1.5.4 source
(`src/execution/operator/schema/physical_attach.cpp`):

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
`StorageExtension` registered under that exact name — which is exactly
`"moraine"`, the name `RegisterMoraineStorageExtension`
(`cpp/storage_extension.cpp`) already registers for the `TYPE moraine`
form. So `moraine:<path>` and `<path>` + `TYPE moraine` converge on the
identical `MoraineCatalog::Attach` call with an identical, already-stripped
`info.path` — verified live, no code change needed:

```
$ duckdb -unsigned \
    -c "LOAD '/…/moraine_duckdb.duckdb_extension';" \
    -c "ATTACH 'moraine:<store>' AS m;" \
    -c "SELECT database_name FROM duckdb_databases() WHERE database_name='m';"
┌───────────────┐
│ database_name │
├───────────────┤
│ m             │
└───────────────┘
```

DuckLake's own `DuckLakeAttach` (`src/storage/ducklake_storage.cpp`) is
what constructs the nested path: `options.metadata_path = info.path` (the
literal string after `ducklake:` is stripped by the *same* mechanism one
level up), and `options.metadata_database = "__ducklake_metadata_" +
name`. `DuckLakeInitializer::Initialize` then issues `ATTACH OR REPLACE
{METADATA_PATH} AS {METADATA_CATALOG_NAME_IDENTIFIER}` — i.e. literally
`ATTACH OR REPLACE 'moraine:<path>' AS __ducklake_metadata_lake` — through
the exact same top-level-statement machinery as any other `ATTACH`, so the
prefix dispatch above fires again, unmodified.

### Which schema DuckLake queries

`main`. `DuckLakeInitializer::Initialize` falls back to
`DuckLakeTransaction::GetDefaultSchemaName()` (`ducklake_transaction.cpp`)
when no explicit `METADATA_SCHEMA` option is given, which reads
`metadb->GetCatalog().GetDefaultSchema()` on the attached moraine catalog —
`duckdb::Catalog`'s own base-class default, `"main"`, which `MoraineCatalog`
never overrides. This is also the schema Task 2 (bootstrap) mints from
snapshot 0, so every moraine store is DuckLake-attachable from birth: the
synthesized `ducklake_*` tables and any user-created tables/schemas share
the *same* attached catalog and the *same* `main` schema namespace (a
same-named real table would shadow a synthesized one — not exercised, not
expected to occur in practice).

### Pinned `ducklake_*` table shapes

Every column list below is transcribed verbatim from
`DuckLakeMetadataManager::InitializeDuckLake`'s bootstrap SQL text in the
pinned DuckLake source (`src/storage/ducklake_metadata_manager.cpp`), not
from memory. `not null` marks only columns DuckLake itself declares
`NOT NULL`/`PRIMARY KEY`. `moraine-duckdb`'s C++ source is the single
source of truth (`cpp/metadata_tables.cpp`'s `MetadataTableSpecsImpl`);
this table is a human-readable mirror of it.

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
| `ducklake_tag`, `ducklake_column_tag`, `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters`, `ducklake_partition_info`, `ducklake_partition_column`, `ducklake_sort_info`, `ducklake_sort_expression`, `ducklake_file_partition_value`, `ducklake_file_variant_stats`, `ducklake_files_scheduled_for_deletion`, `ducklake_column_mapping`, `ducklake_name_mapping` | always empty | store models none of these kinds this slice — see "Discovered: absence isn't tolerated" below. The last five were added once `ducklake_flush_inlined_data`'s generic cleanup batch (`DELETE FROM`/`INSERT INTO` unconditionally, not gated on partitioning/variant-stats/mapping actually being in use) proved they must at least exist — see "Data inlining" below |

Every table DuckLake's own schema defines is served, either with real data
or as an always-empty stand-in; none are left unbound.

### `ducklake_column.column_type`: two type vocabularies, one stored string

Moraine's own catalog stores column types as DuckDB SQL syntax
(`"BIGINT"`, `"DOUBLE"`, ...) — what `ColumnDef::column_type` carries, and
what `MapColumnType` (used for the standalone attach's own `DESCRIBE`)
already parses. DuckLake's `ducklake_column.column_type`, read back through
its own `DuckLakeTypes::FromString` (`src/common/ducklake_types.cpp`),
accepts a *different*, lowercase vocabulary instead (`"int64"`,
`"float64"`, `"timestamptz"`, ...). Serving the stored string verbatim
throws live: `Invalid Input Error: Failed to parse DuckLake type -
unsupported type 'BIGINT'`. Fixed with one translation point
(`DuckLakeColumnType` in `cpp/metadata_tables.cpp`): reparse the stored SQL
string through the already-trusted `MapColumnType`, then name the
resulting `LogicalTypeId` DuckLake's way — never two independently
maintained type tables. Every type `MapColumnType` currently supports maps
exactly, except `DECIMAL`'s width/scale suffix (DuckLake's own
`ToStringBaseType` returns the bare `"decimal"` for the base type;
precision/scale plumbing is unexercised this slice).

### `ducklake_metadata` synthesis

Pinned from `DuckLakeInitializer::LoadExistingDuckLake`
(`src/storage/ducklake_initializer.cpp`) — the exact keys it reads after
the exists-probe (`SELECT NULL FROM ducklake_metadata LIMIT 1`, itself
zero-real-column and thus dependent on the projection-pushdown fix below)
succeeds:

| Key | Served value | Why |
|---|---|---|
| `version` | `"1.0"` | compared against `"1.0"` exactly; anything else triggers migration logic (`MigrateV01`/`V02`/...) this slice never needs and never wires up — the schema served is already 1.0-shaped |
| `encrypted` | `"false"` | read unconditionally, sets `DuckLakeEncryption`; moraine has no encryption support this slice |
| `created_by` | `"moraine"` | never read back by DuckLake's own init path; served anyway since DuckLake itself writes it at bootstrap and it costs nothing |
| `data_path` | **not served** | `LoadExistingDuckLake` only acts on this key if the row exists (loads/validates `options.data_path` against it); moraine has no store-level source of truth for a lake-wide data path to serve faithfully, so the row is omitted — the ATTACH statement's own `DATA_PATH` option is left as the sole authority, exactly the value the live proof below supplies |

All rows are global (`scope`/`scope_id` `NULL`) — no schema/table-scoped
DuckLake settings exist to serve this slice.

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

Two DuckDB-internal contracts the import path depends on (learned the hard
way, both silent on violation): `ArrowToDuckDB` reads `output.size()` as
the row count to convert, so the output `DataChunk`'s cardinality must be
set *before* the call; and the per-column `ColumnArrowToDuckDB` does not
apply a column's validity itself — its caller must run `SetValidityMask`
first, or every null silently reads back as a default value. `inline/insert`
carries the record-batch body only (no schema message), decoded against the
version's `inline/schema` schema-only stream so the schema is not
re-serialized per chunk; `inline/schema` also reconstructs a looked-up
table's columns. See RFC 0005 for the encoding rationale and costs.

`ducklake_flush_inlined_data` and DuckLake's compaction/rewrite cleanup
also unconditionally touch five more fixed tables this shim did not
previously serve (`ducklake_file_partition_value`,
`ducklake_file_variant_stats`, `ducklake_files_scheduled_for_deletion`,
`ducklake_column_mapping`, `ducklake_name_mapping`) even when none of the
features they back (partitioning, variant stats, name/column mapping) are
in use — discovered live the same way the always-empty list above was:
a `CALL ducklake_flush_inlined_data('lake')` failing commit with `Table
"...ducklake_file_partition_value" could not be found` until it, and then
each of the other four in turn, was added as an always-empty stand-in.

Live proof (`crates/moraine-duckdb/tests/ducklake_load.rs`'s
`ducklake_inline_data_round_trip_through_flush`, run un-ignored by
`cargo xtask e2e`): `CREATE TABLE` + two small `INSERT`s (mixed types,
`NULL`s, two chunks) inline; `SELECT` returns every row correctly through
DuckLake's own inlined-data reader (not this crate's scan); `DELETE` of
one row stages an `inline/inline_delete` and a follow-up `SELECT` no longer sees
it; `CALL ducklake_flush_inlined_data('lake')` moves the remaining rows
to a real Parquet file (DuckLake registers a genuine delete file for the
pre-flush `DELETE`, not a shrunk record count) and a post-flush `SELECT`
is still correct; the standalone `moraine:` attach confirms the drained
`ducklake_inlined_data_<t>_<v>` entry (`0` rows) and the newly-registered
`ducklake_data_file`.

### Discovered: zero-column scans need `projection_pushdown = true`

The exists-probe itself, `SELECT NULL FROM ducklake_metadata LIMIT 1`,
references zero real columns — DuckDB's optimizer only takes that
"virtual column" scan shape for a table function that advertises
`projection_pushdown = true`; without it, `Not implemented Error: Virtual
columns require projection pushdown` fires before this shim's scan
callback is ever reached — a hard blocker here, since DuckLake's probe is
unconditional. Fixed in
`cpp/metadata_tables.cpp`'s `MetadataScanTableFunction`: the flag is set,
and `MetadataScanGlobalState` now carries `TableFunctionInitInput`'s
`column_ids` through to the `.function` callback, which projects exactly
those columns (real pushdown, not just tolerance of the zero-column case)
straight out of the already-materialized row set.

### Discovered: absence isn't tolerated, only absence of *rows*

The plan's stated rule — "project only what the store models; absent
kinds are absent tables" — turned out to need a correction, found live:
`DuckLakeMetadataManager::BuildCatalogForSnapshot`, the query DuckLake's
own attach/snapshot-load always runs (not a lazy, feature-conditional
path), correlated-subqueries and joins `ducklake_tag`, `ducklake_column_tag`,
`ducklake_inlined_data_tables`, `ducklake_macro(_impl/_parameters)`, and
`ducklake_partition_info`/`_column` unconditionally while resolving basic
table/view/schema info — a *missing* table is a bind-time `Catalog Error`
even though the query would otherwise happily return zero rows for it. So
"absent kinds are absent tables" means absent store-modeled row *data*,
not an absent SQL table: every table in that always-run query exists here,
always empty (`ProvideEmpty` in `cpp/metadata_tables.cpp`), documented at
its definition.

### Data-file path resolution is DuckLake's own, not this shim's

The standalone attach's own scan never resolves data-file paths at all
now — it always redirects before touching a path (see "User-table data is
served only through DuckLake" above). This section is about DuckLake's own
reader's resolution rule, exercised only through the `ducklake:moraine:`
path: a relative data-file path resolves against `<DATA_PATH from
ATTACH>/<schema.path>/<table.path>/` — read from `ducklake_schema.path`/
`ducklake_table.path` (this shim's own projections, fed from the store's
`SchemaValue`/`TableValue` `path` fields, unrelated to the attached
catalog's own directory). `table.path` is fixed at `CREATE TABLE` time and
is untouched by a later rename, matching real DuckLake semantics (renaming
a catalog entry never moves files on disk) — confirmed live in the proof
below, where the table is created as `t_old`, gets its data file, and is
renamed to `t`, yet its data still resolves under `.../t_old/`.

### Live proof

Seeded a store through the `moraine` API directly: `main` from bootstrap
(Task 2), table `t_old` (columns `id BIGINT`/`amount DOUBLE`), a
relative-path data file registered against it (real stats — see below),
then `rename_table` to `t` (history depth: `ducklake_table` now carries both
the `t_old` and `t` versions). The Parquet file's bytes come from the
DuckDB CLI's own `COPY ... TO`, written under
`<DATA_PATH>/main/t_old/data.parquet` per the path rule above — with its
*real* `file_size_bytes`/`footer_size` registered (DuckLake's own reader
seeks straight to the footer using the registered `footer_size`; a
placeholder `0` throws `Invalid Input Error: Invalid footer length` the
moment DuckLake reads the file — also discovered live).

```
$ duckdb -unsigned \
    -c "SET extension_directory='target/duckdb-extensions';" \
    -c "INSTALL ducklake;" -c "LOAD ducklake;" \
    -c "LOAD '/…/moraine_duckdb.duckdb_extension';" \
    -c "ATTACH 'ducklake:moraine:<store>' AS lake (DATA_PATH '<data>');" \
    -c "SELECT * FROM lake.main.t ORDER BY id;"
   id | amount
    0 |    0.0
    1 |    1.5
    2 |    3.0
    3 |    4.5
    4 |    6.0
$ … -c "SELECT count(*) FROM lake.main.t;"          # DuckLake's own pushdown
5
$ … -c "SELECT count(*) FROM ducklake_snapshots('lake');"
1
$ … -c "SELECT count(*) FROM lake.main.t AT (VERSION => 1);"   # t exists at v1
5
$ … -c "SELECT count(*) FROM lake.main.t AT (VERSION => 0);"   # t doesn't exist yet at v0
Catalog Error: Table with name t does not exist!
$ echo $?
0
```

Every row above is read through **DuckLake's own reader** (its own
`read_parquet`/multi-file-list machinery, driven by the `ducklake_data_file`
row this shim served) — the standalone attach's own scan, which always
redirects (see "User-table data is served only through DuckLake" above),
is never reached in this chain at all.

Encoded as `crates/moraine-duckdb/tests/ducklake_load.rs`
(`moraine_prefix_attach_without_type_clause`,
`ducklake_attach_reads_through_moraine_metadata`), run un-ignored by
`cargo xtask e2e` alongside `duckdb_load.rs` — the gate now proves the
`moraine:` prefix and the full DuckLake read chain on every run, live,
not just as a recorded manual proof.
