# RFC 0001: Repository structure and conventions

- **Status:** Draft
- **Date:** 2026-07-08

## Summary

Moraine brings a [SlateDB](https://slatedb.io) backend to [DuckLake](https://ducklake.select): a DuckLake catalog implemented on a transactional KV store over object storage, instead of the usual relational catalog database. This RFC establishes the repository layout, crate boundaries, and engineering conventions for the project.

## Goals

- Open-source release quality from the start (licensing, CI, semver, docs), growing into a production dependency.
- Core catalog logic developed and tested as pure Rust, independent of DuckDB.
- Conventions enforced by tooling wherever possible; prose only for what can't be mechanized.

## Crate structure

A Cargo workspace with a virtual root (no root crate):

| Crate | Type | Role |
|---|---|---|
| `moraine` | lib | Core: DuckLake catalog semantics on SlateDB. The flagship crate. |
| `moraine-duckdb` | cdylib | DuckDB extension wrapping the core. Thin by policy: extension entry points and the sync↔async bridge only. If logic accumulates here, it belongs in the core. |
| `xtask` | bin (unpublished) | Automation: build/package the extension, orchestrate the e2e suite. Rust instead of shell scripts/Makefiles — cross-platform and type-checked. |

Rationale for library-first (vs. extension-first): all the hard problems — mapping DuckLake's catalog model (snapshots, schema evolution, transactional commits) onto SlateDB's KV model — are pure Rust problems. Keeping them out of the extension crate avoids paying the FFI/build tax on every test cycle, and keeps the core embeddable in any Rust host (the interim consumption path outside DuckDB is deliberately left open).

## Repository layout

```
moraine/
├── Cargo.toml                    # workspace root: [workspace.dependencies], [workspace.lints], shared metadata
├── rust-toolchain.toml           # pinned stable toolchain + components
├── rustfmt.toml
├── deny.toml                     # cargo-deny: licenses, advisories, sources
├── AGENTS.md                     # conventions source of truth (for agents and humans)
├── CLAUDE.md -> AGENTS.md        # symlink
├── README.md                     # what/why, status banner, quickstart, architecture sketch, license
├── ROADMAP.md                    # phased checkbox roadmap
├── CONTRIBUTING.md
├── LICENSE-MIT / LICENSE-APACHE  # dual license (Rust ecosystem standard)
├── .github/
│   ├── workflows/
│   │   ├── ci.yml                # fmt, clippy, test, doc, deny, e2e
│   │   └── release.yml           # release-plz (dormant until first release)
│   ├── ISSUE_TEMPLATE/           # bug report + feature request
│   └── PULL_REQUEST_TEMPLATE.md
├── crates/
│   ├── moraine/
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   └── tests/                # Tier 2 integration tests
│   └── moraine-duckdb/
│       ├── Cargo.toml
│       ├── src/
│       └── tests/                # Tier 3 e2e tests
├── xtask/
│   └── src/main.rs
└── docs/
    └── rfcs/
        ├── README.md             # the RFC process
        └── 0000-template.md
```

Document roles: **README** = what this is and how to use it. **ROADMAP** = where it's going. **RFCs** = why it is the way it is. **AGENTS.md** = how to work in it.

## RFC process

- Files are `docs/rfcs/NNNN-kebab-title.md` with a status header: `Draft → Accepted → Implemented / Superseded`.
- An RFC is **required** for decisions that are expensive to reverse: on-disk/KV key layout, commit/transaction protocol, public API shape. Optional for everything else.
- RFCs double as an ADR log. RFC 0002 is expected to be the SlateDB key encoding for DuckLake catalog state.
- Design documents produced by brainstorming/design sessions are written directly as RFCs in this directory (entering as `Draft`, graduating to `Accepted` on sign-off). There is no separate specs directory; this location overrides any tooling default (e.g. `docs/superpowers/specs/`).

## Core crate skeleton (`crates/moraine`)

```
src/
├── lib.rs            # crate docs + re-exports only; no logic
├── error.rs          # thiserror-based Error enum + crate Result alias
├── catalog.rs        # DuckLake domain: snapshots, schemas, tables, data-file metadata
├── store.rs          # SlateDB layer: key layout, value codec
└── txn.rs            # commit protocol: catalog transaction → atomic SlateDB write
```

- **Layering:** `catalog` never touches SlateDB directly; `store` knows nothing about DuckLake semantics. The small API between them keeps catalog logic testable against an in-memory store and concentrates key-encoding decisions in one reviewable place.
- **Module growth:** start as `foo.rs`; split into `foo.rs` + `foo/` submodules when needed (no `mod.rs` files — enforced via clippy `mod_module_files`).
- **Visibility:** private by default; `pub` only what `lib.rs` deliberately re-exports. `#![warn(missing_docs)]` on the core crate.
- **Async:** the core API is async end-to-end (SlateDB requires tokio). The core never creates a runtime; `moraine-duckdb` owns a tokio runtime and blocks on core futures at the FFI boundary.
- **Errors:** one crate-level `Error` enum (`thiserror`) with variants per failure domain: store I/O, corrupt/unexpected state, commit conflict, invalid catalog operation. No panics in library code (mechanized below).

This skeleton is a starting guess at the seams. If an implementation RFC (key layout, commit protocol) reveals better boundaries, the skeleton bends to the RFC, not vice versa.

## Testing strategy

| Tier | Home | What | CI cadence |
|---|---|---|---|
| 1 — Unit | colocated `#[cfg(test)]` mods | Tricky internals: key encoding, codecs, conflict resolution. Property-based tests (`proptest`) are **mandatory** for anything in `store` that encodes/decodes: roundtrips, key-ordering preservation. Encoding bugs in a catalog are data-corruption bugs. | every push/PR |
| 2 — Integration | `crates/moraine/tests/` | Public API against real SlateDB on in-memory `object_store`. **No mocks of the store layer.** Covers snapshot visibility, concurrent-commit conflicts, crash-shaped sequences (commit, reopen, verify). | every push/PR |
| 3 — E2E | `crates/moraine-duckdb/tests/`, via `cargo xtask e2e` | Build the cdylib, load into real DuckDB, run actual DuckLake SQL. Validates our assumptions about what DuckLake demands, not just that the code does what we think. | separate required PR job |
| 4 — Real object storage | (future) | MinIO/localstack backend tests. Roadmap item, not built now. | — |

Process: TDD (test first, watch it fail, implement). Every bugfix lands with a regression test. Tests assert on behavior through public APIs, except Tier 1.

## Lint and format policy

Lives in `[workspace.lints]`, inherited by every crate:

- `clippy::pedantic` at `warn`, with a small allow-list — each entry documented where it's allowed.
- `clippy::unwrap_used`, `clippy::expect_used`, `clippy::panic` denied in library code; allowed in tests and `xtask`.
- `unsafe_code` **forbidden in `moraine`**; allowed in `moraine-duckdb` (FFI), where every `unsafe` block carries a `// SAFETY:` comment (`clippy::undocumented_unsafe_blocks` at deny).
- `rustfmt.toml`: near-defaults plus `imports_granularity = "Crate"` and grouped import ordering.

## CI

One workflow (`ci.yml`), parallel jobs, all required:

| Job | Enforces |
|---|---|
| `fmt` | `cargo fmt --check` |
| `clippy` | `cargo clippy --workspace --all-targets`, warnings denied |
| `test` | `cargo test --workspace` (Tiers 1–2) |
| `doc` | `cargo doc` with `RUSTDOCFLAGS="-D warnings"` |
| `deny` | `cargo deny check` |
| `e2e` | `cargo xtask e2e` (Tier 3; separate job so it doesn't gate fast feedback) |

Toolchain pinned via `rust-toolchain.toml`. `rust-version` declared in workspace metadata; a dedicated MSRV-check job is deferred until there are external users. Rust build caching (e.g. `Swatinem/rust-cache`) for CI speed.

## Release and git conventions

- **Conventional commits** (`feat:`, `fix:`, `docs:`, `refactor:`, …). Load-bearing, not cosmetic: `release-plz` derives changelogs and version bumps from them.
- **release-plz** on `release.yml`: opens a release PR with version bumps + CHANGELOG; merging publishes to crates.io. Configured from the start, dormant until the first release.
- Both published crates share a workspace version (`[workspace.package] version`) and release in lockstep. Decoupling is a post-1.0 problem.
- Pre-1.0 semver: breaking changes bump the minor version. Stated in the README.
- PRs into `main`; squash-merge so `main` stays a clean conventional-commit history.

## Alternatives considered

- **Extension-first (cdylib as the primary crate):** rejected — pays the FFI/build/version-pinning tax during the phase where all the hard problems are pure Rust. Risk of drift from real DuckLake behavior is mitigated by the Tier 3 e2e suite instead.
- **Single crate, split later:** rejected — both crates are already committed; converting a root crate to a workspace later touches paths, CI, and metadata for no savings.
- **Lean scaffold, add tooling later (no xtask/release config/templates up front):** rejected in favor of full scaffold, given the stated open-source-release ambition; cargo-deny and licensing especially are cheap now and painful to retrofit.
