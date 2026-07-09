# moraine

[![CI](https://github.com/alexpropp/moraine/actions/workflows/ci.yml/badge.svg)](https://github.com/alexpropp/moraine/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
<!-- Enable after first release:
[![crates.io](https://img.shields.io/crates/v/moraine.svg)](https://crates.io/crates/moraine)
[![docs.rs](https://docs.rs/moraine/badge.svg)](https://docs.rs/moraine)
-->

Moraine brings a [SlateDB](https://slatedb.io) backend to
[DuckLake](https://ducklake.select): a DuckLake catalog implemented on a
transactional KV store over object storage, instead of the usual relational
catalog database.

> **Status: pre-alpha.** Nothing works yet. The repository structure and
> conventions are established (see [RFC 0001](docs/rfcs/0001-repository-structure-and-conventions.md));
> catalog semantics are in design.

## Architecture

- **`crates/moraine`** — the core library: DuckLake catalog semantics
  (snapshots, schemas, tables, transactional commits) mapped onto SlateDB's
  KV model. Pure Rust, embeddable in any tokio host.
- **`crates/moraine-duckdb`** — a DuckDB extension (cdylib) wrapping the
  core, so DuckDB/DuckLake can use moraine as a catalog directly.

Design decisions are recorded as RFCs in [`docs/rfcs/`](docs/rfcs/). Where
this is going: [`ROADMAP.md`](ROADMAP.md).

## Versioning

Pre-1.0 semver: breaking changes bump the **minor** version.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at
your option. Contributions are accepted under the same terms; see
[CONTRIBUTING.md](CONTRIBUTING.md).
