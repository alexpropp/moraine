# moraine

[![CI](https://github.com/alexpropp/moraine/actions/workflows/ci.yml/badge.svg)](https://github.com/alexpropp/moraine/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![crates.io](https://img.shields.io/crates/v/moraine.svg)](https://crates.io/crates/moraine)
[![docs.rs](https://docs.rs/moraine/badge.svg)](https://docs.rs/moraine)

Moraine brings a [SlateDB](https://slatedb.io) backend to
[DuckLake](https://ducklake.select): a DuckLake catalog implemented on a
transactional KV store over object storage, instead of the usual relational
catalog database.

> **Status: pre-1.0, actively developed.** The catalog core and DuckDB
> extension work end-to-end: DuckLake SQL — `CREATE`/`INSERT`/`UPDATE`/
> `DELETE`, time travel, maintenance — runs against moraine as its catalog,
> validated against real DuckDB in CI. Most of the v0.1 feature set (below)
> is in. Released on crates.io; APIs may still change before 1.0.

## Why

DuckLake keeps table data in object storage but stores its catalog — the
transactional source of truth — in a SQL database (a DuckDB file locally,
Postgres/MySQL for concurrent access). That catalog is the one stateful
server left in an otherwise serverless lakehouse: something to provision,
back up, fail over, and pay for while idle.

Moraine removes it. [SlateDB](https://slatedb.io) is a transactional KV
store whose entire state lives in object storage, so the catalog sits in the
bucket next to the Parquet files:

- **Nothing to operate.** No catalog endpoint to deploy, monitor, or
  upgrade. A deployment is a bucket and credentials.
- **Durability for free.** Catalog durability *is* object-store
  durability. No backup schedule, no WAL shipping. Losing the catalog
  means losing the bucket — in which case the data was lost anyway.
- **The bucket is the whole lake.** Copying or replicating the bucket
  copies data *and* catalog together. Environments, migration, and
  disaster recovery become object-storage operations, not "dump the
  catalog database and hope it matches the data files."
- **Scale-to-zero.** An idle lake costs storage, not a 24/7 instance
  waiting for the occasional commit.
- **Embeddable.** The core is a plain Rust library. Any host — not just
  DuckDB — can read and commit against the catalog directly, with no
  service in the path.

The trade-off is commit latency: a commit is durable only once an
object-store PUT lands (~5–10 ms on S3 Express One Zone, ~50–100 ms on S3
Standard). For lakehouse workloads that commit after writing Parquet files
for seconds, this is noise; small inserts use DuckLake **data inlining** to
skip the per-commit Parquet-file tax. Workloads needing sub-PUT commit
latency want a hot server with local state — moraine stays serverless and
won't compete there.

## Architecture

- **`crates/moraine`** — the core library: DuckLake catalog semantics
  (snapshots, schemas, tables, transactional commits) mapped onto SlateDB's
  KV model. Pure Rust, embeddable in any tokio host.
- **`crates/moraine-duckdb`** — a DuckDB extension wrapping the core, so
  DuckLake can use moraine as its catalog backend.

How the pieces fit — layering, storage model, commit protocol — is in
[`ARCHITECTURE.md`](ARCHITECTURE.md); design decisions are recorded as RFCs
in [`docs/rfcs/`](docs/rfcs/).

The bar for **v0.1** is DuckLake consistency: every one of the DuckLake v1.0
spec's 28 `ducklake_*` catalog tables mapped onto SlateDB and validated
against real DuckLake SQL. The [roadmap](ROADMAP.md) tracks each feature.

## Versioning

Pre-1.0 semver: breaking changes bump the **minor** version.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at
your option. Contributions are accepted under the same terms; see
[CONTRIBUTING.md](CONTRIBUTING.md).
