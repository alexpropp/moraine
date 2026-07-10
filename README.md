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

## Why

DuckLake keeps table data in object storage but stores its catalog — the
transactional source of truth — in a SQL database: a DuckDB file for local
use, or a Postgres/MySQL server for concurrent access. Either way, the
catalog lives on somebody's disk behind an always-on process: something to
provision, back up, fail over, and pay for while idle. It is the one
stateful server left in an otherwise serverless lakehouse.

Moraine removes it. [SlateDB](https://slatedb.io) is a transactional KV
store whose entire state lives in object storage, so with moraine the
catalog sits in the bucket right next to the Parquet files:

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

The honest trade-off is commit latency: a catalog commit is durable only
once an object-store PUT lands, so the floor is one PUT (~5–10 ms on
S3 Express One Zone via a [dedicated WAL
bucket](https://slatedb.io/rfcs/0009-separate-wal/), ~50–100 ms on S3
Standard). In lakehouse workloads that commit after writing Parquet files
for seconds, this is noise. For small inserts, moraine supports DuckLake
**data inlining**: rows land as appends in the catalog itself and flush to
Parquet later, so tiny commits skip the Parquet-file tax — an access
pattern a log-structured store is built for. What remains is the latency
floor: workloads needing sub-PUT commit latency want a hot server with
local state, and staying serverless means moraine will not compete there.

## Architecture

- **`crates/moraine`** — the core library: DuckLake catalog semantics
  (snapshots, schemas, tables, transactional commits) mapped onto SlateDB's
  KV model. Pure Rust, embeddable in any tokio host.
- **`crates/moraine-duckdb`** — a DuckDB extension wrapping the core, so
  DuckLake can use moraine as its catalog backend.

A fuller map of how the pieces fit — layering, storage model, commit
protocol — is in [`ARCHITECTURE.md`](ARCHITECTURE.md). Design decisions are
recorded as RFCs in [`docs/rfcs/`](docs/rfcs/). Where this is going:
[`ROADMAP.md`](ROADMAP.md).

The bar for **1.0** is parity with the complete DuckLake spec **v1.0**
catalog feature set: every one of its 28 `ducklake_*` catalog tables mapped
onto SlateDB and validated against real DuckLake SQL. The
[roadmap](ROADMAP.md#10--full-ducklake-catalog-parity) tracks each feature.

## Versioning

Pre-1.0 semver: breaking changes bump the **minor** version.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at
your option. Contributions are accepted under the same terms; see
[CONTRIBUTING.md](CONTRIBUTING.md).
