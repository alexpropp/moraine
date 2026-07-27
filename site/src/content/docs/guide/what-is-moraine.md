---
title: What is moraine?
description: Why replace DuckLake's catalog database with a KV store in the bucket.
sidebar:
  order: 1
---

[DuckLake](https://ducklake.select) keeps table data in object storage but
stores its catalog — the transactional source of truth — in a SQL database:
a DuckDB file locally, Postgres/MySQL for concurrent access. That catalog is
the one stateful server left in an otherwise serverless lakehouse: something
to provision, back up, fail over, and pay for while idle.

Moraine removes it. [SlateDB](https://slatedb.io) is a transactional KV
store whose entire state lives in object storage, so the catalog sits in the
bucket next to the Parquet files:

- **Nothing to operate.** No catalog endpoint to deploy, monitor, or
  upgrade. A deployment is a bucket and credentials.
- **Durability for free.** Catalog durability *is* object-store durability.
  No backup schedule, no WAL shipping.
- **The bucket is the whole lake.** Copying or replicating the bucket copies
  data *and* catalog together — environments, migration, and disaster
  recovery become object-storage operations.
- **Scale-to-zero.** An idle lake costs storage, not a 24/7 instance.
- **Embeddable.** The core is a plain Rust library; any host — not just
  DuckDB — can read and commit against the catalog directly.

## The trade-off

A commit is durable only once an object-store PUT lands: ~5–10 ms on S3
Express One Zone, ~50–100 ms on S3 Standard. For lakehouse workloads that
commit after writing Parquet files for seconds, this is noise; small inserts
use DuckLake **data inlining** to skip the per-commit Parquet-file tax.
Workloads needing sub-PUT commit latency want a hot server with local state —
moraine stays serverless and won't compete there.

## Status

Pre-1.0, actively developed. The catalog core and DuckDB extension work
end-to-end: DuckLake SQL — `CREATE`/`INSERT`/`UPDATE`/`DELETE`, time travel,
maintenance — runs against moraine as its catalog, validated against real
DuckDB in CI. Released on [crates.io](https://crates.io/crates/moraine);
APIs may still change before 1.0. The
[roadmap](https://github.com/alexpropp/moraine/blob/main/ROADMAP.md) tracks
each feature.
