---
title: Getting started
description: Load the extension and attach your first moraine-backed lake.
sidebar:
  order: 2
---

## Install the extension

Moraine is not yet in the DuckDB community extension repository, so the
extension loads unsigned. Grab `moraine.<platform>.duckdb_extension` (e.g.
`moraine.osx_arm64.duckdb_extension`) from the latest `ext-v*` entry on the
[releases page](https://github.com/morainedb/moraine/releases), then start
DuckDB with `-unsigned` (the setting cannot be changed on a running
database):

```sh
duckdb -unsigned
```

```sql
LOAD 'path/to/moraine.osx_arm64.duckdb_extension';
INSTALL ducklake;
```

Once moraine is published as a community extension this becomes
`INSTALL moraine FROM community; LOAD moraine;`.

## Attach a lake

A local lake needs nothing but paths:

```sql
ATTACH 'ducklake:moraine:/tmp/demo-lake' AS lake
  (DATA_PATH '/tmp/demo-lake-data/', META_DATA_PATH '/tmp/demo-lake-data/');

CREATE TABLE lake.events (id BIGINT, payload VARCHAR);
INSERT INTO lake.events VALUES (1, 'hello, bucket');
SELECT * FROM lake.events;
```

Pass **both** path options, with the same value. `DATA_PATH` is DuckLake's:
where Parquet files go. `META_DATA_PATH` is forwarded to moraine and
**recorded in the catalog** at bootstrap — after that, re-attaches can omit
`DATA_PATH` entirely, and operations that need the data root (like indexing
a table that already holds data) work. The recorded value is fixed: an
attach supplying a conflicting `META_DATA_PATH` is refused rather than
silently diverging.

## S3 lakes need READ_WRITE

DuckDB opens any attach whose path starts with a remote prefix (`s3://`,
`gcs://`, `azure://`, …) **read-only by default**, and a read-only attach
cannot create a catalog. To create or write a lake on S3, say `READ_WRITE`:

```sql
CREATE SECRET s (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-west-2');
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/',
   META_DATA_PATH 's3://bucket/prefix-data/', READ_WRITE);
```

Once the lake is bootstrapped, a later writer attach needs only the flag —
the data path is served back from the catalog:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_WRITE);
```

## One writer, many readers

A moraine lake is **single-writer, many-readers**. The selector is DuckDB's
standard attach flag — no moraine-specific grammar:

- **Read-write** (the default for local paths) opens the one SlateDB
  writer. Exactly one process should attach read-write at a time.
- **`READ_ONLY`** opens a follower that serves consistent snapshots and
  tracks the writer's commits, and never becomes a writer:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_ONLY);
```

Take the one-writer rule seriously: SlateDB fencing means the *newest*
writer wins, so a second read-write attach doesn't fail — it fences the
incumbent's committer. Every process past the first should attach
`READ_ONLY`.

## Faster repeat queries on S3

Point SlateDB's on-disk object cache at a local directory so fetched object
parts survive restarts and repeat queries skip the GETs:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine');
```

That cache is capped at 16 GiB per attached store, and the cap is per store
rather than per directory — four stores sharing a directory can fill four
times as much. Set it yourself with `META_CACHE_SIZE`, a byte count:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine', META_CACHE_SIZE 2147483648);
```

By default that cache fills only as queries read, so an object the writer just
wrote is fetched back from S3 the first time it is read. `META_CACHE_PUTS true`
caches it at write time instead, and since store objects are immutable, the
read-only sessions sharing that directory on the same host get it too:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine', META_CACHE_PUTS true);
```

It is off by default because compaction output is cached the same way, and a
large merge can evict what queries had warmed.

All of this is the disk tier only. SlateDB also keeps an in-memory block cache
and metadata cache per attached store; moraine leaves both at SlateDB's own
sizes and neither is settable on the attach.
