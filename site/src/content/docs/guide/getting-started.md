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

moraine keeps one block cache per process, shared by every store the process
attaches. It holds SST metadata — the indexes and filters every lookup walks
first — in memory, and data blocks in memory over an optional disk tier.

Give it a directory and the data blocks spill there, so a warm working set
survives more than memory alone would hold:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine');
```

Two byte counts size it, both for the whole process rather than per store:
`META_CACHE_MEMORY` caps memory across both halves, `META_CACHE_SIZE` caps the
directory (16 GiB if unset).

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine',
   META_CACHE_MEMORY 1073741824, META_CACHE_SIZE 2147483648);
```

`META_CACHE_MEMORY` is the number to weigh against DuckDB's own `memory_limit`
when sizing a host: DuckDB's budget covers its buffers and its Parquet cache,
and this is the one memory consumer beside it.

By default the cache fills only as queries read, so blocks the writer just
flushed are fetched back from S3 the first time something reads them.
`META_CACHE_PUTS true` admits them as they are written instead:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine', META_CACHE_PUTS true);
```

It is off by default because compaction output goes through the same policy,
and a large merge can evict what queries had warmed.

A fresh process still starts cold, and the first query pays every first touch.
`META_CACHE_PRELOAD` moves that cost into the ATTACH — `'l0'` warms every
subspace's SST metadata, `'all'` additionally reads the catalog subspaces
whole, `'none'` (the default) warms nothing:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine', META_CACHE_PRELOAD 'all');
```

Neither level pulls the index subspace's data blocks, which on an
index-carrying store is most of its bytes — so `'all'` costs metadata-sized
reads, not store-sized ones, and equality lookups stay one fetch per block
behind the filters it just warmed. `moraine_store_census` reports how the
bytes are distributed.

The first attach in a process sizes the cache; later attaches share what it
built. On a host that attaches several catalogs, set these on the first one.

## Caching the data files

The cache above is the *catalog's*. Parquet data files are read by DuckDB
itself, through its external file cache, and land inside `memory_limit` —
moraine never touches a data byte and adds no cache of its own for them.

DuckLake data files are immutable once written, so a process serving repeat
queries wants three DuckDB settings that are off by default:

```sql
SET validate_external_file_cache = 'NO_VALIDATION';
SET parquet_metadata_cache = true;
SET enable_http_metadata_cache = true;
```

These are global, so moraine will not set them from an ATTACH — that would
reach into every other database in the process. Set them in the session that
attaches the lake.
