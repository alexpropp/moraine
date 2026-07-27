---
title: Getting started
description: Load the extension and attach your first moraine-backed lake.
sidebar:
  order: 2
---

## Install the extension

Moraine is not yet in the DuckDB community extension repository, so the
extension loads unsigned. Grab `moraine.duckdb_extension` for your platform
from the [latest release](https://github.com/alexpropp/moraine/releases),
then start DuckDB with `-unsigned` (the setting cannot be changed on a
running database):

```sh
duckdb -unsigned
```

```sql
LOAD 'path/to/moraine.duckdb_extension';
INSTALL ducklake;
```

Once moraine is published as a community extension this becomes
`INSTALL moraine FROM community; LOAD moraine;`.

## Attach a lake

A local lake needs nothing but paths:

```sql
ATTACH 'ducklake:moraine:/tmp/demo-lake' AS lake
  (DATA_PATH '/tmp/demo-lake-data/');

CREATE TABLE lake.events (id BIGINT, payload VARCHAR);
INSERT INTO lake.events VALUES (1, 'hello, bucket');
SELECT * FROM lake.events;
```

## S3 lakes need READ_WRITE

DuckDB opens any attach whose path starts with a remote prefix (`s3://`,
`gcs://`, `azure://`, …) **read-only by default**, and a read-only attach
cannot create a catalog. To create or write a lake on S3, say `READ_WRITE`:

```sql
CREATE SECRET s (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-west-2');
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE);
```

## Faster repeat queries on S3

Point SlateDB's disk cache at a local directory so warm catalog blocks
survive restarts and repeat queries skip the GETs:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE,
   META_CACHE_DIR '/var/cache/moraine');
```
