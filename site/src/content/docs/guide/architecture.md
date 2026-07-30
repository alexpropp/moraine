---
title: Architecture
description: How DuckDB, DuckLake, and moraine fit together.
sidebar:
  order: 3
---

Moraine is **DuckLake's catalog backend** — it occupies exactly the slot a
Postgres/SQLite/DuckDB catalog database occupies, and implements DuckLake's
contract rather than inventing its own semantics:

```
DuckDB engine
  └─ ducklake extension        planner, transactions, query execution
       └─ moraine catalog       DuckDB StorageExtension  (the extension surface)
            └─ moraine core      DuckLake catalog semantics on SlateDB  (Rust)
                 └─ SlateDB → object store
```

DuckLake stays the query/planner/transaction layer. Moraine serves the
`ducklake_*` metadata tables **row-faithfully** — the tables *are* the
catalog state, not a re-modeled projection — out of SlateDB.

## Crates

- **`moraine`** — the core: DuckLake catalog semantics (snapshots, schemas,
  tables, transactional commits) mapped onto SlateDB's KV model. Pure Rust,
  async, embeddable in any tokio host. All the hard problems live here.
- **`moraine-duckdb`** — the DuckDB extension wrapping the core. Thin by
  policy: no domain logic, only `StorageExtension` registration, C-ABI
  marshalling, and the sync↔async bridge.

## Going deeper

The full map — storage model, commit protocol, layering — is in
[ARCHITECTURE.md](https://github.com/morainedb/moraine/blob/main/ARCHITECTURE.md);
every design decision is recorded as an RFC in the
[Design section](../../rfcs/0001-repository-structure-and-conventions/) of
this site.
