---
title: Embedding the core
description: Using the moraine crate directly from Rust, without DuckDB.
sidebar:
  order: 4
---

The `moraine` crate is a plain Rust library: any tokio host can open a
catalog and read or commit against it directly, with no DuckDB and no
service in the path. The extension is one consumer of the core, not the
center of gravity.

```toml
[dependencies]
moraine = "0.3"
```

The crate-root docs teach the API by worked example — snapshots, schemas,
tables, and the transactional commit protocol:

- [API documentation on docs.rs](https://docs.rs/moraine)
- [The crate on crates.io](https://crates.io/crates/moraine)

Pre-1.0 semver: breaking changes bump the **minor** version.
