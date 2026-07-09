# Roadmap

## Phase 1 — Catalog core on SlateDB
- [ ] RFC 0002: SlateDB key encoding for DuckLake catalog state
- [ ] RFC: commit/transaction protocol
- [ ] `store`: key layout + codecs (proptest roundtrips)
- [ ] `catalog`: snapshots, schemas, tables, data-file metadata
- [ ] `txn`: atomic commit with conflict detection
- [ ] First runnable example in `crates/moraine/examples/` once the API exists

## Phase 2 — DuckDB extension loads
- [ ] Extension entry points in `moraine-duckdb`
- [ ] `cargo xtask e2e` loads the extension into a real DuckDB

## Phase 3 — DuckLake end-to-end
- [ ] DuckLake SQL operations against moraine as the catalog
- [ ] Tier 4 tests: real object storage (MinIO/localstack)
- [ ] Tier 5: `cargo-fuzz` targets for store codecs

## Phase 4 — Publish
- [ ] First crates.io release (switch `release.yml` trigger to `push`)
- [ ] Extension distribution story
