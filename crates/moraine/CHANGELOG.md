# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/alexpropp/moraine/releases/tag/v0.1.0) - 2026-07-15

### Added

- flush interval through CatalogOptions and ATTACH
- garbage-decode proptests, real S3 tests, extension distribution, and an armed release pipeline ([#4](https://github.com/alexpropp/moraine/pull/4))
- macros and column/name mapping served and staged from SlateDB ([#6](https://github.com/alexpropp/moraine/pull/6))
- tags, snapshot expiry/GC, and compaction through DuckLake ([#5](https://github.com/alexpropp/moraine/pull/5))
- change data feed served through DuckLake over moraine projections ([#3](https://github.com/alexpropp/moraine/pull/3))
- partitioning and sort orders served and staged through DuckLake
- support encrypted DuckLake catalogs
- read-only attach and multi-statement ACID transactions
- data inlining on SlateDB with schema evolution; overall
- Arrow IPC encoding for inlined data
- inline-data store, read model, and ABI (RFC 0005 foundation)
- DuckLake integration — moraine as a metadata catalog
- Implementing more transaction and catalog features
- Implement catalog enhacements and transactions
- Initial build-out for transactions and catalog
- Initial build-out of store interface
- Initial dependencies
- convert to virtual workspace with moraine core crate

### Fixed

- close review findings across the commit protocol, staged writes, and FFI ([#2](https://github.com/alexpropp/moraine/pull/2))

### Other

- serve DuckLake's re-read projections from a commit-folded cache
- rename txn identifiers to tx
- RFC/roadmap updates
- Update AGENTS.md
- Initial planning documentation
- add README, roadmap, contributing guide, and agent conventions
