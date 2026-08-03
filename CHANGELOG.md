# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.2](https://github.com/morainedb/moraine/compare/v0.4.1...v0.4.2) - 2026-08-03

### Fixed

- derive no equality-index entries for a commit that only compacts ([#70](https://github.com/morainedb/moraine/pull/70))
- leave a row deleted out of the file its commit registers unindexed ([#69](https://github.com/morainedb/moraine/pull/69))

### Other

- fix the two cargo package failures blocking release-plz ([#72](https://github.com/morainedb/moraine/pull/72))

## [0.4.1](https://github.com/morainedb/moraine/compare/v0.4.0...v0.4.1) - 2026-08-02

### Added

- settle the extension surface — checkpoint attach and per-version builds ([#67](https://github.com/morainedb/moraine/pull/67))
- close out the RFC 0004 commit-protocol follow-ups ([#66](https://github.com/morainedb/moraine/pull/66))
- complete the RFC 0003 verb surface — partitioning, tags, inlining ([#65](https://github.com/morainedb/moraine/pull/65))
- Type the genesis open race and settle the crash-recovery follow-ups ([#64](https://github.com/morainedb/moraine/pull/64))
- batch several catalog commits into one WriteBatch and one flush ([#62](https://github.com/morainedb/moraine/pull/62))
- Expose the migrate verb through the DuckDB extension ([#63](https://github.com/morainedb/moraine/pull/63))
- Add the crash-resumable format-migration driver and migrate verb ([#61](https://github.com/morainedb/moraine/pull/61))
- Guard readers against format migration and add crash-injection seams ([#50](https://github.com/morainedb/moraine/pull/50))
- Complete the error taxonomy and settle format migration ([#53](https://github.com/morainedb/moraine/pull/53))
- Route diagnostics per attached catalog

### Fixed

- keep ducklake_schema_versions rows across snapshot expiry ([#68](https://github.com/morainedb/moraine/pull/68))
- Close the format-migration reader gate and settle the version check ([#58](https://github.com/morainedb/moraine/pull/58))

### Other

- resolve unique index probes with concurrent point reads, not a scan ([#60](https://github.com/morainedb/moraine/pull/60))
- Add the crash-recovery suite and rescope RFC 0011 ([#59](https://github.com/morainedb/moraine/pull/59))
- Serve catalog reads from the cached view ([#56](https://github.com/morainedb/moraine/pull/56))
- Reclaim dead index entries off the durability path ([#52](https://github.com/morainedb/moraine/pull/52))
- Settle the open DuckLake behaviour questions ([#51](https://github.com/morainedb/moraine/pull/51))
- project website on GitHub Pages, branded, at morainedb.github.io ([#41](https://github.com/morainedb/moraine/pull/41))

## [0.4.0](https://github.com/alexpropp/moraine/compare/v0.3.3...v0.4.0) - 2026-07-28

### Added

- Drive staged index builds from moraine_index_create
- Add batch channel for large-scale index building
- Surface moraine diagnostics in DuckDB's logs

## [0.3.3](https://github.com/alexpropp/moraine/compare/v0.3.2...v0.3.3) - 2026-07-28

### Added

- Forward moraine's tracing events to DuckDB's logger

### Fixed

- Disable expensive codec test
- Report an exhausted commit retry budget as a terminal error

### Other

- Stage index entries onto the transaction and bound commit size
- Move index bound arithmetic into store and cut duplicatio

## [0.3.2](https://github.com/alexpropp/moraine/compare/v0.3.1...v0.3.2) - 2026-07-27

### Added

- Composite and range queries over equality indexes
- Fold dump project w/ history and relax 0ms flush constraint

## [0.3.1](https://github.com/alexpropp/moraine/compare/v0.3.0...v0.3.1) - 2026-07-25

### Added

- Make moraine index maintenance accessible, couple with other lake maintenance ([#35](https://github.com/alexpropp/moraine/pull/35))

### Other

- Updated cargo fmt defaults and re-ran

## [0.3.0](https://github.com/alexpropp/moraine/compare/v0.2.0...v0.3.0) - 2026-07-22

### Added

- Make equality indexes ordered — range, IS NULL, and reverse scans` ([#27](https://github.com/alexpropp/moraine/pull/27))
- Move to async, narrow reads for index creation ([#24](https://github.com/alexpropp/moraine/pull/24))

### Fixed

- Fix overflowing canonical key issue ([#23](https://github.com/alexpropp/moraine/pull/23))
- Remove equality-index entries when their rows are deleted ([#21](https://github.com/alexpropp/moraine/pull/21))

### Other

- Move common abi code to generic functions ([#29](https://github.com/alexpropp/moraine/pull/29))
- Move index checks out of abi into moraine ([#28](https://github.com/alexpropp/moraine/pull/28))
- Clean up file index interfaces ([#26](https://github.com/alexpropp/moraine/pull/26))
- Deduplicate scan, snapshot, dump boilerplate, and move to cbindgen  ([#25](https://github.com/alexpropp/moraine/pull/25))

## [0.2.0](https://github.com/alexpropp/moraine/compare/v0.1.1...v0.2.0) - 2026-07-20

### Added

- Add function-based syntax for key-value based SlateDB indexes ([#18](https://github.com/alexpropp/moraine/pull/18))

## [0.1.1](https://github.com/alexpropp/moraine/compare/v0.1.0...v0.1.1) - 2026-07-16

### Added

- Add cache option to DbWriter and DbReader, fix release-plz pipeline
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This is a single changelog for the whole workspace: entries for the core
`moraine` crate and the `moraine-duckdb` extension are folded together here,
maintained by release-plz on each release PR.
