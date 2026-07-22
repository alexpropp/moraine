# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
