# RFC 0017: Read-write and read-only attach paths

- **Date:** 2026-07-12

## Summary

A moraine attach is either read-write (opening the one SlateDB `Db` writer)
or read-only (opening a `DbReader`). This RFC decides how a user *selects*
between them and how that selection reaches the store open. The selector is
the standard DuckDB/DuckLake `READ_ONLY` attach flag —
`ATTACH 'ducklake:moraine:<uri>' AS lake (READ_ONLY)` — not a
moraine-specific prefix, path suffix, or option. Read-write is the default
(flag absent). moraine invents no grammar; it reads the access mode DuckDB
already threads through `ATTACH` and maps it to the topology RFC 0004 fixed.
The single load-bearing unknown — whether DuckLake forwards its outer
`READ_ONLY` into the nested metadata-catalog attach — is recorded as an open
question with a self-contained fallback, not asserted.

## Goals

- **One selector, no new grammar.** The read-write/read-only choice is the
  ordinary `READ_ONLY` attach flag. A user who knows DuckDB `ATTACH` already
  knows how to open moraine read-only.
- **The default is read-write.** Absent `READ_ONLY`, an attach opens the
  `Db` writer, matching the RFC 0004 expectation that a deployment
  designates exactly one read-write process.
- **Defined propagation.** The path from the attach flag to `Db`-vs-`DbReader`
  is specified end to end across both entry points — the DuckLake nested
  attach and the standalone `TYPE moraine` attach.
- **Honest about the unknown.** Where the design depends on DuckLake
  behavior not yet verified, it says so and provides a fallback that does
  not depend on it.

Non-goals:

- **Why the topology is single-writer/many-readers**, fencing semantics, and
  the newest-writer-wins hazard — RFC 0004. This RFC consumes that topology;
  it does not re-argue it.
- **The core read-only *handle* shape** — which operations a read-only
  `Catalog` exposes, and how `commit` is typed out rather than failing at
  runtime — RFC 0003. This RFC specifies only the attach→mode plumbing that
  decides *which* handle is opened.
- **The extension surface mechanics** (StorageExtension registration, the
  C ABI, the sync↔async bridge) — RFC 0006. This RFC adds one bit to that
  surface and leaves the rest.
- **The zero-write checkpoint reader.** RFC 0006 already exposes it as a
  moraine attach option; this RFC places it as a sub-mode of the read-only
  path and does not re-specify it.

## Background

RFC 0004 fixes the topology: one process holds the read-write `Db` and
commits; every other process attaches through `DbReader`, which follows the
manifest, never fences, and never becomes a writer. Because SlateDB fencing
means the *newest* writer wins, a second read-write attach fences the
incumbent's committer instead of failing itself — so the operational rule is
that exactly one process attaches read-write and all others read-only.

RFC 0006 enforces that rule at the attach surface and states that an attach
is either read-write (`Db`) or `READ_ONLY` (`DbReader`), but stops at
"`READ_ONLY` maps to `DbReader`" — it does not say how the flag reaches
moraine across DuckLake's nested attach. DuckLake reaches moraine by
executing a literal nested `ATTACH` of everything after its `ducklake:`
prefix, so `ducklake:moraine:<uri>` runs `ATTACH 'moraine:<uri>'`, resolved
by DuckDB's ordinary attach dispatch to moraine's registered storage
extension. Whether the outer attach's `READ_ONLY` rides that nested attach is
the question this RFC has to answer.

**Implemented.** `moraine_attach` now takes a `read_only` bit, `Catalog`
exposes `open`/`open_read_only` over a `Store::{Writer(Db), Reader(DbReader)}`
split, and every typed read runs through a `ReadHandle` that dispatches to a
transaction (read-write) or the reader (read-only). The shim reads DuckDB's
`AttachOptions::access_mode` and forwards it. Verified live: standalone
`moraine: (READ_ONLY)` and a read-only DuckLake chain both read through the
`DbReader` (`ducklake_load.rs`), and the core suite pins reads, the
write-rejection (typed `Constraint`, not a fence), and that a reader never
fences the live writer (`tests/catalog.rs`). Writes on a read-only catalog
return `Error::Constraint`; the fully typed-out read-only handle is RFC 0003's
concern (below).

## Design

### The selector is `READ_ONLY`

Read-only is requested exactly as for any other DuckLake catalog:

```sql
ATTACH 'ducklake:moraine:<slatedb-uri>' AS lake (DATA_PATH '<uri>', READ_ONLY);
```

Without `READ_ONLY` the attach is read-write and opens the `Db` writer. There
is no `moraine-ro:` prefix, no `?mode=ro` path suffix, and no bespoke moraine
option in the primary surface. DuckDB already resolves `READ_ONLY` to an
access mode on the attached database; moraine reads that mode. The read-only
default for a chained DuckLake attach is whatever DuckLake resolves for its
outer attach — moraine does not impose one.

### Propagation: attach flag → access mode → open

The bit travels one path, whichever entry point produced it:

1. **The C++ shim reads DuckDB's access mode** for the moraine attach —
   `READ_ONLY` versus `READ_WRITE`/`AUTOMATIC` — and passes a `read_only`
   bit across the C ABI. `moraine_attach` gains that parameter; it takes
   none today.
2. **`Catalog::open` opens by mode.** Read-write opens the one SlateDB `Db`
   (the RFC 0004 writer); read-only opens `DbReader`. The finished handle's
   *shape* — which operations it offers — is RFC 0003's concern; this RFC
   requires only that the read-only mode never opens a `Db` and so never
   fences a live writer.

### Two entry points, one bit

- **The DuckLake path** (`ducklake:moraine:<uri>` → nested
  `ATTACH 'moraine:<uri>'`) reaches step 1 only if DuckLake forwards its
  outer `READ_ONLY` into the access mode of the nested metadata-catalog
  attach. This is the primary surface and the one unverified link — see Open
  questions.
- **The standalone path** (`ATTACH '<uri>' AS m (TYPE moraine, READ_ONLY)`)
  has no chain: DuckDB sets the access mode directly from the attach flag and
  the shim reads it. This path always works and is the reference case the
  e2e suite pins first.

### The checkpoint reader is a sub-mode of read-only

RFC 0006's zero-write reader — attaching a `DbReader` against a pre-created
checkpoint id rather than following head — is a moraine attach option that
composes *inside* the read-only path: it presupposes `read_only` and selects
which `DbReader` construction to use. It is orthogonal to the `READ_ONLY`
selector this RFC decides and is not re-specified here.

### Test obligations

- **Standalone read-only opens a reader.** `TYPE moraine, READ_ONLY` opens a
  `DbReader`; a commit attempt through the resulting handle is unavailable
  (not a runtime fence). The read-write default opens a `Db`.
- **No fencing from a reader.** A read-only attach opened while a read-write
  committer is live does not fence the committer (the committer's next
  durable write still succeeds) — the direct check that read-only never
  opened a `Db`.
- **DuckLake forwarding (e2e).** Attach `ducklake:moraine:` with outer
  `READ_ONLY` and assert the moraine metadata attach opened read-only —
  pinned against the tracked DuckLake version (Open questions).

## Open questions

- **Does DuckLake forward `READ_ONLY` to the nested metadata attach?**
  Still not directly confirmable single-process: a read-only DuckLake chain
  reads correctly and DuckDB rejects writes at the outer `lake` database, but
  whether moraine's *nested* metadata attach opened a `DbReader` (forwarded)
  or a `Db` (not forwarded) reads identically, so the e2e cannot isolate it —
  it only pins that the read-only chain works. The **standalone** `moraine:
  (READ_ONLY)` path *is* directly confirmed to open a `DbReader`, and it is
  the fallback below, so the read-only role is always reachable regardless of
  forwarding. Isolating the nested-attach mode would need the two-process
  no-fence probe (RFC 0011's territory), not a single-CLI e2e.
  **Fallback if it does not forward:** expose `READ_ONLY` as a moraine attach
  option on the `moraine:` attach itself, so the read-only role is selectable
  from the string moraine parses regardless of DuckLake forwarding. This is a
  documented escape hatch, not the primary surface — adding it does not
  change steps 1–2, only where the bit originates.

## Alternatives considered

- **A moraine-specific prefix or path suffix** (`ducklake:moraine-ro:<uri>`,
  `moraine:<uri>?mode=ro`). Rejected as the primary surface: it invents
  grammar for a choice DuckDB's `READ_ONLY` flag already expresses, and it
  splits the attach string into two forms a user must remember instead of one
  flag they already know. The self-contained `moraine:`-attach option is kept
  only as the Open-questions fallback, because it survives DuckLake not
  forwarding the flag — but even then it is an option, not a second prefix.
- **A bespoke moraine attach option as the primary knob** (`(TYPE moraine,
  READ_ONLY)` everywhere, ignoring DuckLake's outer flag). Rejected: it makes
  a moraine attach behave unlike every other DuckLake catalog for the one
  attach option users most expect to work, and it cannot be set on the outer
  `ducklake:` attach where the user actually types `READ_ONLY`. Reading
  DuckDB's access mode subsumes it.
- **Runtime read-only enforcement** (open a `Db` always, reject writes at
  commit). Rejected: opening a `Db` fences the live writer (RFC 0004,
  newest-writer-wins) before any write is even attempted, so a "read-only"
  attach that opened a `Db` would break the committer merely by attaching.
  The mode must be chosen at open, in the store handle, not at the write.
