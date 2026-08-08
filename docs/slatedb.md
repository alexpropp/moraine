# SlateDB upstream requests

This document collects changes moraine would like SlateDB to make. These are
not moraine implementation tasks: moraine keeps a correct fallback at the
version it pins, and adopts an upstream capability only after tests cover the
new path.

When an upstream change lands, update the owning RFC, add or adjust its tests,
and remove the corresponding workaround or deferral from
[`rfcs/tasks.md`](rfcs/tasks.md).

## Cache identity and control

### Accept a caller-supplied stable cache scope

SlateDB scopes shared-cache keys with a process-local counter allocated in
handle-open order. That prevents recovered foyer disk-cache entries from
matching after a restart unless stores are opened in the same order. Reversing
the attach order of two stores can also associate a recovered scope with the
wrong store. Compacted SST ids are globally unique, but WAL ids are sequential
within a store and require correct scoping.

The upstream request is an option allowing the caller to provide a stable,
store-specific cache scope, or for SlateDB to derive one from stable store
identity. It must survive process restart and remain distinct for different
stores sharing one cache.

This would make the byte cache restart-warm without moraine re-fetching data
through preload. Moraine correctness does not depend on it.

Before filing or adopting the change, reproduce the mismatch with two lakes
sharing one cache directory: attach them in one order, restart, and attach in
the opposite order. Source inspection predicts the mismatch, but the existing
experiment has not reproduced it.

Owner: [RFC 0009](rfcs/0009-reader-consistency-and-caching.md).

### Make per-SST cache operations callable through public types

`DbCacheManagerOps::warm_sst` and `evict_cached_sst` accept `SsTableId`, but
the identifier is defined in a private module and cannot be named by an
external caller.

The upstream request is to export `SsTableId`, or change these methods to take
an equivalent public identifier. Moraine currently preloads by reading the
desired key ranges, which is cheaper and more precise for its normal workload;
the public API would add SST-grained control rather than unblock caching.

Owner: [RFC 0009](rfcs/0009-reader-consistency-and-caching.md).

## Mutation primitives

### Add an atomic range delete

SlateDB exposes per-key deletion but no operation that tombstones a contiguous
key range in one batch. Moraine must scan dead index ranges, collect their
keys, and delete them in bounded batches.

The upstream request is an atomic range-delete primitive with ordinary
transaction and snapshot semantics. It must not require materializing every
key in the range and must compose with the same write batch that advances
moraine's maintenance stamp.

Once available, one range delete can replace the batched orphan-index sweep.

Owners: [RFC 0016](rfcs/0016-equality-indexes.md) and
[RFC 0021](rfcs/0021-maintenance-model.md).

### Support cooperative multi-writer commits

SlateDB currently fences competing writers. Its WAL SST objects already use
create-only writes, so a lost object-creation race could instead be treated as
commit contention and retried or ordered without invalidating the writer.

The upstream request is a supported cooperative multi-writer protocol that
preserves atomic batches, conflict detection, recovery, and WAL ordering over
object storage. It must distinguish contention from a genuinely fenced or
damaged store.

This is moraine's preferred long-term replacement for its external commit log.
If it lands, the `moraine-wal` serialization layer can disappear while the
folding, grouping, and leader optimizations remain.

Owner: [RFC 0022](rfcs/0022-commit-log-and-leader-role.md).

## Typed failure contracts

### Expose a typed lost-genesis race

During first-store creation, a process that loses the manifest create race and
a process that encounters a damaged manifest currently surface through the
same public error shape. Moraine distinguishes them by matching SlateDB's
message text so it can return `OpenRaced` for the benign case and a store error
for damage.

The upstream request is a stable public error variant for losing the
transactional-object or initial-manifest create race. Callers must be able to
retry that outcome without classifying corruption as contention.

Owner: [RFC 0011](rfcs/0011-crash-recovery.md).

## Maintenance control

### Provide a compaction completion signal

SlateDB lets moraine submit `CompactionRequest::Full` and `FullSegment`, then
inspect `CompactionStatus`. Waiting for completion currently requires polling
`Admin::read_compaction` until the status becomes `Completed` or `Failed`.

The upstream request is an awaitable completion notification for a submitted
compaction, including its terminal status. This would remove polling and make
timeouts more precise; the existing polling path remains correct.

Owner: [RFC 0021](rfcs/0021-maintenance-model.md).

## Capabilities already available

Descending iteration is not an upstream request. SlateDB 0.15 exposes
`IterationOrder::Descending`; moraine can use it directly to stream reverse
index scans instead of materializing and reversing row ids.

DuckLake-owned requests are tracked separately in
[`ducklake.md`](ducklake.md).
