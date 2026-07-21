//! Inline-data translation: turning staged inline operations into their
//! `inline/*` store writes.

use super::{
    DbTransaction, HashMap, InlineKey, InlineOperation, InlineScanKind, Key, ReadHandle, Result,
    RowOperation, commit, materialize_inline_rows, proto, store_inline, value,
};

/// Allocates `inline/insert` chunk sequence numbers within one commit: the
/// first [`RowOp::InlineInsert`] staged for a given `(table_id,
/// schema_version, begin_snapshot)` gets `chunk_seq` `0`, the next `1`,
/// and so on — disambiguating multiple chunks the same commit stages
/// against the same key prefix.
#[derive(Default)]
pub(super) struct ChunkSeqAllocator(HashMap<(u64, u64, u64), u64>);

impl ChunkSeqAllocator {
    fn next(&mut self, table_id: u64, schema_version: u64, begin_snapshot: u64) -> u64 {
        let seq = self
            .0
            .entry((table_id, schema_version, begin_snapshot))
            .or_insert(0);
        let allocated = *seq;
        *seq += 1;
        allocated
    }
}

/// Removes every `inline/insert` chunk begun at or before `flush_snapshot`
/// for `(table_id, schema_version)`, plus the `inline/inline_delete` tombstones
/// those chunks' rows consumed. Reads `db_tx`'s current (pre-commit)
/// inline records — the flush op only ever names the table and snapshot,
/// never the keys to remove.
pub(super) async fn translate_inline_flush_delete(
    db_tx: &DbTransaction,
    table_id: u64,
    schema_version: u64,
    flush_snapshot: u64,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id).await?;
    let inline_deletes =
        store_inline::scan_inline_inline_deletes(ReadHandle::Tx(db_tx), table_id).await?;

    let scoped: Vec<(InlineOperation, proto::InlineChunkValue)> = chunks
        .into_iter()
        .filter(
            |(op, _)| matches!(op, InlineOperation::Insert { schema_version: v, .. } if *v == schema_version),
        )
        .collect();

    for (op, _) in &scoped {
        if let InlineOperation::Insert { begin_snapshot, .. } = op {
            if *begin_snapshot <= flush_snapshot {
                writes.push((Key::Inline(InlineKey::Live(*op)).encode(), None));
            }
        }
    }

    // The rows the flushed chunks carried, including already-tombstoned
    // ones (`ForFlush`) — their `inline/inline_delete` records become orphaned
    // once the owning chunk is gone above, and must go with it.
    let rows = materialize_inline_rows(&scoped, &inline_deletes);
    for row in InlineScanKind::ForFlush.select(&rows, flush_snapshot, 0) {
        if row.end_snapshot.is_some() {
            writes.push((
                Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
                    table_id,
                    row_id: row.row_id,
                }))
                .encode(),
                None,
            ));
        }
    }

    Ok(())
}

/// Removes every `inline/*` record for `table_id`: schema, chunks, and
/// tombstones, read from `db_tx`'s current (pre-commit) state.
pub(super) async fn translate_inline_drop(
    db_tx: &DbTransaction,
    table_id: u64,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    for (op, _) in store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id).await? {
        writes.push((Key::Inline(InlineKey::Live(op)).encode(), None));
    }
    for (row_id, _) in
        store_inline::scan_inline_inline_deletes(ReadHandle::Tx(db_tx), table_id).await?
    {
        writes.push((
            Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
                table_id,
                row_id,
            }))
            .encode(),
            None,
        ));
    }
    for (data_file_id, row_id, _) in
        store_inline::scan_inline_file_deletes(ReadHandle::Tx(db_tx), table_id).await?
    {
        writes.push((
            Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
                table_id,
                data_file_id,
                row_id,
            }))
            .encode(),
            None,
        ));
    }
    for (schema_version, _) in
        store_inline::scan_inline_schemas(ReadHandle::Tx(db_tx), table_id).await?
    {
        writes.push((
            Key::Inline(InlineKey::Schema {
                table_id,
                schema_version,
            })
            .encode(),
            None,
        ));
    }
    Ok(())
}

/// Translates every staged inline op into direct `inline/*` key writes —
/// a separate pass from [`translate`], since inline records live outside
/// `CatalogSnapshot`'s entity model and are never diffed. `db_tx` is read
/// (for `InlineFlushDelete`/`InlineDrop`, which name a table rather than
/// the keys to remove) at its pre-commit state, before any of this
/// commit's own writes are staged onto it.
pub(super) fn inline_schema_write(
    table_id: u64,
    schema_version: u64,
    arrow_schema: &[u8],
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Schema {
            table_id,
            schema_version,
        })
        .encode(),
        Some(value::encode_value(&proto::InlineSchemaValue {
            arrow_schema: arrow_schema.to_vec(),
        })),
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn inline_insert_write(
    table_id: u64,
    schema_version: u64,
    begin_snapshot: u64,
    chunk_seq: u64,
    row_id_start: u64,
    row_count: u64,
    arrow_body: &[u8],
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Live(InlineOperation::Insert {
            table_id,
            schema_version,
            begin_snapshot,
            chunk_seq,
        }))
        .encode(),
        Some(value::encode_value(&proto::InlineChunkValue {
            body: arrow_body.to_vec(),
            row_id_start,
            row_count,
            data_file_id: None,
        })),
    )
}

pub(super) fn inline_inline_delete_write(
    table_id: u64,
    row_id: u64,
    end_snapshot: u64,
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
            table_id,
            row_id,
        }))
        .encode(),
        Some(value::encode_value(&proto::InlineInlineDeleteValue {
            end_snapshot,
        })),
    )
}

pub(super) fn inline_file_delete_write(
    table_id: u64,
    data_file_id: u64,
    row_id: u64,
    begin_snapshot: u64,
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
            table_id,
            data_file_id,
            row_id,
        }))
        .encode(),
        Some(value::encode_value(&proto::InlineFileDeleteValue {
            begin_snapshot,
        })),
    )
}

pub(super) async fn translate_inline(
    db_tx: &DbTransaction,
    ops: &[RowOperation],
) -> Result<Vec<commit::StagedWrite>> {
    let mut writes = Vec::new();
    let mut chunk_seqs = ChunkSeqAllocator::default();

    for op in ops {
        match op {
            RowOperation::InlineSchema {
                table_id,
                schema_version,
                arrow_schema,
            } => writes.push(inline_schema_write(
                *table_id,
                *schema_version,
                arrow_schema,
            )),
            RowOperation::InlineInsert {
                table_id,
                schema_version,
                begin_snapshot,
                row_id_start,
                row_count,
                arrow_body,
            } => {
                let chunk_seq = chunk_seqs.next(*table_id, *schema_version, *begin_snapshot);
                writes.push(inline_insert_write(
                    *table_id,
                    *schema_version,
                    *begin_snapshot,
                    chunk_seq,
                    *row_id_start,
                    *row_count,
                    arrow_body,
                ));
            }
            RowOperation::InlineInlineDelete {
                table_id,
                row_id,
                end_snapshot,
            } => writes.push(inline_inline_delete_write(
                *table_id,
                *row_id,
                *end_snapshot,
            )),
            RowOperation::InlineFileDelete {
                table_id,
                data_file_id,
                row_id,
                begin_snapshot,
            } => writes.push(inline_file_delete_write(
                *table_id,
                *data_file_id,
                *row_id,
                *begin_snapshot,
            )),
            RowOperation::InlineFlushDelete {
                table_id,
                schema_version,
                flush_snapshot,
            } => {
                translate_inline_flush_delete(
                    db_tx,
                    *table_id,
                    *schema_version,
                    *flush_snapshot,
                    &mut writes,
                )
                .await?;
            }
            RowOperation::InlineDrop { table_id } => {
                translate_inline_drop(db_tx, *table_id, &mut writes).await?;
            }
            RowOperation::InlineSchemaDrop {
                table_id,
                schema_version,
            } => {
                writes.push((
                    Key::Inline(InlineKey::Schema {
                        table_id: *table_id,
                        schema_version: *schema_version,
                    })
                    .encode(),
                    None,
                ));
            }
            RowOperation::Insert { .. }
            | RowOperation::Delete { .. }
            | RowOperation::UpdateSetEnd { .. }
            | RowOperation::UpdateSetBegin { .. } => {}
        }
    }

    Ok(writes)
}
