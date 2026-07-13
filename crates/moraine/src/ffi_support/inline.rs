//! The inline read seam: materializes DuckLake's four inline scan variants
//! over the `inline/*` keyspace and re-exports
//! [`InlineScanKind`](crate::catalog::inline::InlineScanKind) from the
//! otherwise-private `catalog`. Each function opens a fresh read-only
//! transaction, scans, and rolls back.

#[doc(hidden)]
pub use crate::catalog::inline::InlineScanKind;
use crate::{
    catalog::{
        Catalog,
        inline::{InlineRow, materialize_inline_rows},
    },
    error::Result,
    store::{inline as store_inline, key::InlineOperation},
};

/// One inlined row selected by [`scan_inline`]: the materialized row plus
/// an owned copy of its chunk's full Arrow IPC body, so each row is
/// self-contained (one independently-freed element per row across the ABI).
#[doc(hidden)]
pub struct InlineRowRecord {
    /// The row's dense id.
    pub row_id: u64,
    /// The schema version the owning chunk was written under — selects the
    /// `inline/schema` record its body decodes against.
    pub schema_version: u64,
    /// The commit snapshot that inserted this row.
    pub begin_snapshot: u64,
    /// The commit snapshot that tombstoned this row, if any.
    pub end_snapshot: Option<u64>,
    /// The owning chunk's full Arrow IPC record-batch body.
    pub chunk_body: Vec<u8>,
    /// The row's offset within `chunk_body`.
    pub offset_in_chunk: u64,
}

/// Materializes `table_id`'s inlined rows and selects `kind`'s variant at
/// `snapshot` (windowed from `start` for the incremental variants) — the
/// read model behind `moraine_inline_scan`.
///
/// # Errors
///
/// Returns an error if the underlying store scan fails or decodes
/// corrupt bytes.
#[doc(hidden)]
pub async fn scan_inline(
    catalog: &Catalog,
    table_id: u64,
    kind: InlineScanKind,
    snapshot: u64,
    start: u64,
) -> Result<Vec<InlineRowRecord>> {
    let session = catalog.begin_read().await?;
    let chunks = store_inline::scan_inline_chunks(session.handle(), table_id).await;
    let inline_deletes = store_inline::scan_inline_inline_deletes(session.handle(), table_id).await;
    session.finish();
    let chunks = chunks?;
    let inline_deletes = inline_deletes?;

    let rows: Vec<InlineRow> = materialize_inline_rows(&chunks, &inline_deletes);
    Ok(kind
        .select(&rows, snapshot, start)
        .into_iter()
        .map(|row| InlineRowRecord {
            row_id: row.row_id,
            schema_version: chunk_schema_version(&chunks[row.chunk].0),
            begin_snapshot: row.begin_snapshot,
            end_snapshot: row.end_snapshot,
            chunk_body: chunks[row.chunk].1.body.clone(),
            offset_in_chunk: row.offset_in_chunk,
        })
        .collect())
}

/// The schema version an inline chunk was written under. Every chunk key
/// `scan_inline_chunks` returns is an `Insert`; the other arms are
/// unreachable by construction.
fn chunk_schema_version(op: &InlineOperation) -> u64 {
    match op {
        InlineOperation::Insert { schema_version, .. } => *schema_version,
        InlineOperation::InlineDelete { .. } | InlineOperation::FileDelete { .. } => 0,
    }
}

/// Every `(schema_version, arrow_schema)` recorded for `table_id`, in
/// schema-version order — the read model behind `moraine_inline_schemas`.
///
/// # Errors
///
/// Returns an error if the underlying store scan fails or decodes
/// corrupt bytes.
#[doc(hidden)]
pub async fn inline_schemas(catalog: &Catalog, table_id: u64) -> Result<Vec<(u64, Vec<u8>)>> {
    let session = catalog.begin_read().await?;
    let schemas = store_inline::scan_inline_schemas(session.handle(), table_id).await;
    session.finish();
    Ok(schemas?
        .into_iter()
        .map(|(schema_version, value)| (schema_version, value.arrow_schema))
        .collect())
}

/// Every `(table_id, schema_version)` with a recorded inline schema,
/// across every table — feeds the `ducklake_inlined_data_tables`
/// projection behind `moraine_inline_registered_tables`.
///
/// # Errors
///
/// Returns an error if the underlying store scan fails or decodes
/// corrupt bytes.
#[doc(hidden)]
pub async fn inline_registered_tables(catalog: &Catalog) -> Result<Vec<(u64, u64)>> {
    let session = catalog.begin_read().await?;
    let schemas = store_inline::scan_all_inline_schemas(session.handle()).await;
    session.finish();
    Ok(schemas?
        .into_iter()
        .map(|(table_id, schema_version, _)| (table_id, schema_version))
        .collect())
}

/// Whether `table_id` has at least one recorded `inline/file_delete` record.
/// DuckLake probes the inlined-delete table's existence via a catalog bind
/// that must error until the first `inline/file_delete` is staged, so this reports
/// the table missing (not merely unreferenced) until then.
///
/// # Errors
///
/// Returns an error if the underlying store scan fails or decodes
/// corrupt bytes.
#[doc(hidden)]
pub async fn inline_file_delete_table_exists(catalog: &Catalog, table_id: u64) -> Result<bool> {
    let session = catalog.begin_read().await?;
    let file_deletes = store_inline::scan_inline_file_deletes(session.handle(), table_id).await;
    session.finish();
    Ok(!file_deletes?.is_empty())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::{
        catalog::CatalogOptions,
        transaction::staged::{RowOperation, StagedTransaction, TableKind},
    };

    fn snapshot_row(id: u64) -> Vec<crate::transaction::staged::Cell> {
        use crate::transaction::staged::Cell;
        vec![
            Cell::U64(id),
            Cell::I64(1),
            Cell::U64(0),
            Cell::U64(1),
            Cell::U64(0),
        ]
    }

    fn snapshot_changes_row(id: u64) -> Vec<crate::transaction::staged::Cell> {
        use crate::transaction::staged::Cell;
        vec![
            Cell::U64(id),
            Cell::Str("inlined_insert:1".to_string()),
            Cell::Null,
            Cell::Null,
            Cell::Null,
        ]
    }

    async fn open() -> Catalog {
        Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap()
    }

    /// Two chunks (rows 0-1, row 2) staged in one commit, one tombstone
    /// on row 1: `scan_inline` with `Table` at the tombstone's snapshot
    /// returns rows 0 and 2 with their chunk bodies attached, and
    /// `inline_schemas`/`inline_registered_tables` see the recorded
    /// schema.
    #[tokio::test]
    async fn scan_inline_materializes_rows_with_chunk_bodies() {
        let catalog = open().await;
        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut tx = StagedTransaction::begin(db_tx);

        tx.stage(RowOperation::InlineSchema {
            table_id: 1,
            schema_version: 0,
            arrow_schema: b"schema".to_vec(),
        });
        tx.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 0,
            row_count: 2,
            arrow_body: b"chunk-a".to_vec(),
        });
        tx.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 2,
            row_count: 1,
            arrow_body: b"chunk-b".to_vec(),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1),
        });
        tx.commit().await.unwrap();

        let db_tx2 = catalog.begin_write_tx().await.unwrap();
        let mut inline_delete = StagedTransaction::begin(db_tx2);
        inline_delete.stage(RowOperation::InlineInlineDelete {
            table_id: 1,
            row_id: 1,
            end_snapshot: 2,
        });
        inline_delete.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(2),
        });
        inline_delete.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(2),
        });
        inline_delete.commit().await.unwrap();

        let rows = scan_inline(&catalog, 1, InlineScanKind::Table, 2, 0)
            .await
            .unwrap();
        let mut by_id: Vec<(u64, Vec<u8>, u64)> = rows
            .iter()
            .map(|r| (r.row_id, r.chunk_body.clone(), r.offset_in_chunk))
            .collect();
        by_id.sort_by_key(|(id, ..)| *id);
        assert_eq!(
            by_id,
            vec![(0, b"chunk-a".to_vec(), 0), (2, b"chunk-b".to_vec(), 0)]
        );

        let schemas = inline_schemas(&catalog, 1).await.unwrap();
        assert_eq!(schemas, vec![(0, b"schema".to_vec())]);

        let registered = inline_registered_tables(&catalog).await.unwrap();
        assert_eq!(registered, vec![(1, 0)]);
    }
}
