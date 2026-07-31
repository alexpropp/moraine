//! The inline read seam: materializes DuckLake's four inline scan variants
//! over the `inline/*` keyspace and re-exports
//! [`InlineScanKind`](crate::catalog::inline::InlineScanKind) from the
//! otherwise-private `catalog`. Each function opens a fresh dump read and,
//! on a slot-backed attach, overlays the unfolded tail — so inline data
//! committed to a slot but not yet folded is served (a folder-less
//! multi-writer attach is the operating condition until one exists).

#[doc(hidden)]
pub use crate::catalog::inline::InlineScanKind;
use crate::{
    catalog::{
        Catalog,
        inline::{InlineRow, materialize_inline_rows},
    },
    error::{Error, Result},
    store::{inline as store_inline, key::InlineOperation},
};

/// One inlined row selected by [`scan_inline`], referencing its chunk's
/// body by index into the scan's deduplicated
/// [`chunk_bodies`](InlineScanRecord::chunk_bodies) — a chunk's body
/// crosses the ABI once however many rows it holds, and the reader
/// decodes it once.
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
    /// The owning chunk: an index into the scan's `chunk_bodies`.
    pub chunk_index: u64,
    /// The row's index within its chunk (`0..row_count`).
    pub offset_in_chunk: u64,
}

/// A selected scan: the rows plus the chunk bodies they reference.
#[doc(hidden)]
pub struct InlineScanRecord {
    /// The selected rows, in the scan variant's order.
    pub rows: Vec<InlineRowRecord>,
    /// The referenced chunks' full Arrow IPC record-batch bodies, each
    /// appearing once, indexed by [`InlineRowRecord::chunk_index`].
    pub chunk_bodies: Vec<Vec<u8>>,
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
) -> Result<InlineScanRecord> {
    let read = catalog.begin_dump().await?;
    let chunks = store_inline::scan_inline_chunks(read.handle(), read.overlay(), table_id).await;
    let inline_deletes =
        store_inline::scan_inline_inline_deletes(read.handle(), read.overlay(), table_id).await;
    read.finish().await;
    let chunks = chunks?;
    let inline_deletes = inline_deletes?;

    let rows: Vec<InlineRow> = materialize_inline_rows(&chunks, &inline_deletes);

    // Referenced chunk bodies dense-index in first-reference order.
    let mut chunk_indexes: std::collections::HashMap<usize, u64> = std::collections::HashMap::new();
    let mut chunk_bodies: Vec<Vec<u8>> = Vec::new();
    let selected = kind
        .select(&rows, snapshot, start)
        .into_iter()
        .map(|row| {
            let (operation, chunk) = &chunks[row.chunk];
            // Every chunk a row references is an insert — the row was
            // materialized from it.
            let InlineOperation::Insert { schema_version, .. } = operation else {
                return Err(Error::Corruption(format!(
                    "inline row {} references a non-insert chunk key: {operation:?}",
                    row.row_id
                )));
            };
            let chunk_index = *chunk_indexes.entry(row.chunk).or_insert_with(|| {
                chunk_bodies.push(chunk.body.clone());
                chunk_bodies.len() as u64 - 1
            });

            Ok(InlineRowRecord {
                row_id: row.row_id,
                schema_version: *schema_version,
                begin_snapshot: row.begin_snapshot,
                end_snapshot: row.end_snapshot,
                chunk_index,
                offset_in_chunk: row.offset_in_chunk,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(InlineScanRecord {
        rows: selected,
        chunk_bodies,
    })
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
    let read = catalog.begin_dump().await?;
    let schemas = store_inline::scan_inline_schemas(read.handle(), read.overlay(), table_id).await;
    read.finish().await;
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
    let read = catalog.begin_dump().await?;
    let schemas = store_inline::scan_all_inline_schemas(read.handle(), read.overlay()).await;
    read.finish().await;
    Ok(schemas?
        .into_iter()
        .map(|(table_id, schema_version, _)| (table_id, schema_version))
        .collect())
}

/// Whether `table_id` has at least one recorded `inline/file_delete` record.
/// DuckLake probes the inlined-delete table's existence via a catalog bind
/// that must error until the first `inline/file_delete` is staged, so this
/// reports the table missing (not merely unreferenced) until then.
///
/// # Errors
///
/// Returns an error if the underlying store scan fails or decodes
/// corrupt bytes.
#[doc(hidden)]
pub async fn inline_file_delete_table_exists(catalog: &Catalog, table_id: u64) -> Result<bool> {
    let read = catalog.begin_dump().await?;
    let file_deletes =
        store_inline::scan_inline_file_deletes(read.handle(), read.overlay(), table_id).await;
    read.finish().await;
    Ok(!file_deletes?.is_empty())
}

/// Every `inline/file_delete` record for `table_id` as
/// `(data_file_id, row_id, begin_snapshot)` in key order — the rows behind
/// the `ducklake_inlined_delete_<t>` projection.
///
/// # Errors
///
/// Returns an error if the underlying store scan fails or decodes
/// corrupt bytes.
#[doc(hidden)]
pub async fn inline_file_deletes(catalog: &Catalog, table_id: u64) -> Result<Vec<(u64, u64, u64)>> {
    let read = catalog.begin_dump().await?;
    let file_deletes =
        store_inline::scan_inline_file_deletes(read.handle(), read.overlay(), table_id).await;
    read.finish().await;
    Ok(file_deletes?
        .into_iter()
        .map(|(data_file_id, row_id, value)| (data_file_id, row_id, value.begin_snapshot))
        .collect())
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
        // Drives the single-writer inline path directly through
        // `begin_write_tx`, which no attach builds after the flip.
        Catalog::open_single_writer(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap()
    }

    /// Opens a slot-backed (multi-writer) catalog: inline data committed
    /// through the log is served through the unfolded tail until a folder
    /// applies it.
    async fn open_slots() -> Catalog {
        let options = CatalogOptions::default();
        Catalog::open(Arc::new(InMemory::new()), options)
            .await
            .unwrap()
    }

    /// Inline rows committed to a slot with no folder running are served by
    /// the inline dump through the tail overlay — the reroute the fix made.
    /// Against the pre-fix folded-only read the tail is invisible and this
    /// scan returns nothing.
    #[tokio::test]
    async fn scan_inline_serves_unfolded_rows_on_a_slot_backed_attach() {
        let catalog = open_slots().await;

        let mut tx = crate::ffi_support::staged::staged_begin(&catalog, None, String::new())
            .await
            .unwrap();
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
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1),
        });
        tx.commit().await.unwrap();

        // No folder has run: the rows live only in the unfolded tail.
        let record = scan_inline(&catalog, 1, InlineScanKind::Table, 1, 0)
            .await
            .unwrap();
        let mut ids: Vec<u64> = record.rows.iter().map(|r| r.row_id).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![0, 1],
            "unfolded inline rows are served through the overlay"
        );
        assert_eq!(record.chunk_bodies, vec![b"chunk-a".to_vec()]);

        let schemas = inline_schemas(&catalog, 1).await.unwrap();
        assert_eq!(schemas, vec![(0, b"schema".to_vec())]);
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
        let mut tx = StagedTransaction::begin_detached(db_tx);

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
        let mut inline_delete = StagedTransaction::begin_detached(db_tx2);
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

        let record = scan_inline(&catalog, 1, InlineScanKind::Table, 2, 0)
            .await
            .unwrap();
        let mut by_id: Vec<(u64, Vec<u8>, u64)> = record
            .rows
            .iter()
            .map(|r| {
                (
                    r.row_id,
                    record.chunk_bodies[usize::try_from(r.chunk_index).unwrap()].clone(),
                    r.offset_in_chunk,
                )
            })
            .collect();
        by_id.sort_by_key(|(id, ..)| *id);
        assert_eq!(
            by_id,
            vec![(0, b"chunk-a".to_vec(), 0), (2, b"chunk-b".to_vec(), 0)]
        );
        // Each referenced chunk's body crosses once.
        assert_eq!(record.chunk_bodies.len(), 2);

        let schemas = inline_schemas(&catalog, 1).await.unwrap();
        assert_eq!(schemas, vec![(0, b"schema".to_vec())]);

        let registered = inline_registered_tables(&catalog).await.unwrap();
        assert_eq!(registered, vec![(1, 0)]);
    }

    /// Staged `inline/file_delete` rows read back as
    /// `(data_file_id, row_id, begin_snapshot)` in key order — the rows
    /// behind the `ducklake_inlined_delete_<t>` projection.
    #[tokio::test]
    async fn inline_file_deletes_read_back_in_key_order() {
        let catalog = open().await;
        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut tx = StagedTransaction::begin_detached(db_tx);

        tx.stage(RowOperation::InlineFileDelete {
            table_id: 1,
            data_file_id: 7,
            row_id: 2,
            begin_snapshot: 6,
        });
        tx.stage(RowOperation::InlineFileDelete {
            table_id: 1,
            data_file_id: 7,
            row_id: 0,
            begin_snapshot: 6,
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(6),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(6),
        });
        tx.commit().await.unwrap();

        let rows = inline_file_deletes(&catalog, 1).await.unwrap();
        assert_eq!(rows, vec![(7, 0, 6), (7, 2, 6)]);

        assert!(inline_file_delete_table_exists(&catalog, 1).await.unwrap());
        assert_eq!(inline_file_deletes(&catalog, 9).await.unwrap(), vec![]);
    }
}
