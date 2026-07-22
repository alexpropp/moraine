//! Equality-index upkeep for staged commits: deriving entry adds and
//! removals from registered files and inline chunks by scoped-reading
//! them.

use super::{
    Arc, CatalogSnapshot, Cell, ColumnInfo, DbTransaction, Error, HashMap, HashSet, IndexInfo,
    InlineOperation, ObjectStore, ReadHandle, Result, RowOperation, ScopedReadEntry,
    StagedIndexEntry, TableId, TableKind, commit,
    decode::{decode_data_file, decode_delete_file},
    encode_ordered_values, proto, scoped_read, stage_index_entries, store_inline,
};

/// Derives and appends the equality-index entries for one registered data
/// file (a `RowOperation::Insert` into `TableKind::DataFile`), by scoped-
/// reading the file. A no-op if the file's table has no live index. Refuses
/// (rather than silently under-covering) when the file must be read but no
/// store is available.
pub(super) async fn stage_data_file_index_entries(
    base: &CatalogSnapshot,
    cells: &[Cell],
    data_store: Option<&Arc<dyn ObjectStore>>,
    data_prefix: &str,
    entries: &mut Vec<StagedIndexEntry>,
) -> Result<()> {
    let file = decode_data_file(cells)?;
    let table = TableId::new(file.table_id);
    let indexes = base.indexes_of(table);
    if indexes.is_empty() {
        return Ok(());
    }
    // The file must be read to maintain the index; no store to read it means
    // the index would silently miss these rows.
    let data_store = data_store.ok_or_else(|| {
        Error::Constraint(format!(
            "data file {} on indexed table {} cannot be read to maintain its equality index: no \
             data-path store is available",
            file.data_file_id, file.table_id
        ))
    })?;
    let path = data_file_object_path(base, &file, data_prefix)?;
    let per_index =
        per_index_scoped_entries(base, &indexes, table, data_store, &file, &path).await?;

    for (index, scoped) in indexes.iter().zip(per_index) {
        push_index_entries(entries, index, scoped, false)?;
    }
    Ok(())
}

/// One scoped read of `file` covering every index's columns at once — the
/// footer and any shared column chunks are fetched a single time — split
/// back into per-index entry lists, ordered as `indexes`.
pub(super) async fn per_index_scoped_entries(
    base: &CatalogSnapshot,
    indexes: &[IndexInfo],
    table: TableId,
    data_store: &Arc<dyn ObjectStore>,
    file: &proto::DataFileValue,
    path: &object_store::path::Path,
) -> Result<Vec<Vec<ScopedReadEntry>>> {
    let live_columns = base.columns_of(table);
    let mut all_positions = Vec::new();
    let mut spans = Vec::with_capacity(indexes.len());
    for index in indexes {
        let positions = index_positions(&live_columns, index, table)?;
        spans.push((all_positions.len(), positions.len()));
        all_positions.extend(positions);
    }

    // Values come back ordered exactly as `all_positions`, so each index's
    // slice of a row's values is its own columns in its own order.
    let scoped = scoped_read::scoped_read_entries(
        Arc::clone(data_store),
        path,
        &all_positions,
        scoped_read::RowIdSource::Resolve {
            row_id_start: file.row_id_start,
        },
        Some(file.file_size_bytes),
    )
    .await?;

    Ok(spans
        .into_iter()
        .map(|(start, len)| {
            scoped
                .iter()
                .map(|entry| ScopedReadEntry {
                    row_id: entry.row_id,
                    values: entry.values[start..start + len].to_vec(),
                })
                .collect()
        })
        .collect())
}

pub(super) async fn stage_index_maintenance(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    data_store: Option<&Arc<dyn ObjectStore>>,
    data_prefix: &str,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    let pending_schemas = pending_inline_schemas(ops);

    let mut entries: Vec<StagedIndexEntry> = Vec::new();
    // Rows this commit kills, grouped by where their values must be read
    // from: an inline chunk, or a position range of one data file.
    let mut inline_deletes: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut file_deletes: HashMap<(u64, u64), KilledRows> = HashMap::new();

    for op in ops {
        match op {
            RowOperation::Insert {
                table: TableKind::DataFile,
                cells,
            } => {
                stage_data_file_index_entries(base, cells, data_store, data_prefix, &mut entries)
                    .await?;
            }
            RowOperation::Insert {
                table: TableKind::DeleteFile,
                cells,
            } => {
                collect_delete_file_rows(base, cells, data_store, data_prefix, &mut file_deletes)
                    .await?;
            }
            RowOperation::InlineInsert {
                table_id,
                schema_version,
                row_id_start,
                row_count,
                arrow_body,
                ..
            } => {
                stage_inline_chunk_entries(
                    db_tx,
                    base,
                    &pending_schemas,
                    *table_id,
                    &InlineChunk {
                        schema_version: *schema_version,
                        row_id_start: *row_id_start,
                        row_count: *row_count,
                        body: arrow_body,
                    },
                    None,
                    &mut entries,
                )
                .await?;
            }
            RowOperation::InlineInlineDelete {
                table_id, row_id, ..
            } => {
                inline_deletes.entry(*table_id).or_default().push(*row_id);
            }
            RowOperation::InlineFileDelete {
                table_id,
                data_file_id,
                row_id: position,
                ..
            } => {
                // An inlined file-delete names a physical position in the
                // file, exactly as a delete file's `pos` does.
                file_deletes
                    .entry((*table_id, *data_file_id))
                    .or_default()
                    .insert_position(*position);
            }
            _ => {}
        }
    }

    for (table_id, row_ids) in &inline_deletes {
        stage_inline_delete_entries(
            db_tx,
            base,
            ops,
            &pending_schemas,
            *table_id,
            row_ids,
            &mut entries,
        )
        .await?;
    }
    for ((table_id, data_file_id), killed) in &file_deletes {
        stage_file_delete_entries(
            base,
            *table_id,
            *data_file_id,
            killed,
            data_store,
            data_prefix,
            &mut entries,
        )
        .await?;
    }

    if entries.is_empty() {
        return Ok(());
    }
    stage_index_entries(ReadHandle::Tx(db_tx), &entries, writes).await
}

/// The inline schemas this commit registers, for a chunk whose
/// `inline/schema` record is not committed yet (the first insert).
pub(super) fn pending_inline_schemas(ops: &[RowOperation]) -> HashMap<(u64, u64), &[u8]> {
    ops.iter()
        .filter_map(|op| match op {
            RowOperation::InlineSchema {
                table_id,
                schema_version,
                arrow_schema,
            } => Some(((*table_id, *schema_version), arrow_schema.as_slice())),
            _ => None,
        })
        .collect()
}

/// One chunk version's Arrow IPC schema: this commit's staged record if it
/// registered one, else the committed one.
pub(super) async fn inline_schema_for<'a>(
    db_tx: &DbTransaction,
    pending_schemas: &HashMap<(u64, u64), &'a [u8]>,
    table_id: u64,
    schema_version: u64,
) -> Result<std::borrow::Cow<'a, [u8]>> {
    if let Some(bytes) = pending_schemas.get(&(table_id, schema_version)) {
        return Ok(std::borrow::Cow::Borrowed(bytes));
    }
    let stored = store_inline::read_inline_schema(ReadHandle::Tx(db_tx), table_id, schema_version)
        .await?
        .ok_or_else(|| {
            Error::Corruption(format!(
                "no inline schema for table {table_id} version {schema_version}"
            ))
        })?;
    Ok(std::borrow::Cow::Owned(stored.arrow_schema))
}

/// Appends the index entries for one inline chunk's rows: every row when
/// `held` is `None` (the chunk is being inserted), or just those rows when
/// it is `Some` (they are being deleted, so their entries are removals).
pub(super) async fn stage_inline_chunk_entries(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    pending_schemas: &HashMap<(u64, u64), &[u8]>,
    table_id: u64,
    chunk: &InlineChunk<'_>,
    held: Option<&HashSet<u64>>,
    entries: &mut Vec<StagedIndexEntry>,
) -> Result<()> {
    let table = TableId::new(table_id);
    let indexes = base.indexes_of(table);
    if indexes.is_empty() {
        return Ok(());
    }

    let schema_ipc =
        inline_schema_for(db_tx, pending_schemas, table_id, chunk.schema_version).await?;
    let live_columns = base.columns_of(table);
    for index in &indexes {
        let positions = index_positions(&live_columns, index, table)?;
        let scoped = scoped_read::inline_batch_entries(
            &schema_ipc,
            chunk.body,
            &positions,
            chunk.row_id_start,
        )?
        .into_iter()
        .filter(|entry| held.is_none_or(|rows| rows.contains(&entry.row_id)))
        .collect();
        push_index_entries(entries, index, scoped, held.is_some())?;
    }
    Ok(())
}

/// One inline chunk this commit can read: already committed, or staged by
/// the commit itself.
pub(super) struct InlineChunk<'a> {
    schema_version: u64,
    row_id_start: u64,
    row_count: u64,
    body: &'a [u8],
}

impl InlineChunk<'_> {
    fn holds(&self, row_id: u64) -> bool {
        row_id >= self.row_id_start && row_id < self.row_id_start.saturating_add(self.row_count)
    }
}

/// Derives the entry removals for rows tombstoned out of inline chunks. A
/// tombstoned row's indexed values come from the chunk holding it, so the
/// removal rides the same batch that kills the row.
pub(super) async fn stage_inline_delete_entries(
    db_tx: &DbTransaction,
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    pending_schemas: &HashMap<(u64, u64), &[u8]>,
    table_id: u64,
    row_ids: &[u64],
    entries: &mut Vec<StagedIndexEntry>,
) -> Result<()> {
    let table = TableId::new(table_id);
    let indexes = base.indexes_of(table);
    if indexes.is_empty() {
        return Ok(());
    }

    let committed = store_inline::scan_inline_chunks(ReadHandle::Tx(db_tx), table_id).await?;
    let mut chunks: Vec<InlineChunk<'_>> = committed
        .iter()
        .filter_map(|(op, value)| match op {
            InlineOperation::Insert {
                schema_version: version,
                ..
            } => Some(InlineChunk {
                schema_version: *version,
                row_id_start: value.row_id_start,
                row_count: value.row_count,
                body: &value.body,
            }),
            _ => None,
        })
        .collect();
    // A row inserted and deleted inside one commit lives in a chunk that is
    // still only staged.
    chunks.extend(ops.iter().filter_map(|op| match op {
        RowOperation::InlineInsert {
            table_id: owner,
            schema_version,
            row_id_start,
            row_count,
            arrow_body,
            ..
        } if *owner == table_id => Some(InlineChunk {
            schema_version: *schema_version,
            row_id_start: *row_id_start,
            row_count: *row_count,
            body: arrow_body,
        }),
        _ => None,
    }));

    let mut covered: HashSet<u64> = HashSet::new();
    for chunk in &chunks {
        let held: HashSet<u64> = row_ids
            .iter()
            .copied()
            .filter(|&row_id| chunk.holds(row_id))
            .collect();
        if held.is_empty() {
            continue;
        }
        stage_inline_chunk_entries(
            db_tx,
            base,
            pending_schemas,
            table_id,
            chunk,
            Some(&held),
            entries,
        )
        .await?;
        covered.extend(held);
    }

    // A tombstone naming no live chunk would leave its entries behind, which
    // is the leak this derivation exists to prevent.
    if let Some(missing) = row_ids.iter().find(|row_id| !covered.contains(row_id)) {
        return Err(Error::Corruption(format!(
            "inline delete of row {missing} on indexed table {table_id} names no inline chunk, so \
             its index entries cannot be derived"
        )));
    }
    Ok(())
}

/// The physical row positions a commit kills inside one data file. Both a
/// delete file's `pos` column and an inlined file-delete name positions,
/// not row ids; the target's scoped read resolves each position to the row
/// it holds.
#[derive(Debug, Default)]
pub(super) struct KilledRows {
    positions: HashSet<u64>,
}

impl KilledRows {
    /// Records a killed physical row position.
    pub(super) fn insert_position(&mut self, position: u64) {
        self.positions.insert(position);
    }
}

/// Records the positions a staged `register_delete_file` kills, read out
/// of the delete file verbatim — resolving them to row ids would bake in
/// the dense assumption the target's scoped read decides.
pub(super) async fn collect_delete_file_rows(
    base: &CatalogSnapshot,
    cells: &[Cell],
    data_store: Option<&Arc<dyn ObjectStore>>,
    data_prefix: &str,
    file_deletes: &mut HashMap<(u64, u64), KilledRows>,
) -> Result<()> {
    let delete_file = decode_delete_file(cells)?;
    let table = TableId::new(delete_file.table_id);
    if base.indexes_of(table).is_empty() {
        return Ok(());
    }

    let data_store = data_store.ok_or_else(|| {
        Error::Constraint(format!(
            "delete file {} on indexed table {} cannot be read to maintain its equality index: no \
             data-path store is available",
            delete_file.delete_file_id, delete_file.table_id
        ))
    })?;

    let path = delete_file_object_path(base, &delete_file, data_prefix)?;
    let positions = scoped_read::delete_file_positions(data_store, &path).await?;
    file_deletes
        .entry((delete_file.table_id, delete_file.data_file_id))
        .or_default()
        .positions
        .extend(positions);
    Ok(())
}

/// Derives the entry removals for rows killed inside one data file, by
/// scoped-reading the file and keeping the rows this commit marks dead.
pub(super) async fn stage_file_delete_entries(
    base: &CatalogSnapshot,
    table_id: u64,
    data_file_id: u64,
    killed: &KilledRows,
    data_store: Option<&Arc<dyn ObjectStore>>,
    data_prefix: &str,
    entries: &mut Vec<StagedIndexEntry>,
) -> Result<()> {
    let table = TableId::new(table_id);
    let indexes = base.indexes_of(table);
    if indexes.is_empty() {
        return Ok(());
    }

    let file = live_data_file(base, table_id, data_file_id)?;
    let data_store = data_store.ok_or_else(|| {
        Error::Constraint(format!(
            "data file {data_file_id} on indexed table {table_id} cannot be read to maintain its \
             equality index: no data-path store is available"
        ))
    })?;

    // Positions are physical row ordinals read out of the delete file; one
    // naming a row the target does not hold could never match a scoped
    // entry and would silently orphan index rows, so refuse it here.
    for &position in &killed.positions {
        if position >= file.record_count {
            return Err(Error::Constraint(format!(
                "delete file for data file {data_file_id} on table {table_id} names position \
                 {position} outside the file's record count {}",
                file.record_count
            )));
        }
    }

    let path = data_file_object_path(base, &file, data_prefix)?;
    let per_index =
        per_index_scoped_entries(base, &indexes, table, data_store, &file, &path).await?;
    // An entry dies when a delete names its physical position; the scoped
    // read resolves that position to the row it holds — one rule for dense
    // and per-row-id targets alike.
    for (index, scoped) in indexes.iter().zip(per_index) {
        let scoped = scoped
            .into_iter()
            .enumerate()
            .filter(|(ordinal, _)| killed.positions.contains(&(*ordinal as u64)))
            .map(|(_, entry)| entry)
            .collect();
        push_index_entries(entries, index, scoped, true)?;
    }
    Ok(())
}

/// One live data file of a table.
pub(super) fn live_data_file(
    base: &CatalogSnapshot,
    table_id: u64,
    data_file_id: u64,
) -> Result<proto::DataFileValue> {
    base.data_files
        .get(&table_id)
        .and_then(|files| files.get(&data_file_id))
        .cloned()
        .ok_or_else(|| Error::NotFound(format!("data file {data_file_id} of table {table_id}")))
}

/// The object path of a registered data file: its stored path relative to the
/// table's data directory (`<schema path><table path>`) under `DATA_PATH`,
/// whose bucket-relative prefix leads for an `s3://` store.
pub(super) fn data_file_object_path(
    base: &CatalogSnapshot,
    file: &proto::DataFileValue,
    data_prefix: &str,
) -> Result<object_store::path::Path> {
    table_object_path(
        base,
        file.table_id,
        &file.path,
        file.path_is_relative,
        data_prefix,
    )
}

/// The object path of a registered delete file, resolved exactly as its
/// target data file's is.
pub(super) fn delete_file_object_path(
    base: &CatalogSnapshot,
    file: &proto::DeleteFileValue,
    data_prefix: &str,
) -> Result<object_store::path::Path> {
    table_object_path(
        base,
        file.table_id,
        &file.path,
        file.path_is_relative,
        data_prefix,
    )
}

pub(super) fn table_object_path(
    base: &CatalogSnapshot,
    table_id: u64,
    path: &str,
    path_is_relative: bool,
    data_prefix: &str,
) -> Result<object_store::path::Path> {
    // A missing table here is corruption, not a caller mistake: the file
    // row itself named it.
    let table_prefix = base
        .table_data_prefix(TableId::new(table_id))
        .map_err(|err| match err {
            Error::NotFound(_) => {
                Error::Corruption(format!("registered file names unknown table {table_id}"))
            }
            other => other,
        })?;
    let relative = match (path_is_relative, data_prefix.is_empty()) {
        (false, _) => path.to_owned(),
        (true, true) => format!("{table_prefix}{path}"),
        (true, false) => format!("{data_prefix}/{table_prefix}{path}"),
    };
    Ok(object_store::path::Path::from(relative.as_str()))
}

/// The physical positions of an index's columns in a file or chunk written
/// under the current schema: each column's 0-based rank among the table's
/// columns (the order `columns_of` returns).
pub(super) fn index_positions(
    live_columns: &[ColumnInfo],
    index: &IndexInfo,
    table: TableId,
) -> Result<Vec<usize>> {
    index
        .columns
        .iter()
        .map(|column| {
            live_columns
                .iter()
                .position(|c| c.id == *column)
                .ok_or_else(|| Error::NotFound(format!("indexed column {column} of table {table}")))
        })
        .collect()
}

/// Turns scoped-read entries into staged index entries — puts when `delete`
/// is false, removals when it is true. A row with a NULL indexed value is
/// stored multi-shaped (so `IS NULL` finds it) rather than skipped.
pub(super) fn push_index_entries(
    entries: &mut Vec<StagedIndexEntry>,
    index: &IndexInfo,
    scoped: Vec<ScopedReadEntry>,
    delete: bool,
) -> Result<()> {
    for entry in scoped {
        // A row with any NULL indexed column is stored so `IS NULL` finds it,
        // but multi-shaped and collision-exempt — a unique index still admits
        // any number of NULL rows.
        let has_null = entry.values.iter().any(Option::is_none);
        entries.push(StagedIndexEntry {
            index_id: index.id.get(),
            unique: index.unique && !has_null,
            key: encode_ordered_values(&entry.values, &index.directions, &index.nulls)?,
            row_id: entry.row_id,
            delete,
        });
    }
    Ok(())
}
