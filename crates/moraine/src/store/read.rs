//! Typed reads over an open transaction: decode keys and values into the
//! wire types. No interpretation — the domain layer owns meaning.

use crate::{
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{CurrentKey, EntityKey, Key, Subspace, SysKey, subspace_prefix},
        proto::{
            ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue, FormatValue,
            GcFileValue, HeadValue, MacroValue, MigrationValue, OptionScopeValue, PartitionValue,
            SchemaValue, SnapshotValue, SortValue, TableColumnStatsValue, TableStatsValue,
            TableValue, TagValue, ViewValue,
        },
        value,
    },
};

/// A decoded entity record of a kind the catalog currently models.
/// Reading a kind outside this set fails loudly rather than dropping it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EntityRecord {
    /// A schema record.
    Schema(SchemaValue),
    /// A table record.
    Table(TableValue),
    /// A view record.
    View(ViewValue),
    /// A column record.
    Column(ColumnValue),
    /// A data file record.
    File(DataFileValue),
    /// A delete file record.
    DeleteFile(DeleteFileValue),
    /// A partition spec record.
    Partition(PartitionValue),
    /// A sort spec record.
    Sort(SortValue),
    /// A macro record.
    Macro(MacroValue),
    /// File-level column statistics record.
    FileColumnStats(FileColumnStatsValue),
    /// Table-level statistics record.
    TableStats(TableStatsValue),
    /// Table-level column statistics record.
    TableColumnStats(TableColumnStatsValue),
    /// An option-scope record; the scope lives in the key, not the value.
    Option {
        /// Scope kind: global = 0, schema = 1, table = 2.
        scope_kind: u64,
        /// Scope id (0 for global).
        scope_id: u64,
        /// The scope's options map.
        value: OptionScopeValue,
    },
    /// A tag container: one record per tagged object, entries embedded.
    Tag(TagValue),
    /// A `ducklake_files_scheduled_for_deletion` row — `current`-only
    /// bookkeeping, not a temporal entity.
    GcFile(GcFileValue),
}

async fn read_singleton<M: prost::Message + Default>(
    tx: ReadHandle<'_>,
    key: Key,
) -> Result<Option<M>> {
    tx.get(key.encode())
        .await
        .map_err(Error::from)?
        .map(|bytes| value::decode_value(&bytes))
        .transpose()
}

/// The layout-format stamp, if the store has been initialized.
pub(crate) async fn read_format(tx: ReadHandle<'_>) -> Result<Option<FormatValue>> {
    read_singleton(tx, Key::Sys(SysKey::Format)).await
}

/// The structural-migration marker, present only mid-migration.
pub(crate) async fn read_migration(tx: ReadHandle<'_>) -> Result<Option<MigrationValue>> {
    read_singleton(tx, Key::Sys(SysKey::Migration)).await
}

/// The head pointer: the latest committed snapshot id.
pub(crate) async fn read_head(tx: ReadHandle<'_>) -> Result<Option<HeadValue>> {
    read_singleton(tx, Key::Sys(SysKey::Head)).await
}

/// One snapshot record.
pub(crate) async fn read_snapshot(
    tx: ReadHandle<'_>,
    snapshot_id: u64,
) -> Result<Option<SnapshotValue>> {
    read_singleton(tx, Key::Snapshot { snapshot_id }).await
}

/// Every committed snapshot record (`ducklake_snapshot` +
/// `ducklake_snapshot_changes`, merged), in key order.
pub(crate) async fn scan_snapshots(tx: ReadHandle<'_>) -> Result<Vec<SnapshotValue>> {
    let mut iter = tx
        .scan_prefix(subspace_prefix(Subspace::Snapshot), ..)
        .await
        .map_err(Error::from)?;
    let mut records = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        match Key::decode(&entry.key)? {
            Key::Snapshot { .. } => records.push(value::decode_value(&entry.value)?),
            other => {
                return Err(Error::Corruption(format!(
                    "non-snapshot key in snapshot scan: {other:?}"
                )));
            }
        }
    }

    Ok(records)
}

fn decode_entity(entity: EntityKey, bytes: &[u8]) -> Result<EntityRecord> {
    match entity {
        EntityKey::Schema { .. } => Ok(EntityRecord::Schema(value::decode_value(bytes)?)),
        EntityKey::Table { .. } => Ok(EntityRecord::Table(value::decode_value(bytes)?)),
        EntityKey::View { .. } => Ok(EntityRecord::View(value::decode_value(bytes)?)),
        EntityKey::Column { .. } => Ok(EntityRecord::Column(value::decode_value(bytes)?)),
        EntityKey::File { .. } => Ok(EntityRecord::File(value::decode_value(bytes)?)),
        EntityKey::DeleteFile { .. } => Ok(EntityRecord::DeleteFile(value::decode_value(bytes)?)),
        EntityKey::Partition { .. } => Ok(EntityRecord::Partition(value::decode_value(bytes)?)),
        EntityKey::Sort { .. } => Ok(EntityRecord::Sort(value::decode_value(bytes)?)),
        EntityKey::Macro { .. } => Ok(EntityRecord::Macro(value::decode_value(bytes)?)),
        EntityKey::FileColumnStats { .. } => {
            Ok(EntityRecord::FileColumnStats(value::decode_value(bytes)?))
        }
        EntityKey::TableStats { .. } => Ok(EntityRecord::TableStats(value::decode_value(bytes)?)),
        EntityKey::TableColumnStats { .. } => {
            Ok(EntityRecord::TableColumnStats(value::decode_value(bytes)?))
        }
        EntityKey::Option {
            scope_kind,
            scope_id,
        } => Ok(EntityRecord::Option {
            scope_kind,
            scope_id,
            value: value::decode_value(bytes)?,
        }),
        EntityKey::Tag { .. } => Ok(EntityRecord::Tag(value::decode_value(bytes)?)),
    }
}

/// Every live entity record.
pub(crate) async fn scan_current_entities(tx: ReadHandle<'_>) -> Result<Vec<EntityRecord>> {
    let mut iter = tx
        .scan_prefix(subspace_prefix(Subspace::Current), ..)
        .await
        .map_err(Error::from)?;
    let mut records = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        match Key::decode(&entry.key)? {
            Key::Current(CurrentKey::Entity(entity)) => {
                records.push(decode_entity(entity, &entry.value)?);
            }
            Key::Current(CurrentKey::GcFile { .. }) => {
                records.push(EntityRecord::GcFile(value::decode_value(&entry.value)?));
            }
            other => {
                return Err(Error::Corruption(format!(
                    "non-current key in current scan: {other:?}"
                )));
            }
        }
    }
    Ok(records)
}

/// Every ended entity-version record. Unversioned kinds (statistics,
/// options, tags) are overwritten in place and never mirrored to history;
/// finding one there is store damage, refused rather than replayed over
/// the live record.
pub(crate) async fn scan_history_entities(tx: ReadHandle<'_>) -> Result<Vec<EntityRecord>> {
    let mut iter = tx
        .scan_prefix(subspace_prefix(Subspace::History), ..)
        .await
        .map_err(Error::from)?;
    let mut records = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        match Key::decode(&entry.key)? {
            Key::History(history) => {
                if matches!(
                    history.entity,
                    EntityKey::FileColumnStats { .. }
                        | EntityKey::TableStats { .. }
                        | EntityKey::TableColumnStats { .. }
                        | EntityKey::Option { .. }
                        | EntityKey::Tag { .. }
                ) {
                    return Err(Error::Corruption(format!(
                        "unversioned key in history scan: {:?}",
                        history.entity
                    )));
                }
                records.push(decode_entity(history.entity, &entry.value)?);
            }
            other => {
                return Err(Error::Corruption(format!(
                    "non-history key in history scan: {other:?}"
                )));
            }
        }
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::{IsolationLevel, config::WriteOptions};

    use super::*;
    use crate::store::open::open_store;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn reads_decode_what_was_written() {
        let db = open_store("t", Arc::new(InMemory::new())).await.unwrap();

        let head = HeadValue { snapshot_id: 3 };
        let schema = SchemaValue {
            schema_id: 1,
            schema_uuid: "u".into(),
            begin_snapshot: 1,
            end_snapshot: None,
            schema_name: "main".into(),
            path: "main/".into(),
            path_is_relative: true,
        };
        let ended = SchemaValue {
            schema_id: 0,
            end_snapshot: Some(2),
            ..schema.clone()
        };

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(Key::Sys(SysKey::Head).encode(), value::encode_value(&head))
            .unwrap();
        tx.put(
            Key::current(EntityKey::Schema { schema_id: 1 }).encode(),
            value::encode_value(&schema),
        )
        .unwrap();
        tx.put(
            Key::history(EntityKey::Schema { schema_id: 0 }, 2).encode(),
            value::encode_value(&ended),
        )
        .unwrap();
        let tstat = TableStatsValue {
            table_id: 7,
            record_count: 10,
            next_row_id: 10,
            file_size_bytes: 1024,
        };
        tx.put(
            Key::current(EntityKey::TableStats { table_id: 7 }).encode(),
            value::encode_value(&tstat),
        )
        .unwrap();
        let file = DataFileValue {
            data_file_id: 3,
            table_id: 7,
            begin_snapshot: 1,
            end_snapshot: None,
            file_order: None,
            path: "f.parquet".into(),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: 10,
            file_size_bytes: 1024,
            footer_size: 64,
            row_id_start: Some(0),
            partition_id: None,
            encryption_key: None,
            mapping_id: None,
            partial_max: None,
            partition_values: vec![],
        };
        tx.put(
            Key::current(EntityKey::File {
                table_id: 7,
                data_file_id: 3,
            })
            .encode(),
            value::encode_value(&file),
        )
        .unwrap();

        let view = ViewValue {
            view_id: 4,
            view_uuid: "uv".into(),
            begin_snapshot: 1,
            end_snapshot: None,
            schema_id: 1,
            view_name: "v".into(),
            dialect: "duckdb".into(),
            sql: "SELECT 1".into(),
            column_aliases: None,
        };
        tx.put(
            Key::current(EntityKey::View { view_id: 4 }).encode(),
            value::encode_value(&view),
        )
        .unwrap();

        let mut options = std::collections::HashMap::new();
        options.insert("key1".into(), "value1".into());
        let option = OptionScopeValue { options };
        tx.put(
            Key::current(EntityKey::Option {
                scope_kind: 0,
                scope_id: 0,
            })
            .encode(),
            value::encode_value(&option),
        )
        .unwrap();

        tx.commit_with_options(&WriteOptions {
            await_durable: true,
            ..Default::default()
        })
        .await
        .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert_eq!(read_head(ReadHandle::Tx(&tx)).await.unwrap(), Some(head));
        assert_eq!(read_format(ReadHandle::Tx(&tx)).await.unwrap(), None);
        assert_eq!(read_migration(ReadHandle::Tx(&tx)).await.unwrap(), None);
        assert_eq!(read_snapshot(ReadHandle::Tx(&tx), 0).await.unwrap(), None);

        let current = scan_current_entities(ReadHandle::Tx(&tx)).await.unwrap();
        assert_eq!(current.len(), 5);
        assert!(current.contains(&EntityRecord::Schema(schema)));
        assert!(current.contains(&EntityRecord::File(file)));
        assert!(current.contains(&EntityRecord::TableStats(tstat)));
        assert!(current.contains(&EntityRecord::View(view)));
        assert!(current.contains(&EntityRecord::Option {
            scope_kind: 0,
            scope_id: 0,
            value: option,
        }));
        let history = scan_history_entities(ReadHandle::Tx(&tx)).await.unwrap();
        assert_eq!(history, vec![EntityRecord::Schema(ended)]);
        tx.rollback();
        db.close().await.unwrap();
    }

    /// Unversioned kinds are never written to history; one found there is
    /// store damage. Refusing it keeps the later current-then-history
    /// replay from silently overwriting the live record.
    #[tokio::test]
    async fn unversioned_kind_in_history_is_refused() {
        let db = open_store("t", Arc::new(InMemory::new())).await.unwrap();

        let stats = TableStatsValue {
            table_id: 7,
            record_count: 10,
            next_row_id: 10,
            file_size_bytes: 1024,
        };
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            Key::history(EntityKey::TableStats { table_id: 7 }, 2).encode(),
            value::encode_value(&stats),
        )
        .unwrap();
        tx.commit_with_options(&WriteOptions {
            await_durable: true,
            ..Default::default()
        })
        .await
        .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let err = scan_history_entities(ReadHandle::Tx(&tx))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        tx.rollback();
        db.close().await.unwrap();
    }
}
