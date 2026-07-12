//! Typed reads over an open transaction: decode keys and values into the
//! wire types. No interpretation — the domain layer owns meaning.

use slatedb::DbTransaction;

use crate::{
    error::{Error, Result},
    store::{
        key::{CurKey, EntityKey, Key, Subspace, SysKey, subspace_prefix},
        proto::{
            ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue, FormatValue,
            HeadValue, MigrationValue, OptionScopeValue, SchemaValue, SnapshotValue,
            TableColumnStatsValue, TableStatsValue, TableValue, ViewValue,
        },
        value,
    },
};

/// A decoded entity record of a kind the catalog currently models.
/// Reading a kind outside this set fails loudly instead of being silently
/// dropped: the store contains state this binary does not understand.
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
}

async fn read_singleton<M: prost::Message + Default>(
    tx: &DbTransaction,
    key: Key,
) -> Result<Option<M>> {
    match tx.get(key.encode()).await.map_err(Error::from)? {
        Some(bytes) => Ok(Some(value::decode_value(&bytes)?)),
        None => Ok(None),
    }
}

/// The layout-format stamp, if the store has been initialized.
pub(crate) async fn read_format(tx: &DbTransaction) -> Result<Option<FormatValue>> {
    read_singleton(tx, Key::Sys(SysKey::Format)).await
}

/// The structural-migration marker, present only mid-migration.
pub(crate) async fn read_migration(tx: &DbTransaction) -> Result<Option<MigrationValue>> {
    read_singleton(tx, Key::Sys(SysKey::Migration)).await
}

/// The head pointer: the latest committed snapshot id.
pub(crate) async fn read_head(tx: &DbTransaction) -> Result<Option<HeadValue>> {
    read_singleton(tx, Key::Sys(SysKey::Head)).await
}

/// One snapshot record.
pub(crate) async fn read_snapshot(
    tx: &DbTransaction,
    snapshot_id: u64,
) -> Result<Option<SnapshotValue>> {
    read_singleton(tx, Key::Snap { snapshot_id }).await
}

/// Every committed snapshot record (`ducklake_snapshot` +
/// `ducklake_snapshot_changes`, merged), in key order — snapshots are
/// append-only, so there is no cur/hist split to scan separately.
pub(crate) async fn scan_snapshots(tx: &DbTransaction) -> Result<Vec<SnapshotValue>> {
    let mut iter = tx
        .scan_prefix(subspace_prefix(Subspace::Snap), ..)
        .await
        .map_err(Error::from)?;
    let mut records = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        match Key::decode(&entry.key)? {
            Key::Snap { .. } => records.push(value::decode_value(&entry.value)?),
            other => {
                return Err(Error::Corruption(format!(
                    "non-snap key in snap scan: {other:?}"
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
        other => Err(Error::Corruption(format!(
            "entity kind not modeled by this binary: {other:?}"
        ))),
    }
}

/// Every live entity record.
pub(crate) async fn scan_cur_entities(tx: &DbTransaction) -> Result<Vec<EntityRecord>> {
    let mut iter = tx
        .scan_prefix(subspace_prefix(Subspace::Cur), ..)
        .await
        .map_err(Error::from)?;
    let mut records = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        match Key::decode(&entry.key)? {
            Key::Cur(CurKey::Entity(entity)) => {
                records.push(decode_entity(entity, &entry.value)?);
            }
            // Gc-file bookkeeping has no catalog meaning — unlike an
            // unrecognized kind, it is skipped by design, not because
            // this binary fails to understand it.
            Key::Cur(CurKey::GcFile { .. }) => {}
            other => {
                return Err(Error::Corruption(format!(
                    "non-cur key in cur scan: {other:?}"
                )));
            }
        }
    }
    Ok(records)
}

/// Every ended entity-version record.
pub(crate) async fn scan_hist_entities(tx: &DbTransaction) -> Result<Vec<EntityRecord>> {
    let mut iter = tx
        .scan_prefix(subspace_prefix(Subspace::Hist), ..)
        .await
        .map_err(Error::from)?;
    let mut records = Vec::new();
    while let Some(entry) = iter.next().await.map_err(Error::from)? {
        match Key::decode(&entry.key)? {
            Key::Hist(hist) => records.push(decode_entity(hist.entity, &entry.value)?),
            other => {
                return Err(Error::Corruption(format!(
                    "non-hist key in hist scan: {other:?}"
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
            Key::cur(EntityKey::Schema { schema_id: 1 }).encode(),
            value::encode_value(&schema),
        )
        .unwrap();
        tx.put(
            Key::hist(EntityKey::Schema { schema_id: 0 }, 2).encode(),
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
            Key::cur(EntityKey::TableStats { table_id: 7 }).encode(),
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
            row_id_start: 0,
            partition_id: None,
            encryption_key: None,
            mapping_id: None,
            partial_max: None,
            partition_values: vec![],
        };
        tx.put(
            Key::cur(EntityKey::File {
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
            Key::cur(EntityKey::View { view_id: 4 }).encode(),
            value::encode_value(&view),
        )
        .unwrap();

        let mut options = std::collections::HashMap::new();
        options.insert("key1".into(), "value1".into());
        let option = OptionScopeValue { options };
        tx.put(
            Key::cur(EntityKey::Option {
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
        assert_eq!(read_head(&tx).await.unwrap(), Some(head));
        assert_eq!(read_format(&tx).await.unwrap(), None);
        assert_eq!(read_migration(&tx).await.unwrap(), None);
        assert_eq!(read_snapshot(&tx, 0).await.unwrap(), None);

        let cur = scan_cur_entities(&tx).await.unwrap();
        assert_eq!(cur.len(), 5);
        assert!(cur.contains(&EntityRecord::Schema(schema)));
        assert!(cur.contains(&EntityRecord::File(file)));
        assert!(cur.contains(&EntityRecord::TableStats(tstat)));
        assert!(cur.contains(&EntityRecord::View(view)));
        assert!(cur.contains(&EntityRecord::Option {
            scope_kind: 0,
            scope_id: 0,
            value: option,
        }));
        let hist = scan_hist_entities(&tx).await.unwrap();
        assert_eq!(hist, vec![EntityRecord::Schema(ended)]);
        tx.rollback();
        db.close().await.unwrap();
    }
}
