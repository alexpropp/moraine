//! Typed reads over an open transaction: decode keys and values into the
//! wire types. No interpretation — the domain layer owns meaning.

use slatedb::DbTransaction;

use crate::{
    error::{Error, Result},
    store::{
        key::{CurKey, EntityKey, Key, Subspace, SysKey, subspace_prefix},
        proto, value,
    },
};

/// A decoded entity record of a kind the catalog currently models.
/// Reading a kind outside this set fails loudly instead of being silently
/// dropped: the store contains state this binary does not understand.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EntityRecord {
    /// A schema record.
    Schema(proto::SchemaValue),
    /// A table record.
    Table(proto::TableValue),
    /// A column record.
    Column(proto::ColumnValue),
}

async fn read_singleton<M: prost::Message + Default>(
    txn: &DbTransaction,
    key: Key,
) -> Result<Option<M>> {
    match txn.get(key.encode()).await.map_err(Error::from)? {
        Some(bytes) => Ok(Some(value::decode_value(&bytes)?)),
        None => Ok(None),
    }
}

/// The layout-format stamp, if the store has been initialized.
pub(crate) async fn read_format(txn: &DbTransaction) -> Result<Option<proto::FormatValue>> {
    read_singleton(txn, Key::Sys(SysKey::Format)).await
}

/// The structural-migration marker, present only mid-migration.
pub(crate) async fn read_migration(txn: &DbTransaction) -> Result<Option<proto::MigrationValue>> {
    read_singleton(txn, Key::Sys(SysKey::Migration)).await
}

/// The head pointer: the latest committed snapshot id.
pub(crate) async fn read_head(txn: &DbTransaction) -> Result<Option<proto::HeadValue>> {
    read_singleton(txn, Key::Sys(SysKey::Head)).await
}

/// One snapshot record.
pub(crate) async fn read_snapshot(
    txn: &DbTransaction,
    snapshot_id: u64,
) -> Result<Option<proto::SnapshotValue>> {
    read_singleton(txn, Key::Snap { snapshot_id }).await
}

fn decode_entity(entity: EntityKey, bytes: &[u8]) -> Result<EntityRecord> {
    match entity {
        EntityKey::Schema { .. } => Ok(EntityRecord::Schema(value::decode_value(bytes)?)),
        EntityKey::Table { .. } => Ok(EntityRecord::Table(value::decode_value(bytes)?)),
        EntityKey::Column { .. } => Ok(EntityRecord::Column(value::decode_value(bytes)?)),
        other => Err(Error::Corruption(format!(
            "entity kind not modeled by this binary: {other:?}"
        ))),
    }
}

/// Every live entity record.
pub(crate) async fn scan_cur_entities(txn: &DbTransaction) -> Result<Vec<EntityRecord>> {
    let mut iter = txn
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
pub(crate) async fn scan_hist_entities(txn: &DbTransaction) -> Result<Vec<EntityRecord>> {
    let mut iter = txn
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
    async fn reads_decode_what_was_written() {
        let db = open_store("t", Arc::new(InMemory::new())).await.unwrap();

        let head = proto::HeadValue { snapshot_id: 3 };
        let schema = proto::SchemaValue {
            schema_id: 1,
            schema_uuid: "u".into(),
            begin_snapshot: 1,
            end_snapshot: None,
            schema_name: "main".into(),
            path: "main/".into(),
            path_is_relative: true,
        };
        let ended = proto::SchemaValue {
            schema_id: 0,
            end_snapshot: Some(2),
            ..schema.clone()
        };

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        txn.put(Key::Sys(SysKey::Head).encode(), value::encode_value(&head))
            .unwrap();
        txn.put(
            Key::cur(EntityKey::Schema { schema_id: 1 }).encode(),
            value::encode_value(&schema),
        )
        .unwrap();
        txn.put(
            Key::hist(EntityKey::Schema { schema_id: 0 }, 2).encode(),
            value::encode_value(&ended),
        )
        .unwrap();
        txn.commit_with_options(&WriteOptions {
            await_durable: true,
            ..Default::default()
        })
        .await
        .unwrap();

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert_eq!(read_head(&txn).await.unwrap(), Some(head));
        assert_eq!(read_format(&txn).await.unwrap(), None);
        assert_eq!(read_migration(&txn).await.unwrap(), None);
        assert_eq!(read_snapshot(&txn, 0).await.unwrap(), None);

        let cur = scan_cur_entities(&txn).await.unwrap();
        assert_eq!(cur, vec![EntityRecord::Schema(schema)]);
        let hist = scan_hist_entities(&txn).await.unwrap();
        assert_eq!(hist, vec![EntityRecord::Schema(ended)]);
        txn.rollback();
        db.close().await.unwrap();
    }
}
