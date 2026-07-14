//! Data-plane integration: registration, expiry, statistics, and the
//! data-only schema-version path — public API only, real SlateDB on
//! in-memory object storage.

use std::sync::Arc;

use moraine::{
    Catalog, CatalogOptions, ColumnDef, ColumnId, ColumnStats, DataFile, DataFileId, DeleteFile,
    Error, FileColumnStats, TableId,
};
use object_store::memory::InMemory;

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
    }
}

fn datafile(rows: u64) -> DataFile {
    DataFile {
        path: format!("data-{rows}.parquet"),
        path_is_relative: true,
        file_format: "parquet".into(),
        record_count: rows,
        file_size_bytes: rows * 10,
        footer_size: 4,
        encryption_key: None,
        column_stats: vec![],
    }
}

#[allow(clippy::unwrap_used)]
async fn seeded() -> (Catalog, TableId) {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "t", &[col("a")])?;
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = catalog.snapshot().await.unwrap();
    let s = snapshot.schema_by_name("s").unwrap();
    let t = snapshot.table_by_name(s.id, "t").unwrap();

    (catalog, t.id)
}

#[tokio::test]
async fn registration_is_data_only_and_visible() {
    let (catalog, t) = seeded().await;
    let before = catalog.snapshot().await.unwrap().current_snapshot();

    catalog
        .commit(move |tx| tx.register_data_file(t, datafile(100)).map(|_| ()))
        .await
        .unwrap();

    let head = catalog.snapshot().await.unwrap();
    // Data-only commits mint a snapshot but carry the schema version.
    assert_eq!(head.current_snapshot().id.get(), before.id.get() + 1);
    assert_eq!(
        head.current_snapshot().schema_version,
        before.schema_version
    );

    let files = head.data_files_of(t);
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].record_count, 100);
    assert_eq!(files[0].row_id_start, Some(0));
    assert_eq!(head.table_stats(t).unwrap().next_row_id, 100);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn expiry_time_travels_and_row_ids_stay_dense() {
    let (catalog, t) = seeded().await;
    catalog
        .commit(move |tx| tx.register_data_file(t, datafile(100)).map(|_| ()))
        .await
        .unwrap();
    let registered = catalog.snapshot().await.unwrap().current_snapshot().id;

    let file_id = catalog.snapshot().await.unwrap().data_files_of(t)[0].id;
    catalog
        .commit(move |tx| tx.expire_data_file(t, file_id))
        .await
        .unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(head.data_files_of(t).is_empty());
    // The expired file is still visible at its snapshot.
    let past = catalog.snapshot_at(registered).await.unwrap();
    assert_eq!(past.data_files_of(t).len(), 1);

    // A later registration allocates above the expired rows.
    catalog
        .commit(move |tx| tx.register_data_file(t, datafile(10)).map(|_| ()))
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.data_files_of(t)[0].row_id_start, Some(100));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn encryption_keys_round_trip_verbatim() {
    let (catalog, t) = seeded().await;

    catalog
        .commit(move |tx| {
            let file = tx.register_data_file(
                t,
                DataFile {
                    encryption_key: Some("ZGF0YS1rZXk=".into()),
                    ..datafile(10)
                },
            )?;
            tx.register_delete_file(
                t,
                DeleteFile {
                    data_file_id: file,
                    path: "d.parquet".into(),
                    path_is_relative: true,
                    format: "parquet".into(),
                    delete_count: 1,
                    file_size_bytes: 50,
                    footer_size: 4,
                    encryption_key: Some("ZGVsZXRlLWtleQ==".into()),
                },
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert_eq!(
        head.data_files_of(t)[0].encryption_key.as_deref(),
        Some("ZGF0YS1rZXk=")
    );
    assert_eq!(
        head.delete_files_of(t)[0].encryption_key.as_deref(),
        Some("ZGVsZXRlLWtleQ==")
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn delete_files_cascade_with_their_data_file() {
    let (catalog, t) = seeded().await;
    catalog
        .commit(move |tx| {
            let f = tx.register_data_file(t, datafile(100))?;
            tx.register_delete_file(
                t,
                DeleteFile {
                    data_file_id: f,
                    path: "d.parquet".into(),
                    path_is_relative: true,
                    format: "parquet".into(),
                    delete_count: 5,
                    file_size_bytes: 50,
                    footer_size: 4,
                    encryption_key: None,
                },
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.delete_files_of(t).len(), 1);
    let file_id = head.data_files_of(t)[0].id;

    catalog
        .commit(move |tx| tx.expire_data_file(t, file_id))
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert!(head.delete_files_of(t).is_empty());

    // Registering a delete file against a missing data file fails.
    let err = catalog
        .commit(move |tx| {
            tx.register_delete_file(
                t,
                DeleteFile {
                    data_file_id: DataFileId::new(999),
                    path: "d2.parquet".into(),
                    path_is_relative: true,
                    format: "parquet".into(),
                    delete_count: 1,
                    file_size_bytes: 10,
                    footer_size: 4,
                    encryption_key: None,
                },
            )
            .map(|_| ())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn column_stats_round_trip_verbatim() {
    let (catalog, t) = seeded().await;
    catalog
        .commit(move |tx| {
            tx.register_data_file(
                t,
                DataFile {
                    column_stats: vec![FileColumnStats {
                        column_id: ColumnId::new(1),
                        column_size_bytes: 10,
                        value_count: 100,
                        null_count: 0,
                        min_value: Some("9".into()),
                        max_value: Some("10".into()),
                        contains_nan: None,
                        extra_stats: None,
                    }],
                    ..datafile(100)
                },
            )?;
            tx.update_column_stats(
                t,
                ColumnId::new(1),
                ColumnStats {
                    contains_null: Some(false),
                    contains_nan: None,
                    min_value: Some("9".into()),
                    max_value: Some("10".into()),
                    extra_stats: None,
                },
            )
        })
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    let stats = head.column_stats(t, ColumnId::new(1)).unwrap();
    // '9' > '10' lexicographically — stored verbatim, never compared.
    assert_eq!(stats.min_value.as_deref(), Some("9"));
    assert_eq!(stats.max_value.as_deref(), Some("10"));
    // Stats-only commits mint a snapshot too.
    let before = head.current_snapshot().id;
    catalog
        .commit(move |tx| tx.update_table_stats(t, 100, 1000))
        .await
        .unwrap();
    let after = catalog.snapshot().await.unwrap().current_snapshot();
    assert_eq!(after.id.get(), before.get() + 1);
    catalog.close().await.unwrap();
}
