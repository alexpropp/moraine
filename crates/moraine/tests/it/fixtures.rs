//! Fixtures shared across the suite's modules.
//!
//! `unwrap_used` is a library-code lint, not exempted automatically for a
//! plain (non-`#[test]`) function even in an integration-test crate, so
//! the async helpers carry targeted allows.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, DataFile, SchemaId, TableId};
use object_store::memory::InMemory;

/// A nullable BIGINT column.
pub fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
    }
}

/// A parquet data file whose path and sizes derive from its row count.
pub fn datafile(rows: u64) -> DataFile {
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

/// Opens a fresh catalog over in-memory object storage.
#[allow(clippy::unwrap_used)]
pub async fn open_memory() -> Catalog {
    Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap()
}

/// Opens a catalog pre-seeded with tables `a` and `b` in schema `s`.
#[allow(clippy::unwrap_used)]
pub async fn seeded() -> (Catalog, SchemaId, TableId, TableId) {
    let catalog = open_memory().await;
    catalog
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "a", &[col("x")])?;
            tx.create_table(s, "b", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = catalog.snapshot().await.unwrap();
    let s = snapshot.schema_by_name("s").unwrap().id;
    let a = snapshot.table_by_name(s, "a").unwrap().id;
    let b = snapshot.table_by_name(s, "b").unwrap().id;
    (catalog, s, a, b)
}
