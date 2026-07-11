//! A worked tour: open a catalog on (in-memory) object storage, evolve a
//! schema across commits, and time-travel back through the history.
//!
//! Run with: `cargo run -p moraine --example walkthrough`

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, Error, Result};
use object_store::memory::InMemory;

fn bigint(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // A deployment is a bucket and credentials; here, in-memory.
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;

    let v1 = catalog
        .commit(|txn| {
            let sales = txn.create_schema("sales")?;
            txn.create_table(sales, "orders", &[bigint("id"), bigint("qty")])?;
            Ok(())
        })
        .await?;

    let v2 = catalog
        .commit(|txn| {
            let sales = txn
                .schema_by_name("sales")
                .ok_or_else(|| Error::NotFound("schema sales".to_string()))?;
            let orders = txn
                .table_by_name(sales.id, "orders")
                .ok_or_else(|| Error::NotFound("table orders".to_string()))?;
            txn.add_column(orders.id, &bigint("discount"))?;
            txn.rename_table(orders.id, "orders_v2")?;
            Ok(())
        })
        .await?;

    // The head sees the evolved shape…
    let head = catalog.snapshot().await?;
    let sales = head
        .schema_by_name("sales")
        .ok_or_else(|| Error::NotFound("schema sales".to_string()))?;
    for table in head.tables_in(sales.id) {
        println!(
            "at head:  {} ({} columns)",
            table.name,
            head.columns_of(table.id).len()
        );
    }

    // …while the history remains queryable at every snapshot.
    for snapshot in [v1, v2] {
        let past = catalog.snapshot_at(snapshot).await?;
        let sales = past
            .schema_by_name("sales")
            .ok_or_else(|| Error::NotFound("schema sales".to_string()))?;
        let names: Vec<String> = past
            .tables_in(sales.id)
            .into_iter()
            .map(|t| format!("{} ({} cols)", t.name, past.columns_of(t.id).len()))
            .collect();
        println!("at {snapshot:?}: {}", names.join(", "));
    }

    catalog.close().await
}
