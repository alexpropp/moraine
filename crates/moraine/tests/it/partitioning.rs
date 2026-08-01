//! Partition specs through the verb surface: setting, replacing, clearing,
//! and reading one back — at head and by time travel.

use moraine::{Error, PartitionColumnDef, TableId};

use crate::fixtures::{col, seeded};

fn key(column: moraine::ColumnId, transform: &str) -> PartitionColumnDef {
    PartitionColumnDef {
        column,
        transform: transform.into(),
    }
}

#[tokio::test]
async fn a_spec_reads_back_with_its_columns_in_declared_order() {
    let (catalog, _, table, _) = seeded().await;
    catalog
        .commit(|tx| {
            tx.add_column(table, &col("y"))?;
            Ok(())
        })
        .await
        .unwrap();

    let columns = catalog.snapshot().await.unwrap().columns_of(table);
    let (x, y) = (columns[0].id, columns[1].id);
    catalog
        .commit(move |tx| {
            tx.set_partitioning(table, &[key(y, "bucket(16)"), key(x, "identity")])?;
            Ok(())
        })
        .await
        .unwrap();

    let spec = catalog
        .snapshot()
        .await
        .unwrap()
        .partitioning_of(table)
        .unwrap();
    assert_eq!(
        spec.columns,
        vec![key(y, "bucket(16)"), key(x, "identity")],
        "keys keep their declared order and verbatim transforms"
    );
}

#[tokio::test]
async fn an_unpartitioned_table_has_no_spec() {
    let (catalog, _, table, _) = seeded().await;
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .partitioning_of(table)
            .is_none()
    );
}

/// Repartitioning ends the old spec into history and mints a new one, so a
/// snapshot before the change still reconstructs the spec in force then —
/// the property files written under an older spec depend on.
#[tokio::test]
async fn repartitioning_leaves_the_old_spec_readable_by_time_travel() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog.snapshot().await.unwrap().columns_of(table)[0].id;

    let first = catalog
        .commit(move |tx| {
            tx.set_partitioning(table, &[key(x, "identity")])?;
            Ok(())
        })
        .await
        .unwrap();
    let second = catalog
        .commit(move |tx| {
            tx.set_partitioning(table, &[key(x, "year")])?;
            Ok(())
        })
        .await
        .unwrap();

    let at_first = catalog.snapshot_at(first).await.unwrap();
    let spec_then = at_first.partitioning_of(table).unwrap();
    assert_eq!(spec_then.columns, vec![key(x, "identity")]);

    let at_second = catalog.snapshot_at(second).await.unwrap();
    let spec_now = at_second.partitioning_of(table).unwrap();
    assert_eq!(spec_now.columns, vec![key(x, "year")]);
    assert_ne!(spec_then.id, spec_now.id, "a new spec gets a fresh id");
}

#[tokio::test]
async fn clearing_unpartitions_the_table_but_keeps_the_past() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog.snapshot().await.unwrap().columns_of(table)[0].id;

    let partitioned = catalog
        .commit(move |tx| {
            tx.set_partitioning(table, &[key(x, "identity")])?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(move |tx| tx.clear_partitioning(table))
        .await
        .unwrap();

    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .partitioning_of(table)
            .is_none()
    );
    assert!(
        catalog
            .snapshot_at(partitioned)
            .await
            .unwrap()
            .partitioning_of(table)
            .is_some()
    );
}

#[tokio::test]
async fn clearing_an_unpartitioned_table_is_not_found() {
    let (catalog, _, table, _) = seeded().await;
    let err = catalog
        .commit(move |tx| tx.clear_partitioning(table))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err}");
}

#[tokio::test]
async fn a_spec_is_refused_when_it_is_empty_repeats_a_column_or_names_a_stranger() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog.snapshot().await.unwrap().columns_of(table)[0].id;

    let empty = catalog
        .commit(move |tx| {
            tx.set_partitioning(table, &[])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(empty, Error::Constraint(_)), "{empty}");

    let repeated = catalog
        .commit(move |tx| {
            tx.set_partitioning(table, &[key(x, "identity"), key(x, "year")])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(repeated, Error::Constraint(_)), "{repeated}");

    let stranger = catalog
        .commit(move |tx| {
            tx.set_partitioning(table, &[key(moraine::ColumnId::new(999), "identity")])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(stranger, Error::NotFound(_)), "{stranger}");

    let absent_table = catalog
        .commit(|tx| {
            tx.set_partitioning(TableId::new(9999), &[key(x, "identity")])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(absent_table, Error::NotFound(_)), "{absent_table}");
}

/// Dropping the table takes its spec with it — the per-table cascade the
/// snapshot applies must not leave a spec addressable by a reused view.
#[tokio::test]
async fn dropping_the_table_drops_its_spec() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog.snapshot().await.unwrap().columns_of(table)[0].id;
    catalog
        .commit(move |tx| {
            tx.set_partitioning(table, &[key(x, "identity")])?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(move |tx| tx.drop_table(table))
        .await
        .unwrap();

    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .partitioning_of(table)
            .is_none()
    );
}

/// Partitioning is table DDL: it bumps the catalog schema version, so a
/// client keying a schema cache on it re-reads.
#[tokio::test]
async fn setting_a_spec_bumps_the_schema_version() {
    let (catalog, _, table, _) = seeded().await;
    let x = catalog.snapshot().await.unwrap().columns_of(table)[0].id;
    let before = catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .schema_version;

    catalog
        .commit(move |tx| {
            tx.set_partitioning(table, &[key(x, "identity")])?;
            Ok(())
        })
        .await
        .unwrap();

    let after = catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .schema_version;
    assert_eq!(after, before + 1);
}
