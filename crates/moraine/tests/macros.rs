//! Macros through the public API: versioned lifecycle, the macro-only
//! namespace, and time travel — real SlateDB on in-memory object storage.

use std::sync::Arc;

use moraine::{
    Catalog, CatalogOptions, ColumnDef, Error, MacroImplementationDef, MacroParameterDef,
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

fn scalar_impl(sql: &str, parameters: Vec<MacroParameterDef>) -> MacroImplementationDef {
    MacroImplementationDef {
        dialect: "duckdb".into(),
        sql: sql.into(),
        macro_type: "scalar".into(),
        parameters,
    }
}

fn parameter(name: &str) -> MacroParameterDef {
    MacroParameterDef {
        name: name.into(),
        parameter_type: "unknown".into(),
        default_value: None,
        default_value_type: "unknown".into(),
    }
}

#[allow(clippy::unwrap_used)]
async fn open_memory() -> Catalog {
    Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap()
}

#[tokio::test]
async fn macros_commit_version_and_time_travel() {
    let catalog = open_memory().await;
    let created = catalog
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_macro(
                s,
                "add",
                &[
                    scalar_impl("(a + 1)", vec![parameter("a")]),
                    scalar_impl("(a + b)", vec![parameter("a"), parameter("b")]),
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let head = catalog.snapshot().await.unwrap();
    let s = head.schema_by_name("s").unwrap();
    let m = head.macro_by_name(s.id, "add").unwrap();
    assert_eq!(m.implementations.len(), 2);
    assert_eq!(m.implementations[1].parameters[1].name, "b");

    // Macros have their own namespace: a table named "add" coexists.
    catalog
        .commit(move |tx| {
            tx.create_table(s.id, "add", &[col("a")])?;
            Ok(())
        })
        .await
        .unwrap();

    // Replacement is drop + create under a fresh id.
    let m_id = m.id;
    catalog
        .commit(move |tx| {
            tx.drop_macro(m_id)?;
            tx.create_macro(s.id, "add", &[scalar_impl("(a + 2)", vec![parameter("a")])])?;
            Ok(())
        })
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    let replacement = head.macro_by_name(s.id, "add").unwrap();
    assert_ne!(replacement.id, m_id);
    assert_eq!(replacement.implementations[0].sql, "(a + 2)");

    // The pre-replacement definition is visible at the old snapshot.
    let past = catalog.snapshot_at(created).await.unwrap();
    assert_eq!(past.macro_by_id(m_id).unwrap().implementations.len(), 2);

    // Live-name collision among macros is rejected.
    let err = catalog
        .commit(move |tx| {
            tx.create_macro(s.id, "add", &[scalar_impl("1", vec![])])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
}

#[tokio::test]
async fn drop_schema_rejects_live_macros() {
    let catalog = open_memory().await;
    catalog
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_macro(s, "m", &[scalar_impl("1", vec![])])?;
            Ok(())
        })
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    let s = head.schema_by_name("s").unwrap();
    let err = catalog
        .commit(move |tx| tx.drop_schema(s.id))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)));
}
