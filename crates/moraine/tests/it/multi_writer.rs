use std::sync::Arc;

use moraine::{Catalog, CatalogOptions};
use object_store::memory::InMemory;

fn multi_writer_options() -> CatalogOptions {
    let mut options = CatalogOptions::default();
    options.multi_writer = true;
    options
}

#[allow(clippy::unwrap_used)]
async fn open_multi_writer(store: &Arc<InMemory>) -> Catalog {
    Catalog::open(
        store.clone() as Arc<dyn object_store::ObjectStore>,
        multi_writer_options(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn multi_writer_open_bootstraps_and_serves_the_empty_catalog() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;
    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(snapshot.current_snapshot().id, moraine::SnapshotId::new(0));
    // A second full open finds the initialized store rather than
    // re-bootstrapping, and does not fence anything (there is no writer).
    let second = open_multi_writer(&store).await;
    second.snapshot().await.unwrap();
    catalog.snapshot().await.unwrap();
}
