use std::sync::Arc;

use futures::StreamExt;
use moraine::{Catalog, CatalogOptions, Error};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

fn multi_writer_options() -> CatalogOptions {
    let mut options = CatalogOptions::default();
    options.multi_writer = true;
    options
}

#[allow(clippy::unwrap_used)]
async fn open_multi_writer(store: &Arc<InMemory>) -> Catalog {
    Catalog::open(
        store.clone() as Arc<dyn ObjectStore>,
        multi_writer_options(),
    )
    .await
    .unwrap()
}

/// How many objects sit under `prefix`.
async fn objects_under(store: &Arc<InMemory>, prefix: &str) -> usize {
    let mut listing = store.list(Some(&Path::from(prefix)));
    let mut count = 0;
    while listing.next().await.is_some() {
        count += 1;
    }
    count
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

/// A prefix holding objects but no readable manifest is a damaged store, not
/// a fresh one: the attach refuses instead of stamping a new catalog over
/// whatever is there.
#[tokio::test]
async fn multi_writer_open_refuses_a_store_it_cannot_read_but_is_not_empty() {
    let store = Arc::new(InMemory::new());
    store
        .put(&Path::from("cat/leftover"), "not a slatedb object".into())
        .await
        .unwrap();

    let mut options = multi_writer_options();
    options.path = "cat".to_string();
    let err = Catalog::open(store.clone() as Arc<dyn ObjectStore>, options)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Store(_)), "got {err:?}");

    // The refusal wrote nothing: the planted object is still all there is.
    assert_eq!(objects_under(&store, "cat").await, 1);
}
