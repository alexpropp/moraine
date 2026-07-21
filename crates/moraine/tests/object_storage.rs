//! Real object storage tests: public-API round-trips against an
//! S3-compatible endpoint. Ignored by default; `cargo xtask s3` starts
//! MinIO and runs them with the endpoint environment set.
//!
//! Run manually against any S3-compatible endpoint:
//!
//! ```text
//! MORAINE_S3_ENDPOINT=http://127.0.0.1:9124 MORAINE_S3_BUCKET=moraine \
//! cargo test -p moraine --test object_storage -- --ignored
//! ```

// The tests-exempt lints (`clippy.toml`) reach `#[test]` functions and
// `#[cfg(test)]` modules, not an integration crate's plain helper
// functions — exempted here instead, crate-wide, as tests.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions};
use object_store::{ObjectStore, aws::AmazonS3Builder};

/// Credentials matching the MinIO server `cargo xtask s3` runs.
fn s3_store() -> Arc<dyn ObjectStore> {
    let endpoint = std::env::var("MORAINE_S3_ENDPOINT")
        .expect("MORAINE_S3_ENDPOINT must be set (see this module's doc comment)");
    let bucket = std::env::var("MORAINE_S3_BUCKET")
        .expect("MORAINE_S3_BUCKET must be set (see this module's doc comment)");
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .expect("S3 store from test configuration"),
    )
}

/// Options rooted at a per-test prefix so the suite shares one bucket.
fn options_at(path: &str) -> CatalogOptions {
    let mut options = CatalogOptions::default();
    options.path = path.to_string();
    options
}

#[tokio::test]
#[ignore = "needs a live S3 endpoint; run through `cargo xtask s3`"]
async fn bootstrap_commit_and_reopen_on_s3() {
    let store = s3_store();
    let catalog = Catalog::open(store.clone(), options_at("reopen"))
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    catalog.close().await.unwrap();

    // Reopen: state persisted through the real endpoint.
    let catalog = Catalog::open(store, options_at("reopen")).await.unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert!(head.schema_by_name("sales").is_some());
    catalog.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs a live S3 endpoint; run through `cargo xtask s3`"]
async fn read_only_catalog_reads_s3_state() {
    let store = s3_store();
    let catalog = Catalog::open(store.clone(), options_at("reader"))
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("analytics").map(|_| ()))
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let reader = Catalog::open_read_only(store, options_at("reader"))
        .await
        .unwrap();
    let head = reader.snapshot().await.unwrap();
    assert!(head.schema_by_name("analytics").is_some());
    reader.close().await.unwrap();
}
