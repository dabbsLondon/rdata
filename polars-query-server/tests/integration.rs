use axum::body::Body;
use axum::http::{Request, StatusCode};
use hyper;
use tower::ServiceExt;

use polars::prelude::ParquetWriter;
use polars::prelude::*;
use std::fs::File;
use tempfile::NamedTempFile;

use polars_query_server::{api::app, api::AppState, scheduler::Scheduler};

#[tokio::test]
async fn post_query_returns_data() {
    let scheduler = Scheduler::new();
    let app = app(AppState { scheduler });

    // prepare parquet file
    let mut df = df!["name" => ["a", "b"], "age" => [20, 40]].unwrap();
    let file = NamedTempFile::new().unwrap();
    ParquetWriter::new(File::create(file.path()).unwrap())
        .finish(&mut df)
        .unwrap();

    let query = format!(
        "df = pl.read_parquet(\"{}\")\ndf = df.filter(pl.col(\"age\") > 30)",
        file.path().to_str().unwrap()
    );

    let response = app
        .oneshot(Request::post("/run-query").body(Body::from(query)).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(v.get("job_id").is_some());
    assert!(v.get("output").is_some());
}

#[tokio::test]
async fn post_query_large_output_file() {
    let scheduler = Scheduler::new();
    let app = app(AppState { scheduler });

    let data: Vec<i32> = (0..1_000_000).collect();
    let mut df = df!["val" => &data].unwrap();
    let file = NamedTempFile::new().unwrap();
    ParquetWriter::new(File::create(file.path()).unwrap())
        .finish(&mut df)
        .unwrap();

    let query = format!("df = pl.read_parquet(\"{}\")", file.path().display());
    let response = app
        .oneshot(Request::post("/run-query").body(Body::from(query)).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let path = v.get("output").and_then(|o| o.as_str()).unwrap();
    assert!(std::path::Path::new(path).exists());
    std::fs::remove_file(path).unwrap();
}
