use axum::{extract::State, response::IntoResponse, routing::post, Json, Router};
use serde_json::json;
use tower_http::cors::CorsLayer;
use tracing::info;
use std::sync::Arc;

use crate::scheduler::Scheduler;

#[derive(Clone)]
pub struct AppState {
    scheduler: Scheduler,
}

/// Handler for `/run-query` which logs the incoming body and
/// returns a simple JSON status response.
async fn run_query(State(state): State<Arc<AppState>>, body: String) -> impl IntoResponse {
    info!(%body, "received query");
    let (job_id, status) = state.scheduler.enqueue(body).await;
    Json(json!({ "job_id": job_id, "status": status, "output": null }))
}

/// Build the application router with CORS support.
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/run-query", post(run_query))
        .layer(CorsLayer::permissive())
        .with_state(Arc::new(state))
}

/// Start the HTTP server on `127.0.0.1:3000`.
pub async fn start_server() {
    let scheduler = Scheduler::new();
    let app = app(AppState { scheduler });
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
