use axum::{extract::State, response::IntoResponse, routing::post, Json, Router};
use base64::engine::general_purpose::STANDARD as B64_ENGINE;
use base64::Engine;
use serde_json::json;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::scheduler::Scheduler;

#[derive(Clone)]
pub struct AppState {
    pub scheduler: Scheduler,
}

/// Handler for `/run-query` which logs the incoming body and
/// returns a simple JSON status response.
async fn run_query(State(state): State<Arc<AppState>>, body: String) -> impl IntoResponse {
    info!(%body, "received query");
    let (job_id, status, rx) = state.scheduler.enqueue(body).await;
    let result = rx.await.ok();
    let output = result.as_ref().and_then(|r| {
        if let Some(bytes) = &r.bytes {
            Some(B64_ENGINE.encode(bytes))
        } else {
            r.path.clone()
        }
    });
    Json(json!({
        "job_id": job_id,
        "status": status,
        "duration_ms": result.as_ref().map(|r| r.duration.as_millis()),
        "cost": result.as_ref().map(|r| r.cost),
        "output": output
    }))
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
