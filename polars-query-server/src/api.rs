use axum::{response::IntoResponse, routing::post, Json, Router};
use serde_json::json;
use tower_http::cors::CorsLayer;
use tracing::info;

/// Handler for `/run-query` which logs the incoming body and
/// returns a simple JSON status response.
async fn run_query(body: String) -> impl IntoResponse {
    info!(%body, "received query");
    Json(json!({ "status": "received" }))
}

/// Build the application router with CORS support.
pub fn app() -> Router {
    Router::new()
        .route("/run-query", post(run_query))
        .layer(CorsLayer::permissive())
}

/// Start the HTTP server on `127.0.0.1:3000`.
pub async fn start_server() {
    let app = app();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
