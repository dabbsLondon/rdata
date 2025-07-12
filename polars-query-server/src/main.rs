mod api;
mod executor;
mod parser;
mod scheduler;
mod utils;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    if std::env::var("SKIP_SERVER").is_ok() {
        // Used in tests to avoid starting the server
        return;
    }

    api::start_server().await;
}
