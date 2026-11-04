//! FlashDrop application binary entry point.

use flashdrop::{create_app, initialize_system, DEFAULT_QUEUE_CAPACITY};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Entry point initializing tracing, application state, worker daemon, and TCP listener.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "flashdrop=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let (app_state, _worker_handle) = initialize_system(DEFAULT_QUEUE_CAPACITY);
    let app = create_app(app_state);

    let server_bind_port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .unwrap_or(8080);

    let bind_address = SocketAddr::from(([0, 0, 0, 0], server_bind_port));
    info!(address = %bind_address, "Starting FlashDrop high-concurrency order server");

    let tcp_listener = TcpListener::bind(bind_address).await?;
    axum::serve(tcp_listener, app).await?;

    Ok(())
}
