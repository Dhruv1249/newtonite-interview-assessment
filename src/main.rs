//! FlashDrop application binary entry point with optional Redis and PostgreSQL integration.

use flashdrop::db::{connect_postgres, connect_redis};
use flashdrop::{create_app, initialize_system, DEFAULT_QUEUE_CAPACITY};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Entry point initializing tracing, state, optional external datastores, worker daemon, and TCP listener.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "flashdrop=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let (mut app_state, _worker_handle) = initialize_system(DEFAULT_QUEUE_CAPACITY);

    if let Ok(redis_url) = std::env::var("REDIS_URL") {
        info!(redis_url, "Connecting to external Redis cluster/service");
        match connect_redis(&redis_url).await {
            Ok(redis_manager) => {
                app_state = app_state.with_redis(redis_manager);
                app_state.initialize_redis_inventory("iphone_16", 100).await;
                info!("Initialized Redis distributed inventory lease for 'iphone_16'");
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed connecting to Redis; continuing with in-memory state");
            }
        }
    }

    if let Ok(database_url) = std::env::var("DATABASE_URL") {
        info!("Connecting to external PostgreSQL durable database");
        match connect_postgres(&database_url).await {
            Ok(pg_client) => {
                app_state = app_state.with_postgres(pg_client);
                info!("Initialized PostgreSQL schema and hooked into application state");
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed connecting to PostgreSQL; continuing with in-memory state");
            }
        }
    }

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
