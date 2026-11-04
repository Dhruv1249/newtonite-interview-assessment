//! Core library interface for the FlashDrop high-concurrency flash sale engine.

pub mod db;
pub mod handlers;
pub mod models;
pub mod state;
pub mod worker;

use axum::{
    routing::{get, post},
    Router,
};
use state::AppState;
use tokio::sync::mpsc;
use worker::OrderWorker;

/// Channel capacity for the in-memory asynchronous worker buffer.
pub const DEFAULT_QUEUE_CAPACITY: usize = 20_000;

/// Constructs the Axum application router wired to state and background workers.
pub fn create_app(state: AppState) -> Router {
    Router::new()
        .route("/orders", post(handlers::create_order_handler))
        .route("/orders/{orderId}", get(handlers::get_order_status_handler))
        .route("/health", get(handlers::health_check_handler))
        .with_state(state)
}

/// Initializes application state along with the background asynchronous worker.
pub fn initialize_system(queue_capacity: usize) -> (AppState, tokio::task::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(queue_capacity);
    let app_state = AppState::new(sender);
    let worker = OrderWorker::new(receiver, app_state.clone());
    let worker_handle = worker.spawn();

    (app_state, worker_handle)
}
