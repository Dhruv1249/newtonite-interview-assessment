//! HTTP route handlers implementing the FlashDrop REST application interface.

use crate::models::{
    CreateOrderRequest, CreateOrderResponse, ErrorResponse, Order, OrderQueueMessage, OrderStatus,
};
use crate::state::{AppState, StateError};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use dashmap::mapref::entry::Entry;
use std::time::Duration;
use uuid::Uuid;

/// HTTP handler processing requests to submit a flash sale order.
pub async fn create_order_handler(
    State(state): State<AppState>,
    Json(payload): Json<CreateOrderRequest>,
) -> Response {
    if let Some(validation_error) = validate_create_order_request(&payload) {
        return validation_error;
    }

    let idempotency_resolution = {
        match state
            .idempotency_store
            .entry(payload.idempotency_key.clone())
        {
            Entry::Occupied(occupied_entry) => {
                let existing_order_id = occupied_entry.get().clone();
                Err(existing_order_id)
            }
            Entry::Vacant(vacant_entry) => {
                let generated_order_id = format!("ord_{}", Uuid::new_v4().simple());
                vacant_entry.insert(generated_order_id.clone());
                Ok(generated_order_id)
            }
        }
    };

    match idempotency_resolution {
        Err(existing_order_id) => handle_idempotent_duplicate(&state, existing_order_id).await,
        Ok(generated_order_id) => {
            execute_order_placement(&state, payload, generated_order_id).await
        }
    }
}

/// HTTP handler querying the active processing status and entity details of a given order.
pub async fn get_order_status_handler(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> Response {
    match state.get_order(&order_id) {
        Some(order) => (StatusCode::OK, Json(order)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "ORDER_NOT_FOUND".to_string(),
                message: format!("No order located with identifier '{order_id}'"),
            }),
        )
            .into_response(),
    }
}

/// HTTP handler providing an active health check and remaining inventory overview.
pub async fn health_check_handler(State(state): State<AppState>) -> Response {
    let available_iphone_stock = state.get_product_stock("iphone_16").unwrap_or(0);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "UP",
            "inventory": {
                "iphone_16": available_iphone_stock
            }
        })),
    )
        .into_response()
}

/// Validates syntax and domain constraints on incoming order placement payloads.
fn validate_create_order_request(request: &CreateOrderRequest) -> Option<Response> {
    if request.quantity <= 0 {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "INVALID_QUANTITY".to_string(),
                    message: "Order quantity must be strictly greater than zero".to_string(),
                }),
            )
                .into_response(),
        );
    }

    if request.user_id.trim().is_empty() {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "INVALID_USER_ID".to_string(),
                    message: "User identifier must be provided".to_string(),
                }),
            )
                .into_response(),
        );
    }

    if request.product_id.trim().is_empty() {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "INVALID_PRODUCT_ID".to_string(),
                    message: "Product identifier must be provided".to_string(),
                }),
            )
                .into_response(),
        );
    }

    if request.idempotency_key.trim().is_empty() {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "INVALID_IDEMPOTENCY_KEY".to_string(),
                    message: "Idempotency key must be provided".to_string(),
                }),
            )
                .into_response(),
        );
    }

    None
}

/// Dispatches an order to inventory reservation and asynchronous worker queuing.
async fn execute_order_placement(
    state: &AppState,
    payload: CreateOrderRequest,
    assigned_order_id: String,
) -> Response {
    match state.reserve_inventory(&payload.product_id, payload.quantity) {
        Ok(_) => {
            let order_record = Order {
                order_id: assigned_order_id.clone(),
                user_id: payload.user_id,
                product_id: payload.product_id.clone(),
                quantity: payload.quantity,
                idempotency_key: payload.idempotency_key,
                status: OrderStatus::Pending,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };

            state.orders.insert(assigned_order_id.clone(), order_record);

            let queue_message = OrderQueueMessage {
                order_id: assigned_order_id.clone(),
                product_id: payload.product_id,
                quantity: payload.quantity,
            };

            let _ = state.queue_sender.send(queue_message).await;

            (
                StatusCode::ACCEPTED,
                Json(CreateOrderResponse {
                    order_id: assigned_order_id,
                    status: OrderStatus::Pending,
                }),
            )
                .into_response()
        }
        Err(StateError::InsufficientStock) => {
            state.idempotency_store.remove(&payload.idempotency_key);
            (
                StatusCode::CONFLICT,
                Json(ErrorResponse {
                    error: "INSUFFICIENT_STOCK".to_string(),
                    message: "Not enough items in stock to satisfy requested quantity".to_string(),
                }),
            )
                .into_response()
        }
        Err(StateError::ProductNotFound) => {
            state.idempotency_store.remove(&payload.idempotency_key);
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "PRODUCT_NOT_FOUND".to_string(),
                    message: format!("Target product '{}' not found in catalog", payload.product_id),
                }),
            )
                .into_response()
        }
        Err(StateError::OrderNotFound) => {
            state.idempotency_store.remove(&payload.idempotency_key);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "INTERNAL_ERROR".to_string(),
                    message: "Unexpected order placement state failure".to_string(),
                }),
            )
                .into_response()
        }
    }
}

/// Resolves duplicate requests matching a registered idempotency key.
async fn handle_idempotent_duplicate(state: &AppState, order_id: String) -> Response {
    let mut retry_count = 0;
    while retry_count < 10 {
        if let Some(existing_order) = state.get_order(&order_id) {
            return (
                StatusCode::OK,
                Json(CreateOrderResponse {
                    order_id: existing_order.order_id,
                    status: existing_order.status,
                }),
            )
                .into_response();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        retry_count += 1;
    }

    (
        StatusCode::ACCEPTED,
        Json(CreateOrderResponse {
            order_id,
            status: OrderStatus::Pending,
        }),
    )
        .into_response()
}
