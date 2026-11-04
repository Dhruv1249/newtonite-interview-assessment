//! Integration tests validating flash sale concurrency, idempotency, input validation, and zero overselling.

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use flashdrop::{
    create_app, initialize_system,
    models::{Order, OrderStatus},
};
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

/// Helper function serializing and executing an HTTP request against an Axum router.
async fn send_json_request(
    app: &axum::Router,
    method: axum::http::Method,
    uri: &str,
    json_body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match json_body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(serde_json::to_vec(&value).unwrap())
        }
        None => Body::empty(),
    };

    let request = builder.body(body).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let parsed_json: serde_json::Value =
        serde_json::from_slice(&body_bytes).unwrap_or(serde_json::Value::Null);

    (status, parsed_json)
}

/// Verifies basic order placement, pending status response, and eventual worker confirmation.
#[tokio::test]
async fn test_order_creation_and_worker_confirmation() {
    let (state, _worker_handle) = initialize_system(100);
    let app = create_app(state);

    let order_payload = serde_json::json!({
        "userId": "user_basic",
        "productId": "iphone_16",
        "quantity": 1,
        "idempotencyKey": "req_key_basic"
    });

    let (status, body) = send_json_request(
        &app,
        axum::http::Method::POST,
        "/orders",
        Some(order_payload),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    let order_id = body["orderId"].as_str().unwrap().to_string();
    assert_eq!(body["status"], "PENDING");

    tokio::time::sleep(Duration::from_millis(60)).await;

    let (query_status, query_body) = send_json_request(
        &app,
        axum::http::Method::GET,
        &format!("/orders/{order_id}"),
        None,
    )
    .await;

    assert_eq!(query_status, StatusCode::OK);
    assert_eq!(query_body["orderId"], order_id);
    assert_eq!(query_body["status"], "CONFIRMED");
}

/// Validates that duplicate requests with the identical idempotency key return the cached order without deducting stock.
#[tokio::test]
async fn test_idempotency_prevents_duplicate_deduction() {
    let (state, _worker_handle) = initialize_system(100);
    let app = create_app(state.clone());

    let idempotent_payload = serde_json::json!({
        "userId": "user_retry",
        "productId": "iphone_16",
        "quantity": 2,
        "idempotencyKey": "unique_retry_token_42"
    });

    let (first_status, first_body) = send_json_request(
        &app,
        axum::http::Method::POST,
        "/orders",
        Some(idempotent_payload.clone()),
    )
    .await;
    assert_eq!(first_status, StatusCode::ACCEPTED);
    let assigned_order_id = first_body["orderId"].as_str().unwrap().to_string();

    let initial_remaining_stock = state.get_product_stock("iphone_16").unwrap();
    assert_eq!(initial_remaining_stock, 98);

    let (retry_status, retry_body) = send_json_request(
        &app,
        axum::http::Method::POST,
        "/orders",
        Some(idempotent_payload),
    )
    .await;

    assert!(retry_status == StatusCode::OK || retry_status == StatusCode::ACCEPTED);
    assert_eq!(retry_body["orderId"], assigned_order_id);

    let post_retry_stock = state.get_product_stock("iphone_16").unwrap();
    assert_eq!(post_retry_stock, 98);
}

/// Verifies that invalid inputs such as non-positive quantities and empty fields are rejected with HTTP 400.
#[tokio::test]
async fn test_input_validation() {
    let (state, _worker_handle) = initialize_system(100);
    let app = create_app(state);

    let zero_qty_payload = serde_json::json!({
        "userId": "user_val",
        "productId": "iphone_16",
        "quantity": 0,
        "idempotencyKey": "val_1"
    });
    let (zero_status, zero_body) = send_json_request(
        &app,
        axum::http::Method::POST,
        "/orders",
        Some(zero_qty_payload),
    )
    .await;
    assert_eq!(zero_status, StatusCode::BAD_REQUEST);
    assert_eq!(zero_body["error"], "INVALID_QUANTITY");

    let empty_user_payload = serde_json::json!({
        "userId": "",
        "productId": "iphone_16",
        "quantity": 1,
        "idempotencyKey": "val_2"
    });
    let (user_status, user_body) = send_json_request(
        &app,
        axum::http::Method::POST,
        "/orders",
        Some(empty_user_payload),
    )
    .await;
    assert_eq!(user_status, StatusCode::BAD_REQUEST);
    assert_eq!(user_body["error"], "INVALID_USER_ID");
}

/// Simulates the exact flash sale scenario: 10,000 customers competing for 100 available units.
///
/// Asserts that zero overselling occurs: exactly 100 orders succeed, exactly 9,900 are rejected,
/// and remaining inventory is exactly 0.
#[tokio::test]
async fn test_flash_sale_10k_requests_zero_oversell() {
    let total_competing_customers = 10_000;
    let initial_stock = 100;

    let (state, worker_handle) = initialize_system(15_000);
    state.seed_inventory("iphone_16", initial_stock);

    let state_arc = Arc::new(state);
    let mut task_join_handles = Vec::with_capacity(total_competing_customers);

    for customer_index in 0..total_competing_customers {
        let cloned_state = Arc::clone(&state_arc);
        let task = tokio::spawn(async move {
            let user_id = format!("user_{customer_index}");
            let idempotency_key = format!("flash_key_{customer_index}");

            cloned_state.reserve_inventory("iphone_16", 1).map(|_| {
                let order_id = format!("ord_{customer_index}");
                let order = Order {
                    order_id: order_id.clone(),
                    user_id,
                    product_id: "iphone_16".to_string(),
                    quantity: 1,
                    idempotency_key,
                    status: OrderStatus::Pending,
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                };
                cloned_state.insert_order(order);
                order_id
            })
        });
        task_join_handles.push(task);
    }

    let mut accepted_orders_count = 0;
    let mut rejected_orders_count = 0;

    for handle in task_join_handles {
        match handle.await.unwrap() {
            Ok(_) => accepted_orders_count += 1,
            Err(_) => rejected_orders_count += 1,
        }
    }

    let remaining_stock = state_arc.get_product_stock("iphone_16").unwrap();

    assert_eq!(
        accepted_orders_count, initial_stock,
        "Exactly 100 orders should be accepted"
    );
    assert_eq!(
        rejected_orders_count,
        total_competing_customers - initial_stock as usize,
        "Exactly 9,900 orders should be rejected"
    );
    assert_eq!(
        remaining_stock, 0,
        "Remaining inventory must be exactly zero, proving zero overselling"
    );
    assert_eq!(
        state_arc.orders.len(),
        initial_stock as usize,
        "Order store must contain exactly 100 orders"
    );

    drop(state_arc);
    drop(worker_handle);
}

/// Validates concurrent retries using identical idempotency tokens do not cause double reservations.
#[tokio::test]
async fn test_concurrent_idempotent_duplicate_requests() {
    let (state, _worker_handle) = initialize_system(500);
    state.seed_inventory("iphone_16", 10);
    let app = create_app(state.clone());

    let concurrent_retries = 25;
    let shared_idempotency_token = "identical_concurrent_token_777";
    let mut handles = Vec::with_capacity(concurrent_retries);

    for _ in 0..concurrent_retries {
        let app_clone = app.clone();
        let token = shared_idempotency_token.to_string();
        handles.push(tokio::spawn(async move {
            let payload = serde_json::json!({
                "userId": "user_race",
                "productId": "iphone_16",
                "quantity": 1,
                "idempotencyKey": token
            });
            send_json_request(
                &app_clone,
                axum::http::Method::POST,
                "/orders",
                Some(payload),
            )
            .await
        }));
    }

    let mut distinct_order_ids = std::collections::HashSet::new();
    for handle in handles {
        let (status, body) = handle.await.unwrap();
        assert!(status == StatusCode::ACCEPTED || status == StatusCode::OK);
        if let Some(id) = body["orderId"].as_str() {
            distinct_order_ids.insert(id.to_string());
        }
    }

    assert_eq!(
        distinct_order_ids.len(),
        1,
        "All concurrent identical retry requests must yield the exact same order identifier"
    );
    assert_eq!(
        state.get_product_stock("iphone_16").unwrap(),
        9,
        "Stock should only decrement once despite 25 concurrent requests with identical idempotency key"
    );
}
