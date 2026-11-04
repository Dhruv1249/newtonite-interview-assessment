//! Domain models and data transfer objects for the FlashDrop order processing engine.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Represents the life-cycle states of a flash sale order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    /// The order has been received and inventory is reserved, awaiting background worker processing.
    Pending,
    /// The asynchronous worker has successfully processed payment and verified fulfillment.
    Confirmed,
    /// The order processing has failed or encountered an irrecoverable error.
    Failed,
}

/// Incoming client payload for placing a high-concurrency flash sale order.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderRequest {
    /// Unique identifier of the purchasing customer.
    pub user_id: String,
    /// Identifier of the flash sale item being purchased.
    pub product_id: String,
    /// Requested quantity of items, which must be strictly greater than zero.
    pub quantity: i64,
    /// Client-supplied idempotency key used to deduplicate identical retry requests.
    pub idempotency_key: String,
}

/// Response returned to the client immediately upon successful order acceptance and inventory reservation.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderResponse {
    /// Unique identifier assigned to the created or deduplicated order.
    pub order_id: String,
    /// Current processing status of the order.
    pub status: OrderStatus,
}

/// Stored representation of an order entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    /// Unique system identifier for this order.
    pub order_id: String,
    /// Unique customer identifier.
    pub user_id: String,
    /// Target product identifier.
    pub product_id: String,
    /// Reserved quantity.
    pub quantity: i64,
    /// Idempotency token associated with this order.
    pub idempotency_key: String,
    /// Current life-cycle status.
    pub status: OrderStatus,
    /// Timestamp when the order was initially accepted.
    pub created_at: DateTime<Utc>,
    /// Timestamp when the order status was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Standardized error payload returned across all API endpoints.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    /// Machine-readable error code.
    pub error: String,
    /// Human-readable explanation of the error condition.
    pub message: String,
}

/// Message payload transferred across the asynchronous Tokio MPSC order queue.
#[derive(Debug, Clone)]
pub struct OrderQueueMessage {
    /// Identifier of the order scheduled for asynchronous fulfillment.
    pub order_id: String,
    /// Product identifier for audit and downstream processing.
    pub product_id: String,
    /// Quantity reserved for this order.
    pub quantity: i64,
}
