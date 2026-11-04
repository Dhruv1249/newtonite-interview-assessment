//! Shared in-memory thread-safe state management for inventory, orders, and idempotency tracking.

use crate::models::{Order, OrderQueueMessage, OrderStatus};
use chrono::Utc;
use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

/// Specific domain errors that can arise during state operations.
#[derive(Debug, PartialEq, Eq)]
pub enum StateError {
    /// The requested product does not exist in the active catalog.
    ProductNotFound,
    /// The requested quantity exceeds currently available product stock.
    InsufficientStock,
    /// An order with the specified identifier was not located in memory.
    OrderNotFound,
}

/// Thread-safe inventory counter managing stock levels using non-blocking atomic operations.
#[derive(Debug)]
pub struct ProductInventory {
    /// Identifier of the target product.
    pub product_id: String,
    /// Atomic signed integer tracking remaining stock units.
    pub stock: AtomicI64,
}

impl ProductInventory {
    /// Instantiates a new atomic product inventory entry with an initial stock allocation.
    pub fn new(product_id: impl Into<String>, initial_stock: i64) -> Self {
        Self {
            product_id: product_id.into(),
            stock: AtomicI64::new(initial_stock),
        }
    }

    /// Atomically decrements available inventory using compare-and-swap semantics.
    ///
    /// Rejects requests when available inventory is lower than the requested quantity,
    /// strictly guaranteeing that stock levels never become negative under extreme concurrency.
    pub fn reserve(&self, requested_quantity: i64) -> Result<i64, StateError> {
        let update_result = self.stock.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current_stock| {
                if current_stock >= requested_quantity {
                    Some(current_stock - requested_quantity)
                } else {
                    None
                }
            },
        );

        match update_result {
            Ok(previous_stock) => Ok(previous_stock - requested_quantity),
            Err(_) => Err(StateError::InsufficientStock),
        }
    }

    /// Retrieves the current stock count using acquire memory ordering.
    pub fn current_stock(&self) -> i64 {
        self.stock.load(Ordering::Acquire)
    }

    /// Atomically adds units back into available stock in the event of an unrecoverable failure.
    pub fn restore(&self, restored_quantity: i64) {
        self.stock.fetch_add(restored_quantity, Ordering::AcqRel);
    }
}

/// Central application state shared across all HTTP request handlers and background workers.
#[derive(Debug, Clone)]
pub struct AppState {
    /// Concurrent map indexing atomic inventory containers by product identifier.
    pub inventory: Arc<DashMap<String, ProductInventory>>,
    /// Concurrent map recording mapping from client idempotency keys to created order identifiers.
    pub idempotency_store: Arc<DashMap<String, String>>,
    /// Concurrent storage container holding full order domain records by order identifier.
    pub orders: Arc<DashMap<String, Order>>,
    /// Bounded transmission sender feeding asynchronous order processing workers.
    pub queue_sender: Sender<OrderQueueMessage>,
}

impl AppState {
    /// Constructs a initialized application state instance with a designated async worker channel.
    pub fn new(queue_sender: Sender<OrderQueueMessage>) -> Self {
        let state = Self {
            inventory: Arc::new(DashMap::new()),
            idempotency_store: Arc::new(DashMap::new()),
            orders: Arc::new(DashMap::new()),
            queue_sender,
        };

        state.seed_inventory("iphone_16", 100);
        state
    }

    /// Seeds initial inventory quantities for a specified catalog item.
    pub fn seed_inventory(&self, product_id: &str, initial_stock: i64) {
        self.inventory.insert(
            product_id.to_string(),
            ProductInventory::new(product_id, initial_stock),
        );
    }

    /// Locates an existing order identifier registered under a client idempotency key.
    pub fn get_existing_order_by_idempotency_key(&self, key: &str) -> Option<Order> {
        self.idempotency_store
            .get(key)
            .and_then(|order_id| self.orders.get(order_id.value()).map(|o| o.clone()))
    }

    /// Atomically attempts to reserve inventory for a given product and quantity.
    pub fn reserve_inventory(&self, product_id: &str, quantity: i64) -> Result<i64, StateError> {
        match self.inventory.get(product_id) {
            Some(inventory_ref) => inventory_ref.reserve(quantity),
            None => Err(StateError::ProductNotFound),
        }
    }

    /// Restores previously reserved inventory when an asynchronous process fails.
    pub fn rollback_inventory(&self, product_id: &str, quantity: i64) {
        if let Some(inventory_ref) = self.inventory.get(product_id) {
            inventory_ref.restore(quantity);
        }
    }

    /// Registers a newly created order and associates its idempotency key.
    pub fn insert_order(&self, order: Order) {
        self.idempotency_store
            .insert(order.idempotency_key.clone(), order.order_id.clone());
        self.orders.insert(order.order_id.clone(), order);
    }

    /// Retrieves an order record by its unique order identifier.
    pub fn get_order(&self, order_id: &str) -> Option<Order> {
        self.orders.get(order_id).map(|entry| entry.clone())
    }

    /// Updates the life-cycle status of an existing order.
    pub fn update_order_status(
        &self,
        order_id: &str,
        new_status: OrderStatus,
    ) -> Result<(), StateError> {
        match self.orders.get_mut(order_id) {
            Some(mut order_entry) => {
                order_entry.status = new_status;
                order_entry.updated_at = Utc::now();
                Ok(())
            }
            None => Err(StateError::OrderNotFound),
        }
    }

    /// Reads the active stock count for a given product.
    pub fn get_product_stock(&self, product_id: &str) -> Option<i64> {
        self.inventory
            .get(product_id)
            .map(|inventory_ref| inventory_ref.current_stock())
    }
}
