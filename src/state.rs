//! Shared state management for inventory, orders, idempotency, and database coordination.

use crate::db::{persist_order_postgres, reserve_redis_inventory, seed_redis_inventory};
use crate::models::{Order, OrderQueueMessage, OrderStatus};
use chrono::Utc;
use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_postgres::Client as PgClient;

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
    pub fn reserve(&self, requested_quantity: i64) -> Result<i64, StateError> {
        let update_result =
            self.stock
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current_stock| {
                    if current_stock >= requested_quantity {
                        Some(current_stock - requested_quantity)
                    } else {
                        None
                    }
                });

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

/// Central application state shared across HTTP handlers, background workers, and databases.
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
    /// Optional Redis connection manager for distributed atomic reservations.
    pub redis_connection: Option<Arc<tokio::sync::Mutex<redis::aio::ConnectionManager>>>,
    /// Optional PostgreSQL client for durable ACID relational persistence.
    pub postgres_client: Option<Arc<PgClient>>,
}

impl AppState {
    /// Constructs an initialized application state instance with an async worker channel.
    pub fn new(queue_sender: Sender<OrderQueueMessage>) -> Self {
        let state = Self {
            inventory: Arc::new(DashMap::new()),
            idempotency_store: Arc::new(DashMap::new()),
            orders: Arc::new(DashMap::new()),
            queue_sender,
            redis_connection: None,
            postgres_client: None,
        };

        state.seed_inventory("iphone_16", 100);
        state
    }

    /// Configures external Redis connection manager on application state.
    pub fn with_redis(mut self, redis_manager: redis::aio::ConnectionManager) -> Self {
        self.redis_connection = Some(Arc::new(tokio::sync::Mutex::new(redis_manager)));
        self
    }

    /// Configures external PostgreSQL durable database client on application state.
    pub fn with_postgres(mut self, postgres_client: Arc<PgClient>) -> Self {
        self.postgres_client = Some(postgres_client);
        self
    }

    /// Seeds initial inventory quantities for a specified catalog item across in-memory and Redis.
    pub fn seed_inventory(&self, product_id: &str, initial_stock: i64) {
        self.inventory.insert(
            product_id.to_string(),
            ProductInventory::new(product_id, initial_stock),
        );
    }

    /// Asynchronously initializes Redis inventory counters when Redis is connected.
    pub async fn initialize_redis_inventory(&self, product_id: &str, stock: i64) {
        if let Some(ref redis_mutex) = self.redis_connection {
            let mut conn = redis_mutex.lock().await;
            let _ = seed_redis_inventory(&mut conn, product_id, stock).await;
        }
    }

    /// Locates an existing order identifier registered under a client idempotency key.
    pub fn get_existing_order_by_idempotency_key(&self, key: &str) -> Option<Order> {
        self.idempotency_store
            .get(key)
            .and_then(|order_id| self.orders.get(order_id.value()).map(|o| o.clone()))
    }

    /// Atomically attempts to reserve inventory using Redis Lua script or in-memory CAS.
    pub async fn reserve_inventory_async(
        &self,
        product_id: &str,
        quantity: i64,
    ) -> Result<i64, StateError> {
        if let Some(ref redis_mutex) = self.redis_connection {
            let mut conn = redis_mutex.lock().await;
            return reserve_redis_inventory(&mut conn, product_id, quantity).await;
        }

        self.reserve_inventory(product_id, quantity)
    }

    /// Atomically attempts to reserve in-memory inventory using non-blocking CAS.
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

    /// Updates the life-cycle status of an existing order and persists to PostgreSQL if configured.
    pub async fn update_order_status_async(
        &self,
        order_id: &str,
        new_status: OrderStatus,
    ) -> Result<(), StateError> {
        let updated_order = match self.orders.get_mut(order_id) {
            Some(mut order_entry) => {
                order_entry.status = new_status;
                order_entry.updated_at = Utc::now();
                order_entry.clone()
            }
            None => return Err(StateError::OrderNotFound),
        };

        if let Some(ref pg_client) = self.postgres_client {
            let _ = persist_order_postgres(pg_client, &updated_order).await;
        }

        Ok(())
    }

    /// Reads the active stock count for a given product.
    pub fn get_product_stock(&self, product_id: &str) -> Option<i64> {
        self.inventory
            .get(product_id)
            .map(|inventory_ref| inventory_ref.current_stock())
    }
}
