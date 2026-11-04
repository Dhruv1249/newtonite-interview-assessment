//! Database and distributed cache integration providing PostgreSQL persistence and Redis coordination.

use crate::models::{Order, OrderStatus};
use crate::state::StateError;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info};

/// Lua script executed atomically on Redis to reserve stock without race conditions.
pub const REDIS_LUA_STOCK_RESERVATION_SCRIPT: &str = r#"
local current_stock = tonumber(redis.call('GET', KEYS[1]) or '0')
local requested_quantity = tonumber(ARGV[1])
if current_stock >= requested_quantity then
    local remaining_stock = redis.call('DECRBY', KEYS[1], requested_quantity)
    return remaining_stock
else
    return -1
end
"#;

/// Establishes an asynchronous connection manager to a target Redis instance.
pub async fn connect_redis(
    redis_url: &str,
) -> Result<redis::aio::ConnectionManager, Box<dyn std::error::Error + Send + Sync>> {
    let client = redis::Client::open(redis_url)?;
    let connection_manager = redis::aio::ConnectionManager::new(client).await?;
    info!(
        redis_url,
        "Successfully connected to Redis cluster/instance"
    );
    Ok(connection_manager)
}

/// Seeds initial inventory into Redis if the key is not already populated.
pub async fn seed_redis_inventory(
    connection: &mut redis::aio::ConnectionManager,
    product_id: &str,
    initial_stock: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let inventory_cache_key = format!("inventory:{product_id}");
    let _: () = connection
        .set_nx(inventory_cache_key, initial_stock)
        .await?;
    Ok(())
}

/// Executes an atomic Lua reservation script against Redis.
pub async fn reserve_redis_inventory(
    connection: &mut redis::aio::ConnectionManager,
    product_id: &str,
    quantity: i64,
) -> Result<i64, StateError> {
    let inventory_cache_key = format!("inventory:{product_id}");
    let script = redis::Script::new(REDIS_LUA_STOCK_RESERVATION_SCRIPT);

    let script_invocation = script
        .key(&inventory_cache_key)
        .arg(quantity)
        .invoke_async::<i64>(connection)
        .await;

    match script_invocation {
        Ok(remaining) if remaining >= 0 => Ok(remaining),
        Ok(_) => Err(StateError::InsufficientStock),
        Err(redis_err) => {
            error!(?redis_err, "Redis atomic reservation execution failure");
            Err(StateError::InsufficientStock)
        }
    }
}

/// Establishes a background asynchronous connection to a PostgreSQL database.
pub async fn connect_postgres(
    database_url: &str,
) -> Result<Arc<Client>, Box<dyn std::error::Error + Send + Sync>> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!(error = %e, "PostgreSQL connection background error");
        }
    });

    info!("Successfully connected to PostgreSQL durable database");
    initialize_postgres_schema(&client).await?;
    Ok(Arc::new(client))
}

/// Prepares the necessary durable relational schema inside PostgreSQL.
pub async fn initialize_postgres_schema(
    client: &Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ddl_statement = r#"
    CREATE TABLE IF NOT EXISTS orders (
        order_id VARCHAR(64) PRIMARY KEY,
        user_id VARCHAR(64) NOT NULL,
        product_id VARCHAR(64) NOT NULL,
        quantity BIGINT NOT NULL,
        idempotency_key VARCHAR(128) UNIQUE NOT NULL,
        status VARCHAR(32) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_orders_idempotency ON orders(idempotency_key);
    "#;

    client.batch_execute(ddl_statement).await?;
    info!("Verified PostgreSQL orders schema and indexes");
    Ok(())
}

/// Persists a confirmed or pending order entity into PostgreSQL.
pub async fn persist_order_postgres(
    client: &Client,
    order: &Order,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let status_string = match order.status {
        OrderStatus::Pending => "PENDING",
        OrderStatus::Confirmed => "CONFIRMED",
        OrderStatus::Failed => "FAILED",
    };

    let sql_query = r#"
    INSERT INTO orders (order_id, user_id, product_id, quantity, idempotency_key, status, created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (idempotency_key) DO UPDATE
    SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at;
    "#;

    client
        .execute(
            sql_query,
            &[
                &order.order_id,
                &order.user_id,
                &order.product_id,
                &order.quantity,
                &order.idempotency_key,
                &status_string,
                &order.created_at,
                &order.updated_at,
            ],
        )
        .await?;

    Ok(())
}

/// Fetches an order record directly from PostgreSQL by its identifier.
pub async fn fetch_order_postgres(
    client: &Client,
    order_id: &str,
) -> Result<Option<Order>, Box<dyn std::error::Error + Send + Sync>> {
    let sql_query = r#"
    SELECT order_id, user_id, product_id, quantity, idempotency_key, status, created_at, updated_at
    FROM orders
    WHERE order_id = $1
    LIMIT 1;
    "#;

    let rows = client.query(sql_query, &[&order_id]).await?;
    if rows.is_empty() {
        return Ok(None);
    }

    let row = &rows[0];
    let status_str: String = row.get(5);
    let status = match status_str.as_str() {
        "CONFIRMED" => OrderStatus::Confirmed,
        "FAILED" => OrderStatus::Failed,
        _ => OrderStatus::Pending,
    };

    let created_at: DateTime<Utc> = row.get(6);
    let updated_at: DateTime<Utc> = row.get(7);

    Ok(Some(Order {
        order_id: row.get(0),
        user_id: row.get(1),
        product_id: row.get(2),
        quantity: row.get(3),
        idempotency_key: row.get(4),
        status,
        created_at,
        updated_at,
    }))
}
