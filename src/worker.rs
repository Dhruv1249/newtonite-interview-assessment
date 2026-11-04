//! Background asynchronous worker processing simulating decoupled order fulfillment and payment confirmation.

use crate::models::{OrderQueueMessage, OrderStatus};
use crate::state::AppState;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tracing::{error, info};

/// Simulated network latency in milliseconds representing downstream payment gateway authorization.
pub const PAYMENT_PROCESSING_LATENCY_MS: u64 = 25;

/// Background asynchronous daemon processing queued flash-sale orders.
#[derive(Debug)]
pub struct OrderWorker {
    /// Ingoing receiver consuming queued order processing tasks.
    pub queue_receiver: Receiver<OrderQueueMessage>,
    /// Application state handle for persisting updated fulfillment statuses.
    pub app_state: AppState,
}

impl OrderWorker {
    /// Instantiates a new worker instance configured with a message channel and shared state.
    pub fn new(queue_receiver: Receiver<OrderQueueMessage>, app_state: AppState) -> Self {
        Self {
            queue_receiver,
            app_state,
        }
    }

    /// Spawns the processing loop on a dedicated Tokio asynchronous runtime task.
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    /// Primary execution loop reading incoming order events until channel termination.
    pub async fn run(&mut self) {
        info!("FlashDrop background order worker started listening for incoming events");

        while let Some(task) = self.queue_receiver.recv().await {
            self.process_order(task).await;
        }

        info!("FlashDrop background order worker gracefully terminated as the channel closed");
    }

    /// Executes simulated fulfillment, payment gateway processing, and status transition.
    async fn process_order(&self, task: OrderQueueMessage) {
        tokio::time::sleep(Duration::from_millis(PAYMENT_PROCESSING_LATENCY_MS)).await;

        match self
            .app_state
            .update_order_status_async(&task.order_id, OrderStatus::Confirmed)
            .await
        {
            Ok(_) => {
                info!(
                    order_id = %task.order_id,
                    product_id = %task.product_id,
                    quantity = task.quantity,
                    "Order successfully fulfilled and confirmed"
                );
            }
            Err(error) => {
                error!(
                    order_id = %task.order_id,
                    ?error,
                    "Failed to transition order status in state store"
                );
                self.app_state
                    .rollback_inventory(&task.product_id, task.quantity);
            }
        }
    }
}
