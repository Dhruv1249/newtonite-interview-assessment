//! Standalone load generator simulating 10,000 concurrent customers firing flash-sale order requests.

use reqwest::StatusCode;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

/// Target total count of distinct concurrent customer requests fired during the simulation.
pub const TOTAL_SIMULATED_CUSTOMERS: usize = 10_000;

/// Default target HTTP host base uniform resource locator.
pub const DEFAULT_TARGET_BASE_URL: &str = "http://127.0.0.1:8080";

/// Maximum concurrent in-flight HTTP requests allowed to avoid socket file descriptor exhaustion.
pub const CONCURRENCY_WINDOW_LIMIT: usize = 500;

/// Entry point executing the 10,000 customer HTTP load simulation against FlashDrop.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target_base_url = std::env::var("TARGET_URL")
        .unwrap_or_else(|_| DEFAULT_TARGET_BASE_URL.to_string());

    println!("===========================================================");
    println!(" FlashDrop: 10,000 Customer High-Concurrency Flash Sale Sim");
    println!(" Target URL: {}/orders", target_base_url);
    println!(" Customers:  {}", TOTAL_SIMULATED_CUSTOMERS);
    println!(" Product:    iphone_16 (Initial Stock: 100 units)");
    println!("===========================================================\n");

    let http_client = Arc::new(
        reqwest::Client::builder()
            .pool_max_idle_per_host(CONCURRENCY_WINDOW_LIMIT)
            .timeout(std::time::Duration::from_secs(10))
            .build()?,
    );

    let accepted_orders_counter = Arc::new(AtomicUsize::new(0));
    let conflict_rejections_counter = Arc::new(AtomicUsize::new(0));
    let anomalous_failures_counter = Arc::new(AtomicUsize::new(0));

    let simulation_start_time = Instant::now();
    let mut task_join_set = JoinSet::new();
    let concurrency_semaphore = Arc::new(tokio::sync::Semaphore::new(CONCURRENCY_WINDOW_LIMIT));

    for customer_index in 0..TOTAL_SIMULATED_CUSTOMERS {
        let client_ref = Arc::clone(&http_client);
        let accepted_ref = Arc::clone(&accepted_orders_counter);
        let conflict_ref = Arc::clone(&conflict_rejections_counter);
        let failures_ref = Arc::clone(&anomalous_failures_counter);
        let base_url = target_base_url.clone();
        let permit_semaphore = Arc::clone(&concurrency_semaphore);

        task_join_set.spawn(async move {
            let _permit = permit_semaphore.acquire().await.unwrap();
            let payload = json!({
                "userId": format!("user_{customer_index}"),
                "productId": "iphone_16",
                "quantity": 1,
                "idempotencyKey": format!("sim_idem_token_{customer_index}")
            });

            match client_ref
                .post(format!("{base_url}/orders"))
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => match response.status() {
                    StatusCode::ACCEPTED | StatusCode::OK => {
                        accepted_ref.fetch_add(1, Ordering::SeqCst);
                    }
                    StatusCode::CONFLICT => {
                        conflict_ref.fetch_add(1, Ordering::SeqCst);
                    }
                    unexpected_status => {
                        eprintln!("Unexpected HTTP status code: {unexpected_status}");
                        failures_ref.fetch_add(1, Ordering::SeqCst);
                    }
                },
                Err(network_error) => {
                    eprintln!("Network dispatch error: {network_error}");
                    failures_ref.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
    }

    while let Some(task_completion) = task_join_set.join_next().await {
        task_completion?;
    }

    let elapsed_duration = simulation_start_time.elapsed();
    let total_accepted = accepted_orders_counter.load(Ordering::SeqCst);
    let total_conflicts = conflict_rejections_counter.load(Ordering::SeqCst);
    let total_failures = anomalous_failures_counter.load(Ordering::SeqCst);
    let requests_per_second =
        (TOTAL_SIMULATED_CUSTOMERS as f64) / elapsed_duration.as_secs_f64();

    println!("\n---------------- Simulation Results ----------------");
    println!("Elapsed Time:           {:.2?}", elapsed_duration);
    println!("Throughput:             {:.2} requests/sec", requests_per_second);
    println!("202 ACCEPTED (Purchased): {}", total_accepted);
    println!("409 CONFLICT (Sold Out):  {}", total_conflicts);
    println!("Anomalous Failures:     {}", total_failures);

    let health_response = http_client
        .get(format!("{target_base_url}/health"))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    println!("\nFinal System State:");
    println!("{}", serde_json::to_string_pretty(&health_response)?);

    let remaining_stock = health_response["inventory"]["iphone_16"]
        .as_i64()
        .unwrap_or(-1);

    println!("\n---------------- Correctness Audit ----------------");
    if total_accepted == 100 && total_conflicts == 9_900 && remaining_stock == 0 {
        println!("PASSED: Zero overselling confirmed!");
        println!("Exactly 100 orders accepted, 9,900 rejected, remaining stock is 0.");
        Ok(())
    } else {
        eprintln!("FAILED: Invariant violated!");
        eprintln!(
            "Expected accepted=100, got={}; Expected conflicts=9900, got={}; Expected stock=0, got={}",
            total_accepted, total_conflicts, remaining_stock
        );
        std::process::exit(1);
    }
}
