//! Deep unit and stress tests targeting hardware atomic operations, CAS race conditions, and memory consistency.

use flashdrop::state::{ProductInventory, StateError};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::task::JoinSet;

/// Verifies that multiple concurrent tasks contending for the single last remaining item results in exactly one winner.
#[tokio::test]
async fn test_atomic_cas_race_for_last_item() {
    let inventory = Arc::new(ProductInventory::new("iphone_16", 1));
    let competing_threads_count = 200;
    let successful_reservations = Arc::new(AtomicUsize::new(0));
    let rejected_reservations = Arc::new(AtomicUsize::new(0));

    let mut join_set = JoinSet::new();

    for _ in 0..competing_threads_count {
        let inv = Arc::clone(&inventory);
        let successes = Arc::clone(&successful_reservations);
        let rejections = Arc::clone(&rejected_reservations);

        join_set.spawn(async move {
            match inv.reserve(1) {
                Ok(_) => {
                    successes.fetch_add(1, Ordering::SeqCst);
                }
                Err(StateError::InsufficientStock) => {
                    rejections.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {}
            }
        });
    }

    while let Some(task_result) = join_set.join_next().await {
        task_result.unwrap();
    }

    assert_eq!(
        successful_reservations.load(Ordering::SeqCst),
        1,
        "Exactly one task must succeed in acquiring the single remaining atomic stock unit"
    );
    assert_eq!(
        rejected_reservations.load(Ordering::SeqCst),
        competing_threads_count - 1,
        "All other tasks must receive InsufficientStock error"
    );
    assert_eq!(
        inventory.current_stock(),
        0,
        "Inventory stock must reach exactly zero without becoming negative"
    );
}

/// Verifies boundary condition behaviors for atomic inventory reservations.
#[test]
fn test_atomic_inventory_boundary_conditions() {
    let inventory = ProductInventory::new("iphone_16", 5);

    let over_reservation_result = inventory.reserve(10);
    assert_eq!(over_reservation_result, Err(StateError::InsufficientStock));
    assert_eq!(inventory.current_stock(), 5);

    let exact_reservation_result = inventory.reserve(5);
    assert_eq!(exact_reservation_result, Ok(0));
    assert_eq!(inventory.current_stock(), 0);

    let post_exhaustion_result = inventory.reserve(1);
    assert_eq!(post_exhaustion_result, Err(StateError::InsufficientStock));
    assert_eq!(inventory.current_stock(), 0);
}

/// Tests atomic restore operations ensuring unfulfilled reservations are safely returned to available stock.
#[tokio::test]
async fn test_atomic_rollback_and_restore_concurrency() {
    let inventory = Arc::new(ProductInventory::new("iphone_16", 100));
    let restoration_iterations = 50;
    let mut join_set = JoinSet::new();

    for _ in 0..restoration_iterations {
        let inv = Arc::clone(&inventory);
        join_set.spawn(async move {
            let reservation_outcome = inv.reserve(1);
            if reservation_outcome.is_ok() {
                inv.restore(1);
            }
        });
    }

    while let Some(task_result) = join_set.join_next().await {
        task_result.unwrap();
    }

    assert_eq!(
        inventory.current_stock(),
        100,
        "Stock level must be fully conserved after balanced atomic reserve and restore operations"
    );
}

/// Validates strict conservation of inventory invariants under high-throughput mixed operations.
#[tokio::test]
async fn test_atomic_conservation_invariant_under_heavy_mixed_contention() {
    let initial_stock = 500;
    let inventory = Arc::new(ProductInventory::new("iphone_16", initial_stock));
    let total_operations = 2_000;
    let total_permanently_reserved = Arc::new(AtomicI64::new(0));

    let mut join_set = JoinSet::new();

    for operation_index in 0..total_operations {
        let inv = Arc::clone(&inventory);
        let ledger = Arc::clone(&total_permanently_reserved);

        join_set.spawn(async move {
            let requested_units = (operation_index % 3) + 1;
            if inv.reserve(requested_units).is_ok() {
                if operation_index % 5 == 0 {
                    inv.restore(requested_units);
                } else {
                    ledger.fetch_add(requested_units, Ordering::SeqCst);
                }
            }
        });
    }

    while let Some(task_result) = join_set.join_next().await {
        task_result.unwrap();
    }

    let remaining_stock = inventory.current_stock();
    let recorded_deductions = total_permanently_reserved.load(Ordering::SeqCst);

    assert_eq!(
        remaining_stock + recorded_deductions,
        initial_stock,
        "Sum of remaining stock and net successful reservations must strictly equal the initial stock"
    );
    assert!(
        remaining_stock >= 0,
        "Stock count must never drop below zero under any interleaving of CAS executions"
    );
}
