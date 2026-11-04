#!/usr/bin/env python3
"""
FlashDrop: Zero-Dependency Python 10,000 Customer Flash Sale Simulator
Requires only standard Python 3 (no pip dependencies required).
"""

import sys
import time
import json
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

TARGET_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
TOTAL_CUSTOMERS = 10_000
CONCURRENCY_WORKERS = 100

print("=" * 60)
print(" FlashDrop: Zero-Dependency Python Simulation (10,000 Requests)")
print(f" Target URL: {TARGET_URL}/orders")
print(f" Concurrency: {CONCURRENCY_WORKERS} worker threads")
print(f" Total Requests: {TOTAL_CUSTOMERS}")
print("=" * 60)

def send_order(customer_idx):
    payload = json.dumps({
        "userId": f"py_user_{customer_idx}",
        "productId": "iphone_16",
        "quantity": 1,
        "idempotencyKey": f"py_idem_{customer_idx}"
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{TARGET_URL}/orders",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:
        return f"ERR_{type(e).__name__}"

start_time = time.time()
accepted_count = 0
conflict_count = 0
error_count = 0

with ThreadPoolExecutor(max_workers=CONCURRENCY_WORKERS) as executor:
    futures = [executor.submit(send_order, i) for i in range(TOTAL_CUSTOMERS)]
    for f in as_completed(futures):
        status = f.result()
        if status in (200, 201, 202):
            accepted_count += 1
        elif status == 409:
            conflict_count += 1
        else:
            error_count += 1

elapsed = time.time() - start_time
rps = TOTAL_CUSTOMERS / elapsed if elapsed > 0 else 0

print("\n" + "-" * 20 + " Simulation Results " + "-" * 20)
print(f"Elapsed Time:           {elapsed:.2f}s")
print(f"Throughput:             {rps:.2f} requests/sec")
print(f"202 ACCEPTED (Purchased): {accepted_count}")
print(f"409 CONFLICT (Sold Out):  {conflict_count}")
print(f"Anomalous Errors:       {error_count}")

# Health check audit
try:
    with urllib.request.urlopen(f"{TARGET_URL}/health", timeout=5) as health_resp:
        health_data = json.loads(health_resp.read().decode("utf-8"))
        print("\nFinal System State:")
        print(json.dumps(health_data, indent=2))
        remaining_stock = health_data.get("inventory", {}).get("iphone_16", -1)
        
        print("\n" + "-" * 20 + " Correctness Audit " + "-" * 20)
        if accepted_count == 100 and conflict_count == 9900 and remaining_stock == 0:
            print("PASSED: Zero overselling confirmed!")
            print("Exactly 100 orders accepted, 9,900 rejected, remaining stock is 0.")
        else:
            print("FAILED: Invariant violated!")
            sys.exit(1)
except Exception as e:
    print(f"Health check query failed: {e}")
    sys.exit(1)
