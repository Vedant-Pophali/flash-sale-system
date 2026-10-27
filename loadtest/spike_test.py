import asyncio
import time
import uuid
# pyrefly: ignore[missing-import]
import httpx

GATEWAY_URL = "http://localhost:8000"
TOTAL_CONCURRENT_REQUESTS = 2500  # Simulates 5x oversubscription spike locally
CONCURRENCY_LIMIT = 200           # Max parallel open sockets on Windows host

async def send_checkout(client: httpx.AsyncClient, sem: asyncio.Semaphore, idx: int):
    async with sem:
        user_id = f"load_user_{idx}"
        idem_key = str(uuid.uuid4())
        try:
            resp = await client.post(
                f"{GATEWAY_URL}/api/v1/checkout",
                json={"user_id": user_id, "product_id": 1},
                headers={"X-Idempotency-Key": idem_key},
                timeout=15.0
            )
            return resp.status_code
        except Exception:
            return 500

async def run_spike_test():
    print("=" * 65)
    print("🚀 INITIALIZING FLASH-SALE HIGH-CONCURRENCY SPIKE TEST")
    print("=" * 65)

    limits = httpx.Limits(max_keepalive_connections=CONCURRENCY_LIMIT, max_connections=CONCURRENCY_LIMIT)
    async with httpx.AsyncClient(limits=limits) as client:
        # 1. Reset inventory to 500
        print("[1/4] Resetting Redis & MySQL inventory to exactly 500 units...")
        await client.post(f"{GATEWAY_URL}/api/v1/admin/reset")

        # 2. Fire concurrent traffic spike
        print(f"[2/4] Firing {TOTAL_CONCURRENT_REQUESTS} concurrent checkout requests...")
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        start_time = time.perf_counter()

        tasks = [send_checkout(client, sem, i) for i in range(TOTAL_CONCURRENT_REQUESTS)]
        results = await asyncio.gather(*tasks)

        elapsed = time.perf_counter() - start_time
        rps = TOTAL_CONCURRENT_REQUESTS / elapsed

        code_202 = results.count(202)
        code_409 = results.count(409)
        code_429 = results.count(429)
        errors = len(results) - (code_202 + code_409 + code_429)

        print(f"\n⚡ SPIKE COMPLETED IN {elapsed:.2f}s ({rps:.0f} req/sec)")
        print(f"   ├── 202 Accepted (Reserved in Redis & Queued): {code_202}")
        print(f"   ├── 409 Conflict (Sold Out Fast-Rejections)  : {code_409}")
        print(f"   ├── 429 Too Many Requests (Bot Rate-Limited) : {code_429}")
        print(f"   └── 5xx Dropped / Timeout Errors             : {errors}")

        # 3. Wait for async RabbitMQ worker to drain the 500 queued orders into MySQL
        print("\n[3/4] Waiting for RabbitMQ async consumer to persist orders to MySQL...")
        while True:
            m_resp = await client.get(f"{GATEWAY_URL}/api/v1/metrics")
            metrics = m_resp.json()
            confirmed = metrics["confirmed_orders_count"]
            print(f"   -> MySQL Confirmed Orders: {confirmed} / 500", end="\r")
            if confirmed >= code_202 or confirmed == 500:
                print()
                break
            await asyncio.sleep(0.5)

        # 4. Final Data Consistency & Reconciliation Audit
        print("\n[4/4] EXECUTING POST-SALE ACID RECONCILIATION AUDIT:")
        print("-" * 65)
        print(f"   • Initial Physical Inventory        : 500")
        print(f"   • Final Redis Atomic Token Count    : {metrics['redis_atomic_stock']}")
        print(f"   • Final MySQL Inventory Stock       : {metrics['mysql_persisted_stock']}")
        print(f"   • Final MySQL Confirmed Order Rows  : {metrics['confirmed_orders_count']}")
        print("-" * 65)

        if metrics["redis_atomic_stock"] == 0 and metrics["mysql_persisted_stock"] == 0 and metrics["confirmed_orders_count"] == 500:
            print("✅ AUDIT PASSED: EXACTLY 500 SOLD. ZERO OVERSELLING. ZERO RACE CONDITIONS.")
        else:
            print("❌ AUDIT WARNING: State mismatch detected!")
        print("=" * 65)

if __name__ == "__main__":
    asyncio.run(run_spike_test())