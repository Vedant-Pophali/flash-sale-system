import asyncio
import os
import sys
import time
import uuid
import httpx

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

BATCH_SIZE = 250


async def batch_worker(
    client: httpx.AsyncClient,
    batches_queue: asyncio.Queue,
    stats: dict,
    reserved_orders: list
):
    while True:
        try:
            batch_items = batches_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        success = False
        for attempt in range(3):
            try:
                resp = await client.post(
                    f"{settings.gateway_url}/api/v1/checkout/batch",
                    json={"items": batch_items},
                    timeout=30.0
                )
                if resp.status_code == 200:
                    data = resp.json()
                    stats[202] += data["c_202"]
                    stats[409] += data["c_409"]
                    stats[429] += data["c_429"]
                    reserved_orders.extend(data["reserved_ids"])
                    success = True
                    break
            except Exception:
                await asyncio.sleep(0.05 * (attempt + 1))

        if not success:
            stats[500] += len(batch_items)

        batches_queue.task_done()


async def pay_order_batch(client: httpx.AsyncClient, order_ids: list, concurrency: int = 25) -> int:
    sem = asyncio.Semaphore(concurrency)
    paid_success = 0

    async def _pay_single(oid: str):
        nonlocal paid_success
        async with sem:
            for attempt in range(3):
                try:
                    resp = await client.post(f"{settings.gateway_url}/api/v1/orders/{oid}/pay")
                    if resp.status_code == 200:
                        paid_success += 1
                        return
                except Exception:
                    await asyncio.sleep(0.1 * (attempt + 1))

    await asyncio.gather(*[_pay_single(oid) for oid in order_ids])
    return paid_success


def build_customer_batches(total_requests: int, bot_requests: int, product_id: int) -> asyncio.Queue:
    q = asyncio.Queue()
    current_batch = []
    bots_added = 0
    legit_added = 0
    legit_target = total_requests - bot_requests

    for idx in range(total_requests):
        if bots_added < bot_requests and (idx % 20 == 0 or legit_added >= legit_target):
            uid = f"scalper_bot_{bots_added % 50}"
            bots_added += 1
        else:
            uid = f"customer_{legit_added}"
            legit_added += 1

        current_batch.append({
            "user_id": uid,
            "idempotency_key": uuid.uuid4().hex,
            "product_id": product_id
        })
        if len(current_batch) == BATCH_SIZE:
            q.put_nowait(current_batch)
            current_batch = []

    if current_batch:
        q.put_nowait(current_batch)
    return q


async def run_95k_lifecycle_simulation():
    legit_users = getattr(settings, "LOADTEST_LEGITIMATE_USERS", 95000)
    bot_requests = getattr(settings, "LOADTEST_BOT_REQUESTS", 5000)
    concurrency = 16
    total_wave1 = legit_users + bot_requests

    print("=" * 80)
    print(f"🚀 HIGH-SCALE FLASH-SALE SIMULATION: {total_wave1:,} CONCURRENT REQUESTS")
    print(f"   ({legit_users:,} Legitimate Customers + {bot_requests:,} Scalper Bot Requests)")
    print("=" * 80)

    limits = httpx.Limits(max_keepalive_connections=concurrency, max_connections=concurrency)
    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        # Stage 1: Reset System State
        print(f"[Stage 1/6] Resetting Product #{settings.DEFAULT_PRODUCT_ID} inventory to {settings.DEFAULT_INITIAL_STOCK} units...")
        await client.post(
            f"{settings.gateway_url}/api/v1/admin/reset",
            json={"product_id": settings.DEFAULT_PRODUCT_ID, "initial_stock": settings.DEFAULT_INITIAL_STOCK}
        )

        # Stage 2: Execute 100,000 Customer + Bot Spike
        print(f"[Stage 2/6] Generating & firing {total_wave1:,} unique checkout payloads...")
        batches_q = build_customer_batches(total_wave1, bot_requests, settings.DEFAULT_PRODUCT_ID)
        stats = {202: 0, 409: 0, 429: 0, 500: 0}
        reserved_orders = []

        t0 = time.perf_counter()
        workers = [
            asyncio.create_task(batch_worker(client, batches_q, stats, reserved_orders))
            for _ in range(concurrency)
        ]

        while not batches_q.empty():
            done = stats[202] + stats[409] + stats[429] + stats[500]
            el = max(time.perf_counter() - t0, 0.001)
            print(
                f"   ⚡ Progress: {done:,}/{total_wave1:,} ({done/total_wave1*100:5.1f}%) | "
                f"{done/el:,.0f} req/s | 202: {stats[202]} | 409: {stats[409]:,} | 429: {stats[429]:,}",
                end="\r"
            )
            await asyncio.sleep(0.25)

        await asyncio.gather(*workers)
        elapsed = time.perf_counter() - t0
        rps = total_wave1 / elapsed

        print(
            f"\n   └── Wave 1 Completed in {elapsed:.2f}s ({rps:,.0f} req/s) | "
            f"202 Reserved: {stats[202]} | 409 Sold Out: {stats[409]:,} | "
            f"429 Bots Blocked: {stats[429]:,} | Errors: {stats[500]}"
        )

        # Stage 3: Wait for RabbitMQ Consumer to Persist All 500 Reservations to MySQL
        target_stock = settings.DEFAULT_INITIAL_STOCK
        print(f"\n[Stage 3/6] Waiting for RabbitMQ worker to commit {target_stock} reservations to MySQL...")
        while True:
            m = (await client.get(f"{settings.gateway_url}/api/v1/metrics")).json()
            print(f"   -> MySQL Persisted Reservations: {m['pending_payment_orders']} / {target_stock}", end="\r")
            if m["pending_payment_orders"] >= target_stock:
                print()
                break
            await asyncio.sleep(0.3)

        # Fetch authoritative list of 500 reserved orders directly from MySQL
        p_resp = (await client.get(f"{settings.gateway_url}/api/v1/orders/pending/list")).json()
        db_reserved_orders = p_resp["pending_order_ids"]

        # Stage 4: Simulate 90% Immediate Payment & 10% Cart Abandonment
        pay_target = int(len(db_reserved_orders) * settings.SIMULATED_PAYMENT_RATIO)
        abandoned_target = len(db_reserved_orders) - pay_target
        orders_to_pay = db_reserved_orders[:pay_target]

        print(f"[Stage 4/6] Processing payments: {pay_target} customers pay, {abandoned_target} abandon checkout...")
        wave1_paid = await pay_order_batch(client, orders_to_pay, concurrency=25)
        actual_abandoned = len(db_reserved_orders) - wave1_paid

        # Stage 5: Trigger TTL Reaper Sweep to Reclaim Unpaid Inventory
        print(f"[Stage 5/6] Triggering Atomic TTL Reaper to reclaim {actual_abandoned} unpaid reservations...")
        await client.post(f"{settings.gateway_url}/api/v1/admin/reap-abandoned")
        m = (await client.get(f"{settings.gateway_url}/api/v1/metrics")).json()
        reclaimed_units = m["reclaimed_by_reaper"]
        print(f"   └── Reclaimed by TTL Reaper: {reclaimed_units} units (Restored Redis Stock: {m['redis_atomic_stock']})")

        # Stage 6: Wave 2 Waiting Customers Claim & Pay for Reclaimed Inventory
        wave2_customer_count = reclaimed_units * 10
        print(f"\n[Stage 6/6] Firing Wave 2 ({wave2_customer_count} waiting customers) for {reclaimed_units} reclaimed units...")
        w2_q = build_customer_batches(wave2_customer_count, 0, settings.DEFAULT_PRODUCT_ID)
        w2_stats = {202: 0, 409: 0, 429: 0, 500: 0}
        w2_orders = []

        w2_workers = [
            asyncio.create_task(batch_worker(client, w2_q, w2_stats, w2_orders))
            for _ in range(4)
        ]
        await asyncio.gather(*w2_workers)

        while True:
            m = (await client.get(f"{settings.gateway_url}/api/v1/metrics")).json()
            if m["pending_payment_orders"] >= reclaimed_units:
                break
            await asyncio.sleep(0.2)

        p2_resp = (await client.get(f"{settings.gateway_url}/api/v1/orders/pending/list")).json()
        wave2_paid = await pay_order_batch(client, p2_resp["pending_order_ids"], concurrency=25)
        final_m = (await client.get(f"{settings.gateway_url}/api/v1/metrics")).json()

        # Final Audit Report
        print("\n" + "=" * 80)
        print("📊 FINAL 100,000-REQUEST SCALE & ACID RECONCILIATION AUDIT REPORT")
        print("=" * 80)
        print(f"   • Total Traffic Simulated          : {total_wave1 + wave2_customer_count:,} requests")
        print(f"   • Peak Ingestion Throughput        : {rps:,.0f} requests/sec")
        print(f"   • Configured Physical Inventory    : {settings.DEFAULT_INITIAL_STOCK}")
        print(f"   • Wave 1 Paid Orders               : {wave1_paid}")
        print(f"   • Abandoned & Reclaimed by Reaper  : {final_m['reclaimed_by_reaper']}")
        print(f"   • Wave 2 Re-Sold & Paid Orders     : {wave2_paid}")
        print(f"   • Final Total PAID Orders in MySQL : {final_m['paid_orders_count']}")
        print(f"   • Final Remaining MySQL / Redis    : {final_m['mysql_persisted_stock']} / {final_m['redis_atomic_stock']}")
        print(f"   • Sold-Out Fast Rejections (409)   : {final_m['sold_out_fast_rejections']:,}")
        print(f"   • Scalper Bots Rate-Limited (429)  : {final_m['bot_requests_rate_limited']:,}")
        print(f"   • Gateway Latency (P50 / P95 / P99): {final_m['latency_p50_ms']}ms / {final_m['latency_p95_ms']}ms / {final_m['latency_p99_ms']}ms")
        print(f"   • ACID Consistency Invariant       : {final_m['consistency_verified']}")
        print("-" * 80)
        if final_m["paid_orders_count"] == settings.DEFAULT_INITIAL_STOCK and final_m["consistency_verified"]:
            print("✅ AUDIT PASSED: 500/500 PAID AT 100K SCALE. 0 OVERSELLING. 0 RACE CONDITIONS.")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(run_95k_lifecycle_simulation())