import os
import sys
import time
from datetime import datetime
import redis
import mysql.connector

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, decode_responses=True)
db_conn = None

def get_db():
    global db_conn
    if db_conn is None or not db_conn.is_connected():
        db_conn = mysql.connector.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            database=settings.MYSQL_DATABASE,
            autocommit=False
        )
    else:
        db_conn.ping(reconnect=True, attempts=3, delay=1)
    return db_conn

def log_audit(msg: str):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    redis_client.lpush("audit:recent_logs", f"[{ts}] {msg}")
    redis_client.ltrim("audit:recent_logs", 0, 9)

def sweep_expired_reservations(force_expire_all_pending: bool = False):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    try:
        # Step 1: Non-blocking read of candidate expired orders
        if force_expire_all_pending:
            cursor.execute(
                "SELECT order_id, product_id FROM orders WHERE status = 'PENDING_PAYMENT' LIMIT 200"
            )
        else:
            cursor.execute(
                "SELECT order_id, product_id FROM orders WHERE status = 'PENDING_PAYMENT' AND expires_at <= NOW() LIMIT 200"
            )
        candidates = cursor.fetchall()

        if not candidates:
            conn.commit()
            return 0

        reclaimed_by_product = {}
        expired_ids = []

        # Step 2: Lock orders and transition atomically
        for row in candidates:
            oid = row["order_id"]
            pid = row["product_id"]
            if force_expire_all_pending:
                cursor.execute(
                    "UPDATE orders SET status = 'EXPIRED' WHERE order_id = %s AND status = 'PENDING_PAYMENT'",
                    (oid,)
                )
            else:
                cursor.execute(
                    "UPDATE orders SET status = 'EXPIRED' WHERE order_id = %s AND status = 'PENDING_PAYMENT' AND expires_at <= NOW()",
                    (oid,)
                )
            if cursor.rowcount == 1:
                expired_ids.append(oid)
                reclaimed_by_product[pid] = reclaimed_by_product.get(pid, 0) + 1

        if not expired_ids:
            conn.commit()
            return 0

        # Step 3: Restore physical stock in MySQL
        for pid, count in reclaimed_by_product.items():
            cursor.execute(
                "UPDATE inventory SET stock = stock + %s WHERE product_id = %s",
                (count, pid)
            )

        conn.commit()

        # Step 4: Restore atomic tokens in Redis pipeline
        pipe = redis_client.pipeline()
        for oid in expired_ids:
            pipe.set(f"order:status:{oid}", "EXPIRED", ex=3600)

        for pid, count in reclaimed_by_product.items():
            pipe.incrby(f"inventory:product:{pid}", count)
            pipe.incrby(f"metrics:mysql_persisted_stock:{pid}", count)
            pipe.decrby(f"metrics:pending_payment:{pid}", count)
            pipe.incrby(f"metrics:reclaimed_stock:{pid}", count)

        pipe.execute()

        total_reclaimed = len(expired_ids)
        log_audit(f"TTL REAPER -> RECLAIMED {total_reclaimed} ABANDONED UNITS BACK TO POOL")
        print(f"[♻️] Reaper reclaimed {total_reclaimed} expired reservations -> Restored to MySQL & Redis.")
        return total_reclaimed

    except Exception as e:
        conn.rollback()
        print(f"[!] Reaper transient retry: {e}")
        return 0
    finally:
        cursor.close()

def start_reaper():
    print(
        f"[*] Starting TTL Reservation Reaper (Sweep Interval: {settings.REAPER_INTERVAL_SECONDS}s, "
        f"Order TTL: {settings.RESERVATION_TTL_SECONDS}s)..."
    )
    while True:
        try:
            sweep_expired_reservations()
        except Exception as e:
            print(f"[!] Reaper connection retry: {e}")
        time.sleep(settings.REAPER_INTERVAL_SECONDS)

if __name__ == "__main__":
    start_reaper()