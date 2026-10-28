import json
import os
import sys
import time
from datetime import datetime
import pika
import redis
import mysql.connector

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, decode_responses=True)
db_conn = None

def get_persistent_db():
    global db_conn
    if db_conn is None or not db_conn.is_connected():
        db_conn = mysql.connector.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            database=settings.MYSQL_DATABASE,
            autocommit=False,
            connection_timeout=30
        )
    else:
        db_conn.ping(reconnect=True, attempts=3, delay=1)
    return db_conn

def log_audit(msg: str):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    redis_client.lpush("audit:recent_logs", f"[{ts}] {msg}")
    redis_client.ltrim("audit:recent_logs", 0, 9)

def process_order(ch, method, properties, body):
    data = json.loads(body.decode())
    order_id = data["order_id"]
    user_id = data["user_id"]
    product_id = data["product_id"]
    ttl_seconds = data.get("ttl_seconds", settings.RESERVATION_TTL_SECONDS)
    status_key = f"order:status:{order_id}"

    try:
        conn = get_persistent_db()
        cursor = conn.cursor()

        cursor.execute(
            "UPDATE inventory SET stock = stock - 1 WHERE product_id = %s AND stock > 0",
            (product_id,)
        )

        if cursor.rowcount == 1:
            cursor.execute(
                """
                INSERT INTO orders (order_id, user_id, product_id, status, expires_at)
                VALUES (%s, %s, %s, 'PENDING_PAYMENT', DATE_ADD(NOW(), INTERVAL %s SECOND))
                """,
                (order_id, user_id, product_id, ttl_seconds)
            )
            conn.commit()

            pipe = redis_client.pipeline()
            pipe.set(status_key, "PENDING_PAYMENT", ex=3600)
            pipe.decr(f"metrics:mysql_persisted_stock:{product_id}")
            pipe.incr(f"metrics:pending_payment:{product_id}")
            pipe.execute()

            log_audit(f"ORDER {order_id[:8]} ({user_id}) -> RESERVED (TTL {ttl_seconds}s)")
            print(f"[✓] Order {order_id[:8]} RESERVED (PENDING_PAYMENT) in MySQL.")
        else:
            conn.rollback()
            redis_client.set(status_key, "FAILED_OUT_OF_STOCK", ex=3600)
            log_audit(f"ORDER {order_id[:8]} -> REJECTED BY SQL GUARD")

        cursor.close()
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except mysql.connector.Error as db_err:
        print(f"[!] MySQL Error on {order_id[:8]}: {db_err}")
        global db_conn
        try:
            if db_conn:
                db_conn.close()
        except Exception:
            pass
        db_conn = None
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    except Exception as e:
        print(f"[!] Poison message for {order_id[:8]}: {e} -> Routing to DLQ")
        redis_client.incr("metrics:dlq_count")
        log_audit(f"ORDER {order_id[:8]} -> ROUTED TO DLQ")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_worker():
    print("[*] Initializing Persistent MySQL Connection & RabbitMQ Consumer...")
    while True:
        try:
            get_persistent_db()
            params = pika.ConnectionParameters(
                host=settings.RABBITMQ_HOST, heartbeat=600, blocked_connection_timeout=300
            )
            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            channel.exchange_declare(exchange=settings.DLX_NAME, exchange_type="direct", durable=True)
            channel.queue_declare(queue=settings.DLQ_NAME, durable=True)
            channel.queue_bind(exchange=settings.DLX_NAME, queue=settings.DLQ_NAME, routing_key=settings.DLQ_NAME)

            channel.queue_declare(
                queue=settings.QUEUE_NAME,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": settings.DLX_NAME,
                    "x-dead-letter-routing-key": settings.DLQ_NAME
                }
            )
            channel.basic_qos(prefetch_count=settings.WORKER_PREFETCH_COUNT)
            channel.basic_consume(queue=settings.QUEUE_NAME, on_message_callback=process_order)
            print("[*] Worker ready. Processing checkout queue...")
            channel.start_consuming()
        except Exception as e:
            print(f"[!] Worker reconnecting in 2s: {e}")
            time.sleep(2)

if __name__ == "__main__":
    start_worker()