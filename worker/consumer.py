import json
import os
import time
import pika
import redis
import mysql.connector

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
QUEUE_NAME = "checkout_queue"

redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user="root",
        password="rootpassword",
        database="flashsale",
        autocommit=False
    )

def process_order(ch, method, properties, body):
    data = json.loads(body.decode())
    order_id = data["order_id"]
    user_id = data["user_id"]
    product_id = data["product_id"]
    status_key = f"order:status:{order_id}"

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Strict ACID Conditional Update (Phase C Requirement)
        cursor.execute(
            "UPDATE inventory SET stock = stock - 1 WHERE product_id = %s AND stock > 0",
            (product_id,)
        )

        if cursor.rowcount == 1:
            cursor.execute(
                "INSERT INTO orders (order_id, user_id, product_id, status) VALUES (%s, %s, %s, 'CONFIRMED')",
                (order_id, user_id, product_id)
            )
            conn.commit()
            redis_client.set(status_key, "CONFIRMED", ex=3600)
            print(f"[✓] Order {order_id} CONFIRMED in MySQL.")
        else:
            conn.rollback()
            redis_client.set(status_key, "FAILED_OUT_OF_STOCK", ex=3600)
            print(f"[✗] Order {order_id} rejected by DB conditional check.")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        conn.rollback()
        print(f"[!] Database error for {order_id}: {e}")
        # Requeue message on transient DB failure so zero orders are lost
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    finally:
        cursor.close()
        conn.close()

def start_worker():
    print("[*] Connecting to RabbitMQ broker...")
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            # Prefetch=1 ensures sequential, controlled pressure on MySQL
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_order)
            print("[*] Worker ready. Processing checkout queue sequentially...")
            channel.start_consuming()
        except Exception as e:
            print(f"[!] Broker connection retry in 3s: {e}")
            time.sleep(3)

if __name__ == "__main__":
    start_worker()