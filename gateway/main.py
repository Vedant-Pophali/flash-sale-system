import json
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import redis.asyncio as redis
import aio_pika
import mysql.connector

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
QUEUE_NAME = "checkout_queue"
PRODUCT_ID = 1
INITIAL_STOCK = 500

redis_client: redis.Redis = None
rabbit_connection: aio_pika.RobustConnection = None
rabbit_channel: aio_pika.Channel = None
rabbit_queue: aio_pika.Queue = None
lua_script_sha: str = None

class CheckoutRequest(BaseModel):
    user_id: str
    product_id: int = 1

def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user="root",
        password="rootpassword",
        database="flashsale"
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, rabbit_connection, rabbit_channel, rabbit_queue, lua_script_sha
    
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.setnx(f"inventory:product:{PRODUCT_ID}", INITIAL_STOCK)
    await redis_client.setnx("metrics:total_requests", 0)
    await redis_client.setnx("metrics:rate_limited", 0)
    await redis_client.setnx("metrics:sold_out_rejections", 0)
    
    with open(os.path.join(os.path.dirname(__file__), "stock.lua"), "r") as f:
        lua_script_sha = await redis_client.script_load(f.read())
        
    rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)
    rabbit_channel = await rabbit_connection.channel()
    rabbit_queue = await rabbit_channel.declare_queue(QUEUE_NAME, durable=True)
    
    yield
    
    await rabbit_connection.close()
    await redis_client.close()

app = FastAPI(title="Flash Sale API Gateway", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def serve_frontend():
    return FileResponse(os.path.join(os.path.dirname(__file__), "../frontend/index.html"))

@app.post("/api/v1/checkout", status_code=status.HTTP_202_ACCEPTED)
async def checkout(
    payload: CheckoutRequest,
    request: Request,
    response: Response,
    x_idempotency_key: str = Header(..., description="Unique UUID per click")
):
    await redis_client.incr("metrics:total_requests")

    # 1. Edge Rate Limiting (Max 5 requests per 10s per user/IP pair)
    client_ip = request.client.host if request.client else "unknown"
    rate_key = f"ratelimit:{client_ip}:{payload.user_id}"
    req_count = await redis_client.incr(rate_key)
    if req_count == 1:
        await redis_client.expire(rate_key, 10)
    if req_count > 5:
        await redis_client.incr("metrics:rate_limited")
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Bot activity blocked.")

    # 2. Atomic Redis Lua Gatekeeper
    stock_key = f"inventory:product:{payload.product_id}"
    idem_key = f"idempotency:{x_idempotency_key}"
    status_key = f"order:status:{x_idempotency_key}"

    result = await redis_client.evalsha(lua_script_sha, 3, stock_key, idem_key, status_key)

    if result == -2:
        return {
            "status": "DUPLICATE",
            "message": "Duplicate request blocked by Idempotency Gatekeeper.",
            "order_id": x_idempotency_key,
            "poll_url": f"/api/v1/orders/{x_idempotency_key}"
        }
    
    if result == -1:
        await redis_client.incr("metrics:sold_out_rejections")
        response.status_code = status.HTTP_409_CONFLICT
        return {"status": "SOLD_OUT", "message": "Flash sale inventory exhausted."}

    # 3. Publish to RabbitMQ for Async DB Write
    order_event = {
        "order_id": x_idempotency_key,
        "user_id": payload.user_id,
        "product_id": payload.product_id
    }
    await rabbit_channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(order_event).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        ),
        routing_key=QUEUE_NAME
    )

    return {
        "status": "QUEUED",
        "remaining_stock_estimate": result,
        "order_id": x_idempotency_key,
        "poll_url": f"/api/v1/orders/{x_idempotency_key}"
    }

@app.get("/api/v1/orders/{order_id}")
async def get_order_status(order_id: str):
    state = await redis_client.get(f"order:status:{order_id}")
    if not state:
        raise HTTPException(status_code=404, detail="Order ID not found")
    return {"order_id": order_id, "status": state}

@app.get("/api/v1/metrics")
async def get_live_metrics():
    redis_stock = int(await redis_client.get(f"inventory:product:{PRODUCT_ID}") or 0)
    total_req = int(await redis_client.get("metrics:total_requests") or 0)
    rate_limited = int(await redis_client.get("metrics:rate_limited") or 0)
    sold_out_rej = int(await redis_client.get("metrics:sold_out_rejections") or 0)

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT stock FROM inventory WHERE product_id = %s", (PRODUCT_ID,))
    row = cursor.fetchone()
    mysql_stock = row["stock"] if row else 0

    cursor.execute("SELECT COUNT(*) as cnt FROM orders WHERE status = 'CONFIRMED'")
    confirmed_orders = cursor.fetchone()["cnt"]
    cursor.close()
    conn.close()

    return {
        "redis_atomic_stock": redis_stock,
        "mysql_persisted_stock": mysql_stock,
        "confirmed_orders_count": confirmed_orders,
        "total_requests_ingested": total_req,
        "bot_requests_rate_limited": rate_limited,
        "sold_out_fast_rejections": sold_out_rej,
        "consistency_verified": (mysql_stock + confirmed_orders == INITIAL_STOCK)
    }

@app.post("/api/v1/admin/reset")
async def reset_system():
    # Reset Redis
    await redis_client.flushdb()
    await redis_client.set(f"inventory:product:{PRODUCT_ID}", INITIAL_STOCK)
    await redis_client.set("metrics:total_requests", 0)
    await redis_client.set("metrics:rate_limited", 0)
    await redis_client.set("metrics:sold_out_rejections", 0)
    with open(os.path.join(os.path.dirname(__file__), "stock.lua"), "r") as f:
        global lua_script_sha
        lua_script_sha = await redis_client.script_load(f.read())

    # Reset MySQL
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE inventory SET stock = %s WHERE product_id = %s", (INITIAL_STOCK, PRODUCT_ID))
    cursor.execute("TRUNCATE TABLE orders")
    conn.commit()
    cursor.close()
    conn.close()

    return {"status": "RESET_COMPLETE", "stock_restored": INITIAL_STOCK}