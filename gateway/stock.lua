-- KEYS[1]: inventory stock key (e.g., "inventory:product:1")
-- KEYS[2]: idempotency key (e.g., "idempotency:<uuid>")
-- KEYS[3]: order status key (e.g., "order:status:<uuid>")

-- 1. Idempotency Check: Block duplicate clicks / retries
if redis.call("EXISTS", KEYS[2]) == 1 then
    return -2 -- Code -2: Duplicate Request
end

-- 2. Atomic Stock Check
local stock = tonumber(redis.call("GET", KEYS[1]) or "0")
if stock <= 0 then
    return -1 -- Code -1: Sold Out
end

-- 3. Atomic Decrement & State Reservation
redis.call("DECR", KEYS[1])
redis.call("SET", KEYS[2], "1", "EX", 3600)
redis.call("SET", KEYS[3], "QUEUED", "EX", 3600)

return stock - 1 -- Returns remaining stock (>= 0)