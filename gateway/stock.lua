-- KEYS[1]: inventory stock key ("inventory:product:1")
-- KEYS[2]: idempotency key ("idempotency:<uuid>")
-- KEYS[3]: order status key ("order:status:<uuid>")
-- KEYS[4]: rate limit key ("ratelimit:<ip>:<user_id>")
-- ARGV[1]: max requests per window (e.g., 5)
-- ARGV[2]: rate limit window in seconds (e.g., 10)
-- ARGV[3]: optional TTL in seconds (defaults to 3600 if not passed)

local max_req = tonumber(ARGV[1]) or 5
local window_sec = tonumber(ARGV[2]) or 10
local state_ttl = tonumber(ARGV[3]) or 3600

redis.call("INCR", "metrics:total_requests")

-- 1. Atomic Edge Rate Limiting (Blocks Bot Attacks in 1-RTT)
local req_count = redis.call("INCR", KEYS[4])
if req_count == 1 then
    redis.call("EXPIRE", KEYS[4], window_sec)
end
if req_count > max_req then
    redis.call("INCR", "metrics:rate_limited")
    return -3 -- Code -3: Bot Rate Limit Exceeded (HTTP 429)
end

-- 2. Atomic Idempotency Check (Blocks Rapid UI Double-Clicks)
if redis.call("EXISTS", KEYS[2]) == 1 then
    redis.call("INCR", "metrics:duplicate_requests")
    return -2 -- Code -2: Duplicate Request
end

-- 3. Atomic Inventory Check
local stock = tonumber(redis.call("GET", KEYS[1]) or "0")
if stock <= 0 then
    redis.call("INCR", "metrics:sold_out_rejections")
    return -1 -- Code -1: Sold Out (HTTP 409)
end

-- 4. Atomic Decrement & Reservation State
local remaining = redis.call("DECR", KEYS[1])
redis.call("SET", KEYS[2], "1", "EX", state_ttl)
redis.call("SET", KEYS[3], "QUEUED", "EX", state_ttl)
redis.call("INCR", "metrics:reserved_queued")

return remaining