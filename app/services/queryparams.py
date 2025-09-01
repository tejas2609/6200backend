import redis

# connect to Redis
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# ⚠️ Danger: this deletes ALL keys in the current DB!
r.flushdb()