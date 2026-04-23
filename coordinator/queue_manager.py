import redis
from rq import Queue
import os

# configuración (luego usaremos .env)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# conexión
redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

# cola principal
job_queue = Queue("jobs", connection=redis_conn)