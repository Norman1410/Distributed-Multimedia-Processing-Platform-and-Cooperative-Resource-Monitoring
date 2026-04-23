import redis
from rq import Worker
import os

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

if __name__ == "__main__":
    print("Worker iniciado, esperando tareas...")

    worker = Worker(["jobs"], connection=redis_conn)
    worker.work()