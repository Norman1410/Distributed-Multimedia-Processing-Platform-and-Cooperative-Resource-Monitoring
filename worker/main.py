import os

import redis
from rq import Worker


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
WORKER_QUEUES = [
    queue_name.strip()
    for queue_name in os.getenv("WORKER_QUEUES", os.getenv("JOB_QUEUE_NAME", "jobs")).split(",")
    if queue_name.strip()
]

redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

if __name__ == "__main__":
    print(f"Worker iniciado. Escuchando colas: {WORKER_QUEUES}")
    worker = Worker(WORKER_QUEUES, connection=redis_conn)
    worker.work()
