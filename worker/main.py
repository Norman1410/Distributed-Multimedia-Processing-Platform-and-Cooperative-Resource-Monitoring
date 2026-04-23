import os
import socket
import threading

import psutil
import redis
from rq import Worker

from shared.job_store import JobStore


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
WORKER_ID = os.getenv("WORKER_ID", "worker-unknown")
WORKER_HOSTNAME = socket.gethostname()
HEARTBEAT_INTERVAL_SECONDS = int(os.getenv("WORKER_HEARTBEAT_INTERVAL_SECONDS", 5))
WORKER_QUEUES = [
    queue_name.strip()
    for queue_name in os.getenv("WORKER_QUEUES", os.getenv("JOB_QUEUE_NAME", "jobs")).split(",")
    if queue_name.strip()
]

redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
job_store = JobStore()


def emit_worker_heartbeat(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        job_store.upsert_worker_node(
            WORKER_ID,
            hostname=WORKER_HOSTNAME,
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_percent=psutil.virtual_memory().percent,
        )
        stop_event.wait(HEARTBEAT_INTERVAL_SECONDS)

if __name__ == "__main__":
    print(f"Worker iniciado: {WORKER_ID}. Escuchando colas: {WORKER_QUEUES}")
    job_store.upsert_worker_node(
        WORKER_ID,
        hostname=WORKER_HOSTNAME,
        status="ready",
        cpu_percent=psutil.cpu_percent(interval=None),
        memory_percent=psutil.virtual_memory().percent,
    )
    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=emit_worker_heartbeat,
        args=(stop_event,),
        daemon=True,
    )
    heartbeat_thread.start()
    worker = Worker(WORKER_QUEUES, connection=redis_conn)
    try:
        worker.work()
    finally:
        stop_event.set()
