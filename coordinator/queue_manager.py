import os

import redis
from rq import Queue


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
JOB_QUEUE_NAME = os.getenv("JOB_QUEUE_NAME", "jobs")

redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
job_queue = Queue(JOB_QUEUE_NAME, connection=redis_conn)
