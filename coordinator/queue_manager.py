import os

import redis
from rq import Queue


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
JOB_QUEUE_HIGH_NAME = os.getenv("JOB_QUEUE_HIGH_NAME", "jobs_high")
JOB_QUEUE_NORMAL_NAME = os.getenv("JOB_QUEUE_NORMAL_NAME", "jobs_normal")
JOB_QUEUE_LOW_NAME = os.getenv("JOB_QUEUE_LOW_NAME", "jobs_low")

HIGH_PRIORITY_MAX = int(os.getenv("JOB_PRIORITY_HIGH_MAX", 3))
NORMAL_PRIORITY_MAX = int(os.getenv("JOB_PRIORITY_NORMAL_MAX", 7))

redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

QUEUE_PRIORITY_ORDER = [
    JOB_QUEUE_HIGH_NAME,
    JOB_QUEUE_NORMAL_NAME,
    JOB_QUEUE_LOW_NAME,
]
job_queues = {
    queue_name: Queue(queue_name, connection=redis_conn)
    for queue_name in QUEUE_PRIORITY_ORDER
}

# Legacy alias kept to avoid breaking existing imports/usages.
JOB_QUEUE_NAME = JOB_QUEUE_NORMAL_NAME
job_queue = job_queues[JOB_QUEUE_NORMAL_NAME]


def resolve_queue_name_for_priority(priority: int) -> str:
    if priority <= HIGH_PRIORITY_MAX:
        return JOB_QUEUE_HIGH_NAME
    if priority <= NORMAL_PRIORITY_MAX:
        return JOB_QUEUE_NORMAL_NAME
    return JOB_QUEUE_LOW_NAME


def get_queue_for_priority(priority: int) -> Queue:
    queue_name = resolve_queue_name_for_priority(priority)
    return job_queues[queue_name]


def get_pending_jobs_by_queue() -> dict[str, int]:
    return {queue_name: job_queues[queue_name].count for queue_name in QUEUE_PRIORITY_ORDER}
