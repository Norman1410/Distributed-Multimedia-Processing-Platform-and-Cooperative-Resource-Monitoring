import uuid
from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from coordinator.models import (
    JobEventResponse,
    JobListResponse,
    JobRequest,
    JobResponse,
    JobResultResponse,
    JobStatusResponse,
)
from coordinator.persistence import JobStore, VALID_JOB_STATUSES
from coordinator.queue_manager import JOB_QUEUE_NAME, job_queue


app = FastAPI(title="Distributed Multimedia Coordinator")
job_store = JobStore()


@app.get("/")
def root():
    return {
        "message": "Coordinador funcionando",
        "queue_name": JOB_QUEUE_NAME,
    }


@app.post("/jobs", response_model=JobResponse, status_code=201)
def create_job(job: JobRequest):
    job_id = str(uuid.uuid4())
    job_store.create_job(
        job_id=job_id,
        file_path=job.file_path,
        operation=job.operation,
        priority=job.priority,
    )

    try:
        rq_job = job_queue.enqueue(
            "worker.processor.process_task",
            job_id,
            job.file_path,
            job.operation,
        )
    except Exception as exc:
        job_store.mark_job_failed(job_id, f"queue_enqueue_error: {exc}")
        raise HTTPException(
            status_code=503,
            detail="No fue posible encolar el trabajo en este momento.",
        ) from exc

    job_store.mark_job_queued(
        job_id=job_id,
        queue_name=JOB_QUEUE_NAME,
        rq_job_id=getattr(rq_job, "id", None),
    )
    return JobResponse(job_id=job_id, status="queued")


@app.get("/jobs", response_model=JobListResponse)
def list_jobs(
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
):
    if status and status not in VALID_JOB_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"status debe ser uno de: {sorted(VALID_JOB_STATUSES)}",
        )

    jobs = job_store.list_jobs(status=status, limit=limit)
    return JobListResponse(jobs=[JobStatusResponse(**job_data) for job_data in jobs])


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str):
    job_data = job_store.get_job(job_id)
    if job_data is None:
        raise HTTPException(status_code=404, detail="Job no encontrado")
    return JobStatusResponse(**job_data)


@app.get("/jobs/{job_id}/events", response_model=list[JobEventResponse])
def get_job_events(job_id: str):
    job_data = job_store.get_job(job_id)
    if job_data is None:
        raise HTTPException(status_code=404, detail="Job no encontrado")

    events = job_store.list_job_events(job_id)
    return [JobEventResponse(**event) for event in events]


@app.get("/jobs/{job_id}/result", response_model=JobResultResponse)
def get_job_result(job_id: str):
    job_data = job_store.get_job(job_id)
    if job_data is None:
        raise HTTPException(status_code=404, detail="Job no encontrado")

    result = job_store.get_job_result(job_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Resultado no disponible")

    return JobResultResponse(**result)
