import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import psutil
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request

from coordinator.models import (
    JobEventResponse,
    JobListResponse,
    JobRequest,
    JobResponse,
    JobResultResponse,
    JobStatusResponse,
)
from coordinator.queue_manager import (
    QUEUE_PRIORITY_ORDER,
    get_pending_jobs_by_queue,
    get_queue_for_priority,
    resolve_queue_name_for_priority,
)
from shared.job_store import JobStore, VALID_JOB_STATUSES


app = FastAPI(title="Distributed Multimedia Coordinator")
job_store = JobStore()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
DATASET_DIR = Path("/app/dataset")


def build_monitor_summary() -> dict:
    active_cutoff = datetime.now(timezone.utc) - timedelta(seconds=20)
    workers = []
    for worker in job_store.list_worker_nodes():
        last_seen_raw = worker.get("last_seen")
        is_active = False
        if last_seen_raw:
            try:
                is_active = datetime.fromisoformat(last_seen_raw) >= active_cutoff
            except ValueError:
                is_active = False
        worker["is_active"] = is_active
        workers.append(worker)

    jobs = job_store.list_jobs(limit=25)
    status_counts = job_store.get_job_status_counts()
    pending_by_queue = get_pending_jobs_by_queue()

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "coordinator": {
            "cpu_percent": psutil.cpu_percent(interval=None),
            "memory_percent": psutil.virtual_memory().percent,
        },
        "queue": {
            "priority_order": QUEUE_PRIORITY_ORDER,
            "pending_by_queue": pending_by_queue,
            "pending_in_queue": sum(pending_by_queue.values()),
        },
        "jobs": {
            "total": sum(status_counts.values()),
            "status_counts": status_counts,
        },
        "workers": {
            "total_count": len(workers),
            "active_count": sum(1 for worker in workers if worker["is_active"]),
            "nodes": workers,
        },
        "recent_jobs": jobs,
    }


def list_dataset_files() -> list[dict]:
    if not DATASET_DIR.exists():
        return []

    files = []
    for file_path in sorted(DATASET_DIR.iterdir()):
        if not file_path.is_file() or file_path.name.startswith("."):
            continue
        files.append(
            {
                "file_path": f"dataset/{file_path.name}",
                "name": file_path.name,
                "size_bytes": file_path.stat().st_size,
            }
        )
    return files


@app.get("/")
def root():
    return {
        "message": "Coordinador funcionando",
        "queue_priority_order": QUEUE_PRIORITY_ORDER,
    }


@app.get("/monitor/summary")
def monitor_summary():
    summary = build_monitor_summary()
    return summary


@app.get("/monitor/dataset-files")
def monitor_dataset_files():
    return {"files": list_dataset_files()}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "request": request,
        },
    )


@app.post("/jobs", response_model=JobResponse, status_code=201)
def create_job(job: JobRequest):
    job_id = str(uuid.uuid4())
    job_store.create_job(
        job_id=job_id,
        file_path=job.file_path,
        operation=job.operation,
        priority=job.priority,
    )

    selected_queue = get_queue_for_priority(job.priority)
    selected_queue_name = resolve_queue_name_for_priority(job.priority)

    try:
        rq_job = selected_queue.enqueue(
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
        queue_name=selected_queue_name,
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
