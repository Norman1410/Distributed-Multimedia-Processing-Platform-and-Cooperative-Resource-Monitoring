from fastapi import FastAPI
from coordinator.queue_manager import job_queue
from coordinator.models import JobRequest, JobResponse
import uuid

app = FastAPI()

# almacenamiento temporal de estados (luego lo mejoramos)
jobs_status = {}

@app.get("/")
def root():
    return {"message": "Coordinador funcionando 🚀"}


@app.post("/jobs", response_model=JobResponse)
def create_job(job: JobRequest):
    job_id = str(uuid.uuid4())

    # guardamos estado inicial
    jobs_status[job_id] = {
        "file_path": job.file_path,
        "operation": job.operation,
        "status": "pending"
    }

    # encolar tarea (aún no hay worker real)
    job_queue.enqueue("worker.processor.process_task", job_id, job.file_path, job.operation)

    return JobResponse(job_id=job_id, status="pending")


@app.get("/jobs/{job_id}")
def get_job_status(job_id: str):
    return jobs_status.get(job_id, {"error": "Job no encontrado"})