from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class JobRequest(BaseModel):
    file_path: str
    operation: str
    priority: int = Field(default=5, ge=1, le=10)


class JobResponse(BaseModel):
    job_id: str
    status: str


class JobStatusResponse(BaseModel):
    job_id: str
    file_path: str
    operation: str
    priority: int
    status: str
    worker_id: Optional[str] = None
    progress: float = 0.0
    queue_name: Optional[str] = None
    rq_job_id: Optional[str] = None
    attempt_count: int = 0
    max_attempts: int = 1
    result_path: Optional[str] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    retryable: bool = False
    created_at: str
    queued_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    updated_at: str


class JobListResponse(BaseModel):
    jobs: List[JobStatusResponse]


class JobEventResponse(BaseModel):
    event_id: int
    job_id: str
    event_type: str
    status: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: str


class JobResultResponse(BaseModel):
    job_id: str
    output_location: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
