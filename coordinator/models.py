from pydantic import BaseModel
from typing import Optional

class JobRequest(BaseModel):
    file_path: str
    operation: str  # ejemplo: convert, extract_audio, thumbnail

class JobResponse(BaseModel):
    job_id: str
    status: str