import os
import shutil
import socket
import subprocess
from pathlib import Path

import psutil

from shared.job_store import JobStore


RESULTS_DIR = Path(os.getenv("RESULTS_DIR", "results"))
SUPPORTED_OPERATIONS = {"extract_audio"}
job_store = JobStore()


def _resolve_input_path(file_path: str) -> Path:
    return Path(file_path).expanduser().resolve()


def _build_audio_result_path(job_id: str) -> Path:
    return RESULTS_DIR / f"{job_id}_extract_audio.mp3"


def _ensure_supported_operation(operation: str) -> None:
    if operation not in SUPPORTED_OPERATIONS:
        supported = ", ".join(sorted(SUPPORTED_OPERATIONS))
        raise ValueError(f"unsupported_operation: {operation}. supported_operations: {supported}")


def _extract_audio(input_path: Path, output_path: Path) -> None:
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path is None:
        raise RuntimeError("ffmpeg_not_available")

    command = [
        ffmpeg_path,
        "-y",
        "-i",
        str(input_path),
        "-vn",
        "-acodec",
        "libmp3lame",
        str(output_path),
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr_output = (completed.stderr or "").strip()
        raise RuntimeError(f"ffmpeg_extract_audio_failed: {stderr_output}")


def process_task(job_id, file_path, operation):
    worker_id = os.getenv("WORKER_ID") or socket.gethostname()
    worker_hostname = socket.gethostname()
    input_path = _resolve_input_path(file_path)

    if not input_path.exists():
        message = f"input_file_not_found: {input_path}"
        job_store.mark_job_failed(job_id, message)
        raise FileNotFoundError(message)

    try:
        _ensure_supported_operation(operation)
        job_store.upsert_worker_node(
            worker_id,
            hostname=worker_hostname,
            status="busy",
            current_job_id=job_id,
            current_operation=operation,
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_percent=psutil.virtual_memory().percent,
        )
        job_store.update_job_status(
            job_id=job_id,
            status="running",
            worker_id=worker_id,
            progress=10,
            event_type="job_started",
            payload={"input_path": str(input_path), "operation": operation},
        )

        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        output_path = _build_audio_result_path(job_id)

        job_store.update_job_status(
            job_id=job_id,
            status="running",
            worker_id=worker_id,
            progress=35,
            event_type="job_progress_updated",
            payload={"operation": operation, "stage": "validated_input"},
        )

        _extract_audio(input_path, output_path)

        job_store.update_job_status(
            job_id=job_id,
            status="running",
            worker_id=worker_id,
            progress=85,
            event_type="job_progress_updated",
            payload={"operation": operation, "stage": "audio_extracted"},
        )

        output_size_bytes = output_path.stat().st_size
        metadata = {
            "operation": operation,
            "worker_id": worker_id,
            "source_file": str(input_path),
            "result_type": "audio_file",
            "output_extension": output_path.suffix,
            "output_size_bytes": output_size_bytes,
        }
        job_store.record_job_result(
            job_id=job_id,
            output_location=str(output_path.resolve()),
            metadata=metadata,
        )
        job_store.upsert_worker_node(
            worker_id,
            hostname=worker_hostname,
            status="ready",
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_percent=psutil.virtual_memory().percent,
            clear_current_job=True,
        )
    except Exception as exc:
        message = str(exc)
        job_store.mark_job_failed(job_id, message)
        job_store.upsert_worker_node(
            worker_id,
            hostname=worker_hostname,
            status="error",
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_percent=psutil.virtual_memory().percent,
            clear_current_job=True,
        )
        raise

    return {
        "job_id": job_id,
        "source_file": str(input_path),
        "operation": operation,
        "worker_id": worker_id,
        "status": "completed",
        "output_location": str(output_path.resolve()),
    }
