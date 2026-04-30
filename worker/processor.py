import os
import shutil
import socket
import subprocess
from pathlib import Path

import psutil

from shared.operations import SUPPORTED_OPERATIONS, normalize_operation_name
from shared.job_store import JobStore


RESULTS_DIR = Path(os.getenv("RESULTS_DIR", "results"))
job_store = JobStore()


def _resolve_input_path(file_path: str) -> Path:
    return Path(file_path).expanduser().resolve()


def _build_result_path(job_id: str, operation: str) -> Path:
    suffix_by_operation = {
        "extract_audio": ".mp3",
        "generate_thumbnail": ".jpg",
        "transcode_h264": ".mp4",
        "extract_metadata": ".json",
    }
    suffix = suffix_by_operation[operation]
    return RESULTS_DIR / f"{job_id}_{operation}{suffix}"


def _ensure_supported_operation(operation: str) -> None:
    if operation not in SUPPORTED_OPERATIONS:
        supported = ", ".join(sorted(SUPPORTED_OPERATIONS))
        raise ValueError(f"unsupported_operation: {operation}. supported_operations: {supported}")


def _resolve_ffmpeg_path() -> str:
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path is None:
        raise RuntimeError("ffmpeg_not_available")
    return ffmpeg_path


def _resolve_ffprobe_path() -> str:
    ffprobe_path = shutil.which("ffprobe")
    if ffprobe_path is None:
        raise RuntimeError("ffprobe_not_available")
    return ffprobe_path


def _run_command(command: list[str], error_prefix: str) -> None:
    command = [
        item
        for item in command
        if item is not None
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr_output = (completed.stderr or "").strip()
        raise RuntimeError(f"{error_prefix}: {stderr_output}")


def _extract_audio(input_path: Path, output_path: Path) -> None:
    ffmpeg_path = _resolve_ffmpeg_path()
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
    _run_command(command, "ffmpeg_extract_audio_failed")


def _generate_thumbnail(input_path: Path, output_path: Path) -> None:
    ffmpeg_path = _resolve_ffmpeg_path()
    command = [
        ffmpeg_path,
        "-y",
        "-i",
        str(input_path),
        "-ss",
        "00:00:01.000",
        "-vframes",
        "1",
        "-q:v",
        "2",
        str(output_path),
    ]
    _run_command(command, "ffmpeg_generate_thumbnail_failed")


def _transcode_h264(input_path: Path, output_path: Path) -> None:
    ffmpeg_path = _resolve_ffmpeg_path()
    command = [
        ffmpeg_path,
        "-y",
        "-i",
        str(input_path),
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "23",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        str(output_path),
    ]
    _run_command(command, "ffmpeg_transcode_h264_failed")


def _extract_metadata(input_path: Path, output_path: Path) -> None:
    ffprobe_path = _resolve_ffprobe_path()
    command = [
        ffprobe_path,
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(input_path),
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr_output = (completed.stderr or "").strip()
        raise RuntimeError(f"ffprobe_extract_metadata_failed: {stderr_output}")
    output_path.write_text(completed.stdout or "{}", encoding="utf-8")


def _execute_operation(operation: str, input_path: Path, output_path: Path) -> None:
    handlers = {
        "extract_audio": _extract_audio,
        "generate_thumbnail": _generate_thumbnail,
        "transcode_h264": _transcode_h264,
        "extract_metadata": _extract_metadata,
    }
    handlers[operation](input_path, output_path)


def process_task(job_id, file_path, operation):
    operation = normalize_operation_name(operation)
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
        output_path = _build_result_path(job_id, operation)

        job_store.update_job_status(
            job_id=job_id,
            status="running",
            worker_id=worker_id,
            progress=35,
            event_type="job_progress_updated",
            payload={"operation": operation, "stage": "validated_input"},
        )

        _execute_operation(operation, input_path, output_path)

        job_store.update_job_status(
            job_id=job_id,
            status="running",
            worker_id=worker_id,
            progress=85,
            event_type="job_progress_updated",
            payload={"operation": operation, "stage": "operation_completed"},
        )

        output_size_bytes = output_path.stat().st_size
        metadata = {
            "operation": operation,
            "worker_id": worker_id,
            "source_file": str(input_path),
            "result_type": output_path.suffix.replace(".", ""),
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
