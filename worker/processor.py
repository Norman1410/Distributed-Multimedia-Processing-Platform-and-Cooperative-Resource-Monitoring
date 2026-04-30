import os
import shutil
import socket
import subprocess
from pathlib import Path

import psutil
from rq import get_current_job

from shared.operations import SUPPORTED_OPERATIONS, normalize_operation_name
from shared.job_store import JobStore


RESULTS_DIR = Path(os.getenv("RESULTS_DIR", "results"))
PROCESS_TIMEOUT_SECONDS = float(
    os.getenv(
        "WORKER_PROCESS_TIMEOUT_SECONDS",
        os.getenv("JOB_TIMEOUT_SECONDS", "240"),
    )
)
job_store = JobStore()


class ProcessingError(Exception):
    error_type = "processing_error"
    retryable = False


class InputFileNotFoundError(ProcessingError):
    error_type = "input_file_not_found"
    retryable = False


class UnsupportedOperationError(ProcessingError):
    error_type = "unsupported_operation"
    retryable = False


class ToolUnavailableError(ProcessingError):
    error_type = "tool_unavailable"
    retryable = False


class CommandTimeoutError(ProcessingError):
    error_type = "operation_timeout"
    retryable = True


class MultimediaCommandError(ProcessingError):
    error_type = "multimedia_processing_error"
    retryable = False


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
        raise UnsupportedOperationError(
            f"unsupported_operation: {operation}. supported_operations: {supported}"
        )


def _resolve_ffmpeg_path() -> str:
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path is None:
        raise ToolUnavailableError("ffmpeg_not_available")
    return ffmpeg_path


def _resolve_ffprobe_path() -> str:
    ffprobe_path = shutil.which("ffprobe")
    if ffprobe_path is None:
        raise ToolUnavailableError("ffprobe_not_available")
    return ffprobe_path


def _run_command(command: list[str], error_prefix: str) -> None:
    command = [
        item
        for item in command
        if item is not None
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=PROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise CommandTimeoutError(
            f"{error_prefix}_timeout_after_{PROCESS_TIMEOUT_SECONDS:g}s"
        ) from exc
    if completed.returncode != 0:
        stderr_output = (completed.stderr or "").strip()
        raise MultimediaCommandError(f"{error_prefix}: {stderr_output}")


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
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=PROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise CommandTimeoutError(
            f"ffprobe_extract_metadata_timeout_after_{PROCESS_TIMEOUT_SECONDS:g}s"
        ) from exc
    if completed.returncode != 0:
        stderr_output = (completed.stderr or "").strip()
        raise MultimediaCommandError(f"ffprobe_extract_metadata_failed: {stderr_output}")
    output_path.write_text(completed.stdout or "{}", encoding="utf-8")


def _execute_operation(operation: str, input_path: Path, output_path: Path) -> None:
    handlers = {
        "extract_audio": _extract_audio,
        "generate_thumbnail": _generate_thumbnail,
        "transcode_h264": _transcode_h264,
        "extract_metadata": _extract_metadata,
    }
    handlers[operation](input_path, output_path)


def _classify_exception(exc: Exception) -> tuple[str, bool, str]:
    if isinstance(exc, ProcessingError):
        return exc.error_type, exc.retryable, str(exc)
    return "unexpected_worker_error", True, str(exc)


def _get_retries_left() -> int:
    current_job = get_current_job()
    if current_job is None:
        return 0
    value = getattr(current_job, "retries_left", 0)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def process_task(job_id, file_path, operation):
    operation = normalize_operation_name(operation)
    worker_id = os.getenv("WORKER_ID") or socket.gethostname()
    worker_hostname = socket.gethostname()
    input_path = _resolve_input_path(file_path)

    try:
        _ensure_supported_operation(operation)
        if not input_path.exists():
            raise InputFileNotFoundError(f"input_file_not_found: {input_path}")

        job_store.upsert_worker_node(
            worker_id,
            hostname=worker_hostname,
            status="busy",
            current_job_id=job_id,
            current_operation=operation,
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_percent=psutil.virtual_memory().percent,
        )
        job_store.start_job_attempt(
            job_id=job_id,
            worker_id=worker_id,
            progress=10,
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
        error_type, retryable, message = _classify_exception(exc)
        retries_left = _get_retries_left()
        if retryable and retries_left > 0:
            job_store.mark_job_retry_scheduled(
                job_id,
                message,
                error_type=error_type,
                worker_id=worker_id,
                retries_left=retries_left,
            )
        else:
            job_store.mark_job_failed(
                job_id,
                message,
                error_type=error_type,
                retryable=retryable,
                worker_id=worker_id,
            )

        job_store.upsert_worker_node(
            worker_id,
            hostname=worker_hostname,
            status="ready",
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_percent=psutil.virtual_memory().percent,
            clear_current_job=True,
        )
        if retryable:
            raise
        return {
            "job_id": job_id,
            "source_file": str(input_path),
            "operation": operation,
            "worker_id": worker_id,
            "status": "failed",
            "error_type": error_type,
            "error_message": message,
        }

    return {
        "job_id": job_id,
        "source_file": str(input_path),
        "operation": operation,
        "worker_id": worker_id,
        "status": "completed",
        "output_location": str(output_path.resolve()),
    }
