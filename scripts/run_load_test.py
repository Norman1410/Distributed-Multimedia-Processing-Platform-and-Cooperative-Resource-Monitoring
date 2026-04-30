from __future__ import annotations

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest


DEFAULT_OPERATIONS = [
    "extract_metadata",
    "generate_thumbnail",
    "extract_audio",
    "transcode_h264",
]
TERMINAL_STATUSES = {"completed", "failed"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta una prueba formal de carga contra el coordinador.",
    )
    parser.add_argument("--coordinator-url", default="http://localhost:8000")
    parser.add_argument("--dataset-metadata", default="dataset/dataset_metadata.json")
    parser.add_argument("--operations", default=",".join(DEFAULT_OPERATIONS))
    parser.add_argument("--repeat", type=int, default=2)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--priority", type=int, default=5)
    parser.add_argument("--request-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=2.0)
    parser.add_argument("--max-wait-seconds", type=float, default=1800.0)
    parser.add_argument("--output-json", default="results/load_test_metrics.json")
    parser.add_argument("--report-md", default="docs/informe_resultados.md")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def parse_operations(raw: str) -> list[str]:
    operations = [item.strip() for item in raw.split(",") if item.strip()]
    return operations or DEFAULT_OPERATIONS


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"dataset_metadata_not_found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def build_tasks(
    manifest: dict[str, Any],
    operations: list[str],
    repeat: int,
    priority: int,
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for item in manifest.get("files", []):
        file_path = item.get("relative_path") or f"dataset/{item.get('file')}"
        for operation in operations:
            for index in range(max(repeat, 1)):
                tasks.append(
                    {
                        "file_path": file_path,
                        "operation": operation,
                        "priority": max(1, min(priority, 10)),
                        "repeat_index": index + 1,
                    }
                )
    return tasks


def request_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urlrequest.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urlrequest.urlopen(req, timeout=timeout_seconds) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw) if raw else {}


def submit_job(
    coordinator_url: str,
    payload: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    endpoint = f"{coordinator_url.rstrip('/')}/jobs"
    submitted_at = datetime.now(timezone.utc).isoformat()
    try:
        data = request_json(
            "POST",
            endpoint,
            payload={
                "file_path": payload["file_path"],
                "operation": payload["operation"],
                "priority": payload["priority"],
            },
            timeout_seconds=timeout_seconds,
        )
        return {
            "ok": True,
            "submitted_at": submitted_at,
            "job_id": data.get("job_id"),
            "status": data.get("status"),
            "payload": payload,
        }
    except urlerror.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "submitted_at": submitted_at,
            "error": f"http_error_{exc.code}: {details}",
            "payload": payload,
        }
    except Exception as exc:
        return {
            "ok": False,
            "submitted_at": submitted_at,
            "error": str(exc),
            "payload": payload,
        }


def submit_all(args: argparse.Namespace, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(args.concurrency, 1)) as executor:
        futures = [
            executor.submit(
                submit_job,
                args.coordinator_url,
                task,
                args.request_timeout_seconds,
            )
            for task in tasks
        ]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            payload = result["payload"]
            if result["ok"]:
                print(
                    f"OK  {payload['operation']} {payload['file_path']} "
                    f"job_id={result.get('job_id')}"
                )
            else:
                print(
                    f"ERR {payload['operation']} {payload['file_path']} "
                    f"error={result.get('error')}"
                )
    return results


def poll_jobs(args: argparse.Namespace, job_ids: list[str]) -> list[dict[str, Any]]:
    pending = set(job_ids)
    final_jobs: dict[str, dict[str, Any]] = {}
    started = time.monotonic()

    while pending and time.monotonic() - started <= args.max_wait_seconds:
        for job_id in list(pending):
            try:
                data = request_json(
                    "GET",
                    f"{args.coordinator_url.rstrip('/')}/jobs/{job_id}",
                    timeout_seconds=args.request_timeout_seconds,
                )
            except Exception as exc:
                print(f"WARN no se pudo consultar {job_id}: {exc}")
                continue

            if data.get("status") in TERMINAL_STATUSES:
                final_jobs[job_id] = data
                pending.remove(job_id)
        if pending:
            print(f"Pendientes: {len(pending)}")
            time.sleep(args.poll_interval_seconds)

    for job_id in pending:
        final_jobs[job_id] = {"job_id": job_id, "status": "timeout_waiting_result"}
    return list(final_jobs.values())


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def seconds_between(start: str | None, end: str | None) -> float | None:
    start_dt = parse_dt(start)
    end_dt = parse_dt(end)
    if start_dt is None or end_dt is None:
        return None
    return max((end_dt - start_dt).total_seconds(), 0.0)


def percentile(values: list[float], rank: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * rank
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return round(ordered[lower], 3)
    weight = index - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 3)


def count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = item.get(key) or "unknown"
        counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


def summarize(
    manifest: dict[str, Any],
    tasks: list[dict[str, Any]],
    submissions: list[dict[str, Any]],
    final_jobs: list[dict[str, Any]],
    wall_seconds: float,
) -> dict[str, Any]:
    created_jobs = [item for item in submissions if item.get("ok") and item.get("job_id")]
    submit_failures = [item for item in submissions if not item.get("ok")]
    completed = [item for item in final_jobs if item.get("status") == "completed"]
    failed = [item for item in final_jobs if item.get("status") == "failed"]
    unfinished = [
        item
        for item in final_jobs
        if item.get("status") not in {"completed", "failed"}
    ]
    durations = [
        value
        for value in (
            seconds_between(item.get("started_at"), item.get("finished_at"))
            for item in completed
        )
        if value is not None
    ]
    queue_waits = [
        value
        for value in (
            seconds_between(item.get("queued_at"), item.get("started_at"))
            for item in final_jobs
        )
        if value is not None
    ]
    failure_types: dict[str, int] = {}
    for item in failed:
        value = item.get("error_type") or "unknown"
        failure_types[value] = failure_types.get(value, 0) + 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_name": manifest.get("dataset_name"),
        "dataset_files": manifest.get("total_files", len(manifest.get("files", []))),
        "tasks_requested": len(tasks),
        "jobs_created": len(created_jobs),
        "submission_failures": len(submit_failures),
        "completed_jobs": len(completed),
        "failed_jobs": len(failed),
        "unfinished_jobs": len(unfinished),
        "wall_seconds": round(wall_seconds, 3),
        "throughput_jobs_per_second": round(len(completed) / wall_seconds, 4)
        if wall_seconds > 0
        else 0,
        "worker_distribution": count_by(completed, "worker_id"),
        "operation_distribution": count_by(final_jobs, "operation"),
        "status_distribution": count_by(final_jobs, "status"),
        "failure_types": dict(sorted(failure_types.items())),
        "processing_seconds": {
            "min": round(min(durations), 3) if durations else None,
            "avg": round(statistics.mean(durations), 3) if durations else None,
            "p50": percentile(durations, 0.50),
            "p95": percentile(durations, 0.95),
            "max": round(max(durations), 3) if durations else None,
        },
        "queue_wait_seconds": {
            "avg": round(statistics.mean(queue_waits), 3) if queue_waits else None,
            "p50": percentile(queue_waits, 0.50),
            "p95": percentile(queue_waits, 0.95),
        },
        "submit_failures_detail": submit_failures[:20],
        "failed_jobs_detail": failed[:20],
    }


def write_report(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Informe de resultados de pruebas de carga",
        "",
        f"- Fecha UTC: {summary['generated_at']}",
        f"- Dataset: {summary.get('dataset_name')}",
        f"- Archivos base: {summary.get('dataset_files')}",
        f"- Tareas solicitadas: {summary.get('tasks_requested')}",
        f"- Jobs creados: {summary.get('jobs_created')}",
        f"- Jobs completados: {summary.get('completed_jobs')}",
        f"- Jobs fallidos: {summary.get('failed_jobs')}",
        f"- Jobs sin cierre dentro del timeout: {summary.get('unfinished_jobs')}",
        f"- Tiempo total de pared: {summary.get('wall_seconds')} s",
        f"- Throughput: {summary.get('throughput_jobs_per_second')} jobs/s",
        "",
        "## Tiempos de procesamiento",
        "",
        f"- Min: {summary['processing_seconds']['min']} s",
        f"- Promedio: {summary['processing_seconds']['avg']} s",
        f"- P50: {summary['processing_seconds']['p50']} s",
        f"- P95: {summary['processing_seconds']['p95']} s",
        f"- Max: {summary['processing_seconds']['max']} s",
        "",
        "## Espera en cola",
        "",
        f"- Promedio: {summary['queue_wait_seconds']['avg']} s",
        f"- P50: {summary['queue_wait_seconds']['p50']} s",
        f"- P95: {summary['queue_wait_seconds']['p95']} s",
        "",
        "## Distribucion por worker",
        "",
    ]
    for worker_id, count in summary.get("worker_distribution", {}).items():
        lines.append(f"- {worker_id}: {count}")

    lines.extend(["", "## Distribucion por operacion", ""])
    for operation, count in summary.get("operation_distribution", {}).items():
        lines.append(f"- {operation}: {count}")

    lines.extend(["", "## Fallos", ""])
    if summary.get("failure_types"):
        for error_type, count in summary["failure_types"].items():
            lines.append(f"- {error_type}: {count}")
    else:
        lines.append("- No se registraron fallos terminales.")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    manifest = load_manifest(Path(args.dataset_metadata))
    operations = parse_operations(args.operations)
    tasks = build_tasks(manifest, operations, args.repeat, args.priority)
    if not tasks:
        raise RuntimeError("load_test_without_tasks: revisa dataset_metadata.json")

    print(f"Tareas a crear: {len(tasks)}")
    print(f"Concurrencia de envio: {args.concurrency}")
    if args.dry_run:
        for task in tasks:
            print(json.dumps(task, ensure_ascii=False))
        return 0

    started = time.monotonic()
    submissions = submit_all(args, tasks)
    job_ids = [
        item["job_id"]
        for item in submissions
        if item.get("ok") and isinstance(item.get("job_id"), str)
    ]
    final_jobs = poll_jobs(args, job_ids)
    wall_seconds = time.monotonic() - started

    summary = summarize(manifest, tasks, submissions, final_jobs, wall_seconds)
    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    write_report(Path(args.report_md), summary)

    print()
    print(f"Metricas JSON: {output_json}")
    print(f"Informe Markdown: {args.report_md}")
    print(f"Throughput: {summary['throughput_jobs_per_second']} jobs/s")
    return 0 if summary["failed_jobs"] == 0 and summary["submission_failures"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
