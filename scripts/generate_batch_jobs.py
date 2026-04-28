from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest


DEFAULT_EXTENSIONS = [".mp4", ".mkv", ".mov", ".avi", ".webm"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera jobs automaticamente desde archivos del dataset.",
    )
    parser.add_argument(
        "--coordinator-url",
        default="http://localhost:8000",
        help="URL base del coordinador (default: http://localhost:8000).",
    )
    parser.add_argument(
        "--dataset-dir",
        default="dataset",
        help="Directorio local del dataset (default: dataset).",
    )
    parser.add_argument(
        "--operation",
        default="extract_audio",
        help="Operacion por defecto para cada archivo.",
    )
    parser.add_argument(
        "--priority",
        type=int,
        default=5,
        help="Prioridad por defecto (1-10).",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Cuantas veces repetir cada archivo (default: 1).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limita cantidad de archivos base (0 = sin limite).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="Cantidad de requests concurrentes para crear jobs.",
    )
    parser.add_argument(
        "--extensions",
        default=",".join(DEFAULT_EXTENSIONS),
        help="Extensiones permitidas separadas por coma.",
    )
    parser.add_argument(
        "--metadata-json",
        default="",
        help=(
            "Archivo JSON opcional con overrides por archivo "
            "(priority y/o operation)."
        ),
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="Timeout por request al coordinador.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra las tareas a generar sin enviarlas al coordinador.",
    )
    return parser.parse_args()


def normalize_ext_set(raw_extensions: str) -> set[str]:
    ext_set: set[str] = set()
    for raw in raw_extensions.split(","):
        raw = raw.strip().lower()
        if not raw:
            continue
        ext_set.add(raw if raw.startswith(".") else f".{raw}")
    return ext_set or set(DEFAULT_EXTENSIONS)


def discover_dataset_files(dataset_dir: Path, ext_set: set[str]) -> list[Path]:
    if not dataset_dir.exists():
        raise FileNotFoundError(f"dataset_dir_not_found: {dataset_dir}")

    files: list[Path] = []
    for file_path in sorted(dataset_dir.iterdir()):
        if not file_path.is_file():
            continue
        if file_path.name.startswith("."):
            continue
        if file_path.suffix.lower() not in ext_set:
            continue
        files.append(file_path)
    return files


def _normalize_file_key(value: str) -> str:
    return Path(value).name


def load_metadata_map(metadata_path: Path | None) -> dict[str, dict[str, Any]]:
    if metadata_path is None:
        return {}
    if not metadata_path.exists():
        raise FileNotFoundError(f"metadata_file_not_found: {metadata_path}")

    data = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata_map: dict[str, dict[str, Any]] = {}

    if isinstance(data, dict):
        if "files" in data and isinstance(data["files"], list):
            for item in data["files"]:
                if not isinstance(item, dict) or "file" not in item:
                    continue
                file_key = _normalize_file_key(str(item["file"]))
                metadata_map[file_key] = {
                    key: value for key, value in item.items() if key != "file"
                }
        else:
            for key, value in data.items():
                if isinstance(value, dict):
                    metadata_map[_normalize_file_key(str(key))] = value
    elif isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            file_value = item.get("file") or item.get("file_path")
            if not file_value:
                continue
            file_key = _normalize_file_key(str(file_value))
            metadata_map[file_key] = {
                key: value for key, value in item.items() if key not in {"file", "file_path"}
            }
    else:
        raise ValueError("metadata_json_invalid_format")

    return metadata_map


def clamp_priority(priority: int) -> int:
    if priority < 1:
        return 1
    if priority > 10:
        return 10
    return priority


def build_tasks(
    files: list[Path],
    *,
    operation: str,
    priority: int,
    repeat: int,
    metadata_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for file_path in files:
        meta = metadata_map.get(file_path.name, {})
        operation_value = str(meta.get("operation", operation))
        priority_value = clamp_priority(int(meta.get("priority", priority)))

        for run_index in range(repeat):
            tasks.append(
                {
                    "file_path": f"dataset/{file_path.name}",
                    "operation": operation_value,
                    "priority": priority_value,
                    "repeat_index": run_index + 1,
                }
            )
    return tasks


def enqueue_job(
    coordinator_url: str,
    payload: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    endpoint = f"{coordinator_url.rstrip('/')}/jobs"
    request_body = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        endpoint,
        data=request_body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlrequest.urlopen(req, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
            data = json.loads(raw) if raw else {}
        return {
            "ok": True,
            "job_id": data.get("job_id"),
            "status": data.get("status"),
            "payload": payload,
        }
    except urlerror.HTTPError as exc:
        details = ""
        try:
            details = exc.read().decode("utf-8")
        except Exception:
            details = str(exc)
        return {
            "ok": False,
            "error": f"http_error_{exc.code}: {details}",
            "payload": payload,
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "payload": payload,
        }


def main() -> int:
    args = parse_args()
    dataset_dir = Path(args.dataset_dir).expanduser().resolve()
    metadata_path = Path(args.metadata_json).expanduser().resolve() if args.metadata_json else None

    ext_set = normalize_ext_set(args.extensions)
    files = discover_dataset_files(dataset_dir, ext_set)
    if args.limit > 0:
        files = files[: args.limit]

    metadata_map = load_metadata_map(metadata_path)
    tasks = build_tasks(
        files,
        operation=args.operation,
        priority=args.priority,
        repeat=max(args.repeat, 1),
        metadata_map=metadata_map,
    )

    if not tasks:
        print("No se encontraron archivos para generar jobs.")
        return 0

    print(f"Dataset dir: {dataset_dir}")
    print(f"Archivos base: {len(files)}")
    print(f"Tareas totales a crear: {len(tasks)}")

    if args.dry_run:
        for task in tasks:
            print(json.dumps(task, ensure_ascii=False))
        return 0

    success_count = 0
    failed_count = 0
    created_job_ids: list[str] = []

    with ThreadPoolExecutor(max_workers=max(args.concurrency, 1)) as executor:
        futures = [
            executor.submit(
                enqueue_job,
                args.coordinator_url,
                task,
                args.timeout_seconds,
            )
            for task in tasks
        ]

        for future in as_completed(futures):
            result = future.result()
            payload = result["payload"]
            if result["ok"]:
                success_count += 1
                job_id = result.get("job_id")
                if isinstance(job_id, str):
                    created_job_ids.append(job_id)
                print(
                    f"OK  file={payload['file_path']} priority={payload['priority']} "
                    f"job_id={job_id}"
                )
            else:
                failed_count += 1
                print(
                    f"ERR file={payload['file_path']} priority={payload['priority']} "
                    f"error={result['error']}"
                )

    print()
    print(f"Jobs creados: {success_count}")
    print(f"Errores: {failed_count}")
    if created_job_ids:
        print("Primeros job_id:")
        for job_id in created_job_ids[:10]:
            print(f"- {job_id}")

    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
