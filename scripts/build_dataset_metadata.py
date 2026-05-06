from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov", ".avi", ".webm"}
AUDIO_EXTENSIONS = {".wav", ".mp3", ".flac", ".aac", ".m4a", ".ogg"}
DEFAULT_EXTENSIONS = VIDEO_EXTENSIONS | AUDIO_EXTENSIONS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Construye dataset_metadata.json con hashes y metadatos ffprobe.",
    )
    parser.add_argument("--dataset-dir", default="dataset")
    parser.add_argument("--output", default="dataset/dataset_metadata.json")
    parser.add_argument(
        "--extensions",
        default=",".join(sorted(DEFAULT_EXTENSIONS)),
        help="Extensiones separadas por coma.",
    )
    parser.add_argument("--ffprobe-timeout-seconds", type=float, default=60.0)
    return parser.parse_args()


def normalize_ext_set(raw_extensions: str) -> set[str]:
    values: set[str] = set()
    for item in raw_extensions.split(","):
        item = item.strip().lower()
        if not item:
            continue
        values.add(item if item.startswith(".") else f".{item}")
    return values or DEFAULT_EXTENSIONS


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_frame_rate(value: str | None) -> float | None:
    if not value or value == "0/0":
        return None
    if "/" not in value:
        try:
            return float(value)
        except ValueError:
            return None
    numerator, denominator = value.split("/", 1)
    try:
        den = float(denominator)
        return None if den == 0 else round(float(numerator) / den, 3)
    except ValueError:
        return None


def run_ffprobe(path: Path, timeout_seconds: float) -> dict[str, Any]:
    ffprobe_path = shutil.which("ffprobe")
    if ffprobe_path is None:
        return {"available": False, "error": "ffprobe_not_available"}

    command = [
        ffprobe_path,
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(path),
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return {"available": True, "error": "ffprobe_timeout"}

    if completed.returncode != 0:
        return {
            "available": True,
            "error": "ffprobe_failed",
            "stderr": (completed.stderr or "").strip(),
        }

    return {"available": True, "data": json.loads(completed.stdout or "{}")}


def summarize_streams(probe: dict[str, Any]) -> dict[str, Any]:
    if "data" not in probe:
        return {}

    streams = probe["data"].get("streams", [])
    video_stream = next((stream for stream in streams if stream.get("codec_type") == "video"), {})
    audio_stream = next((stream for stream in streams if stream.get("codec_type") == "audio"), {})
    format_data = probe["data"].get("format", {})

    return {
        "duration_seconds": (
            round(float(format_data["duration"]), 3)
            if format_data.get("duration")
            else None
        ),
        "container_format": format_data.get("format_name"),
        "bit_rate": int(format_data["bit_rate"]) if format_data.get("bit_rate") else None,
        "video": {
            "codec": video_stream.get("codec_name"),
            "width": video_stream.get("width"),
            "height": video_stream.get("height"),
            "frame_rate": parse_frame_rate(video_stream.get("avg_frame_rate")),
        },
        "audio": {
            "codec": audio_stream.get("codec_name"),
            "sample_rate": int(audio_stream["sample_rate"])
            if audio_stream.get("sample_rate")
            else None,
            "channels": audio_stream.get("channels"),
        },
    }


def discover_files(dataset_dir: Path, ext_set: set[str]) -> list[Path]:
    return [
        path
        for path in sorted(dataset_dir.iterdir())
        if path.is_file()
        and not path.name.startswith(".")
        and path.suffix.lower() in ext_set
    ]


def media_type_for_extension(extension: str) -> str:
    if extension in VIDEO_EXTENSIONS:
        return "video"
    if extension in AUDIO_EXTENSIONS:
        return "audio"
    return "unknown"


def recommended_operations_for_media(media_type: str) -> list[str]:
    if media_type == "video":
        return [
            "extract_metadata",
            "generate_thumbnail",
            "extract_audio",
            "transcode_h264",
        ]
    if media_type == "audio":
        return ["extract_metadata"]
    return ["extract_metadata"]


def main() -> int:
    args = parse_args()
    dataset_dir = Path(args.dataset_dir).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    if not dataset_dir.exists():
        raise FileNotFoundError(f"dataset_dir_not_found: {dataset_dir}")

    ext_set = normalize_ext_set(args.extensions)
    files = discover_files(dataset_dir, ext_set)

    entries: list[dict[str, Any]] = []
    hashes: dict[str, list[str]] = {}
    for path in files:
        digest = sha256_file(path)
        hashes.setdefault(digest, []).append(path.name)
        probe = run_ffprobe(path, args.ffprobe_timeout_seconds)
        extension = path.suffix.lower()
        media_type = media_type_for_extension(extension)
        entry: dict[str, Any] = {
            "file": path.name,
            "relative_path": f"dataset/{path.name}",
            "media_type": media_type,
            "extension": extension,
            "size_bytes": path.stat().st_size,
            "sha256": digest,
            "ffprobe": {
                "available": probe.get("available", False),
                "error": probe.get("error"),
            },
            "recommended_operations": recommended_operations_for_media(media_type),
        }
        entry.update(summarize_streams(probe))
        entries.append(entry)

    duplicate_groups = [
        {"sha256": digest, "files": names}
        for digest, names in hashes.items()
        if len(names) > 1
    ]
    total_size = sum(item["size_bytes"] for item in entries)
    media_counts = {
        "audio": sum(1 for item in entries if item["media_type"] == "audio"),
        "video": sum(1 for item in entries if item["media_type"] == "video"),
        "unknown": sum(1 for item in entries if item["media_type"] == "unknown"),
    }
    manifest = {
        "dataset_name": "curated_multimedia_real_dataset_400",
        "version": "2026-05-05",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_dir": "dataset",
        "total_files": len(entries),
        "selected_real_videos": media_counts["video"],
        "selected_real_audio": media_counts["audio"],
        "total_size_bytes": total_size,
        "extensions": sorted(ext_set),
        "curation_notes": [
            "Dataset curado con archivos reales de audio y video para pruebas de carga distribuida.",
            "Los audios se usan para extraccion de metadatos; los videos permiten metadata, thumbnails, audio y transcodificacion.",
            "El manifest documenta nombres, tamanos, hashes SHA-256 y metadatos tecnicos obtenidos con ffprobe.",
        ],
        "duplicate_groups": duplicate_groups,
        "files": entries,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Metadata escrita en: {output_path}")
    print(f"Archivos documentados: {len(entries)}")
    print(f"Duplicados detectados: {len(duplicate_groups)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
