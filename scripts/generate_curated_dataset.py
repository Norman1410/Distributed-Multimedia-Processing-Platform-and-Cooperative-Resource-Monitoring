from __future__ import annotations

import argparse
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class VideoSpec:
    filename: str
    duration_seconds: int
    width: int
    height: int
    fps: int
    sine_frequency: int
    crf: int
    profile: str


DATASET_PLAN = [
    VideoSpec("curated_01_short_360p.mp4", 5, 640, 360, 24, 440, 28, "short-small"),
    VideoSpec("curated_02_medium_480p.mp4", 8, 854, 480, 24, 520, 26, "medium-wide"),
    VideoSpec("curated_03_hd_720p.mp4", 12, 1280, 720, 30, 660, 25, "hd-baseline"),
    VideoSpec("curated_04_long_360p.mp4", 18, 640, 360, 30, 330, 24, "long-small"),
    VideoSpec("curated_05_vertical_720p.mp4", 10, 720, 1280, 24, 770, 26, "vertical"),
    VideoSpec("curated_06_high_motion_576p.mp4", 15, 1024, 576, 30, 880, 23, "larger-motion"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un dataset multimedia curado y reproducible con ffmpeg.",
    )
    parser.add_argument(
        "--dataset-dir",
        default="dataset",
        help="Directorio destino del dataset (default: dataset).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenera videos aunque ya existan.",
    )
    return parser.parse_args()


def require_ffmpeg() -> str:
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path is None:
        raise RuntimeError(
            "ffmpeg_not_available: ejecuta este script dentro del contenedor worker "
            "o instala ffmpeg localmente."
        )
    return ffmpeg_path


def build_command(ffmpeg_path: str, spec: VideoSpec, output_path: Path) -> list[str]:
    video_source = (
        f"testsrc2=size={spec.width}x{spec.height}:"
        f"rate={spec.fps}:duration={spec.duration_seconds}"
    )
    audio_source = f"sine=frequency={spec.sine_frequency}:duration={spec.duration_seconds}"
    return [
        ffmpeg_path,
        "-y",
        "-f",
        "lavfi",
        "-i",
        video_source,
        "-f",
        "lavfi",
        "-i",
        audio_source,
        "-shortest",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        str(spec.crf),
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "96k",
        "-movflags",
        "+faststart",
        str(output_path),
    ]


def main() -> int:
    args = parse_args()
    dataset_dir = Path(args.dataset_dir).expanduser().resolve()
    dataset_dir.mkdir(parents=True, exist_ok=True)
    ffmpeg_path = require_ffmpeg()

    created = 0
    skipped = 0
    for spec in DATASET_PLAN:
        output_path = dataset_dir / spec.filename
        if output_path.exists() and not args.force:
            skipped += 1
            print(f"SKIP {output_path.name} ya existe")
            continue

        print(
            f"GEN  {output_path.name} "
            f"{spec.duration_seconds}s {spec.width}x{spec.height}@{spec.fps} "
            f"profile={spec.profile}"
        )
        completed = subprocess.run(
            build_command(ffmpeg_path, spec, output_path),
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            print(completed.stderr)
            raise RuntimeError(f"ffmpeg_failed_for: {output_path.name}")
        created += 1

    print()
    print(f"Videos creados: {created}")
    print(f"Videos omitidos: {skipped}")
    print("Siguiente paso: python scripts/build_dataset_metadata.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
