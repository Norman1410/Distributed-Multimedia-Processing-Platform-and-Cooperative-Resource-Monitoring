from __future__ import annotations

from typing import Final


SUPPORTED_OPERATIONS: Final[tuple[str, ...]] = (
    "extract_audio",
    "generate_thumbnail",
    "transcode_h264",
    "extract_metadata",
)

OPERATION_DESCRIPTIONS: Final[dict[str, str]] = {
    "extract_audio": "Extrae pista de audio del video a MP3.",
    "generate_thumbnail": "Genera miniatura JPG del video.",
    "transcode_h264": "Convierte el video a MP4 (H.264 + AAC).",
    "extract_metadata": "Extrae metadatos tecnicos del archivo en JSON.",
}


def normalize_operation_name(operation: str) -> str:
    return operation.strip().lower()


def is_supported_operation(operation: str) -> bool:
    return normalize_operation_name(operation) in SUPPORTED_OPERATIONS

