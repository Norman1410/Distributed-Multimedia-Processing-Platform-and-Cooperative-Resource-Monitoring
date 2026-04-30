from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Revisa y limpia artefactos generados antes de la entrega.",
    )
    parser.add_argument("--dataset-dir", default="dataset")
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--apply", action="store_true", help="Ejecuta la limpieza.")
    parser.add_argument(
        "--remove-dataset-duplicates",
        action="store_true",
        help="Elimina duplicados exactos del dataset dejando el primero.",
    )
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def generated_results(results_dir: Path) -> list[Path]:
    if not results_dir.exists():
        return []
    return [
        path
        for path in sorted(results_dir.iterdir())
        if path.is_file() and path.name != ".gitkeep"
    ]


def duplicate_dataset_files(dataset_dir: Path) -> list[tuple[Path, list[Path]]]:
    if not dataset_dir.exists():
        return []
    by_hash: dict[str, list[Path]] = {}
    for path in sorted(dataset_dir.iterdir()):
        if not path.is_file() or path.name.startswith(".") or path.name == "dataset_metadata.json":
            continue
        by_hash.setdefault(sha256_file(path), []).append(path)
    return [
        (paths[0], paths[1:])
        for paths in by_hash.values()
        if len(paths) > 1
    ]


def main() -> int:
    args = parse_args()
    dataset_dir = Path(args.dataset_dir).expanduser().resolve()
    results_dir = Path(args.results_dir).expanduser().resolve()

    result_files = generated_results(results_dir)
    duplicate_groups = duplicate_dataset_files(dataset_dir)

    print(f"Results generados detectados: {len(result_files)}")
    for path in result_files:
        print(f"- {path}")

    print(f"Grupos de duplicados en dataset: {len(duplicate_groups)}")
    for kept, duplicates in duplicate_groups:
        print(f"- conservar {kept.name}; duplicados: {', '.join(path.name for path in duplicates)}")

    if not args.apply:
        print()
        print("Dry-run solamente. Agrega --apply para limpiar results.")
        return 0

    for path in result_files:
        path.unlink()
        print(f"DEL {path}")

    if args.remove_dataset_duplicates:
        for _kept, duplicates in duplicate_groups:
            for path in duplicates:
                path.unlink()
                print(f"DEL duplicate {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
