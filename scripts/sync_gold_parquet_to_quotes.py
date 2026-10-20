"""Copy Gold Parquet files to the weekly Quotes staging path."""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from application.services.runtime_service import SingleInstanceError, SingleInstanceLock
from scripts.logging_utils import configure_logger


@dataclass(frozen=True)
class CopyReport:
    source_root: str
    destination_root: str
    copied_files: int
    skipped_files: int


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_config_path(repo_root: Path) -> Path:
    runtime_config = repo_root / ".run" / "cron-config.yaml"
    return runtime_config if runtime_config.exists() else repo_root / "config.yaml"


def _resolve_path(path_value: str, *, repo_root: Path) -> Path:
    path = Path(path_value.strip()).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def copy_gold_parquet_files(*, source_root: Path, destination_root: Path, logger: logging.Logger) -> CopyReport:
    """Copy Parquet files while preserving relative paths and existing target files."""

    if not source_root.exists():
        raise FileNotFoundError(f"gold source root does not exist: {source_root}")
    if not source_root.is_dir():
        raise NotADirectoryError(f"gold source root must be a directory: {source_root}")
    source_root = source_root.resolve()
    destination_root = destination_root.resolve()
    if (
        source_root == destination_root
        or source_root in destination_root.parents
        or destination_root in source_root.parents
    ):
        raise ValueError("source and destination roots must not contain one another")

    copied_files = 0
    skipped_files = 0
    for source_file in source_root.rglob("*.parquet"):
        if not source_file.is_file():
            continue
        destination_file = destination_root / source_file.relative_to(source_root)
        if destination_file.exists():
            source_stat = source_file.stat()
            destination_stat = destination_file.stat()
            if (
                source_stat.st_size == destination_stat.st_size
                and source_stat.st_mtime_ns == destination_stat.st_mtime_ns
            ):
                skipped_files += 1
                continue
        destination_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, destination_file)
        copied_files += 1

    logger.info(
        "Gold Parquet copy completed source_root=%s destination_root=%s copied_files=%s skipped_files=%s",
        source_root,
        destination_root,
        copied_files,
        skipped_files,
    )
    return CopyReport(str(source_root), str(destination_root), copied_files, skipped_files)


def parse_args() -> argparse.Namespace:
    repo_root = _default_repo_root()
    parser = argparse.ArgumentParser(description="Copy Gold Parquet files to /volume1/Temp/Quotes")
    parser.add_argument("--repo-root", default=str(repo_root), help="Repository root path")
    parser.add_argument("--config", default=str(_default_config_path(repo_root)), help="Logging config path")
    parser.add_argument("--source-root", default="lake/gold", help="Canonical Gold lake root")
    parser.add_argument("--destination-root", default="/volume1/Temp/Quotes", help="Parquet copy destination root")
    parser.add_argument("--lock-file", default=str(repo_root / ".run" / "sync-gold-parquet-to-quotes.lock"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    config_path = Path(args.config).resolve()
    source_root = _resolve_path(str(args.source_root), repo_root=repo_root)
    destination_root = _resolve_path(str(args.destination_root), repo_root=repo_root)
    lock_file = Path(args.lock_file).resolve()
    logger = configure_logger("sync-gold-parquet-to-quotes", config_path)
    logger.info(
        "Gold Parquet copy start source_root=%s destination_root=%s lock_file=%s",
        source_root,
        destination_root,
        lock_file,
    )

    try:
        with SingleInstanceLock(str(lock_file)):
            report = copy_gold_parquet_files(
                source_root=source_root,
                destination_root=destination_root,
                logger=logger,
            )
    except SingleInstanceError:
        logger.warning("Gold Parquet copy skipped because another copy job is already running")
        return 1

    logger.info("Gold Parquet copy report=%s", report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
