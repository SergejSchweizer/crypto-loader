"""Tests for the weekly Gold Parquet copy job."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

from scripts import sync_gold_parquet_to_quotes
from scripts.sync_gold_parquet_to_quotes import copy_gold_parquet_files


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _logger() -> logging.Logger:
    logger = logging.getLogger("test.sync-gold-parquet-to-quotes")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    return logger


def test_copy_gold_parquet_files_copies_only_parquet_and_keeps_existing_files(tmp_path: Path) -> None:
    source = tmp_path / "lake" / "gold"
    destination = tmp_path / "volume1" / "Temp" / "Quotes"
    parquet = source / "dataset_id=gold.history.full.m1" / "part-000.parquet"
    _write_file(parquet, "parquet-data")
    _write_file(source / "dataset_id=gold.history.full.m1" / "metadata.json", "{}")
    stale_target = destination / "legacy.parquet"
    _write_file(stale_target, "keep-me")

    first = copy_gold_parquet_files(source_root=source, destination_root=destination, logger=_logger())
    second = copy_gold_parquet_files(source_root=source, destination_root=destination, logger=_logger())

    copied = destination / "dataset_id=gold.history.full.m1" / "part-000.parquet"
    assert copied.read_text(encoding="utf-8") == "parquet-data"
    assert not (destination / "dataset_id=gold.history.full.m1" / "metadata.json").exists()
    assert stale_target.read_text(encoding="utf-8") == "keep-me"
    assert (first.copied_files, first.skipped_files) == (1, 0)
    assert (second.copied_files, second.skipped_files) == (0, 1)


def test_copy_gold_parquet_files_rejects_nested_destination(tmp_path: Path) -> None:
    source = tmp_path / "lake" / "gold"
    source.mkdir(parents=True)

    with pytest.raises(ValueError, match="source and destination roots must not contain one another"):
        copy_gold_parquet_files(
            source_root=source,
            destination_root=source / "nested",
            logger=_logger(),
        )


def test_parse_args_uses_quotes_destination_and_separate_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["sync_gold_parquet_to_quotes.py"])
    args = sync_gold_parquet_to_quotes.parse_args()

    assert args.destination_root == "/volume1/Temp/Quotes"
    assert args.lock_file.endswith(".run/sync-gold-parquet-to-quotes.lock")
