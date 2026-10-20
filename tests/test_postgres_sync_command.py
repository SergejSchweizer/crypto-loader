from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

import api.commands.postgres as postgres_cmd
from api.cli import build_parser
from application.postgres_sync import GoldSyncResult
from application.postgres_sync.service import GoldSyncServiceError
from infra.postgres.gold_repository import PostgresGoldRepositoryError, PostgresSchemaMismatchError


def _set_valid_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGHOST", "10.10.1.3")
    monkeypatch.setenv("PGPORT", "54321")
    monkeypatch.setenv("PGUSER", "crypto-loader")
    monkeypatch.setenv("PGDATABASE", "market_data")
    monkeypatch.setenv("PGPASSWORD", "fake-secret")


def test_parser_exposes_exact_sync_command_and_gold_root() -> None:
    parser = build_parser()
    args = parser.parse_args(["--debug", "gold-sync-postgres", "--gold-root", "/tmp/gold"])
    assert args.command == "gold-sync-postgres"
    assert args.gold_root == "/tmp/gold"
    assert args.debug is True


def test_invalid_config_creates_no_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PGHOST", raising=False)
    monkeypatch.delenv("PGPORT", raising=False)
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.delenv("PGDATABASE", raising=False)
    monkeypatch.delenv("PGPASSWORD", raising=False)
    calls: list[object] = []
    monkeypatch.setattr(
        postgres_cmd,
        "PostgresGoldSyncRepository",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    args = argparse.Namespace(gold_root="lake/gold")
    with pytest.raises(postgres_cmd.PostgresSyncCommandError, match="configuration"):
        postgres_cmd.run_gold_sync_postgres(args, logging.getLogger("test"))
    assert calls == []


def test_success_emits_deterministic_credential_free_payload(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _set_valid_env(monkeypatch)
    repositories: list[object] = []

    class FakeRepository:
        pass

    def fake_repository(settings: object) -> FakeRepository:
        rendered = repr(settings)
        assert "fake-secret" not in rendered
        repo = FakeRepository()
        repositories.append(repo)
        return repo

    monkeypatch.setattr(postgres_cmd, "PostgresGoldSyncRepository", fake_repository)
    monkeypatch.setattr(
        postgres_cmd,
        "synchronize_gold_root",
        lambda root, repository, **kwargs: GoldSyncResult(()),
    )

    postgres_cmd.run_gold_sync_postgres(argparse.Namespace(gold_root="lake/gold"), logging.getLogger("test"))
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "gold-sync-postgres",
        "deleted": 0,
        "inserted": 0,
        "lineages_processed": 0,
        "status": "success",
        "unchanged": 0,
        "updated": 0,
    }
    assert len(repositories) == 1
    assert "fake-secret" not in json.dumps(payload)


def test_service_failure_is_sanitized(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_env(monkeypatch)
    monkeypatch.setattr(postgres_cmd, "PostgresGoldSyncRepository", lambda settings: object())

    def fail(root: object, repository: object, **kwargs: object) -> GoldSyncResult:
        raise OSError("gold root unavailable")

    monkeypatch.setattr(postgres_cmd, "synchronize_gold_root", fail)
    with pytest.raises(postgres_cmd.PostgresSyncCommandError, match="current-gold-inventory"):
        postgres_cmd.run_gold_sync_postgres(argparse.Namespace(gold_root="lake/gold"), logging.getLogger("test"))


def test_declared_certification_failure_is_sanitized(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_env(monkeypatch)
    monkeypatch.setattr(postgres_cmd, "PostgresGoldSyncRepository", lambda settings: object())

    def fail(root: object, repository: object, *, publication_result: object, **kwargs: object) -> GoldSyncResult:
        del root, repository, publication_result, kwargs
        raise ValueError("/private/runtime/path/tampered.parquet SHA-256 mismatch")

    monkeypatch.setattr(postgres_cmd, "synchronize_gold_root", fail)
    args = argparse.Namespace(gold_root="lake/gold", publication_result=".run/gold-publication-result.json")

    with pytest.raises(postgres_cmd.PostgresSyncCommandError) as exc_info:
        postgres_cmd.run_gold_sync_postgres(args, logging.getLogger("test"))

    assert str(exc_info.value) == ("current-gold-inventory: declared Gold publication result could not be certified")
    assert "/private/" not in str(exc_info.value)


@pytest.mark.parametrize(
    ("error", "category"),
    [
        (PostgresSchemaMismatchError("table contract differs"), "compatibility-schema"),
        (
            GoldSyncServiceError(
                SimpleNamespace(lineage=SimpleNamespace(dataset_id="gold", exchange="deribit", symbol="BTC")),
                "compatibility",
                "Gold artifact is incompatible",
            ),
            "compatibility-schema",
        ),
        (
            GoldSyncServiceError(
                SimpleNamespace(lineage=SimpleNamespace(dataset_id="gold", exchange="deribit", symbol="BTC")),
                "postgresql",
                "database unavailable",
            ),
            "postgresql",
        ),
        (PostgresGoldRepositoryError("write rejected"), "postgresql"),
    ],
)
def test_declared_sync_errors_have_stable_categories(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    category: str,
) -> None:
    """Known synchronization failures preserve their safe command category."""

    _set_valid_env(monkeypatch)
    monkeypatch.setattr(postgres_cmd, "PostgresGoldSyncRepository", lambda settings: object())

    def fail(*_: object, **__: object) -> GoldSyncResult:
        raise error

    monkeypatch.setattr(postgres_cmd, "synchronize_gold_root", fail)
    args = argparse.Namespace(gold_root="lake/gold", publication_result=".run/gold-publication-result.json")

    with pytest.raises(postgres_cmd.PostgresSyncCommandError) as exc_info:
        postgres_cmd.run_gold_sync_postgres(args, logging.getLogger("test"))

    assert exc_info.value.category == category
    assert str(error) in str(exc_info.value)


def test_declared_publication_result_is_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    """A declared Gold publication result is passed as a Path to the sync service."""

    _set_valid_env(monkeypatch)
    monkeypatch.setattr(postgres_cmd, "PostgresGoldSyncRepository", lambda settings: object())
    calls: list[tuple[Path, Path, int]] = []

    def synchronize(
        root: Path,
        _: object,
        *,
        publication_result: Path,
        max_parallel_lineages: int,
    ) -> GoldSyncResult:
        calls.append((root, publication_result, max_parallel_lineages))
        return GoldSyncResult(())

    monkeypatch.setattr(postgres_cmd, "synchronize_gold_root", synchronize)
    postgres_cmd.run_gold_sync_postgres(
        argparse.Namespace(gold_root="lake/gold", publication_result=".run/gold-publication-result.json"),
        logging.getLogger("test"),
    )

    assert calls == [(Path("lake/gold"), Path(".run/gold-publication-result.json"), 2)]
