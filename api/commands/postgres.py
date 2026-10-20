"""Operational CLI command for synchronizing current Gold into PostgreSQL."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from application.postgres_sync.config import PostgresSyncConfig
from application.postgres_sync.service import GoldSyncServiceError, synchronize_gold_root
from infra.postgres.gold_repository import (
    PostgresConnectionSettings,
    PostgresGoldRepositoryError,
    PostgresGoldSyncRepository,
    PostgresSchemaMismatchError,
)


class PostgresSyncCommandError(RuntimeError):
    """Sanitized command error with a stable operational category."""

    def __init__(self, category: str, message: str) -> None:
        super().__init__(f"{category}: {message}")
        self.category = category


def add_gold_sync_postgres_parser(subparsers: Any) -> None:
    """Register exactly one PostgreSQL Gold synchronization command."""

    parser = subparsers.add_parser(
        "gold-sync-postgres",
        help="Synchronize current registered Gold datasets into PostgreSQL",
    )
    parser.add_argument("--gold-root", default="lake/gold", help="Gold lake root")
    parser.add_argument(
        "--publication-result",
        help="Successful Gold result declaring the exact lineages eligible for this sync",
    )


def _settings_from_config(config: PostgresSyncConfig) -> PostgresConnectionSettings:
    return PostgresConnectionSettings(
        host=config.host,
        port=config.port,
        user=config.user,
        database=config.database,
        password=config.password,
        connect_timeout_s=config.connect_timeout_s,
        lock_timeout_ms=config.lock_timeout_ms,
        statement_timeout_ms=config.statement_timeout_ms,
        idle_in_transaction_session_timeout_ms=config.idle_in_transaction_session_timeout_ms,
    )


def run_gold_sync_postgres(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run sync-only reconciliation without invoking any Medallion build stage."""

    try:
        config = PostgresSyncConfig.from_env()
    except ValueError as exc:
        raise PostgresSyncCommandError("configuration", str(exc)) from None

    repository = PostgresGoldSyncRepository(_settings_from_config(config))
    publication_result = getattr(args, "publication_result", None)
    try:
        if publication_result:
            result = synchronize_gold_root(
                Path(str(args.gold_root)),
                repository,
                publication_result=Path(str(publication_result)),
                max_parallel_lineages=config.max_parallel_lineages,
            )
        else:
            result = synchronize_gold_root(
                Path(str(args.gold_root)),
                repository,
                max_parallel_lineages=config.max_parallel_lineages,
            )
    except PostgresSchemaMismatchError as exc:
        raise PostgresSyncCommandError("compatibility-schema", str(exc)) from None
    except GoldSyncServiceError as exc:
        category = "compatibility-schema" if exc.category == "compatibility" else "postgresql"
        raise PostgresSyncCommandError(category, str(exc)) from None
    except PostgresGoldRepositoryError as exc:
        raise PostgresSyncCommandError("postgresql", str(exc)) from None
    except (OSError, ValueError, TypeError) as exc:
        if publication_result:
            raise PostgresSyncCommandError(
                "current-gold-inventory",
                "declared Gold publication result could not be certified",
            ) from None
        raise PostgresSyncCommandError("current-gold-inventory", str(exc)) from None

    payload = {
        "command": "gold-sync-postgres",
        "status": "success",
        "lineages_processed": len(result.lineages),
        "inserted": result.inserted,
        "updated": result.updated,
        "deleted": result.deleted,
        "unchanged": result.unchanged,
    }
    logger.info(
        "PostgreSQL Gold sync complete lineages=%s inserted=%s updated=%s deleted=%s unchanged=%s",
        payload["lineages_processed"],
        result.inserted,
        result.updated,
        result.deleted,
        result.unchanged,
    )
    print(json.dumps(payload, sort_keys=True))
