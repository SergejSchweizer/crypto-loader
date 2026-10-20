"""Prepare the isolated CI PostgreSQL service for runtime-privilege tests."""

from __future__ import annotations

import os
from typing import cast

import polars as pl
import psycopg
from psycopg.conninfo import conninfo_to_dict

from application.postgres_sync.schema import bootstrap_migration, build_postgres_table_schema
from scripts.provision_postgres_sync_role import (
    APP_ROLE,
    CONSUMER_SCHEMA,
    SYNC_SCHEMA,
    ProvisioningCursor,
    _apply_migrations,
    _current_user,
    _ensure_role,
    _ensure_schema,
    _grant_runtime_table_privileges,
    _migration_tables,
    _validate_runtime_contract,
)


def main() -> int:
    admin_dsn = os.environ["TEST_POSTGRES_ADMIN_DSN"]
    runtime_dsn = os.environ["TEST_POSTGRES_RUNTIME_DSN"]
    admin = conninfo_to_dict(admin_dsn)
    runtime = conninfo_to_dict(runtime_dsn)
    if any(admin.get(key) != runtime.get(key) for key in ("host", "port", "dbname")):
        raise ValueError("CI PostgreSQL admin/runtime DSNs must target the same database")
    if runtime.get("user") != APP_ROLE or not runtime.get("password"):
        raise ValueError("CI PostgreSQL runtime DSN must configure the crypto-loader role and password")

    source_schema = cast(
        dict[str, pl.DataType],
        {
            "exchange": pl.String,
            "symbol": pl.String,
            "timestamp_m1": pl.Datetime("us", "UTC"),
            "value": pl.Float64,
        },
    )
    schema = build_postgres_table_schema("gold.history.full.m1", source_schema)
    migrations = (bootstrap_migration(schema),)

    with psycopg.connect(admin_dsn, autocommit=False) as connection:
        with connection.cursor() as raw_cursor:
            cursor = cast(ProvisioningCursor, raw_cursor)
            _ensure_role(cursor, str(runtime["password"]))
            admin_role = _current_user(cursor)
            _ensure_schema(cursor, CONSUMER_SCHEMA, admin_role)
            _ensure_schema(cursor, SYNC_SCHEMA, admin_role)
            _apply_migrations(cursor, migrations)
            tables = _migration_tables(migrations)
            _grant_runtime_table_privileges(cursor, tables)
            _validate_runtime_contract(cursor, tables, admin_role)
        connection.commit()

    print("CI PostgreSQL runtime role provisioned with isolated test schemas")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
