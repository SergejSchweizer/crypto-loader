#!/usr/bin/env python3
"""Provision the dedicated PostgreSQL Gold-sync role with least privilege."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, cast

import polars as pl
import psycopg
from psycopg import sql

from application.postgres_sync.inventory import discover_current_gold_lineages
from application.postgres_sync.schema import (
    PostgresBootstrapMigration,
    bootstrap_migration,
    build_postgres_table_schema,
)

POSTGRES_HOST = "10.10.1.3"
POSTGRES_PORT = 54321
APP_ROLE = "crypto-loader"
CONSUMER_SCHEMA = "crypto_loader"
SYNC_SCHEMA = "crypto_loader_sync"
_REQUIRED_ROLE_ATTRIBUTES = {
    "rolsuper": False,
    "rolcreatedb": False,
    "rolcreaterole": False,
    "rolreplication": False,
    "rolbypassrls": False,
    "rolcanlogin": True,
    "rolinherit": False,
}


class ProvisioningCursor(Protocol):
    """Minimal administrator cursor contract used by provisioning helpers."""

    def execute(self, query: object, params: object = None) -> object: ...

    def fetchone(self) -> tuple[object, ...] | None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ProvisioningConfig:
    """Operator-only provisioning settings; secrets are excluded from repr."""

    host: str
    port: int
    database: str
    admin_user: str
    gold_root: Path
    admin_password: str = field(repr=False)
    app_password: str = field(repr=False)

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> ProvisioningConfig:
        """Resolve protected operator settings from environment-style input."""

        host = values.get("PGHOST", POSTGRES_HOST).strip()
        raw_port = values.get("PGPORT", str(POSTGRES_PORT)).strip()
        database = values.get("PGDATABASE", "").strip()
        admin_user = values.get("PGADMINUSER", "").strip()
        gold_root_text = values.get("GOLD_ROOT", "").strip()
        admin_password = values.get("PGADMINPASSWORD", "")
        app_password = values.get("PGPASSWORD", "")
        try:
            port = int(raw_port)
        except ValueError as exc:
            raise ValueError("PGPORT must be an integer") from exc
        if host != POSTGRES_HOST or port != POSTGRES_PORT:
            raise ValueError(f"provisioning endpoint must be {POSTGRES_HOST}:{POSTGRES_PORT}")
        if not database:
            raise ValueError("PGDATABASE is required")
        if not admin_user:
            raise ValueError("PGADMINUSER is required")
        if not gold_root_text:
            raise ValueError("GOLD_ROOT is required")
        if not admin_password:
            raise ValueError("PGADMINPASSWORD is required")
        if not app_password:
            raise ValueError("PGPASSWORD is required for the application role")
        return cls(host, port, database, admin_user, Path(gold_root_text), admin_password, app_password)

    @classmethod
    def from_env(cls) -> ProvisioningConfig:
        return cls.from_mapping(os.environ)


def _validate_role(cursor: ProvisioningCursor) -> bool:
    """Return whether the role exists and fail if its attributes are incompatible."""

    cursor.execute(
        "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolcanlogin, rolinherit "
        "FROM pg_roles WHERE rolname = %s",
        (APP_ROLE,),
    )
    row = cursor.fetchone()
    if row is None:
        return False
    names = tuple(_REQUIRED_ROLE_ATTRIBUTES)
    actual = dict(zip(names, (bool(value) for value in row), strict=True))
    if actual != _REQUIRED_ROLE_ATTRIBUTES:
        raise RuntimeError("existing PostgreSQL application role has incompatible privileges")
    return True


def _ensure_role(cursor: ProvisioningCursor, app_password: str) -> None:
    """Create or harden the application role and set its protected runtime password."""

    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (APP_ROLE,))
    exists = cursor.fetchone() is not None
    role_identifier = sql.Identifier(APP_ROLE)
    password_literal = sql.Literal(app_password)
    if not exists:
        cursor.execute(
            sql.SQL(
                "CREATE ROLE {} WITH LOGIN PASSWORD {} NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOREPLICATION NOBYPASSRLS"
            ).format(role_identifier, password_literal)
        )
    else:
        cursor.execute(
            sql.SQL(
                "ALTER ROLE {} WITH LOGIN PASSWORD {} NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOREPLICATION NOBYPASSRLS"
            ).format(role_identifier, password_literal)
        )


def _schema_owner(cursor: ProvisioningCursor, schema_name: str) -> str | None:
    cursor.execute(
        "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = %s",
        (schema_name,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    owner = row[0]
    if not isinstance(owner, str):
        raise RuntimeError("unexpected PostgreSQL schema owner type")
    return owner


def _current_user(cursor: ProvisioningCursor) -> str:
    cursor.execute("SELECT current_user")
    row = cursor.fetchone()
    if row is None or not isinstance(row[0], str):
        raise RuntimeError("unexpected PostgreSQL administrator identity")
    return row[0]


def _ensure_schema(cursor: ProvisioningCursor, schema_name: str, admin_role: str) -> None:
    """Create an administrator-owned schema and deny runtime object creation."""

    owner = _schema_owner(cursor, schema_name)
    if owner is None:
        cursor.execute(sql.SQL("CREATE SCHEMA {} AUTHORIZATION CURRENT_USER").format(sql.Identifier(schema_name)))
    elif owner == APP_ROLE:
        cursor.execute(
            sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(sql.Identifier(schema_name), sql.Identifier(admin_role))
        )
    elif owner != admin_role:
        raise RuntimeError(f"existing PostgreSQL schema {schema_name!r} has incompatible ownership")
    cursor.execute(
        sql.SQL("REVOKE ALL ON SCHEMA {} FROM {}").format(sql.Identifier(schema_name), sql.Identifier(APP_ROLE))
    )
    cursor.execute(sql.SQL("REVOKE CREATE ON SCHEMA {} FROM PUBLIC").format(sql.Identifier(schema_name)))
    cursor.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(schema_name),
            sql.Identifier(APP_ROLE),
        )
    )


def _migration_tables(migrations: tuple[PostgresBootstrapMigration, ...]) -> tuple[tuple[str, str], ...]:
    tables = {(table.schema_name, table.table_name) for migration in migrations for table in migration.tables}
    return tuple(sorted(tables))


def _apply_migrations(
    cursor: ProvisioningCursor,
    migrations: tuple[PostgresBootstrapMigration, ...],
) -> None:
    """Execute each deterministic PR-85 table migration once as administrator."""

    statements = {
        statement
        for migration in migrations
        for statement in migration.statements
        if statement.startswith("CREATE TABLE")
    }
    for statement in sorted(statements):
        cursor.execute(statement)


def _grant_runtime_table_privileges(
    cursor: ProvisioningCursor,
    tables: tuple[tuple[str, str], ...],
) -> None:
    """Replace runtime table grants with the exact normal-sync DML contract."""

    for schema_name in (CONSUMER_SCHEMA, SYNC_SCHEMA):
        cursor.execute(
            sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM {}").format(
                sql.Identifier(schema_name), sql.Identifier(APP_ROLE)
            )
        )
        cursor.execute(sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM PUBLIC").format(sql.Identifier(schema_name)))
    for schema_name, table_name in tables:
        table = sql.Identifier(schema_name, table_name)
        privileges = "SELECT, INSERT, UPDATE" if table_name == "gold_sync_state" else "SELECT, INSERT, UPDATE, DELETE"
        cursor.execute(sql.SQL(f"GRANT {privileges} ON TABLE {{}} TO {{}}").format(table, sql.Identifier(APP_ROLE)))


def _expected_table_grants(tables: tuple[tuple[str, str], ...]) -> set[tuple[str, str, str]]:
    expected: set[tuple[str, str, str]] = set()
    for schema_name, table_name in tables:
        privileges = {"SELECT", "INSERT", "UPDATE"}
        if table_name != "gold_sync_state":
            privileges.add("DELETE")
        expected.update((schema_name, table_name, privilege) for privilege in privileges)
    return expected


def _validate_runtime_contract(
    cursor: ProvisioningCursor,
    tables: tuple[tuple[str, str], ...],
    admin_role: str,
) -> None:
    """Fail unless catalog ownership and grants exactly match the runtime contract."""

    if not _validate_role(cursor):
        raise RuntimeError("PostgreSQL application role disappeared during provisioning")
    cursor.execute(
        "SELECT nspname, pg_get_userbyid(nspowner), "
        "has_schema_privilege(%s, nspname, 'USAGE'), has_schema_privilege(%s, nspname, 'CREATE') "
        "FROM pg_namespace WHERE nspname IN (%s, %s) ORDER BY nspname",
        (APP_ROLE, APP_ROLE, CONSUMER_SCHEMA, SYNC_SCHEMA),
    )
    schema_rows = cursor.fetchall()
    expected_schemas = [(CONSUMER_SCHEMA, admin_role, True, False), (SYNC_SCHEMA, admin_role, True, False)]
    if schema_rows != expected_schemas:
        raise RuntimeError("PostgreSQL schema ownership or runtime privileges are incompatible")
    cursor.execute(
        "SELECT table_schema, table_name, privilege_type FROM information_schema.role_table_grants "
        "WHERE grantee = %s AND table_schema IN (%s, %s) ORDER BY table_schema, table_name, privilege_type",
        (APP_ROLE, CONSUMER_SCHEMA, SYNC_SCHEMA),
    )
    actual_grants = {
        (str(schema_name), str(table_name), str(privilege)) for schema_name, table_name, privilege in cursor.fetchall()
    }
    if actual_grants != _expected_table_grants(tables):
        raise RuntimeError("PostgreSQL runtime table grants are incompatible")
    cursor.execute(
        "SELECT 1 FROM pg_auth_members AS membership "
        "JOIN pg_roles AS member_role ON member_role.oid = membership.member "
        "WHERE member_role.rolname = %s",
        (APP_ROLE,),
    )
    if cursor.fetchone() is not None:
        raise RuntimeError("PostgreSQL runtime role must not inherit role memberships")


def _load_migrations(gold_root: Path) -> tuple[PostgresBootstrapMigration, ...]:
    """Build administrator migrations from certified current Gold artifacts."""

    snapshots = discover_current_gold_lineages(gold_root)
    if not snapshots:
        raise ValueError("GOLD_ROOT contains no current registered Gold artifacts")
    migrations: list[PostgresBootstrapMigration] = []
    for snapshot in snapshots:
        source_schema = dict(pl.read_parquet_schema(snapshot.artifact_path))
        schema = build_postgres_table_schema(snapshot.lineage.dataset_id, source_schema)
        migrations.append(bootstrap_migration(schema))
    return tuple(migrations)


def provision(config: ProvisioningConfig, migrations: tuple[PostgresBootstrapMigration, ...]) -> None:
    """Provision administrator-owned storage and exact runtime DML grants."""

    if not migrations:
        raise ValueError("at least one PostgreSQL bootstrap migration is required")

    connection = psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.admin_user,
        password=config.admin_password,
        autocommit=False,
    )
    try:
        cursor = cast(ProvisioningCursor, connection.cursor())
        try:
            _ensure_role(cursor, config.app_password)
            admin_role = _current_user(cursor)
            _ensure_schema(cursor, CONSUMER_SCHEMA, admin_role)
            _ensure_schema(cursor, SYNC_SCHEMA, admin_role)
            _apply_migrations(cursor, migrations)
            tables = _migration_tables(migrations)
            _grant_runtime_table_privileges(cursor, tables)
            _validate_runtime_contract(cursor, tables, admin_role)
        finally:
            cursor.close()
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def main() -> int:
    """Operator entrypoint. No password is accepted as a process argument."""

    try:
        config = ProvisioningConfig.from_env()
        provision(config, _load_migrations(config.gold_root))
    except Exception as exc:
        # Error categories are intentionally sanitized. Connection exceptions may
        # contain host/database metadata, but never the protected passwords because
        # we do not interpolate configuration values into this message.
        print(f"PostgreSQL role provisioning failed: {type(exc).__name__}")
        return 1
    print(
        f"PostgreSQL role provisioned: endpoint={POSTGRES_HOST}:{POSTGRES_PORT} "
        f"role={APP_ROLE} schemas={CONSUMER_SCHEMA},{SYNC_SCHEMA}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
