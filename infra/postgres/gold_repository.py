"""Psycopg repository adapter for the rebuildable Gold serving-plane replica."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from heapq import merge as merge_sorted
from typing import Protocol, cast

import psycopg
from psycopg.types.json import Jsonb

from application.postgres_sync import (
    POSTGRES_CONSUMER_SCHEMA,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_ROLE,
    POSTGRES_ROW_HASH_TABLE,
    POSTGRES_SESSION_TIMEZONE,
    POSTGRES_SYNC_SCHEMA,
    POSTGRES_SYNC_STATE_TABLE,
    GoldDeltaPlan,
    GoldLineage,
    GoldRowDigest,
    GoldRowKey,
    GoldSourceSnapshot,
    GoldSyncLineageResult,
    GoldSyncState,
    GoldTargetSummary,
    consumer_table_name,
    expected_target_summary,
    state_matches_snapshot,
)
from application.postgres_sync.contracts import GoldReconcileDecision, GoldReconcilePlanner
from application.postgres_sync.delta import canonical_row_hash
from application.postgres_sync.schema import (
    PostgresCatalogColumn,
    PostgresCatalogTable,
    owned_catalog_tables,
    quote_identifier,
)


class PostgresGoldRepositoryError(RuntimeError):
    """Sanitized PostgreSQL adapter failure."""

    def __init__(self, message: str, *, safe_detail: str | None = None) -> None:
        super().__init__(message)
        self.safe_detail = safe_detail


def _safe_database_error_code(exc: Exception, *, password: str | None = None) -> str:
    error_type = type(exc).__name__
    sqlstate = getattr(exc, "sqlstate", None)
    if isinstance(sqlstate, str) and len(sqlstate) == 5 and sqlstate.isalnum():
        error_type = f"{error_type} (SQLSTATE {sqlstate})"
    if isinstance(exc, psycopg.OperationalError):
        message = str(exc)
        if password:
            message = message.replace(password, "<redacted>")
        message = re.sub(r"(?i)(password\s*[=:]\s*)\S+", r"\1<redacted>", message)
        message = " ".join(message.split())[:240]
        if message:
            return f"{error_type}: {message}"
    return error_type


class PostgresSchemaMismatchError(PostgresGoldRepositoryError):
    """Existing target schema/checkpoint is incompatible with the current source."""


class CursorPort(Protocol):
    @property
    def rowcount(self) -> int: ...

    def execute(self, query: str, params: Sequence[object] | None = None) -> object: ...

    def executemany(self, query: str, params_seq: Iterable[Sequence[object]]) -> object: ...

    def fetchone(self) -> tuple[object, ...] | None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...

    def fetchmany(self, size: int = 1) -> list[tuple[object, ...]]: ...

    def close(self) -> None: ...


class ConnectionPort(Protocol):
    def cursor(self, name: str | None = None) -> CursorPort: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PostgresConnectionSettings:
    """Validated application connection settings with password redacted from repr."""

    host: str
    port: int
    user: str
    database: str
    password: str = field(repr=False)
    connect_timeout_s: int = 10
    lock_timeout_ms: int = 5_000
    statement_timeout_ms: int = 30_000
    idle_in_transaction_session_timeout_ms: int = 60_000

    def __post_init__(self) -> None:
        if self.host != POSTGRES_HOST or self.port != POSTGRES_PORT or self.user != POSTGRES_ROLE:
            raise ValueError("PostgreSQL Gold repository endpoint/user identity is invalid")
        if not self.database.strip():
            raise ValueError("PostgreSQL database must not be empty")
        if not self.password:
            raise ValueError("PostgreSQL password must not be empty")
        for name, value in (
            ("connect timeout", self.connect_timeout_s),
            ("lock timeout", self.lock_timeout_ms),
            ("statement timeout", self.statement_timeout_ms),
            ("idle transaction timeout", self.idle_in_transaction_session_timeout_ms),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"PostgreSQL {name} must be a positive integer")


ConnectionFactory = Callable[[PostgresConnectionSettings], ConnectionPort]


def _default_connection(settings: PostgresConnectionSettings) -> ConnectionPort:
    connection = psycopg.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        dbname=settings.database,
        password=settings.password,
        connect_timeout=settings.connect_timeout_s,
        autocommit=False,
    )
    return cast(ConnectionPort, connection)


def _qualified(schema_name: str, table_name: str) -> str:
    return f"{quote_identifier(schema_name)}.{quote_identifier(table_name)}"


_STATE = _qualified(POSTGRES_SYNC_SCHEMA, POSTGRES_SYNC_STATE_TABLE)
_DIGESTS = _qualified(POSTGRES_SYNC_SCHEMA, POSTGRES_ROW_HASH_TABLE)
_SELECT_STATE = f"""SELECT dataset_id, exchange, symbol, source_fingerprint, schema_signature, row_count,
min_timestamp, max_timestamp, synced_at_utc, source_version, build_id
FROM {_STATE} WHERE dataset_id = %s AND exchange = %s AND symbol = %s"""
_SELECT_DIGESTS = f"""SELECT timestamp_m1, row_sha256 FROM {_DIGESTS}
WHERE dataset_id = %s AND exchange = %s AND symbol = %s ORDER BY timestamp_m1"""
_UPSERT_STATE = f"""INSERT INTO {_STATE} (
    dataset_id, exchange, symbol, source_fingerprint, schema_signature, row_count,
    min_timestamp, max_timestamp, synced_at_utc, source_version, build_id
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (dataset_id, exchange, symbol) DO UPDATE SET
    source_fingerprint = EXCLUDED.source_fingerprint,
    schema_signature = EXCLUDED.schema_signature,
    row_count = EXCLUDED.row_count,
    min_timestamp = EXCLUDED.min_timestamp,
    max_timestamp = EXCLUDED.max_timestamp,
    synced_at_utc = EXCLUDED.synced_at_utc,
    source_version = EXCLUDED.source_version,
    build_id = EXCLUDED.build_id"""
_UPSERT_DIGEST = f"""INSERT INTO {_DIGESTS} (dataset_id, exchange, symbol, timestamp_m1, row_sha256)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (dataset_id, exchange, symbol, timestamp_m1)
DO UPDATE SET row_sha256 = EXCLUDED.row_sha256"""
_DELETE_DIGEST = f"DELETE FROM {_DIGESTS} WHERE dataset_id = %s AND exchange = %s AND symbol = %s AND timestamp_m1 = %s"
_CONSUMER_FETCH_BATCH_SIZE = 2_048
_DIGEST_FETCH_BATCH_SIZE = 4_096
_SOURCE_STAGE_BATCH_SIZE = 1_024
_SOURCE_STAGE_TABLE = 'pg_temp."gold_sync_stage"'

_CATALOG_COLUMNS = """SELECT table_schema, table_name, ordinal_position, column_name, data_type, is_nullable,
character_maximum_length, numeric_precision, numeric_scale, datetime_precision
FROM information_schema.columns
WHERE (table_schema, table_name) IN ({table_pairs})
ORDER BY table_schema, table_name, ordinal_position"""
_CATALOG_PRIMARY_KEYS = """SELECT namespace.nspname, relation.relname, attribute.attname, key_column.ordinality
FROM pg_catalog.pg_index AS index_definition
JOIN pg_catalog.pg_class AS relation ON relation.oid = index_definition.indrelid
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
JOIN LATERAL unnest(index_definition.indkey) WITH ORDINALITY AS key_column(attnum, ordinality) ON TRUE
JOIN pg_catalog.pg_attribute AS attribute
    ON attribute.attrelid = relation.oid AND attribute.attnum = key_column.attnum
WHERE index_definition.indisprimary AND (namespace.nspname, relation.relname) IN ({table_pairs})
ORDER BY namespace.nspname, relation.relname, key_column.ordinality"""


def _lineage_params(lineage: GoldLineage) -> tuple[object, ...]:
    return (lineage.dataset_id, lineage.exchange, lineage.symbol)


def _as_text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"PostgreSQL {name} must be text")
    return value


def _as_optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _as_text(value, name)


def _as_int(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"PostgreSQL {name} must be integer")
    return value


def _as_datetime(value: object, name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"PostgreSQL {name} must be datetime")
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"PostgreSQL {name} must be UTC-aware with a zero offset")
    return value


def _state_from_row(row: tuple[object, ...]) -> GoldSyncState:
    if len(row) != 11:
        raise ValueError("PostgreSQL Gold sync state row has unexpected width")
    synced = _as_datetime(row[8], "synced_at_utc")
    if synced is None:
        raise ValueError("PostgreSQL synced_at_utc cannot be null")
    return GoldSyncState(
        lineage=GoldLineage(_as_text(row[0], "dataset_id"), _as_text(row[1], "exchange"), _as_text(row[2], "symbol")),
        source_fingerprint=_as_text(row[3], "source_fingerprint"),
        schema_signature=_as_text(row[4], "schema_signature"),
        row_count=_as_int(row[5], "row_count"),
        min_timestamp=_as_datetime(row[6], "min_timestamp"),
        max_timestamp=_as_datetime(row[7], "max_timestamp"),
        synced_at_utc=synced,
        source_version=_as_optional_text(row[9], "source_version"),
        build_id=_as_optional_text(row[10], "build_id"),
    )


def _summary_from_row(row: tuple[object, ...]) -> GoldTargetSummary:
    if len(row) != 3:
        raise ValueError("PostgreSQL Gold target summary row has unexpected width")
    return GoldTargetSummary(
        row_count=_as_int(row[0], "row_count"),
        min_timestamp=_as_datetime(row[1], "min_timestamp"),
        max_timestamp=_as_datetime(row[2], "max_timestamp"),
    )


def _state_params(state: GoldSyncState) -> tuple[object, ...]:
    lineage = state.lineage
    return (
        lineage.dataset_id,
        lineage.exchange,
        lineage.symbol,
        state.source_fingerprint,
        state.schema_signature,
        state.row_count,
        state.min_timestamp,
        state.max_timestamp,
        state.synced_at_utc,
        state.source_version,
        state.build_id,
    )


def _safe_table_ddl(ddl: str) -> str:
    upper = ddl.upper()
    for forbidden in ("DROP ", "TRUNCATE ", "ALTER TABLE", "DELETE FROM"):
        if forbidden in upper:
            raise ValueError(f"forbidden normal-sync DDL token: {forbidden.strip()}")
    statements = [statement.strip() for statement in ddl.split(";") if statement.strip()]
    table_statements = [statement for statement in statements if statement.upper().startswith("CREATE TABLE")]
    if len(table_statements) != 1:
        raise ValueError("Gold consumer DDL must contain exactly one CREATE TABLE statement")
    return table_statements[0]


def _optional_catalog_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _as_int(value, name)


def _actual_catalog_tables(
    column_rows: list[tuple[object, ...]],
    primary_key_rows: list[tuple[object, ...]],
) -> dict[tuple[str, str], PostgresCatalogTable]:
    columns: dict[tuple[str, str], list[PostgresCatalogColumn]] = {}
    for row in column_rows:
        if len(row) != 10:
            raise ValueError("PostgreSQL catalog column row has unexpected width")
        key = (_as_text(row[0], "table_schema"), _as_text(row[1], "table_name"))
        expected_ordinal = len(columns.setdefault(key, [])) + 1
        if _as_int(row[2], "ordinal_position") != expected_ordinal:
            raise ValueError("PostgreSQL catalog column order is not contiguous")
        nullable_text = _as_text(row[5], "is_nullable")
        if nullable_text not in {"YES", "NO"}:
            raise ValueError("PostgreSQL catalog nullability is invalid")
        columns[key].append(
            PostgresCatalogColumn(
                name=_as_text(row[3], "column_name"),
                data_type=_as_text(row[4], "data_type"),
                nullable=nullable_text == "YES",
                character_maximum_length=_optional_catalog_int(row[6], "character_maximum_length"),
                numeric_precision=_optional_catalog_int(row[7], "numeric_precision"),
                numeric_scale=_optional_catalog_int(row[8], "numeric_scale"),
                datetime_precision=_optional_catalog_int(row[9], "datetime_precision"),
            )
        )

    primary_keys: dict[tuple[str, str], list[str]] = {}
    for row in primary_key_rows:
        if len(row) != 4:
            raise ValueError("PostgreSQL catalog primary-key row has unexpected width")
        key = (_as_text(row[0], "table_schema"), _as_text(row[1], "table_name"))
        expected_ordinal = len(primary_keys.setdefault(key, [])) + 1
        if _as_int(row[3], "primary_key_ordinal") != expected_ordinal:
            raise ValueError("PostgreSQL catalog primary-key order is not contiguous")
        primary_keys[key].append(_as_text(row[2], "primary_key_column"))

    return {
        key: PostgresCatalogTable(key[0], key[1], tuple(table_columns), tuple(primary_keys.get(key, ())))
        for key, table_columns in columns.items()
    }


def _verify_owned_catalog(cursor: CursorPort, expected_tables: tuple[PostgresCatalogTable, ...]) -> None:
    placeholders = ", ".join("(%s, %s)" for _ in expected_tables)
    params = tuple(value for table in expected_tables for value in (table.schema_name, table.table_name))
    cursor.execute(_CATALOG_COLUMNS.format(table_pairs=placeholders), params)
    column_rows = cursor.fetchall()
    cursor.execute(_CATALOG_PRIMARY_KEYS.format(table_pairs=placeholders), params)
    primary_key_rows = cursor.fetchall()
    try:
        actual = _actual_catalog_tables(column_rows, primary_key_rows)
    except TypeError, ValueError:
        raise PostgresSchemaMismatchError(
            "PostgreSQL Gold schema migration required: actual catalog is malformed"
        ) from None
    expected = {(table.schema_name, table.table_name): table for table in expected_tables}
    if actual != expected:
        raise PostgresSchemaMismatchError("PostgreSQL Gold schema migration required: actual catalog is incompatible")


class PostgresGoldSyncRepository:
    """Transactional repository adapter implementing one-lineage atomic reconciliation."""

    def __init__(
        self,
        settings: PostgresConnectionSettings,
        *,
        connection_factory: ConnectionFactory = _default_connection,
    ) -> None:
        self._settings = settings
        self._connection_factory = connection_factory

    def _open(self) -> ConnectionPort:
        connection: ConnectionPort | None = None
        stage = "connect"
        try:
            connection = self._connection_factory(self._settings)
            cursor = connection.cursor()
            try:
                stage = "set session timezone"
                cursor.execute(f"SET TIME ZONE '{POSTGRES_SESSION_TIMEZONE}'")
                stage = "set lock timeout"
                cursor.execute("SELECT set_config('lock_timeout', %s, false)", (f"{self._settings.lock_timeout_ms}ms",))
                stage = "set statement timeout"
                cursor.execute(
                    "SELECT set_config('statement_timeout', %s, false)",
                    (f"{self._settings.statement_timeout_ms}ms",),
                )
                stage = "set idle transaction timeout"
                cursor.execute(
                    "SELECT set_config('idle_in_transaction_session_timeout', %s, false)",
                    (f"{self._settings.idle_in_transaction_session_timeout_ms}ms",),
                )
            finally:
                cursor.close()
            return connection
        except Exception as exc:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
            raise PostgresGoldRepositoryError(
                "PostgreSQL connection initialization failed",
                safe_detail=f"{stage}: {_safe_database_error_code(exc, password=self._settings.password)}",
            ) from None

    @staticmethod
    def _read_state(cursor: CursorPort, lineage: GoldLineage) -> GoldSyncState | None:
        cursor.execute(_SELECT_STATE, _lineage_params(lineage))
        row = cursor.fetchone()
        return None if row is None else _state_from_row(row)

    @staticmethod
    def _iter_digests(connection: ConnectionPort, lineage: GoldLineage) -> Iterator[GoldRowDigest]:
        cursor = connection.cursor(name="gold_digest_state")
        try:
            cursor.execute(_SELECT_DIGESTS, _lineage_params(lineage))
            while rows := cursor.fetchmany(_DIGEST_FETCH_BATCH_SIZE):
                for row in rows:
                    if len(row) != 2:
                        raise ValueError("PostgreSQL Gold digest row has unexpected width")
                    timestamp = _as_datetime(row[0], "timestamp_m1")
                    if timestamp is None:
                        raise ValueError("PostgreSQL Gold digest timestamp cannot be null")
                    yield GoldRowDigest(
                        GoldRowKey(lineage.exchange, lineage.symbol, timestamp),
                        _as_text(row[1], "row_sha256"),
                    )
        finally:
            cursor.close()

    @classmethod
    def _read_digests(cls, connection: ConnectionPort, lineage: GoldLineage) -> tuple[GoldRowDigest, ...]:
        return tuple(cls._iter_digests(connection, lineage))

    @staticmethod
    def _iter_consumer_digests(
        connection: ConnectionPort,
        lineage: GoldLineage,
        consumer_table: PostgresCatalogTable,
    ) -> Iterator[GoldRowDigest]:
        column_names = tuple(column.name for column in consumer_table.columns)
        table = _qualified(consumer_table.schema_name, consumer_table.table_name)
        query = (
            f"SELECT {', '.join(quote_identifier(name) for name in column_names)} FROM {table} "
            f"WHERE {quote_identifier('exchange')} = %s AND {quote_identifier('symbol')} = %s "
            f"ORDER BY {quote_identifier('timestamp_m1')}"
        )
        cursor = connection.cursor(name="gold_consumer_integrity")
        try:
            cursor.execute(query, (lineage.exchange, lineage.symbol))
            while rows := cursor.fetchmany(_CONSUMER_FETCH_BATCH_SIZE):
                for row in rows:
                    if len(row) != len(column_names):
                        raise ValueError("PostgreSQL Gold consumer row has unexpected width")
                    values = dict(zip(column_names, row, strict=True))
                    exchange = _as_text(values["exchange"], "consumer exchange")
                    symbol = _as_text(values["symbol"], "consumer symbol")
                    timestamp = _as_datetime(values["timestamp_m1"], "consumer timestamp_m1")
                    if timestamp is None:
                        raise ValueError("PostgreSQL Gold consumer timestamp cannot be null")
                    yield GoldRowDigest(
                        GoldRowKey(exchange, symbol, timestamp),
                        canonical_row_hash(values),
                    )
        finally:
            cursor.close()

    def ensure_lineage(self, snapshot: GoldSourceSnapshot, ddl: str, schema_signature: str) -> None:
        """Validate actual target catalogs before any consumer-row mutation."""

        _safe_table_ddl(ddl)
        expected_tables = owned_catalog_tables(ddl)
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                _verify_owned_catalog(cursor, expected_tables)
                cursor.execute(_SELECT_STATE, _lineage_params(snapshot.lineage))
                row = cursor.fetchone()
                if row is not None:
                    existing = _state_from_row(row)
                    if existing.schema_signature != schema_signature:
                        raise PostgresSchemaMismatchError("PostgreSQL Gold schema migration required")
            finally:
                cursor.close()
            connection.commit()
        except PostgresSchemaMismatchError:
            connection.rollback()
            raise
        except Exception:
            connection.rollback()
            raise PostgresGoldRepositoryError("PostgreSQL Gold schema initialization failed") from None
        finally:
            connection.close()

    def read_state(self, lineage: GoldLineage) -> GoldSyncState | None:
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                return self._read_state(cursor, lineage)
            finally:
                cursor.close()
        except Exception:
            raise PostgresGoldRepositoryError("PostgreSQL Gold sync-state read failed") from None
        finally:
            connection.close()

    def read_digests(self, lineage: GoldLineage) -> tuple[GoldRowDigest, ...]:
        connection = self._open()
        try:
            return self._read_digests(connection, lineage)
        except Exception:
            raise PostgresGoldRepositoryError("PostgreSQL Gold digest read failed") from None
        finally:
            connection.close()

    def _summary_sql(self, lineage: GoldLineage) -> tuple[str, tuple[object, ...]]:
        table = _qualified(POSTGRES_CONSUMER_SCHEMA, consumer_table_name(lineage.dataset_id))
        query = (
            f"SELECT COUNT(*), MIN({quote_identifier('timestamp_m1')}), MAX({quote_identifier('timestamp_m1')}) "
            f"FROM {table} WHERE {quote_identifier('exchange')} = %s AND {quote_identifier('symbol')} = %s"
        )
        return query, (lineage.exchange, lineage.symbol)

    def summary(self, lineage: GoldLineage) -> GoldTargetSummary:
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                query, params = self._summary_sql(lineage)
                cursor.execute(query, params)
                row = cursor.fetchone()
            finally:
                cursor.close()
            if row is None:
                raise ValueError("PostgreSQL Gold summary query returned no row")
            return _summary_from_row(row)
        except Exception:
            raise PostgresGoldRepositoryError("PostgreSQL Gold summary read failed") from None
        finally:
            connection.close()

    @staticmethod
    def _verify_integrity(
        lineage: GoldLineage,
        state: GoldSyncState | None,
        consumer_digests: Iterable[GoldRowDigest],
        stored_digests: Iterable[GoldRowDigest],
        source_digests: Iterable[GoldRowDigest] | None = None,
    ) -> None:
        sentinel = object()
        stored = iter(stored_digests)
        source = None if source_digests is None else iter(source_digests)
        count = 0
        minimum: datetime | None = None
        maximum: datetime | None = None
        previous: datetime | None = None

        for consumer_digest in consumer_digests:
            key = consumer_digest.key
            if key.exchange != lineage.exchange or key.symbol != lineage.symbol:
                raise ValueError("PostgreSQL Gold consumer row identity is inconsistent")
            if previous is not None and key.timestamp_m1 <= previous:
                raise ValueError("duplicate logical key in PostgreSQL Gold consumer")
            expected_digest = next(stored, sentinel)
            if expected_digest is sentinel or consumer_digest != expected_digest:
                raise ValueError("PostgreSQL Gold consumer and digest table are inconsistent")
            if source is not None:
                expected_source = next(source, sentinel)
                if expected_source is sentinel or consumer_digest != expected_source:
                    raise ValueError("PostgreSQL Gold target does not exactly match canonical source")
            previous = key.timestamp_m1
            minimum = key.timestamp_m1 if minimum is None else minimum
            maximum = key.timestamp_m1
            count += 1

        if next(stored, sentinel) is not sentinel:
            raise ValueError("PostgreSQL Gold consumer and digest table are inconsistent")
        if source is not None and next(source, sentinel) is not sentinel:
            raise ValueError("PostgreSQL Gold target does not exactly match canonical source")
        if state is None:
            if count:
                raise ValueError("PostgreSQL Gold target rows exist without an authoritative checkpoint")
            return
        if state.lineage != lineage:
            raise ValueError("PostgreSQL Gold checkpoint lineage is inconsistent")
        if (state.row_count, state.min_timestamp, state.max_timestamp) != (count, minimum, maximum):
            raise ValueError("PostgreSQL Gold checkpoint is inconsistent with exact target keys")

    def _insert_row(self, cursor: CursorPort, lineage: GoldLineage, values: tuple[tuple[str, object], ...]) -> None:
        table = _qualified(POSTGRES_CONSUMER_SCHEMA, consumer_table_name(lineage.dataset_id))
        columns = [name for name, _ in values]
        query = (
            f"INSERT INTO {table} ({', '.join(quote_identifier(name) for name in columns)}) VALUES "
            f"({', '.join('%s' for _ in columns)})"
        )
        cursor.execute(query, tuple(value for _, value in values))

    def _update_row(self, cursor: CursorPort, lineage: GoldLineage, values: tuple[tuple[str, object], ...]) -> None:
        table = _qualified(POSTGRES_CONSUMER_SCHEMA, consumer_table_name(lineage.dataset_id))
        mapping = dict(values)
        mutable = [(name, value) for name, value in values if name not in {"exchange", "symbol", "timestamp_m1"}]
        if not mutable:
            raise ValueError("Gold update contains no mutable payload columns")
        query = (
            f"UPDATE {table} SET {', '.join(f'{quote_identifier(name)} = %s' for name, _ in mutable)} "
            f"WHERE {quote_identifier('exchange')} = %s AND {quote_identifier('symbol')} = %s "
            f"AND {quote_identifier('timestamp_m1')} = %s"
        )
        params = tuple(value for _, value in mutable) + (
            mapping["exchange"],
            mapping["symbol"],
            mapping["timestamp_m1"],
        )
        cursor.execute(query, params)
        if cursor.rowcount != 1:
            raise ValueError("PostgreSQL Gold UPDATE affected an unexpected number of rows")

    def _delete_row(self, cursor: CursorPort, lineage: GoldLineage, key: GoldRowKey) -> None:
        table = _qualified(POSTGRES_CONSUMER_SCHEMA, consumer_table_name(lineage.dataset_id))
        query = (
            f"DELETE FROM {table} WHERE {quote_identifier('exchange')} = %s "
            f"AND {quote_identifier('symbol')} = %s AND {quote_identifier('timestamp_m1')} = %s"
        )
        cursor.execute(query, (key.exchange, key.symbol, key.timestamp_m1))
        if cursor.rowcount != 1:
            raise ValueError("PostgreSQL Gold DELETE affected an unexpected number of rows")

    def _apply_plan(self, cursor: CursorPort, lineage: GoldLineage, plan: GoldDeltaPlan) -> None:
        source_digests = iter(plan.source_digests)
        previous_digest_key: GoldRowKey | None = None

        def next_digest() -> GoldRowDigest | None:
            nonlocal previous_digest_key
            digest = next(source_digests, None)
            if digest is not None:
                if previous_digest_key is not None and digest.key <= previous_digest_key:
                    raise ValueError("source digests are not uniquely ordered")
                previous_digest_key = digest.key
            return digest

        current_digest = next_digest()
        mutations = merge_sorted(
            ((payload, True) for payload in sorted(plan.inserts, key=lambda item: item.key)),
            ((payload, False) for payload in sorted(plan.updates, key=lambda item: item.key)),
            key=lambda item: item[0].key,
        )
        for payload, is_insert in mutations:
            while current_digest is not None and current_digest.key < payload.key:
                current_digest = next_digest()
            if current_digest is None or current_digest.key != payload.key:
                raise ValueError("Gold delta mutation is missing its source digest")
            if is_insert:
                self._insert_row(cursor, lineage, payload.values)
            else:
                self._update_row(cursor, lineage, payload.values)
            cursor.execute(
                _UPSERT_DIGEST,
                (
                    lineage.dataset_id,
                    lineage.exchange,
                    lineage.symbol,
                    payload.key.timestamp_m1,
                    current_digest.row_sha256,
                ),
            )
            current_digest = next_digest()
        for key in plan.deletes:
            self._delete_row(cursor, lineage, key)
        for key in plan.deletes:
            cursor.execute(
                _DELETE_DIGEST,
                (lineage.dataset_id, lineage.exchange, lineage.symbol, key.timestamp_m1),
            )
            if cursor.rowcount != 1:
                raise ValueError("PostgreSQL Gold digest DELETE affected an unexpected number of rows")

    def _verify_summary(
        self,
        cursor: CursorPort,
        lineage: GoldLineage,
        expected: GoldTargetSummary,
    ) -> None:
        summary_query, summary_params = self._summary_sql(lineage)
        cursor.execute(summary_query, summary_params)
        summary_row = cursor.fetchone()
        if summary_row is None:
            raise ValueError("PostgreSQL Gold verification query returned no row")
        actual = _summary_from_row(summary_row)
        if actual != expected:
            raise ValueError("PostgreSQL Gold post-write summary does not match source")

    @staticmethod
    def _stage_source_rows(
        cursor: CursorPort,
        rows: Iterable[Mapping[str, object]],
        column_names: tuple[str, ...],
        *,
        expected_count: int,
    ) -> None:
        stage_columns = (*column_names, "row_sha256")
        insert_query = (
            f"INSERT INTO {_SOURCE_STAGE_TABLE} "
            f"({', '.join(quote_identifier(name) for name in stage_columns)}) VALUES "
            f"({', '.join('%s' for _ in stage_columns)})"
        )
        batch: list[tuple[object, ...]] = []
        row_count = 0

        def flush() -> None:
            if batch:
                cursor.executemany(insert_query, batch)
                batch.clear()

        for row in rows:
            if tuple(row) != column_names:
                raise ValueError("Gold source row columns differ from the registered PostgreSQL schema")
            row_hash = canonical_row_hash(row)
            values = tuple(Jsonb(value) if isinstance(value, (dict, list)) else value for value in row.values())
            batch.append((*values, row_hash))
            row_count += 1
            if len(batch) >= _SOURCE_STAGE_BATCH_SIZE:
                flush()
        flush()
        if row_count != expected_count:
            raise ValueError("Gold source row count differs from the current manifest snapshot")

    @staticmethod
    def _stage_difference_counts(cursor: CursorPort, lineage: GoldLineage) -> tuple[int, int, int]:
        digest_table = _qualified(POSTGRES_SYNC_SCHEMA, POSTGRES_ROW_HASH_TABLE)
        params = _lineage_params(lineage)
        cursor.execute(
            f"""SELECT
                COUNT(*) FILTER (WHERE target.timestamp_m1 IS NULL),
                COUNT(*) FILTER (
                    WHERE target.timestamp_m1 IS NOT NULL
                      AND target.row_sha256 IS DISTINCT FROM source.row_sha256
                )
            FROM {_SOURCE_STAGE_TABLE} AS source
            LEFT JOIN {digest_table} AS target
              ON target.dataset_id = %s AND target.exchange = %s AND target.symbol = %s
             AND target.timestamp_m1 = source.timestamp_m1""",
            params,
        )
        row = cursor.fetchone()
        if row is None or len(row) != 2:
            raise ValueError("PostgreSQL Gold staged-delta query returned an invalid row")
        inserted, updated = _as_int(row[0], "staged insert count"), _as_int(row[1], "staged update count")
        cursor.execute(
            f"""SELECT COUNT(*) FROM {digest_table} AS target
            WHERE target.dataset_id = %s AND target.exchange = %s AND target.symbol = %s
              AND NOT EXISTS (
                  SELECT 1 FROM {_SOURCE_STAGE_TABLE} AS source
                  WHERE source.timestamp_m1 = target.timestamp_m1
              )""",
            params,
        )
        row = cursor.fetchone()
        if row is None or len(row) != 1:
            raise ValueError("PostgreSQL Gold staged-delete query returned an invalid row")
        deleted = _as_int(row[0], "staged delete count")
        return inserted, updated, deleted

    def _verify_target_keys_and_checkpoint(
        self,
        cursor: CursorPort,
        lineage: GoldLineage,
        state: GoldSyncState | None,
    ) -> None:
        target_table = _qualified(POSTGRES_CONSUMER_SCHEMA, consumer_table_name(lineage.dataset_id))
        digest_table = _qualified(POSTGRES_SYNC_SCHEMA, POSTGRES_ROW_HASH_TABLE)
        expected = (
            GoldTargetSummary(0, None, None)
            if state is None
            else GoldTargetSummary(state.row_count, state.min_timestamp, state.max_timestamp)
        )
        self._verify_summary(cursor, lineage, expected)
        cursor.execute(
            f"""SELECT
                (SELECT COUNT(*) FROM {target_table} AS consumer
                 LEFT JOIN {digest_table} AS digest
                   ON digest.dataset_id = %s AND digest.exchange = consumer.exchange
                  AND digest.symbol = consumer.symbol AND digest.timestamp_m1 = consumer.timestamp_m1
                 WHERE consumer.exchange = %s AND consumer.symbol = %s
                   AND digest.timestamp_m1 IS NULL)
                +
                (SELECT COUNT(*) FROM {digest_table} AS digest
                 LEFT JOIN {target_table} AS consumer
                   ON consumer.exchange = digest.exchange AND consumer.symbol = digest.symbol
                  AND consumer.timestamp_m1 = digest.timestamp_m1
                 WHERE digest.dataset_id = %s AND digest.exchange = %s AND digest.symbol = %s
                   AND consumer.timestamp_m1 IS NULL)""",
            (*_lineage_params(lineage), *_lineage_params(lineage)),
        )
        row = cursor.fetchone()
        if row is None or len(row) != 1 or _as_int(row[0], "target key mismatch count") != 0:
            raise ValueError("PostgreSQL Gold consumer and digest keys are inconsistent")

    @staticmethod
    def _verify_unchanged_stage_rows(
        cursor: CursorPort,
        lineage: GoldLineage,
        consumer_table: PostgresCatalogTable,
    ) -> None:
        target_table = _qualified(consumer_table.schema_name, consumer_table.table_name)
        digest_table = _qualified(POSTGRES_SYNC_SCHEMA, POSTGRES_ROW_HASH_TABLE)
        differences = " OR ".join(
            f"consumer.{quote_identifier(column.name)} IS DISTINCT FROM source.{quote_identifier(column.name)}"
            for column in consumer_table.columns
        )
        cursor.execute(
            f"""SELECT COUNT(*) FROM {_SOURCE_STAGE_TABLE} AS source
            JOIN {digest_table} AS digest
              ON digest.dataset_id = %s AND digest.exchange = %s AND digest.symbol = %s
             AND digest.timestamp_m1 = source.timestamp_m1
            JOIN {target_table} AS consumer
              ON consumer.exchange = source.exchange AND consumer.symbol = source.symbol
             AND consumer.timestamp_m1 = source.timestamp_m1
            WHERE digest.row_sha256 = source.row_sha256 AND ({differences})""",
            _lineage_params(lineage),
        )
        row = cursor.fetchone()
        if row is None or len(row) != 1 or _as_int(row[0], "unchanged consumer mismatch count") != 0:
            raise ValueError("PostgreSQL Gold unchanged consumer rows differ from their canonical source")

    @staticmethod
    def _verify_staged_consumer(
        cursor: CursorPort,
        lineage: GoldLineage,
        consumer_table: PostgresCatalogTable,
    ) -> None:
        target_table = _qualified(consumer_table.schema_name, consumer_table.table_name)
        differences = " OR ".join(
            f"consumer.{quote_identifier(column.name)} IS DISTINCT FROM source.{quote_identifier(column.name)}"
            for column in consumer_table.columns
        )
        cursor.execute(
            f"""SELECT
                (SELECT COUNT(*) FROM {_SOURCE_STAGE_TABLE} AS source
                 LEFT JOIN {target_table} AS consumer
                   ON consumer.exchange = source.exchange AND consumer.symbol = source.symbol
                  AND consumer.timestamp_m1 = source.timestamp_m1
                 WHERE consumer.timestamp_m1 IS NULL OR ({differences}))
                +
                (SELECT COUNT(*) FROM {target_table} AS consumer
                 LEFT JOIN {_SOURCE_STAGE_TABLE} AS source
                   ON source.exchange = consumer.exchange AND source.symbol = consumer.symbol
                  AND source.timestamp_m1 = consumer.timestamp_m1
                 WHERE consumer.exchange = %s AND consumer.symbol = %s
                   AND source.timestamp_m1 IS NULL)""",
            (lineage.exchange, lineage.symbol),
        )
        row = cursor.fetchone()
        if row is None or len(row) != 1 or _as_int(row[0], "staged consumer mismatch count") != 0:
            raise ValueError("PostgreSQL Gold consumer table does not exactly match the staged source")

    @staticmethod
    def _apply_staged_delta(
        cursor: CursorPort,
        lineage: GoldLineage,
        consumer_table: PostgresCatalogTable,
        counts: tuple[int, int, int],
    ) -> None:
        inserted, updated, deleted = counts
        target_table = _qualified(consumer_table.schema_name, consumer_table.table_name)
        digest_table = _qualified(POSTGRES_SYNC_SCHEMA, POSTGRES_ROW_HASH_TABLE)
        column_names = tuple(column.name for column in consumer_table.columns)
        key_columns = ("exchange", "symbol", "timestamp_m1")
        mutable_columns = tuple(name for name in column_names if name not in key_columns)
        target_projection = ", ".join(quote_identifier(name) for name in column_names)

        cursor.execute(
            f"""INSERT INTO {target_table} ({target_projection})
            SELECT {target_projection} FROM {_SOURCE_STAGE_TABLE} AS source
            WHERE NOT EXISTS (
                SELECT 1 FROM {target_table} AS target
                WHERE target.exchange = source.exchange AND target.symbol = source.symbol
                  AND target.timestamp_m1 = source.timestamp_m1
            )"""
        )
        if cursor.rowcount != inserted:
            raise ValueError("PostgreSQL Gold staged insert count changed during reconciliation")

        if updated:
            if not mutable_columns:
                raise ValueError("Gold update contains no mutable payload columns")
            assignments = ", ".join(
                f"{quote_identifier(name)} = source.{quote_identifier(name)}" for name in mutable_columns
            )
            cursor.execute(
                f"""UPDATE {target_table} AS target SET {assignments}
                FROM {_SOURCE_STAGE_TABLE} AS source
                JOIN {digest_table} AS digest
                  ON digest.dataset_id = %s AND digest.exchange = %s AND digest.symbol = %s
                 AND digest.timestamp_m1 = source.timestamp_m1
                WHERE target.exchange = source.exchange AND target.symbol = source.symbol
                  AND target.timestamp_m1 = source.timestamp_m1
                  AND digest.row_sha256 IS DISTINCT FROM source.row_sha256""",
                _lineage_params(lineage),
            )
            if cursor.rowcount != updated:
                raise ValueError("PostgreSQL Gold staged update count changed during reconciliation")

        cursor.execute(
            f"""DELETE FROM {target_table} AS target
            WHERE target.exchange = %s AND target.symbol = %s
              AND NOT EXISTS (
                  SELECT 1 FROM {_SOURCE_STAGE_TABLE} AS source
                  WHERE source.timestamp_m1 = target.timestamp_m1
              )""",
            (lineage.exchange, lineage.symbol),
        )
        if cursor.rowcount != deleted:
            raise ValueError("PostgreSQL Gold staged consumer-delete count changed during reconciliation")

        cursor.execute(
            f"""DELETE FROM {digest_table} AS target
            WHERE target.dataset_id = %s AND target.exchange = %s AND target.symbol = %s
              AND NOT EXISTS (
                  SELECT 1 FROM {_SOURCE_STAGE_TABLE} AS source
                  WHERE source.timestamp_m1 = target.timestamp_m1
              )""",
            _lineage_params(lineage),
        )
        if cursor.rowcount != deleted:
            raise ValueError("PostgreSQL Gold staged digest-delete count changed during reconciliation")

        cursor.execute(
            f"""INSERT INTO {digest_table} (dataset_id, exchange, symbol, timestamp_m1, row_sha256)
            SELECT %s, %s, %s, source.timestamp_m1, source.row_sha256
            FROM {_SOURCE_STAGE_TABLE} AS source
            ON CONFLICT (dataset_id, exchange, symbol, timestamp_m1) DO UPDATE
            SET row_sha256 = EXCLUDED.row_sha256
            WHERE {digest_table}.row_sha256 IS DISTINCT FROM EXCLUDED.row_sha256""",
            _lineage_params(lineage),
        )
        if cursor.rowcount != inserted + updated:
            raise ValueError("PostgreSQL Gold staged digest upsert count changed during reconciliation")

    @staticmethod
    def _verify_stage_digests(cursor: CursorPort, lineage: GoldLineage, expected_count: int) -> None:
        digest_table = _qualified(POSTGRES_SYNC_SCHEMA, POSTGRES_ROW_HASH_TABLE)
        cursor.execute(
            f"""SELECT
                (SELECT COUNT(*) FROM {_SOURCE_STAGE_TABLE} AS source
                 LEFT JOIN {digest_table} AS target
                   ON target.dataset_id = %s AND target.exchange = %s AND target.symbol = %s
                  AND target.timestamp_m1 = source.timestamp_m1
                 WHERE target.timestamp_m1 IS NULL
                    OR target.row_sha256 IS DISTINCT FROM source.row_sha256)
                +
                (SELECT COUNT(*) FROM {digest_table} AS target
                 LEFT JOIN {_SOURCE_STAGE_TABLE} AS source
                   ON source.timestamp_m1 = target.timestamp_m1
                 WHERE target.dataset_id = %s AND target.exchange = %s AND target.symbol = %s
                   AND source.timestamp_m1 IS NULL)""",
            (*_lineage_params(lineage), *_lineage_params(lineage)),
        )
        row = cursor.fetchone()
        if row is None or len(row) != 1 or _as_int(row[0], "staged digest mismatch count") != 0:
            raise ValueError("PostgreSQL Gold digest table does not exactly match the staged source")
        cursor.execute(f"SELECT COUNT(*) FROM {_SOURCE_STAGE_TABLE}")
        row = cursor.fetchone()
        if row is None or len(row) != 1 or _as_int(row[0], "staged source count") != expected_count:
            raise ValueError("PostgreSQL Gold staged source count differs from the manifest snapshot")

    def reconcile_lineage_stream(
        self,
        snapshot: GoldSourceSnapshot,
        ddl: str,
        schema_signature: str,
        source_rows: Iterable[Mapping[str, object]],
        next_state: GoldSyncState,
    ) -> GoldSyncLineageResult:
        """Stage a lineage in bounded batches, then reconcile it with set-based SQL atomically."""

        table_ddl = _safe_table_ddl(ddl)
        expected_tables = owned_catalog_tables(table_ddl)
        lineage = snapshot.lineage
        if next_state.lineage != lineage or next_state.schema_signature != schema_signature:
            raise ValueError("Gold reconcile state does not match the requested lineage/schema")
        consumer_name = consumer_table_name(lineage.dataset_id)
        consumer_table = next(
            (
                table
                for table in expected_tables
                if table.schema_name == POSTGRES_CONSUMER_SCHEMA and table.table_name == consumer_name
            ),
            None,
        )
        if consumer_table is None:
            raise ValueError("PostgreSQL Gold consumer table is absent from the canonical catalog")

        connection = self._open()
        stage = "advisory lock"
        try:
            cursor = connection.cursor()
            try:
                lock_key = f"{lineage.dataset_id}|{lineage.exchange}|{lineage.symbol}"
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_key,))
                stage = "catalog validation"
                _verify_owned_catalog(cursor, expected_tables)
                stage = "checkpoint read"
                state = self._read_state(cursor, lineage)
                if state is not None and state.schema_signature != schema_signature:
                    raise PostgresSchemaMismatchError("PostgreSQL Gold schema migration required")

                stage = "target key/checkpoint validation"
                self._verify_target_keys_and_checkpoint(cursor, lineage, state)

                stage = "temporary source staging"
                target_table = _qualified(consumer_table.schema_name, consumer_table.table_name)
                cursor.execute(
                    f"CREATE TEMP TABLE {_SOURCE_STAGE_TABLE} (LIKE {target_table} INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                cursor.execute(f"ALTER TABLE {_SOURCE_STAGE_TABLE} ADD COLUMN row_sha256 CHAR(64) NOT NULL")
                cursor.execute(f"CREATE UNIQUE INDEX ON {_SOURCE_STAGE_TABLE} (exchange, symbol, timestamp_m1)")
                column_names = tuple(column.name for column in consumer_table.columns)
                self._stage_source_rows(cursor, source_rows, column_names, expected_count=snapshot.row_count)

                stage = "delta planning"
                counts = self._stage_difference_counts(cursor, lineage)
                inserted, updated, deleted = counts
                if state is not None and state_matches_snapshot(state, snapshot):
                    if counts != (0, 0, 0):
                        raise ValueError("unchanged Gold checkpoint does not match canonical source row digests")
                    self._verify_unchanged_stage_rows(cursor, lineage, consumer_table)
                    self._verify_summary(cursor, lineage, expected_target_summary(snapshot))
                    connection.commit()
                    return GoldSyncLineageResult(
                        lineage,
                        snapshot.source_fingerprint,
                        0,
                        0,
                        0,
                        snapshot.row_count,
                        "unchanged",
                    )

                stage = "unchanged-row integrity check"
                self._verify_unchanged_stage_rows(cursor, lineage, consumer_table)

                stage = "set-based delta writes"
                self._apply_staged_delta(cursor, lineage, consumer_table, counts)
                stage = "checkpoint write"
                cursor.execute(_UPSERT_STATE, _state_params(next_state))
                stage = "post-write verification"
                self._verify_stage_digests(cursor, lineage, snapshot.row_count)
                self._verify_staged_consumer(cursor, lineage, consumer_table)
                verified_state = self._read_state(cursor, lineage)
                if verified_state != next_state:
                    raise ValueError("PostgreSQL Gold saved checkpoint does not match the reconcile decision")
                self._verify_summary(cursor, lineage, expected_target_summary(snapshot))
            finally:
                cursor.close()
            connection.commit()
            return GoldSyncLineageResult(
                lineage,
                snapshot.source_fingerprint,
                inserted,
                updated,
                deleted,
                snapshot.row_count - inserted - updated,
                "synchronized",
            )
        except PostgresSchemaMismatchError, ValueError, TypeError:
            connection.rollback()
            raise
        except Exception as exc:
            connection.rollback()
            raise PostgresGoldRepositoryError(
                "PostgreSQL Gold reconcile transaction failed",
                safe_detail=f"{stage}: {_safe_database_error_code(exc, password=self._settings.password)}",
            ) from None
        finally:
            connection.close()

    def reconcile_lineage(
        self,
        snapshot: GoldSourceSnapshot,
        ddl: str,
        schema_signature: str,
        planner: GoldReconcilePlanner,
    ) -> GoldReconcileDecision:
        """Reconcile one lineage under one lock and transaction from first target read through commit."""

        table_ddl = _safe_table_ddl(ddl)
        expected_tables = owned_catalog_tables(table_ddl)
        lineage = snapshot.lineage
        connection = self._open()
        stage = "advisory lock"
        try:
            cursor = connection.cursor()
            try:
                lock_key = f"{lineage.dataset_id}|{lineage.exchange}|{lineage.symbol}"
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_key,))
                stage = "catalog validation"
                _verify_owned_catalog(cursor, expected_tables)

                stage = "checkpoint read"
                state = self._read_state(cursor, lineage)
                if state is not None and state.schema_signature != schema_signature:
                    raise PostgresSchemaMismatchError("PostgreSQL Gold schema migration required")
                stage = "digest read"
                digests = self._read_digests(connection, lineage)
                consumer_name = consumer_table_name(lineage.dataset_id)
                consumer_table = next(
                    (
                        table
                        for table in expected_tables
                        if table.schema_name == POSTGRES_CONSUMER_SCHEMA and table.table_name == consumer_name
                    ),
                    None,
                )
                if consumer_table is None:
                    raise ValueError("PostgreSQL Gold consumer table is absent from the canonical catalog")
                stage = "consumer integrity read"
                consumer_digests = self._iter_consumer_digests(connection, lineage, consumer_table)
                self._verify_integrity(lineage, state, consumer_digests, digests)
                stage = "reconcile planning"
                decision = planner(state, digests)

                if decision.plan is not None:
                    if decision.next_state is None or decision.next_state.lineage != lineage:
                        raise ValueError("Gold reconcile state lineage does not match requested lineage")
                    stage = "delta writes"
                    self._apply_plan(cursor, lineage, decision.plan)
                if decision.next_state is not None:
                    stage = "checkpoint write"
                    cursor.execute(_UPSERT_STATE, _state_params(decision.next_state))
                stage = "post-write verification"
                verified_state = self._read_state(cursor, lineage)
                verified_digests = self._read_digests(connection, lineage)
                verified_consumer = self._iter_consumer_digests(connection, lineage, consumer_table)
                expected_source = decision.plan.source_digests if decision.plan is not None else digests
                expected_state = decision.next_state if decision.next_state is not None else state
                if verified_state != expected_state:
                    raise ValueError("PostgreSQL Gold saved checkpoint does not match the reconcile decision")
                self._verify_integrity(lineage, verified_state, verified_consumer, verified_digests, expected_source)
            finally:
                cursor.close()
            connection.commit()
            return decision
        except PostgresSchemaMismatchError, ValueError, TypeError:
            connection.rollback()
            raise
        except Exception as exc:
            connection.rollback()
            raise PostgresGoldRepositoryError(
                "PostgreSQL Gold reconcile transaction failed",
                safe_detail=f"{stage}: {_safe_database_error_code(exc, password=self._settings.password)}",
            ) from None
        finally:
            connection.close()

    def apply_delta(self, lineage: GoldLineage, plan: GoldDeltaPlan, state: GoldSyncState) -> None:
        """Apply exactly one supplied lineage delta atomically under an advisory lock."""

        if state.lineage != lineage:
            raise ValueError("Gold sync state lineage does not match requested lineage")
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                lock_key = f"{lineage.dataset_id}|{lineage.exchange}|{lineage.symbol}"
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_key,))

                expected = GoldTargetSummary(state.row_count, state.min_timestamp, state.max_timestamp)
                self._apply_plan(cursor, lineage, plan)
                self._verify_summary(cursor, lineage, expected)
                cursor.execute(_UPSERT_STATE, _state_params(state))
            finally:
                cursor.close()
            connection.commit()
        except Exception:
            connection.rollback()
            raise PostgresGoldRepositoryError("PostgreSQL Gold delta transaction failed") from None
        finally:
            connection.close()
