from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl
import psycopg
import pytest

import infra.postgres.gold_repository as gold_repository
from application.postgres_sync import (
    GoldDeltaPlan,
    GoldLineage,
    GoldRowDigest,
    GoldRowKey,
    GoldRowPayload,
    GoldSourceSnapshot,
    GoldSyncState,
    GoldTargetSummary,
)
from application.postgres_sync.contracts import GoldReconcileDecision
from application.postgres_sync.delta import canonical_row_hash
from application.postgres_sync.schema import PostgresCatalogTable, bootstrap_migration, build_postgres_table_schema
from infra.postgres.gold_repository import (
    PostgresConnectionSettings,
    PostgresGoldRepositoryError,
    PostgresGoldSyncRepository,
    PostgresSchemaMismatchError,
    _as_datetime,
    _safe_database_error_code,
)


class FakeCursor:
    def __init__(
        self,
        trace: list[tuple[str, object]],
        fetchone_queue: list[object],
        fail_token: str | None = None,
        rowcount: int = 1,
    ) -> None:
        self.trace = trace
        self.fetchone_queue = fetchone_queue
        self.fail_token = fail_token
        self.rowcount = rowcount
        self.fetchmany_rows: list[tuple[object, ...]] | None = None

    def execute(self, query: str, params: object = None) -> object:
        self.trace.append((query, params))
        if isinstance(params, tuple) and query.count("%s") != len(params):
            raise AssertionError(f"SQL placeholder count mismatch: {query.count('%s')} != {len(params)}")
        if self.fail_token is not None and self.fail_token in query:
            raise RuntimeError("injected SQL failure")
        return None

    def executemany(self, query: str, params_seq: object) -> object:
        params = list(params_seq)  # type: ignore[arg-type]
        self.trace.append((f"EXECUTEMANY {query}", params))
        if any(len(row) != query.count("%s") for row in params):
            raise AssertionError("SQL placeholder count does not match a staged row")
        if self.fail_token is not None and self.fail_token in query:
            raise RuntimeError("injected SQL failure")
        return None

    def fetchone(self) -> tuple[object, ...] | None:
        if not self.fetchone_queue:
            return None
        value = self.fetchone_queue.pop(0)
        return value if value is None else tuple(value)  # type: ignore[arg-type]

    def fetchall(self) -> list[tuple[object, ...]]:
        if not self.fetchone_queue:
            return []
        value = self.fetchone_queue.pop(0)
        return [tuple(row) for row in value]  # type: ignore[union-attr]

    def fetchmany(self, size: int = 1) -> list[tuple[object, ...]]:
        self.trace.append(("FETCHMANY", size))
        if self.fetchmany_rows is None:
            if not self.fetchone_queue:
                return []
            value = self.fetchone_queue.pop(0)
            self.fetchmany_rows = [tuple(row) for row in value]  # type: ignore[union-attr]
        rows = self.fetchmany_rows[:size]
        self.fetchmany_rows = self.fetchmany_rows[size:]
        return rows

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(
        self,
        fetchone_queue: list[object] | None = None,
        fail_token: str | None = None,
        rowcount: int = 1,
    ) -> None:
        self.trace: list[tuple[str, object]] = []
        self.fetchone_queue = [] if fetchone_queue is None else list(fetchone_queue)
        self.fail_token = fail_token
        self.rowcount = rowcount

    def cursor(self, name: str | None = None) -> FakeCursor:
        self.trace.append(("CURSOR", name))
        return FakeCursor(self.trace, self.fetchone_queue, self.fail_token, self.rowcount)

    def commit(self) -> None:
        self.trace.append(("COMMIT", None))

    def rollback(self) -> None:
        self.trace.append(("ROLLBACK", None))

    def close(self) -> None:
        self.trace.append(("CLOSE", None))


def _settings() -> PostgresConnectionSettings:
    return PostgresConnectionSettings("10.10.1.3", 54321, "crypto-loader", "market_data", "fake-secret")


def _schema() -> Any:
    return build_postgres_table_schema(
        "gold.history.full.m1",
        {
            "exchange": pl.String,
            "symbol": pl.String,
            "timestamp_m1": pl.Datetime("us", "UTC"),
            "value": pl.Float64,
        },
    )


def _snapshot(signature: str) -> GoldSourceSnapshot:
    timestamp = datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    return GoldSourceSnapshot(
        lineage=GoldLineage("gold.history.full.m1", "deribit", "BTC"),
        artifact_path=Path("gold.parquet"),
        source_fingerprint="fingerprint",
        schema_signature=signature,
        row_count=1,
        min_timestamp=timestamp,
        max_timestamp=timestamp,
        source_version="v1.0.0",
    )


def _catalog_rows(
    tables: tuple[PostgresCatalogTable, ...],
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]]]:
    column_rows: list[tuple[object, ...]] = []
    primary_key_rows: list[tuple[object, ...]] = []
    for table in sorted(tables, key=lambda item: (item.schema_name, item.table_name)):
        for ordinal, column in enumerate(table.columns, start=1):
            column_rows.append(
                (
                    table.schema_name,
                    table.table_name,
                    ordinal,
                    column.name,
                    column.data_type,
                    "YES" if column.nullable else "NO",
                    column.character_maximum_length,
                    column.numeric_precision,
                    column.numeric_scale,
                    column.datetime_precision,
                )
            )
        for ordinal, column_name in enumerate(table.primary_key, start=1):
            primary_key_rows.append((table.schema_name, table.table_name, column_name, ordinal))
    return column_rows, primary_key_rows


def test_settings_exact_endpoint_and_secret_redaction() -> None:
    settings = _settings()
    assert settings.host == "10.10.1.3"
    assert settings.port == 54321
    assert settings.user == "crypto-loader"
    assert "fake-secret" not in repr(settings)
    with pytest.raises(ValueError):
        PostgresConnectionSettings("localhost", 54321, "crypto-loader", "db", "secret")
    with pytest.raises(ValueError, match="timeout"):
        PostgresConnectionSettings("10.10.1.3", 54321, "crypto-loader", "db", "secret", lock_timeout_ms=0)


def test_safe_database_error_code_includes_sqlstate_without_database_message() -> None:
    error = RuntimeError("sensitive query detail")
    error.sqlstate = "57014"  # type: ignore[attr-defined]

    detail = _safe_database_error_code(error)

    assert detail == "RuntimeError (SQLSTATE 57014)"
    assert "sensitive query detail" not in detail


def test_safe_operational_error_redacts_password() -> None:
    error = psycopg.OperationalError("password=secret-value connection reset by peer")

    detail = _safe_database_error_code(error, password="secret-value")

    assert "connection reset by peer" in detail
    assert "secret-value" not in detail


def test_open_failure_reports_stage_and_closes_connection() -> None:
    connection = FakeConnection(fail_token="statement_timeout")
    repository = PostgresGoldSyncRepository(_settings(), connection_factory=lambda _: connection)

    with pytest.raises(PostgresGoldRepositoryError) as exc_info:
        repository._open()

    assert exc_info.value.safe_detail == "set statement timeout: RuntimeError"
    assert ("CLOSE", None) in connection.trace


def test_consumer_digests_are_read_in_bounded_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    schema = _schema()
    consumer_table = next(table for table in bootstrap_migration(schema).tables if table.schema_name == "crypto_loader")
    lineage = GoldLineage("gold.history.full.m1", "deribit", "BTC")
    first = datetime(2026, 1, 1, tzinfo=UTC)
    second = datetime(2026, 1, 2, tzinfo=UTC)
    rows = [
        ("deribit", "BTC", first, 1.0),
        ("deribit", "BTC", second, 2.0),
    ]
    connection = FakeConnection(fetchone_queue=[rows])
    monkeypatch.setattr(gold_repository, "_CONSUMER_FETCH_BATCH_SIZE", 1)

    digests = tuple(PostgresGoldSyncRepository._iter_consumer_digests(connection, lineage, consumer_table))

    assert tuple(digest.key.timestamp_m1 for digest in digests) == (first, second)
    assert [item for item in connection.trace if item[0] == "FETCHMANY"] == [
        ("FETCHMANY", 1),
        ("FETCHMANY", 1),
        ("FETCHMANY", 1),
    ]


@pytest.mark.parametrize(
    "value",
    [
        datetime(2026, 1, 1),
        datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1))),
        datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=2))),
        datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=-5))),
    ],
)
def test_database_datetime_reader_rejects_naive_and_non_utc_offsets(value: datetime) -> None:
    with pytest.raises(ValueError, match="UTC-aware"):
        _as_datetime(value, "timestamp_m1")


def test_database_datetime_reader_accepts_utc() -> None:
    timestamp = datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    assert _as_datetime(timestamp, "timestamp_m1") == timestamp


def test_ensure_lineage_uses_utc_and_verifies_catalog_without_ddl() -> None:
    schema = _schema()
    column_rows, primary_key_rows = _catalog_rows(bootstrap_migration(schema).tables)
    connection = FakeConnection(fetchone_queue=[column_rows, primary_key_rows, None])
    repository = PostgresGoldSyncRepository(_settings(), connection_factory=lambda _: connection)
    repository.ensure_lineage(_snapshot(schema.signature), schema.ddl, schema.signature)
    sql_trace = "\n".join(query for query, _ in connection.trace)
    assert "SET TIME ZONE 'UTC'" in sql_trace
    assert "set_config('lock_timeout'" in sql_trace
    assert "set_config('statement_timeout'" in sql_trace
    assert "set_config('idle_in_transaction_session_timeout'" in sql_trace
    assert "information_schema.columns" in sql_trace
    assert "pg_catalog.pg_index" in sql_trace
    assert "CREATE " not in sql_trace
    assert "DROP " not in sql_trace.upper()
    assert "TRUNCATE " not in sql_trace.upper()
    assert ("COMMIT", None) in connection.trace


def test_existing_schema_signature_mismatch_fails_before_rows() -> None:
    schema = _schema()
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    existing = (
        "gold.history.full.m1",
        "deribit",
        "BTC",
        "old-fingerprint",
        "b" * 64,
        1,
        timestamp,
        timestamp,
        timestamp,
        "v1.0.0",
        None,
    )
    column_rows, primary_key_rows = _catalog_rows(bootstrap_migration(schema).tables)
    connection = FakeConnection(fetchone_queue=[column_rows, primary_key_rows, existing])
    repository = PostgresGoldSyncRepository(_settings(), connection_factory=lambda _: connection)
    with pytest.raises(PostgresSchemaMismatchError):
        repository.ensure_lineage(_snapshot(schema.signature), schema.ddl, schema.signature)
    assert ("ROLLBACK", None) in connection.trace
    assert not any(query.startswith('INSERT INTO "crypto_loader"') for query, _ in connection.trace)


def test_apply_delta_order_and_microsecond_preservation() -> None:
    timestamp = datetime(2026, 1, 1, 0, 0, 0, 654321, tzinfo=UTC)
    lineage = GoldLineage("gold.history.full.m1", "deribit", "BTC")
    key = GoldRowKey("deribit", "BTC", timestamp)
    payload = GoldRowPayload(
        key,
        (
            ("exchange", "deribit"),
            ("symbol", "BTC"),
            ("timestamp_m1", timestamp),
            ("value", 1.5),
        ),
    )
    digest = GoldRowDigest(key, "a" * 64)
    plan = GoldDeltaPlan((payload,), (), (), (), (digest,))
    state = GoldSyncState(
        lineage=lineage,
        source_fingerprint="fingerprint",
        schema_signature="c" * 64,
        row_count=1,
        min_timestamp=timestamp,
        max_timestamp=timestamp,
        synced_at_utc=timestamp,
        source_version="v1.0.0",
    )
    connection = FakeConnection(fetchone_queue=[(1, timestamp, timestamp)])
    repository = PostgresGoldSyncRepository(_settings(), connection_factory=lambda _: connection)
    repository.apply_delta(lineage, plan, state)

    queries = [query for query, _ in connection.trace]
    lock_idx = next(index for index, query in enumerate(queries) if "pg_advisory_xact_lock" in query)
    insert_idx = next(index for index, query in enumerate(queries) if query.startswith('INSERT INTO "crypto_loader"'))
    digest_idx = next(
        index for index, query in enumerate(queries) if "gold_row_hashes" in query and query.startswith("INSERT")
    )
    state_idx = next(
        index for index, query in enumerate(queries) if "gold_sync_state" in query and query.startswith("INSERT")
    )
    summary_idx = next(index for index, query in enumerate(queries) if query.startswith("SELECT COUNT"))
    commit_idx = queries.index("COMMIT")
    assert lock_idx < insert_idx < digest_idx < summary_idx < state_idx < commit_idx
    assert timestamp.microsecond == 654321
    assert any(timestamp in params for _, params in connection.trace if isinstance(params, tuple))


def test_apply_delta_rolls_back_on_sql_failure() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    lineage = GoldLineage("gold.history.full.m1", "deribit", "BTC")
    key = GoldRowKey("deribit", "BTC", timestamp)
    payload = GoldRowPayload(
        key,
        (("exchange", "deribit"), ("symbol", "BTC"), ("timestamp_m1", timestamp), ("value", 1.0)),
    )
    plan = GoldDeltaPlan((payload,), (), (), (), (GoldRowDigest(key, "a" * 64),))
    state = GoldSyncState(lineage, "fingerprint", "c" * 64, 1, timestamp, timestamp, timestamp)
    connection = FakeConnection(fail_token='INSERT INTO "crypto_loader"')
    repository = PostgresGoldSyncRepository(_settings(), connection_factory=lambda _: connection)
    with pytest.raises(Exception, match="transaction failed"):
        repository.apply_delta(lineage, plan, state)
    assert ("ROLLBACK", None) in connection.trace
    assert ("COMMIT", None) not in connection.trace


def test_reconcile_lineage_locks_before_reads_and_commits_after_checkpoint() -> None:
    schema = _schema()
    column_rows, primary_key_rows = _catalog_rows(bootstrap_migration(schema).tables)
    snapshot = _snapshot(schema.signature)
    timestamp = snapshot.min_timestamp
    assert timestamp is not None
    key = GoldRowKey("deribit", "BTC", timestamp)
    payload = GoldRowPayload(
        key,
        (("exchange", "deribit"), ("symbol", "BTC"), ("timestamp_m1", timestamp), ("value", 1.0)),
    )
    row = dict(payload.values)
    row_digest = canonical_row_hash(row)
    plan = GoldDeltaPlan((payload,), (), (), (), (GoldRowDigest(key, row_digest),))
    state = GoldSyncState(snapshot.lineage, "fingerprint", schema.signature, 1, timestamp, timestamp, timestamp)
    state_row = (
        snapshot.lineage.dataset_id,
        snapshot.lineage.exchange,
        snapshot.lineage.symbol,
        state.source_fingerprint,
        state.schema_signature,
        state.row_count,
        state.min_timestamp,
        state.max_timestamp,
        state.synced_at_utc,
        state.source_version,
        state.build_id,
    )
    connection = FakeConnection(
        fetchone_queue=[
            column_rows,
            primary_key_rows,
            None,
            [],
            [],
            state_row,
            [(timestamp, row_digest)],
            [tuple(row.values())],
        ]
    )
    repository = PostgresGoldSyncRepository(_settings(), connection_factory=lambda _: connection)

    def planner(current: GoldSyncState | None, digests: tuple[GoldRowDigest, ...]) -> GoldReconcileDecision:
        connection.trace.append(("PLANNER", (current, digests)))
        return GoldReconcileDecision(plan, state, GoldTargetSummary(1, timestamp, timestamp))

    repository.reconcile_lineage(snapshot, schema.ddl, schema.signature, planner)

    queries = [query for query, _ in connection.trace]
    lock_idx = next(index for index, query in enumerate(queries) if "pg_advisory_xact_lock" in query)
    state_read_idx = next(index for index, query in enumerate(queries) if query.startswith("SELECT dataset_id"))
    digest_read_idx = next(index for index, query in enumerate(queries) if query.startswith("SELECT timestamp_m1"))
    planner_idx = queries.index("PLANNER")
    mutation_idx = next(index for index, query in enumerate(queries) if query.startswith('INSERT INTO "crypto_loader"'))
    checkpoint_idx = next(
        index
        for index, query in enumerate(queries)
        if query.startswith('INSERT INTO "crypto_loader_sync"."gold_sync_state"')
    )
    final_consumer_read_idx = max(
        index for index, query in enumerate(queries) if query.startswith('SELECT "exchange", "symbol"')
    )
    commit_idx = queries.index("COMMIT")
    assert lock_idx < state_read_idx < digest_read_idx < planner_idx
    assert planner_idx < mutation_idx < checkpoint_idx < final_consumer_read_idx < commit_idx


def test_stream_reconcile_stages_source_and_applies_set_based_delta_atomically() -> None:
    schema = _schema()
    column_rows, primary_key_rows = _catalog_rows(bootstrap_migration(schema).tables)
    snapshot = _snapshot(schema.signature)
    new_timestamp = snapshot.min_timestamp
    assert new_timestamp is not None
    old_timestamp = datetime(2025, 12, 31, tzinfo=UTC)
    new_row = {
        "exchange": "deribit",
        "symbol": "BTC",
        "timestamp_m1": new_timestamp,
        "value": 1.0,
    }
    new_hash = canonical_row_hash(new_row)
    old_state = GoldSyncState(
        snapshot.lineage,
        "previous-fingerprint",
        schema.signature,
        1,
        old_timestamp,
        old_timestamp,
        old_timestamp,
        "v1.0.0",
    )
    next_state = GoldSyncState(
        snapshot.lineage,
        snapshot.source_fingerprint,
        schema.signature,
        1,
        new_timestamp,
        new_timestamp,
        datetime(2026, 1, 2, tzinfo=UTC),
        snapshot.source_version,
        snapshot.build_id,
    )
    old_state_row = (
        old_state.lineage.dataset_id,
        old_state.lineage.exchange,
        old_state.lineage.symbol,
        old_state.source_fingerprint,
        old_state.schema_signature,
        old_state.row_count,
        old_state.min_timestamp,
        old_state.max_timestamp,
        old_state.synced_at_utc,
        old_state.source_version,
        old_state.build_id,
    )
    next_state_row = (
        next_state.lineage.dataset_id,
        next_state.lineage.exchange,
        next_state.lineage.symbol,
        next_state.source_fingerprint,
        next_state.schema_signature,
        next_state.row_count,
        next_state.min_timestamp,
        next_state.max_timestamp,
        next_state.synced_at_utc,
        next_state.source_version,
        next_state.build_id,
    )
    connection = FakeConnection(
        fetchone_queue=[
            column_rows,
            primary_key_rows,
            old_state_row,
            (1, old_timestamp, old_timestamp),
            (0,),
            (1, 0),
            (1,),
            (0,),
            (0,),
            (1,),
            (0,),
            next_state_row,
            (1, new_timestamp, new_timestamp),
        ]
    )
    repository = PostgresGoldSyncRepository(_settings(), connection_factory=lambda _: connection)

    result = repository.reconcile_lineage_stream(
        snapshot,
        schema.ddl,
        schema.signature,
        [new_row],
        next_state,
    )

    assert (result.inserted, result.updated, result.deleted, result.unchanged) == (1, 0, 1, 0)
    assert result.status == "synchronized"
    queries = [query for query, _ in connection.trace]
    assert any(query.startswith("CREATE TEMP TABLE") for query in queries)
    assert any(query.startswith("EXECUTEMANY INSERT INTO pg_temp.") for query in queries)
    staged_params = next(params for query, params in connection.trace if query.startswith("EXECUTEMANY"))
    assert staged_params[0][-1] == new_hash  # type: ignore[index]
    assert any(query.startswith('INSERT INTO "crypto_loader".') and "SELECT" in query for query in queries)
    assert any(query.startswith('DELETE FROM "crypto_loader".') for query in queries)
    assert any(query.startswith('UPDATE "crypto_loader".') for query in queries) is False
    assert ("COMMIT", None) in connection.trace
    assert ("ROLLBACK", None) not in connection.trace


def test_source_staging_flushes_bounded_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gold_repository, "_SOURCE_STAGE_BATCH_SIZE", 2)
    connection = FakeConnection()
    cursor = connection.cursor()
    rows = [
        {"exchange": "deribit", "symbol": "BTC", "timestamp_m1": datetime(2026, 1, day, tzinfo=UTC), "value": day}
        for day in range(1, 6)
    ]

    PostgresGoldSyncRepository._stage_source_rows(
        cursor,
        rows,
        ("exchange", "symbol", "timestamp_m1", "value"),
        expected_count=5,
    )

    batches = [params for query, params in connection.trace if query.startswith("EXECUTEMANY")]
    assert [len(batch) for batch in batches] == [2, 2, 1]
    assert all(len(row) == 5 for batch in batches for row in batch)


def test_stream_reconcile_rolls_back_if_staging_fails() -> None:
    schema = _schema()
    column_rows, primary_key_rows = _catalog_rows(bootstrap_migration(schema).tables)
    snapshot = _snapshot(schema.signature)
    timestamp = snapshot.min_timestamp
    assert timestamp is not None
    state = GoldSyncState(
        snapshot.lineage,
        "fingerprint",
        schema.signature,
        1,
        timestamp,
        timestamp,
        timestamp,
        snapshot.source_version,
        snapshot.build_id,
    )
    connection = FakeConnection(
        fetchone_queue=[column_rows, primary_key_rows, None, (0, None, None), (0,)],
        fail_token="INSERT INTO pg_temp.",
    )
    repository = PostgresGoldSyncRepository(_settings(), connection_factory=lambda _: connection)

    with pytest.raises(PostgresGoldRepositoryError, match="transaction failed"):
        repository.reconcile_lineage_stream(
            snapshot,
            schema.ddl,
            schema.signature,
            [{"exchange": "deribit", "symbol": "BTC", "timestamp_m1": timestamp, "value": 1.0}],
            state,
        )

    assert ("ROLLBACK", None) in connection.trace
    assert ("COMMIT", None) not in connection.trace


def test_reconcile_lineage_rolls_back_mutations_when_checkpoint_fails() -> None:
    schema = _schema()
    column_rows, primary_key_rows = _catalog_rows(bootstrap_migration(schema).tables)
    snapshot = _snapshot(schema.signature)
    timestamp = snapshot.min_timestamp
    assert timestamp is not None
    key = GoldRowKey("deribit", "BTC", timestamp)
    payload = GoldRowPayload(
        key,
        (("exchange", "deribit"), ("symbol", "BTC"), ("timestamp_m1", timestamp), ("value", 1.0)),
    )
    plan = GoldDeltaPlan((payload,), (), (), (), (GoldRowDigest(key, "a" * 64),))
    state = GoldSyncState(snapshot.lineage, "fingerprint", schema.signature, 1, timestamp, timestamp, timestamp)
    connection = FakeConnection(
        fetchone_queue=[column_rows, primary_key_rows, None, [], []],
        fail_token='INSERT INTO "crypto_loader_sync"."gold_sync_state"',
    )
    repository = PostgresGoldSyncRepository(_settings(), connection_factory=lambda _: connection)

    def planner(_state: GoldSyncState | None, _digests: tuple[GoldRowDigest, ...]) -> GoldReconcileDecision:
        return GoldReconcileDecision(plan, state, GoldTargetSummary(1, timestamp, timestamp))

    with pytest.raises(Exception, match="transaction failed"):
        repository.reconcile_lineage(snapshot, schema.ddl, schema.signature, planner)

    assert any(query.startswith('INSERT INTO "crypto_loader"') for query, _ in connection.trace)
    assert ("ROLLBACK", None) in connection.trace
    assert ("COMMIT", None) not in connection.trace


@pytest.mark.parametrize(
    "corruption",
    ["consumer_tamper", "consumer_missing", "consumer_extra", "stale_digest", "checkpoint"],
)
def test_reconcile_lineage_rejects_locked_target_integrity_corruption(corruption: str) -> None:
    schema = _schema()
    column_rows, primary_key_rows = _catalog_rows(bootstrap_migration(schema).tables)
    snapshot = _snapshot(schema.signature)
    timestamp = snapshot.min_timestamp
    assert timestamp is not None
    canonical_row = ("deribit", "BTC", timestamp, 1.0)
    canonical_digest = canonical_row_hash(
        {
            "exchange": canonical_row[0],
            "symbol": canonical_row[1],
            "timestamp_m1": canonical_row[2],
            "value": canonical_row[3],
        }
    )
    state_row = (
        snapshot.lineage.dataset_id,
        snapshot.lineage.exchange,
        snapshot.lineage.symbol,
        snapshot.source_fingerprint,
        snapshot.schema_signature,
        2 if corruption == "checkpoint" else 1,
        timestamp,
        timestamp,
        timestamp,
        snapshot.source_version,
        snapshot.build_id,
    )
    digest_rows = [(timestamp, canonical_digest)]
    consumer_rows = [canonical_row]
    if corruption == "consumer_tamper":
        consumer_rows = [("deribit", "BTC", timestamp, 2.0)]
    elif corruption == "consumer_missing":
        consumer_rows = []
    elif corruption == "consumer_extra":
        consumer_rows.append(("deribit", "BTC", datetime(2026, 1, 2, tzinfo=UTC), 2.0))
    elif corruption == "stale_digest":
        digest_rows = [(timestamp, "0" * 64)]
    connection = FakeConnection(fetchone_queue=[column_rows, primary_key_rows, state_row, digest_rows, consumer_rows])
    repository = PostgresGoldSyncRepository(_settings(), connection_factory=lambda _: connection)
    planner_called = False

    def planner(_state: GoldSyncState | None, _digests: tuple[GoldRowDigest, ...]) -> GoldReconcileDecision:
        nonlocal planner_called
        planner_called = True
        raise AssertionError("planner must not run against inconsistent target state")

    with pytest.raises(ValueError, match="inconsistent"):
        repository.reconcile_lineage(snapshot, schema.ddl, schema.signature, planner)

    assert not planner_called
    assert ("ROLLBACK", None) in connection.trace
    assert ("COMMIT", None) not in connection.trace


@pytest.mark.parametrize("rowcount", [0, 2])
@pytest.mark.parametrize("operation", ["update", "delete"])
def test_apply_delta_rolls_back_when_mutation_affected_row_count_is_not_exact(
    rowcount: int,
    operation: str,
) -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    lineage = GoldLineage("gold.history.full.m1", "deribit", "BTC")
    key = GoldRowKey("deribit", "BTC", timestamp)
    payload = GoldRowPayload(
        key,
        (("exchange", "deribit"), ("symbol", "BTC"), ("timestamp_m1", timestamp), ("value", 2.0)),
    )
    digest = GoldRowDigest(key, canonical_row_hash(dict(payload.values)))
    plan = GoldDeltaPlan(
        (), (payload,) if operation == "update" else (), (key,) if operation == "delete" else (), (), (digest,)
    )
    state = GoldSyncState(lineage, "fingerprint", "c" * 64, 1, timestamp, timestamp, timestamp)
    connection = FakeConnection(rowcount=rowcount)
    repository = PostgresGoldSyncRepository(_settings(), connection_factory=lambda _: connection)

    with pytest.raises(Exception, match="transaction failed"):
        repository.apply_delta(lineage, plan, state)

    assert ("ROLLBACK", None) in connection.trace
    assert ("COMMIT", None) not in connection.trace
