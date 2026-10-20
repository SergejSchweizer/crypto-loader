"""Application orchestration for deterministic Gold-to-PostgreSQL reconciliation."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import polars as pl

from application.postgres_sync.contracts import (
    GoldReconcileDecision,
    GoldReconcilePlanner,
    GoldRowDigest,
    GoldRowPayload,
    GoldSourceSnapshot,
    GoldStreamingSyncRepository,
    GoldSyncLineageResult,
    GoldSyncRepository,
    GoldSyncResult,
    GoldSyncState,
    expected_target_summary,
    state_matches_snapshot,
)
from application.postgres_sync.delta import build_source_digests, plan_gold_delta_from_source_pairs
from application.postgres_sync.inventory import discover_current_gold_lineages, discover_declared_gold_lineages
from application.postgres_sync.schema import PostgresTableSchema, build_postgres_table_schema

Clock = Callable[[], datetime]
_MAX_PARALLEL_LINEAGES = 2
# Streaming keeps the source working set bounded, regardless of total lineage size.
_PARALLEL_MEMORY_BUDGET_BYTES = 2 * 1024**3
_STREAM_WORKER_BASE_MEMORY_BYTES = 768 * 1024**2
_ESTIMATED_BYTES_PER_CELL = 128
_PARQUET_EXPANSION_FACTOR = 16
_PARQUET_BATCH_ROWS = 2_048


class GoldSyncServiceError(RuntimeError):
    """Typed application failure identifying the first lineage that could not converge."""

    def __init__(self, snapshot: GoldSourceSnapshot, category: str, message: str) -> None:
        lineage = snapshot.lineage
        super().__init__(f"{category}: {lineage.dataset_id}/{lineage.exchange}/{lineage.symbol}: {message}")
        self.lineage = snapshot.lineage
        self.category = category


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _validate_schema_timestamp_contract(schema: dict[str, pl.DataType]) -> None:
    for column_name, dtype in schema.items():
        if str(dtype).startswith("Datetime") and dtype != pl.Datetime("us", "UTC"):
            raise TypeError(f"Gold datetime column {column_name!r} must be Datetime(us, UTC)")
    if schema.get("timestamp_m1") != pl.Datetime("us", "UTC"):
        raise TypeError("Gold timestamp_m1 must be Datetime(us, UTC)")


def _mapped_schema(snapshot: GoldSourceSnapshot) -> PostgresTableSchema:
    schema_obj = pl.read_parquet_schema(snapshot.artifact_path)
    schema = dict(schema_obj)
    _validate_schema_timestamp_contract(schema)
    return build_postgres_table_schema(snapshot.lineage.dataset_id, schema)


def _validated_source_rows(snapshot: GoldSourceSnapshot) -> Iterator[dict[str, object]]:
    schema = dict(pl.read_parquet_schema(snapshot.artifact_path))
    _validate_schema_timestamp_contract(schema)

    def rows() -> Iterator[dict[str, object]]:
        row_count = 0
        minimum: datetime | None = None
        maximum: datetime | None = None
        previous_timestamp: datetime | None = None
        batches = pl.scan_parquet(snapshot.artifact_path).collect_batches(
            chunk_size=_PARQUET_BATCH_ROWS,
            maintain_order=True,
            engine="streaming",
        )
        for batch in batches:
            for raw_row in batch.iter_rows(named=True):
                row = cast(dict[str, object], raw_row)
                timestamp = row["timestamp_m1"]
                if not isinstance(timestamp, datetime):
                    raise TypeError("Gold timestamp_m1 must contain datetimes")
                if timestamp.tzinfo is None:
                    raise ValueError("Gold timestamp bounds must be timezone-aware")
                if timestamp.utcoffset() != timedelta(0):
                    raise ValueError("Gold timestamp bounds must use UTC")
                if previous_timestamp is not None and timestamp <= previous_timestamp:
                    raise ValueError("Gold timestamp_m1 must be strictly increasing within the lineage")
                if row["exchange"] != snapshot.lineage.exchange or row["symbol"] != snapshot.lineage.symbol:
                    raise ValueError("Gold source row identity differs from selected lineage")
                normalized = timestamp.astimezone(UTC)
                previous_timestamp = normalized
                minimum = normalized if minimum is None else min(minimum, normalized)
                maximum = normalized if maximum is None else max(maximum, normalized)
                row_count += 1
                yield row

        if row_count != snapshot.row_count:
            raise ValueError("Gold source row count differs from current manifest snapshot")
        if minimum != snapshot.min_timestamp or maximum != snapshot.max_timestamp:
            raise ValueError("Gold source timestamp bounds differ from current manifest snapshot")

    return rows()


def _snapshot_for_target(snapshot: GoldSourceSnapshot, table_schema: PostgresTableSchema) -> GoldSourceSnapshot:
    return replace(snapshot, schema_signature=table_schema.signature)


def _state_for_snapshot(snapshot: GoldSourceSnapshot, *, clock: Clock) -> GoldSyncState:
    synced_at = clock()
    if synced_at.tzinfo is None or synced_at.utcoffset() != timedelta(0):
        raise ValueError("Gold sync clock must return an aware UTC datetime")
    return GoldSyncState(
        lineage=snapshot.lineage,
        source_fingerprint=snapshot.source_fingerprint,
        schema_signature=snapshot.schema_signature,
        row_count=snapshot.row_count,
        min_timestamp=snapshot.min_timestamp,
        max_timestamp=snapshot.max_timestamp,
        synced_at_utc=synced_at.astimezone(UTC),
        source_version=snapshot.source_version,
        build_id=snapshot.build_id,
    )


def _reconcile_planner(
    snapshot: GoldSourceSnapshot,
    source_pairs: tuple[tuple[GoldRowPayload, GoldRowDigest], ...],
    *,
    clock: Clock,
) -> GoldReconcilePlanner:
    expected_summary = expected_target_summary(snapshot)

    def plan(state: GoldSyncState | None, target_digests: tuple[GoldRowDigest, ...]) -> GoldReconcileDecision:
        delta = plan_gold_delta_from_source_pairs(source_pairs, target_digests, state_exists=state is not None)
        if state is not None and state_matches_snapshot(state, snapshot):
            if delta.inserts or delta.updates or delta.deletes or delta.unchanged_count != snapshot.row_count:
                raise ValueError("unchanged Gold checkpoint does not match canonical source row digests")
            return GoldReconcileDecision(None, None, expected_summary)

        return GoldReconcileDecision(delta, _state_for_snapshot(snapshot, clock=clock), expected_summary)

    return plan


def _estimated_source_memory(snapshot: GoldSourceSnapshot) -> int:
    try:
        column_count = len(pl.read_parquet_schema(snapshot.artifact_path))
        parquet_size = snapshot.artifact_path.stat().st_size
    except Exception:
        return _PARALLEL_MEMORY_BUDGET_BYTES + 1
    batch_rows = min(max(snapshot.row_count, 1), _PARQUET_BATCH_ROWS)
    row_batch_estimate = batch_rows * column_count * _ESTIMATED_BYTES_PER_CELL
    parquet_batch_estimate = parquet_size * _PARQUET_EXPANSION_FACTOR * batch_rows // max(snapshot.row_count, 1)
    return _STREAM_WORKER_BASE_MEMORY_BYTES + max(row_batch_estimate, parquet_batch_estimate)


def _reconcile_one_snapshot(
    raw_snapshot: GoldSourceSnapshot,
    repository: GoldSyncRepository,
    *,
    clock: Clock,
) -> GoldSyncLineageResult:
    try:
        table_schema = _mapped_schema(raw_snapshot)
        snapshot = _snapshot_for_target(raw_snapshot, table_schema)
        if isinstance(repository, GoldStreamingSyncRepository):
            return repository.reconcile_lineage_stream(
                snapshot,
                table_schema.ddl,
                table_schema.signature,
                _validated_source_rows(snapshot),
                _state_for_snapshot(snapshot, clock=clock),
            )
        source_pairs = build_source_digests(_validated_source_rows(snapshot))
        decision = repository.reconcile_lineage(
            snapshot,
            table_schema.ddl,
            table_schema.signature,
            _reconcile_planner(snapshot, source_pairs, clock=clock),
        )
        if decision.plan is None:
            return GoldSyncLineageResult(
                lineage=snapshot.lineage,
                source_fingerprint=snapshot.source_fingerprint,
                inserted=0,
                updated=0,
                deleted=0,
                unchanged=snapshot.row_count,
                status="unchanged",
            )
        plan = decision.plan
        return GoldSyncLineageResult(
            lineage=snapshot.lineage,
            source_fingerprint=snapshot.source_fingerprint,
            inserted=plan.inserted_count,
            updated=plan.updated_count,
            deleted=plan.deleted_count,
            unchanged=plan.unchanged_count,
            status="synchronized",
        )
    except GoldSyncServiceError:
        raise
    except (ValueError, TypeError) as exc:
        raise GoldSyncServiceError(raw_snapshot, "compatibility", str(exc)) from exc
    except Exception as exc:
        detail = getattr(exc, "safe_detail", None)
        raise GoldSyncServiceError(raw_snapshot, "repository", detail or type(exc).__name__) from exc


def reconcile_snapshots(
    snapshots: Sequence[GoldSourceSnapshot],
    repository: GoldSyncRepository,
    *,
    clock: Clock = _utc_now,
    max_parallel_lineages: int = _MAX_PARALLEL_LINEAGES,
) -> GoldSyncResult:
    """Reconcile lineages in stable order with a memory-bounded worker pool."""

    if not isinstance(max_parallel_lineages, int) or isinstance(max_parallel_lineages, bool):
        raise ValueError("max_parallel_lineages must be 1 or 2")
    if not 1 <= max_parallel_lineages <= _MAX_PARALLEL_LINEAGES:
        raise ValueError("max_parallel_lineages must be 1 or 2")

    ordered = sorted(snapshots, key=lambda item: item.lineage)
    batches: list[tuple[GoldSourceSnapshot, ...]] = []
    parallel_batch: list[GoldSourceSnapshot] = []
    parallel_batch_memory = 0

    def flush_parallel_batch() -> None:
        nonlocal parallel_batch_memory
        if parallel_batch:
            batches.append(tuple(parallel_batch))
            parallel_batch.clear()
            parallel_batch_memory = 0

    for snapshot in ordered:
        estimated_memory = _estimated_source_memory(snapshot)
        if max_parallel_lineages == 1 or estimated_memory > _PARALLEL_MEMORY_BUDGET_BYTES:
            flush_parallel_batch()
            batches.append((snapshot,))
            continue
        if parallel_batch and parallel_batch_memory + estimated_memory > _PARALLEL_MEMORY_BUDGET_BYTES:
            flush_parallel_batch()
        parallel_batch.append(snapshot)
        parallel_batch_memory += estimated_memory
        if len(parallel_batch) == max_parallel_lineages:
            flush_parallel_batch()
    flush_parallel_batch()

    results: list[GoldSyncLineageResult] = []
    for batch in batches:
        if len(batch) == 1:
            results.append(_reconcile_one_snapshot(batch[0], repository, clock=clock))
            continue
        with ThreadPoolExecutor(max_workers=len(batch), thread_name_prefix="gold-sync") as executor:
            futures = [
                executor.submit(_reconcile_one_snapshot, snapshot, repository, clock=clock) for snapshot in batch
            ]
            results.extend(future.result() for future in futures)
    return GoldSyncResult(tuple(results))


def synchronize_gold_root(
    gold_root: str | Path,
    repository: GoldSyncRepository,
    *,
    publication_result: str | Path | None = None,
    clock: Clock = _utc_now,
    max_parallel_lineages: int = _MAX_PARALLEL_LINEAGES,
) -> GoldSyncResult:
    """Discover and reconcile either declared-run or backward-compatible current Gold."""

    snapshots = (
        discover_current_gold_lineages(gold_root)
        if publication_result is None
        else discover_declared_gold_lineages(gold_root, publication_result)
    )
    return reconcile_snapshots(
        snapshots,
        repository,
        clock=clock,
        max_parallel_lineages=max_parallel_lineages,
    )
