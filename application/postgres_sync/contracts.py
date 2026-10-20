"""Application contracts for deterministic Gold-to-PostgreSQL synchronization."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, runtime_checkable

POSTGRES_HOST = "10.10.1.3"
POSTGRES_PORT = 54321
POSTGRES_ROLE = "crypto-loader"
POSTGRES_CONSUMER_SCHEMA = "crypto_loader"
POSTGRES_SYNC_SCHEMA = "crypto_loader_sync"
POSTGRES_SYNC_STATE_TABLE = "gold_sync_state"
POSTGRES_ROW_HASH_TABLE = "gold_row_hashes"
POSTGRES_TIMESTAMP_TYPE = "TIMESTAMPTZ(6)"
POSTGRES_SESSION_TIMEZONE = "UTC"
SOURCE_TIMESTAMP_UNIT = "us"
SOURCE_TIMESTAMP_TIMEZONE = "UTC"
GOLD_ROW_KEY_COLUMNS = ("exchange", "symbol", "timestamp_m1")
POSTGRES_IDENTIFIER_LIMIT_BYTES = 63


def _require_utc_datetime(value: datetime, name: str) -> datetime:
    """Validate an aware UTC datetime without changing its instant or precision."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must use UTC")
    # Python datetime stores microseconds exactly. Normalizing to UTC is safe and
    # produces one canonical timezone object for hashing/database parameters.
    return value.astimezone(UTC)


def consumer_table_name(dataset_id: str) -> str:
    """Map a registered Gold dataset ID to its deterministic PostgreSQL table name."""

    if not dataset_id or dataset_id.startswith(".") or dataset_id.endswith("."):
        raise ValueError("dataset_id must be a non-empty dotted identifier")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._")
    if any(character not in allowed for character in dataset_id):
        raise ValueError(f"dataset_id contains unsupported characters: {dataset_id!r}")
    mapped = dataset_id.replace(".", "_")
    if len(mapped.encode("utf-8")) > POSTGRES_IDENTIFIER_LIMIT_BYTES:
        raise ValueError(f"PostgreSQL table name exceeds {POSTGRES_IDENTIFIER_LIMIT_BYTES} bytes")
    return mapped


def validate_unique_table_names(dataset_ids: Sequence[str]) -> dict[str, str]:
    """Return deterministic dataset/table mapping and reject collisions."""

    result: dict[str, str] = {}
    reverse: dict[str, str] = {}
    for dataset_id in dataset_ids:
        table = consumer_table_name(dataset_id)
        previous = reverse.get(table)
        if previous is not None and previous != dataset_id:
            raise ValueError(f"Gold dataset table-name collision: {previous!r} and {dataset_id!r}")
        reverse[table] = dataset_id
        result[dataset_id] = table
    return result


def validate_gold_key_columns(columns: Sequence[str]) -> None:
    """Require the exact logical Gold key columns to be present."""

    missing = [column for column in GOLD_ROW_KEY_COLUMNS if column not in columns]
    if missing:
        raise ValueError(f"Gold row schema missing logical key column(s): {', '.join(missing)}")


@dataclass(frozen=True, slots=True, order=True)
class GoldLineage:
    """Identity of one independently synchronized Gold dataset lineage."""

    dataset_id: str
    exchange: str
    symbol: str

    def __post_init__(self) -> None:
        consumer_table_name(self.dataset_id)
        if not self.exchange.strip():
            raise ValueError("exchange must not be empty")
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")


@dataclass(frozen=True, slots=True, order=True)
class GoldRowKey:
    """Composite logical key of one Gold consumer row."""

    exchange: str
    symbol: str
    timestamp_m1: datetime

    def __post_init__(self) -> None:
        if not self.exchange.strip() or not self.symbol.strip():
            raise ValueError("Gold row key exchange/symbol must not be empty")
        object.__setattr__(self, "timestamp_m1", _require_utc_datetime(self.timestamp_m1, "timestamp_m1"))


@dataclass(frozen=True, slots=True)
class GoldSourceSnapshot:
    """Validated current Gold artifact metadata for one lineage."""

    lineage: GoldLineage
    artifact_path: Path
    source_fingerprint: str
    schema_signature: str
    row_count: int
    min_timestamp: datetime | None
    max_timestamp: datetime | None
    source_version: str | None = None
    build_id: str | None = None
    output_sha256: str | None = None

    def __post_init__(self) -> None:
        if not self.source_fingerprint:
            raise ValueError("source_fingerprint must not be empty")
        if not self.schema_signature:
            raise ValueError("schema_signature must not be empty")
        if self.row_count < 0:
            raise ValueError("row_count must be non-negative")
        if self.min_timestamp is not None:
            object.__setattr__(self, "min_timestamp", _require_utc_datetime(self.min_timestamp, "min_timestamp"))
        if self.max_timestamp is not None:
            object.__setattr__(self, "max_timestamp", _require_utc_datetime(self.max_timestamp, "max_timestamp"))
        if self.row_count == 0 and (self.min_timestamp is not None or self.max_timestamp is not None):
            raise ValueError("empty Gold source must not publish timestamp bounds")
        if self.row_count > 0 and (self.min_timestamp is None or self.max_timestamp is None):
            raise ValueError("non-empty Gold source requires min/max timestamps")
        has_ordered_bounds = (
            self.min_timestamp is not None
            and self.max_timestamp is not None
            and self.min_timestamp > self.max_timestamp
        )
        if has_ordered_bounds:
            raise ValueError("Gold source min_timestamp must not exceed max_timestamp")


@dataclass(frozen=True, slots=True)
class GoldSyncState:
    """Last successful PostgreSQL synchronization checkpoint for one lineage."""

    lineage: GoldLineage
    source_fingerprint: str
    schema_signature: str
    row_count: int
    min_timestamp: datetime | None
    max_timestamp: datetime | None
    synced_at_utc: datetime
    source_version: str | None = None
    build_id: str | None = None

    def __post_init__(self) -> None:
        if self.row_count < 0:
            raise ValueError("row_count must be non-negative")
        object.__setattr__(self, "synced_at_utc", _require_utc_datetime(self.synced_at_utc, "synced_at_utc"))
        if self.min_timestamp is not None:
            object.__setattr__(self, "min_timestamp", _require_utc_datetime(self.min_timestamp, "min_timestamp"))
        if self.max_timestamp is not None:
            object.__setattr__(self, "max_timestamp", _require_utc_datetime(self.max_timestamp, "max_timestamp"))


@dataclass(frozen=True, slots=True)
class GoldRowPayload:
    """Immutable full source row in exact column order."""

    key: GoldRowKey
    values: tuple[tuple[str, object], ...]

    def as_mapping(self) -> dict[str, object]:
        """Return a fresh mapping while preserving the stored column order."""

        return dict(self.values)


@dataclass(frozen=True, slots=True)
class GoldRowDigest:
    """Persisted SHA-256 identity for one logical Gold row."""

    key: GoldRowKey
    row_sha256: str

    def __post_init__(self) -> None:
        if len(self.row_sha256) != 64 or any(character not in "0123456789abcdef" for character in self.row_sha256):
            raise ValueError("row_sha256 must be a lowercase SHA-256 hex digest")


@dataclass(frozen=True, slots=True)
class GoldDeltaPlan:
    """Complete-state mutation plan for one changed Gold lineage."""

    inserts: tuple[GoldRowPayload, ...]
    updates: tuple[GoldRowPayload, ...]
    deletes: tuple[GoldRowKey, ...]
    unchanged: tuple[GoldRowKey, ...]
    source_digests: tuple[GoldRowDigest, ...]

    @property
    def inserted_count(self) -> int:
        return len(self.inserts)

    @property
    def updated_count(self) -> int:
        return len(self.updates)

    @property
    def deleted_count(self) -> int:
        return len(self.deletes)

    @property
    def unchanged_count(self) -> int:
        return len(self.unchanged)


@dataclass(frozen=True, slots=True)
class GoldTargetSummary:
    """Small target-side verification summary for one lineage."""

    row_count: int
    min_timestamp: datetime | None
    max_timestamp: datetime | None


@dataclass(frozen=True, slots=True)
class GoldReconcileDecision:
    """Application decision returned while a repository lineage lock is held."""

    plan: GoldDeltaPlan | None
    next_state: GoldSyncState | None
    expected_summary: GoldTargetSummary

    def __post_init__(self) -> None:
        if (self.plan is None) != (self.next_state is None):
            raise ValueError("Gold reconcile plan and next state must either both be present or both be absent")


@dataclass(frozen=True, slots=True)
class GoldSyncLineageResult:
    """One lineage synchronization result without credentials."""

    lineage: GoldLineage
    source_fingerprint: str
    inserted: int
    updated: int
    deleted: int
    unchanged: int
    status: str


@dataclass(frozen=True, slots=True)
class GoldSyncResult:
    """Aggregate synchronization result returned by the application service."""

    lineages: tuple[GoldSyncLineageResult, ...]

    @property
    def inserted(self) -> int:
        return sum(item.inserted for item in self.lineages)

    @property
    def updated(self) -> int:
        return sum(item.updated for item in self.lineages)

    @property
    def deleted(self) -> int:
        return sum(item.deleted for item in self.lineages)

    @property
    def unchanged(self) -> int:
        return sum(item.unchanged for item in self.lineages)


GoldReconcilePlanner = Callable[
    [GoldSyncState | None, tuple[GoldRowDigest, ...]],
    GoldReconcileDecision,
]


@runtime_checkable
class GoldSyncRepository(Protocol):
    """Application port implemented by the PostgreSQL infrastructure adapter."""

    def reconcile_lineage(
        self,
        snapshot: GoldSourceSnapshot,
        ddl: str,
        schema_signature: str,
        planner: GoldReconcilePlanner,
    ) -> GoldReconcileDecision: ...


@runtime_checkable
class GoldStreamingSyncRepository(Protocol):
    """Optional repository capability for bounded, database-staged reconciliation."""

    def reconcile_lineage_stream(
        self,
        snapshot: GoldSourceSnapshot,
        ddl: str,
        schema_signature: str,
        source_rows: Iterable[Mapping[str, object]],
        next_state: GoldSyncState,
    ) -> GoldSyncLineageResult: ...


def state_matches_snapshot(state: GoldSyncState, snapshot: GoldSourceSnapshot) -> bool:
    """Return whether a checkpoint exactly identifies the current source snapshot."""

    return (
        state.lineage == snapshot.lineage
        and state.source_fingerprint == snapshot.source_fingerprint
        and state.schema_signature == snapshot.schema_signature
        and state.row_count == snapshot.row_count
        and state.min_timestamp == snapshot.min_timestamp
        and state.max_timestamp == snapshot.max_timestamp
        and state.source_version == snapshot.source_version
        and state.build_id == snapshot.build_id
    )


def expected_target_summary(snapshot: GoldSourceSnapshot) -> GoldTargetSummary:
    """Build the target summary that must hold after synchronization."""

    return GoldTargetSummary(snapshot.row_count, snapshot.min_timestamp, snapshot.max_timestamp)


def payload_from_mapping(row: Mapping[str, object]) -> GoldRowPayload:
    """Create a payload from an ordered row mapping after logical-key validation."""

    validate_gold_key_columns(tuple(row))
    exchange = row["exchange"]
    symbol = row["symbol"]
    timestamp = row["timestamp_m1"]
    if not isinstance(exchange, str) or not isinstance(symbol, str) or not isinstance(timestamp, datetime):
        raise TypeError("Gold row key requires string exchange/symbol and datetime timestamp_m1")
    return GoldRowPayload(GoldRowKey(exchange, symbol, timestamp), tuple(row.items()))
