"""Pure deterministic complete-state delta planning for PostgreSQL Gold sync."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from typing import Final

from application.postgres_sync.contracts import (
    GoldDeltaPlan,
    GoldRowDigest,
    GoldRowKey,
    GoldRowPayload,
    payload_from_mapping,
)

_SEPARATOR: Final[bytes] = b"\x1f"


def _utc_datetime_bytes(value: datetime) -> bytes:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime value must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError("datetime value must use UTC")
    normalized = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = normalized - epoch
    epoch_microseconds = ((delta.days * 86_400 + delta.seconds) * 1_000_000) + delta.microseconds
    return f"dt:{epoch_microseconds}".encode("ascii")


def _canonical_value(value: object) -> bytes:
    if value is None:
        return b"null"
    if isinstance(value, bool):
        return b"bool:1" if value else b"bool:0"
    if isinstance(value, int):
        return f"int:{value}".encode("ascii")
    if isinstance(value, float):
        if math.isnan(value):
            return b"float:nan"
        if math.isinf(value):
            return b"float:-inf" if value < 0 else b"float:+inf"
        normalized = 0.0 if value == 0.0 else value
        return f"float:{format(normalized, '.17g')}".encode("ascii")
    if isinstance(value, Decimal):
        if value.is_nan():
            return b"decimal:nan"
        if value.is_infinite():
            return b"decimal:-inf" if value.is_signed() else b"decimal:+inf"
        return f"decimal:{value.normalize()}".encode("ascii")
    if isinstance(value, datetime):
        return _utc_datetime_bytes(value)
    if isinstance(value, date):
        return f"date:{value.isoformat()}".encode("ascii")
    if isinstance(value, str):
        return b"str:" + value.encode("utf-8")
    if isinstance(value, bytes):
        return b"bytes:" + value.hex().encode("ascii")
    if isinstance(value, (tuple, list)):
        encoded = [_canonical_value(item) for item in value]
        return b"list:[" + _SEPARATOR.join(encoded) + b"]"
    if isinstance(value, Mapping):
        items: list[bytes] = []
        for raw_key, raw_value in sorted(value.items(), key=lambda item: str(item[0])):
            key = str(raw_key)
            items.append(b"key:" + key.encode("utf-8") + b"=" + _canonical_value(raw_value))
        return b"map:{" + _SEPARATOR.join(items) + b"}"
    raise TypeError(f"unsupported deterministic hash value type: {type(value).__name__}")


def canonical_row_hash(row: Mapping[str, object]) -> str:
    """Hash a row using exact mapping order, type tags, null markers, and UTC microseconds."""

    digest = hashlib.sha256()
    for column_name, value in row.items():
        digest.update(b"column:")
        digest.update(column_name.encode("utf-8"))
        digest.update(b"=")
        digest.update(_canonical_value(value))
        digest.update(_SEPARATOR)
    return digest.hexdigest()


def build_source_digests(
    rows: Iterable[Mapping[str, object]],
) -> tuple[tuple[GoldRowPayload, GoldRowDigest], ...]:
    """Create deterministic payload/digest pairs and reject duplicate logical keys."""

    result: list[tuple[GoldRowPayload, GoldRowDigest]] = []
    for row in rows:
        payload = payload_from_mapping(row)
        digest = GoldRowDigest(payload.key, canonical_row_hash(row))
        result.append((payload, digest))

    result.sort(key=lambda pair: pair[0].key)
    previous_key: GoldRowKey | None = None
    for payload, _ in result:
        if payload.key == previous_key:
            raise ValueError(f"duplicate source Gold key: {payload.key}")
        previous_key = payload.key
    return tuple(result)


def plan_gold_delta(
    source_rows: Sequence[Mapping[str, object]],
    target_digests: Sequence[GoldRowDigest],
    *,
    state_exists: bool,
) -> GoldDeltaPlan:
    """Compare complete source and digest state without timestamp-watermark assumptions."""

    if not state_exists and target_digests:
        raise ValueError("Gold digest state exists without an authoritative sync checkpoint")

    source_pairs = build_source_digests(source_rows)
    return plan_gold_delta_from_source_pairs(source_pairs, target_digests, state_exists=state_exists)


def plan_gold_delta_from_source_pairs(
    source_pairs: Sequence[tuple[GoldRowPayload, GoldRowDigest]],
    target_digests: Sequence[GoldRowDigest],
    *,
    state_exists: bool,
) -> GoldDeltaPlan:
    """Compare precomputed source payload digests with PostgreSQL's current digest state."""

    if not state_exists and target_digests:
        raise ValueError("Gold digest state exists without an authoritative sync checkpoint")

    source_is_ordered = all(left[0].key < right[0].key for left, right in pairwise(source_pairs))
    target_is_ordered = all(left.key < right.key for left, right in pairwise(target_digests))
    ordered_source = source_pairs if source_is_ordered else sorted(source_pairs, key=lambda pair: pair[0].key)
    ordered_target = target_digests if target_is_ordered else sorted(target_digests, key=lambda digest: digest.key)
    for payload, digest in ordered_source:
        if payload.key != digest.key:
            raise ValueError("source payload and digest keys differ")
    for source_before, source_after in pairwise(ordered_source):
        if source_before[0].key == source_after[0].key:
            raise ValueError("duplicate source Gold key")
    for target_before, target_after in pairwise(ordered_target):
        if target_before.key == target_after.key:
            raise ValueError("duplicate PostgreSQL digest key")

    inserts: list[GoldRowPayload] = []
    updates: list[GoldRowPayload] = []
    deletes: list[GoldRowKey] = []
    unchanged: list[GoldRowKey] = []
    source_digests: list[GoldRowDigest] = []
    source_index = 0
    target_index = 0
    while source_index < len(ordered_source) and target_index < len(ordered_target):
        payload, source_digest = ordered_source[source_index]
        target_digest = ordered_target[target_index]
        if payload.key < target_digest.key:
            inserts.append(payload)
            source_digests.append(source_digest)
            source_index += 1
        elif target_digest.key < payload.key:
            deletes.append(target_digest.key)
            target_index += 1
        else:
            source_digests.append(source_digest)
            if target_digest.row_sha256 == source_digest.row_sha256:
                unchanged.append(payload.key)
            else:
                updates.append(payload)
            source_index += 1
            target_index += 1

    while source_index < len(ordered_source):
        payload, source_digest = ordered_source[source_index]
        inserts.append(payload)
        source_digests.append(source_digest)
        source_index += 1
    while target_index < len(ordered_target):
        deletes.append(ordered_target[target_index].key)
        target_index += 1

    return GoldDeltaPlan(
        inserts=tuple(inserts),
        updates=tuple(updates),
        deletes=tuple(deletes),
        unchanged=tuple(unchanged),
        source_digests=tuple(source_digests),
    )
