from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from application.postgres_sync import GoldRowDigest, GoldRowKey
from application.postgres_sync.delta import canonical_row_hash, plan_gold_delta


def _row(timestamp: datetime, value: float) -> dict[str, object]:
    return {
        "exchange": "deribit",
        "symbol": "BTC",
        "timestamp_m1": timestamp,
        "value": value,
        "nullable": None,
    }


def test_canonical_hash_is_type_and_value_sensitive() -> None:
    timestamp = datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    row = _row(timestamp, 1.5)
    assert canonical_row_hash(row) == canonical_row_hash(dict(row))
    changed = dict(row)
    changed["value"] = 1.6
    assert canonical_row_hash(row) != canonical_row_hash(changed)
    negative_zero = _row(timestamp, -0.0)
    positive_zero = _row(timestamp, 0.0)
    assert canonical_row_hash(negative_zero) == canonical_row_hash(positive_zero)


def test_invalid_timestamps_fail_and_non_finite_floats_hash_stably() -> None:
    aware = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError):
        canonical_row_hash(_row(aware.replace(tzinfo=None), 1.0))
    with pytest.raises(ValueError):
        canonical_row_hash(_row(aware.astimezone(timezone(timedelta(hours=1))), 1.0))
    nan_hash = canonical_row_hash(_row(aware, float("nan")))
    assert nan_hash == canonical_row_hash(_row(aware, float("nan")))
    assert nan_hash != canonical_row_hash(_row(aware, None))
    assert canonical_row_hash(_row(aware, float("inf"))) != canonical_row_hash(_row(aware, float("-inf")))
    assert canonical_row_hash(_row(aware, float("inf"))) != nan_hash


def test_non_finite_decimals_hash_stably_and_unsupported_values_fail() -> None:
    assert canonical_row_hash({"value": Decimal("Infinity")}) == canonical_row_hash({"value": Decimal("Infinity")})
    assert canonical_row_hash({"value": Decimal("NaN")}) == canonical_row_hash({"value": Decimal("NaN")})
    assert canonical_row_hash({"value": Decimal("Infinity")}) != canonical_row_hash({"value": Decimal("-Infinity")})
    with pytest.raises(TypeError, match="unsupported deterministic hash value type"):
        canonical_row_hash({"value": object()})
    assert canonical_row_hash({"nested": {"b": 2, "a": [None, "value"]}}) == canonical_row_hash(
        {"nested": {"a": [None, "value"], "b": 2}}
    )


def test_bootstrap_classifies_every_source_row_as_insert() -> None:
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    t1 = datetime(2026, 1, 2, tzinfo=UTC)
    plan = plan_gold_delta([_row(t1, 2.0), _row(t0, 1.0)], (), state_exists=False)
    assert [payload.key.timestamp_m1 for payload in plan.inserts] == [t0, t1]
    assert plan.updated_count == plan.deleted_count == plan.unchanged_count == 0


def test_digest_without_state_is_rejected() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    key = GoldRowKey("deribit", "BTC", timestamp)
    digest = GoldRowDigest(key, canonical_row_hash(_row(timestamp, 1.0)))
    with pytest.raises(ValueError):
        plan_gold_delta([_row(timestamp, 1.0)], (digest,), state_exists=False)


def test_duplicate_source_or_target_keys_are_rejected() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    source = _row(timestamp, 1.0)
    key = GoldRowKey("deribit", "BTC", timestamp)
    digest = GoldRowDigest(key, canonical_row_hash(source))

    with pytest.raises(ValueError, match="duplicate source Gold key"):
        plan_gold_delta([source, dict(source)], (), state_exists=True)
    with pytest.raises(ValueError, match="duplicate PostgreSQL digest key"):
        plan_gold_delta([source], (digest, digest), state_exists=True)


def test_mixed_complete_state_delta_is_exact() -> None:
    timestamps = [datetime(2026, 1, day, tzinfo=UTC) for day in range(1, 6)]
    source = [
        _row(timestamps[0], 1.0),  # unchanged
        _row(timestamps[1], 22.0),  # update
        _row(timestamps[3], 4.0),  # insert
        _row(timestamps[4], 5.0),  # insert
    ]
    target = (
        GoldRowDigest(GoldRowKey("deribit", "BTC", timestamps[0]), canonical_row_hash(_row(timestamps[0], 1.0))),
        GoldRowDigest(GoldRowKey("deribit", "BTC", timestamps[1]), canonical_row_hash(_row(timestamps[1], 2.0))),
        GoldRowDigest(GoldRowKey("deribit", "BTC", timestamps[2]), canonical_row_hash(_row(timestamps[2], 3.0))),
    )
    plan = plan_gold_delta(source, target, state_exists=True)
    assert plan.inserted_count == 2
    assert plan.updated_count == 1
    assert plan.deleted_count == 1
    assert plan.unchanged_count == 1
    all_keys = (
        [payload.key for payload in plan.inserts]
        + [payload.key for payload in plan.updates]
        + list(plan.deletes)
        + list(plan.unchanged)
    )
    assert len(all_keys) == len(set(all_keys))


def test_delta_merge_handles_unsorted_source_and_target_without_changing_results() -> None:
    timestamps = [datetime(2026, 1, day, tzinfo=UTC) for day in range(1, 5)]
    source = [_row(timestamps[3], 4.0), _row(timestamps[0], 1.0), _row(timestamps[2], 30.0)]
    target = (
        GoldRowDigest(GoldRowKey("deribit", "BTC", timestamps[2]), canonical_row_hash(_row(timestamps[2], 3.0))),
        GoldRowDigest(GoldRowKey("deribit", "BTC", timestamps[1]), canonical_row_hash(_row(timestamps[1], 2.0))),
        GoldRowDigest(GoldRowKey("deribit", "BTC", timestamps[0]), canonical_row_hash(_row(timestamps[0], 1.0))),
    )

    plan = plan_gold_delta(source, target, state_exists=True)

    assert [payload.key.timestamp_m1 for payload in plan.inserts] == [timestamps[3]]
    assert [payload.key.timestamp_m1 for payload in plan.updates] == [timestamps[2]]
    assert plan.deletes == (GoldRowKey("deribit", "BTC", timestamps[1]),)
    assert plan.unchanged == (GoldRowKey("deribit", "BTC", timestamps[0]),)


def test_historical_correction_is_detected_without_watermark() -> None:
    old = datetime(2020, 1, 1, tzinfo=UTC)
    recent = datetime(2026, 1, 1, tzinfo=UTC)
    target = (GoldRowDigest(GoldRowKey("deribit", "BTC", old), canonical_row_hash(_row(old, 1.0))),)
    source = [_row(old, 9.0), _row(recent, 2.0)]
    plan = plan_gold_delta(source, target, state_exists=True)
    assert plan.updated_count == 1
    assert plan.inserted_count == 1
