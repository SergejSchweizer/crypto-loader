"""Read-only selection of current registered Gold artifacts for PostgreSQL sync."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import polars as pl

from application.dataset_contracts import supported_gold_dataset_ids
from application.postgres_sync.contracts import GoldLineage, GoldSourceSnapshot, validate_unique_table_names
from application.services.gold_publication import (
    GOLD_PUBLICATION_RESULT_VERSION,
    validate_gold_artifact_attestation,
)
from application.services.gold_versioning import parse_semver


def _parse_utc_timestamp(value: object, field_name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Gold manifest {field_name} must be an ISO UTC timestamp or null")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"Gold manifest {field_name} must use UTC")
    return parsed.astimezone(UTC)


def _schema_signature(schema: dict[str, pl.DataType]) -> str:
    payload = [(name, str(dtype)) for name, dtype in schema.items()]
    canonical = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_timestamp_schema(schema: dict[str, pl.DataType]) -> None:
    timestamp_dtype = schema.get("timestamp_m1")
    if timestamp_dtype != pl.Datetime("us", "UTC"):
        raise TypeError("Gold source timestamp_m1 must be Polars Datetime(us, UTC)")
    if "exchange" not in schema or "symbol" not in schema:
        raise ValueError("Gold source schema must contain exchange and symbol")


def _manifest_payload(path: Path) -> dict[str, object]:
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid Gold manifest: {path}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"Gold manifest must contain an object: {path}")
    return cast(dict[str, object], raw)


def _lineage_from_manifest(payload: dict[str, object]) -> GoldLineage:
    dataset_id = payload.get("dataset_id")
    exchange = payload.get("exchange")
    symbol = payload.get("symbol")
    if not isinstance(dataset_id, str) or not isinstance(exchange, str) or not isinstance(symbol, str):
        raise ValueError("Gold manifest must contain string dataset_id/exchange/symbol")
    return GoldLineage(dataset_id, exchange, symbol)


def _manifest_version(payload: dict[str, object]) -> str:
    value = payload.get("dataset_version")
    if not isinstance(value, str):
        # Legacy sidecars store the same semantic value in feature_set_version.
        value = payload.get("feature_set_version")
    if not isinstance(value, str) or not value:
        raise ValueError("Gold manifest missing dataset_version")
    parse_semver(value)
    return value


def _manifest_fingerprint(payload: dict[str, object]) -> str:
    for key in ("input_fingerprint", "source_data_hash"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    raise ValueError("Gold manifest missing source fingerprint")


def _manifest_build_date(payload: dict[str, object]) -> datetime:
    """Return the UTC build time used to select the newest artifact revision."""

    parsed = _parse_utc_timestamp(payload.get("build_date_utc"), "build_date_utc")
    return parsed if parsed is not None else datetime.min.replace(tzinfo=UTC)


def _manifest_row_count(payload: dict[str, object]) -> int:
    value = payload.get("rows_out")
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError("Gold manifest rows_out must be a non-negative integer")
    return value


def _snapshot_from_candidate(manifest_path: Path, payload: dict[str, object]) -> GoldSourceSnapshot:
    lineage = _lineage_from_manifest(payload)
    parquet_path = manifest_path.with_suffix(".parquet")
    if not parquet_path.is_file():
        raise ValueError(f"Gold manifest has no matching parquet artifact: {manifest_path}")
    validate_gold_artifact_attestation(parquet_path, manifest_path)
    schema_obj = pl.read_parquet_schema(parquet_path)
    schema = dict(schema_obj)
    _validate_timestamp_schema(schema)
    schema_signature = _schema_signature(schema)
    if payload.get("schema_signature") != schema_signature:
        raise ValueError("Gold manifest schema signature does not match Parquet schema")
    summary = (
        pl.scan_parquet(parquet_path)
        .select(
            pl.len().alias("row_count"),
            (pl.col("exchange").is_null() | (pl.col("exchange") != lineage.exchange)).any().alias("wrong_exchange"),
            (pl.col("symbol").is_null() | (pl.col("symbol") != lineage.symbol)).any().alias("wrong_symbol"),
            pl.col("timestamp_m1").min().alias("min_timestamp"),
            pl.col("timestamp_m1").max().alias("max_timestamp"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    if summary["wrong_exchange"] or summary["wrong_symbol"]:
        raise ValueError("Gold manifest lineage does not match Parquet rows")
    row_count = _manifest_row_count(payload)
    if summary["row_count"] != row_count:
        raise ValueError("Gold manifest row count does not match Parquet rows")
    min_timestamp = _parse_utc_timestamp(payload.get("min_timestamp"), "min_timestamp")
    max_timestamp = _parse_utc_timestamp(payload.get("max_timestamp"), "max_timestamp")
    if row_count == 0:
        min_timestamp = None
        max_timestamp = None
    else:
        if summary["min_timestamp"] != min_timestamp or summary["max_timestamp"] != max_timestamp:
            raise ValueError("Gold manifest timestamp bounds do not match Parquet rows")
    version = _manifest_version(payload)
    build_id_raw = payload.get("build_id")
    build_id = build_id_raw if isinstance(build_id_raw, str) and build_id_raw else None
    return GoldSourceSnapshot(
        lineage=lineage,
        artifact_path=parquet_path.resolve(),
        source_fingerprint=_manifest_fingerprint(payload),
        schema_signature=schema_signature,
        row_count=row_count,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
        source_version=version,
        build_id=build_id,
        output_sha256=cast(str, payload["output_sha256"]),
    )


def _candidate_manifests(gold_root: Path, dataset_id: str) -> Iterable[Path]:
    dataset_root = gold_root / f"dataset_id={dataset_id}"
    if not dataset_root.is_dir():
        return ()
    modern = dataset_root.glob("dataset_type=gold_symbol_dataset/feature_set_version=*/exchange=*/symbol=*/*.json")
    legacy = dataset_root.glob("exchange=*/symbol=*/version=*/build_id=*/manifest.json")
    return (*modern, *legacy)


def _publication_result_payload(path: Path) -> dict[str, object]:
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("invalid Gold publication result") from exc
    if not isinstance(raw, dict):
        raise ValueError("Gold publication result must contain an object")
    payload = cast(dict[str, object], raw)
    if payload.get("result_version") != GOLD_PUBLICATION_RESULT_VERSION or payload.get("status") != "success":
        raise ValueError("Gold publication result is not a certified success")
    deprecations = payload.get("serving_deprecations")
    if deprecations != []:
        raise ValueError("Gold serving deletion requires an explicit supported deprecation policy")
    return payload


def discover_declared_gold_lineages(
    gold_root: str | Path,
    publication_result: str | Path,
) -> tuple[GoldSourceSnapshot, ...]:
    """Certify and return exactly the successful lineages declared by one Gold command."""

    root = Path(gold_root).resolve()
    payload = _publication_result_payload(Path(publication_result))
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list):
        raise ValueError("Gold publication result artifacts must be a list")
    supported = set(supported_gold_dataset_ids())
    validate_unique_table_names(tuple(supported))
    snapshots: list[GoldSourceSnapshot] = []
    declared_lineages: set[GoldLineage] = set()
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise ValueError("Gold publication result artifact must contain an object")
        artifact_payload = cast(dict[str, object], artifact)
        lineage = _lineage_from_manifest(artifact_payload)
        if lineage.dataset_id not in supported:
            raise ValueError("Gold publication result declares an unregistered dataset")
        if lineage in declared_lineages:
            raise ValueError("Gold publication result contains duplicate lineages")
        declared_lineages.add(lineage)
        parquet_value = artifact_payload.get("parquet_path")
        manifest_value = artifact_payload.get("manifest_path")
        if not isinstance(parquet_value, str) or not isinstance(manifest_value, str):
            raise ValueError("Gold publication result artifact paths must be strings")
        parquet_path = Path(parquet_value).resolve()
        manifest_path = Path(manifest_value).resolve()
        if not parquet_path.is_relative_to(root) or not manifest_path.is_relative_to(root):
            raise ValueError("Gold publication result artifact is outside the Gold root")
        if manifest_path.with_suffix(".parquet") != parquet_path:
            raise ValueError("Gold publication result artifact pair does not match")
        manifest_payload = _manifest_payload(manifest_path)
        if _lineage_from_manifest(manifest_payload) != lineage:
            raise ValueError("Gold publication result lineage does not match its manifest")
        snapshots.append(_snapshot_from_candidate(manifest_path, manifest_payload))
    return tuple(sorted(snapshots, key=lambda snapshot: snapshot.lineage))


def discover_current_gold_lineages(gold_root: str | Path) -> tuple[GoldSourceSnapshot, ...]:
    """Return exactly one highest-semver current artifact per registered Gold lineage."""

    root = Path(gold_root)
    if not root.exists():
        return ()
    supported = supported_gold_dataset_ids()
    validate_unique_table_names(supported)
    by_lineage: dict[
        GoldLineage,
        list[tuple[tuple[int, int, int], datetime, Path, dict[str, object]]],
    ] = {}

    for dataset_id in supported:
        for manifest_path in _candidate_manifests(root, dataset_id):
            payload = _manifest_payload(manifest_path)
            lineage = _lineage_from_manifest(payload)
            if lineage.dataset_id != dataset_id:
                raise ValueError(f"Gold manifest dataset/path mismatch: {manifest_path}")
            version = _manifest_version(payload)
            by_lineage.setdefault(lineage, []).append(
                (parse_semver(version), _manifest_build_date(payload), manifest_path, payload)
            )

    snapshots: list[GoldSourceSnapshot] = []
    for lineage in sorted(by_lineage):
        candidates = by_lineage[lineage]
        highest_version = max(version for version, _, _, _ in candidates)
        highest_build_date = max(build_date for version, build_date, _, _ in candidates if version == highest_version)
        current = [
            (path, payload)
            for version, build_date, path, payload in candidates
            if version == highest_version and build_date == highest_build_date
        ]
        if len(current) != 1:
            raise ValueError(f"duplicate current Gold candidates for lineage {lineage}")
        manifest_path, payload = current[0]
        snapshots.append(_snapshot_from_candidate(manifest_path, payload))
    return tuple(snapshots)
