"""Tests for bronze loader command output sidecar metadata."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from api.commands import loader as loader_cmd
from api.commands.loader_execution import FetchAllTaskGroupsResult
from application.dto import CandleFetchTaskDTO, PersistResultDTO, TradeFetchTaskDTO
from application.services.bronze_runtime_service import BronzeRuntimeBoundsContext
from ingestion.spot_ohlcv import SpotCandle
from ingestion.trades import OptionTradeTick, TradeTick


def _empty_fetch_result(
    candle_results: dict[tuple[str, str, str, str], list[object]] | None = None,
) -> FetchAllTaskGroupsResult:
    """Build a stage-execution result with all groups empty except an optional candle override."""

    return FetchAllTaskGroupsResult(
        candle_results=candle_results or {},
        candle_errors={},
        open_interest_results={},
        open_interest_errors={},
        funding_results={},
        funding_errors={},
        volatility_results={},
        volatility_errors={},
        trade_results={},
        trade_errors={},
    )


def test_run_bronze_build_emits_manifest_and_plot_file_lists(tmp_path: Path, monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.chdir(tmp_path)

    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    candle = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )
    parquet_path = (
        tmp_path
        / "dataset_type=spot_ohlcv"
        / "exchange=deribit"
        / "instrument_type=spot_ohlcv"
        / "symbol=BTCUSDT"
        / "timeframe=1m"
        / "month=2026-05"
        / "date=2026-05-01"
        / "data.parquet"
    )

    def _fake_fetch_all_task_groups(**kwargs: object):  # type: ignore[no-untyped-def]
        callback = kwargs.get("on_candle_task_chunk")
        if callable(callback):
            cast(Any, callback)(
                CandleFetchTaskDTO(exchange="deribit", market="spot_ohlcv", symbol="BTCUSDT", timeframe="1m"),
                [candle],
            )
        return FetchAllTaskGroupsResult(
            candle_results={("deribit", "spot_ohlcv", "BTCUSDT", "1m"): [candle]},
            candle_errors={},
            open_interest_results={},
            open_interest_errors={},
            funding_results={},
            funding_errors={},
            volatility_results={},
            volatility_errors={},
            trade_results={},
            trade_errors={},
        )

    monkeypatch.setattr(loader_cmd, "SingleInstanceLock", _NoopLock)
    monkeypatch.setattr(loader_cmd, "_fetch_all_task_groups", _fake_fetch_all_task_groups)
    monkeypatch.setattr(
        loader_cmd,
        "persist_loader_outputs_dto",
        lambda **kwargs: PersistResultDTO([str(parquet_path)]),
    )
    monkeypatch.setattr(loader_cmd, "ensure_bronze_sidecars", lambda **kwargs: [])

    args = argparse.Namespace(
        exchange="deribit",
        exchanges=None,
        market=["spot_ohlcv"],
        symbols=["BTCUSDT"],
        perp_trade_symbols=["BTC"],
        options_trade_symbols=["BTC"],
        save_parquet_lake=True,
        lake_root=str(tmp_path),
        no_json_output=False,
        tail_delta_only=True,
    )
    logger = logging.getLogger("test_bronze_build")
    loader_cmd.run_bronze_build(args=args, logger=logger)

    payload = json.loads(capsys.readouterr().out)
    assert payload["_parquet_files"] == [str(parquet_path)]
    assert payload["_manifest_files"] == [str(parquet_path.with_suffix(".json").resolve())]
    assert payload["_plot_files"] == [str(parquet_path.with_suffix(".png").resolve())]


def test_run_bronze_build_persists_trade_chunks_incrementally(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bronze trade fetches should persist chunks for both perp and option datasets."""
    monkeypatch.chdir(tmp_path)

    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    perp_tick = TradeTick(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        instrument_type="perp",
        trade_id="perp-1",
        trade_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        price=100.0,
        quantity=1.0,
        side="buy",
        is_maker=False,
        source_endpoint="public_trades",
    )
    option_tick = OptionTradeTick(
        exchange="deribit",
        symbol="BTC",
        instrument_type="option",
        instrument_name="BTC-31DEC26-100000-C",
        expiry="31DEC26",
        strike=100000.0,
        option_type="call",
        trade_id="option-1",
        trade_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        price=0.01,
        quantity=2.0,
        side="sell",
        is_maker=True,
        source_endpoint="public_options_trades",
    )
    persisted_markets: list[str] = []

    def _fake_fetch_all_task_groups(**kwargs: object):  # type: ignore[no-untyped-def]
        callback = kwargs.get("on_trade_task_chunk")
        assert callable(callback)
        cast(Any, callback)(TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="BTC"), [perp_tick])
        cast(Any, callback)(TradeFetchTaskDTO(exchange="deribit", market="option", symbol="BTC"), [option_tick])
        return FetchAllTaskGroupsResult(
            candle_results={},
            candle_errors={},
            open_interest_results={},
            open_interest_errors={},
            funding_results={},
            funding_errors={},
            volatility_results={},
            volatility_errors={},
            trade_results={("deribit", "perp", "BTC"): [perp_tick], ("deribit", "option", "BTC"): [option_tick]},
            trade_errors={},
        )

    def _fake_persist_loader_outputs_dto(**kwargs: object) -> PersistResultDTO:
        storage = cast(Any, kwargs["storage"])
        market = next(iter(storage.trades))
        persisted_markets.append(str(market))
        return PersistResultDTO([str(tmp_path / f"{market}.parquet")])

    monkeypatch.setattr(loader_cmd, "SingleInstanceLock", _NoopLock)
    monkeypatch.setattr(loader_cmd, "_fetch_all_task_groups", _fake_fetch_all_task_groups)
    monkeypatch.setattr(loader_cmd, "persist_loader_outputs_dto", _fake_persist_loader_outputs_dto)
    monkeypatch.setattr(loader_cmd, "ensure_bronze_sidecars", lambda **kwargs: [])

    args = argparse.Namespace(
        exchange="deribit",
        exchanges=None,
        market=["perps_trades", "options_trades"],
        symbols=["BTC"],
        perp_trade_symbols=["BTC"],
        options_trade_symbols=["BTC"],
        save_parquet_lake=True,
        lake_root=str(tmp_path),
        no_json_output=True,
        tail_delta_only=True,
    )

    loader_cmd.run_bronze_build(args=args, logger=logging.getLogger("test_trade_incremental_persist"))

    assert persisted_markets == ["perp", "option"]


def test_exchange_symbol_start_dates_override_symbol_and_global_bounds() -> None:
    original_context = loader_cmd._RUNTIME_ADAPTER.context
    try:
        loader_cmd._RUNTIME_ADAPTER.context = BronzeRuntimeBoundsContext(
            tail_delta_only=False,
            global_start_open_ms=1000,
            symbol_start_open_ms={"BTC": 2000},
            exchange_symbol_start_open_ms={"deribit:BTC": 3000},
        )
        assert loader_cmd._symbol_start_open_ms_bound(exchange="deribit", symbol="BTCUSDT") == 3000
        assert loader_cmd._symbol_start_open_ms_bound(exchange="deribit", symbol="ETHUSDT") == 1000
    finally:
        loader_cmd._RUNTIME_ADAPTER.context = original_context


def test_global_start_date_is_not_widened_by_older_symbol_bounds() -> None:
    original_context = loader_cmd._RUNTIME_ADAPTER.context
    try:
        loader_cmd._RUNTIME_ADAPTER.context = BronzeRuntimeBoundsContext(
            tail_delta_only=False,
            global_start_open_ms=3000,
            symbol_start_open_ms={"BTC": 1000},
            exchange_symbol_start_open_ms={},
        )
        assert loader_cmd._symbol_start_open_ms_bound(exchange="deribit", symbol="BTCUSDT") == 3000
    finally:
        loader_cmd._RUNTIME_ADAPTER.context = original_context


def test_symbol_start_bound_caps_to_today_in_tail_mode() -> None:
    original_context = loader_cmd._RUNTIME_ADAPTER.context
    try:
        loader_cmd._RUNTIME_ADAPTER.context = BronzeRuntimeBoundsContext(
            tail_delta_only=True,
            global_start_open_ms=None,
            symbol_start_open_ms={},
            exchange_symbol_start_open_ms={"deribit:BTC": 1000},
        )
        today_start_ms = int(datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        resolved = loader_cmd._symbol_start_open_ms_bound(exchange="deribit", symbol="BTCUSDT")
        assert resolved == today_start_ms
    finally:
        loader_cmd._RUNTIME_ADAPTER.context = original_context


def test_parse_exchange_symbol_start_dates_parses_canonical_pairs() -> None:
    parsed = loader_cmd._parse_exchange_symbol_start_dates(["deribit:BTCUSDT=2023-04-24", "DERIBIT:SOL=2024-02-27"])
    assert parsed["deribit:BTC"] == int(datetime(2023, 4, 24, 0, 0, tzinfo=UTC).timestamp() * 1000)
    assert parsed["deribit:SOL"] == int(datetime(2024, 2, 27, 0, 0, tzinfo=UTC).timestamp() * 1000)


def test_configure_bronze_start_bounds_sets_context() -> None:
    args = argparse.Namespace(
        start_date=None,
        symbol_start_dates=["BTC=2023-04-24"],
        exchange_symbol_start_dates=["deribit:SOL=2024-02-27"],
    )
    logger = logging.getLogger("test_bronze_bounds")

    original_context = loader_cmd._RUNTIME_ADAPTER.context
    try:
        loader_cmd._configure_bronze_start_bounds(args=args, logger=logger)
        assert loader_cmd._RUNTIME_ADAPTER.context.global_start_open_ms is None
        assert loader_cmd._RUNTIME_ADAPTER.context.symbol_start_open_ms["BTC"] == int(
            datetime(2023, 4, 24, 0, 0, tzinfo=UTC).timestamp() * 1000
        )
        assert loader_cmd._RUNTIME_ADAPTER.context.exchange_symbol_start_open_ms["deribit:SOL"] == int(
            datetime(2024, 2, 27, 0, 0, tzinfo=UTC).timestamp() * 1000
        )
    finally:
        loader_cmd._RUNTIME_ADAPTER.context = original_context


def test_run_bronze_build_drops_invalid_symbols_before_scheduling(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    scheduled_candle_tasks: list[tuple[str, str, str, str]] = []
    scheduled_open_interest_tasks: list[tuple[str, str, str]] = []
    scheduled_funding_tasks: list[tuple[str, str, str]] = []

    def _fake_fetch_all_task_groups(**kwargs: object):  # type: ignore[no-untyped-def]
        scheduled_candle_tasks.extend(cast(Any, kwargs["candle_tasks"]))
        scheduled_open_interest_tasks.extend(cast(Any, kwargs["open_interest_tasks"]))
        scheduled_funding_tasks.extend(cast(Any, kwargs["funding_tasks"]))
        return _empty_fetch_result()

    monkeypatch.setattr(loader_cmd, "SingleInstanceLock", _NoopLock)
    monkeypatch.setattr(loader_cmd, "_fetch_all_task_groups", _fake_fetch_all_task_groups)
    monkeypatch.setattr(loader_cmd, "ensure_bronze_sidecars", lambda **kwargs: [])

    args = argparse.Namespace(
        exchange="deribit",
        exchanges=None,
        market=["spot_ohlcv", "perps_ohlcv", "open_interest", "funding"],
        symbols=["BTC", None, " ", "\t", "ETH"],
        perp_trade_symbols=["BTC", None, " ", "\t", "ETH"],
        options_trade_symbols=["BTC", None, " ", "\t", "ETH"],
        save_parquet_lake=False,
        lake_root="lake/bronze",
        no_json_output=True,
        tail_delta_only=True,
    )
    logger = logging.getLogger("test_symbol_sanitization")
    loader_cmd.run_bronze_build(args=args, logger=logger)

    assert all(task[2] in {"BTC", "ETH"} for task in scheduled_candle_tasks)
    assert all(task[1] in {"BTC", "ETH"} for task in scheduled_open_interest_tasks)
    assert all(task[1] in {"BTC", "ETH"} for task in scheduled_funding_tasks)


def test_run_bronze_build_still_routes_through_monkeypatched_compat_bounds_functions(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """Old monkeypatch entrypoints for the bounds compat functions must keep working.

    ``run_bronze_build`` resolves its ``BronzeWorkflowDependencies`` via
    ``_bronze_workflow_dependencies()``, which reads ``_configure_bronze_start_bounds``
    and ``_current_runtime_bounds_context`` by module-level name at call time. Tests
    (and any external caller) that monkeypatch those names on ``loader_cmd`` directly
    must still have their replacements take effect, proving the adapter refactor did
    not close off the existing compatibility surface.
    """

    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    configure_calls: list[argparse.Namespace] = []
    fake_context = BronzeRuntimeBoundsContext(
        tail_delta_only=True,
        global_start_open_ms=None,
        symbol_start_open_ms={},
        exchange_symbol_start_open_ms={},
    )

    def _fake_configure_bronze_start_bounds(args: argparse.Namespace, logger: logging.Logger) -> None:
        del logger
        configure_calls.append(args)

    def _fake_current_runtime_bounds_context() -> BronzeRuntimeBoundsContext:
        return fake_context

    def _fake_fetch_all_task_groups(**kwargs: object):  # type: ignore[no-untyped-def]
        return _empty_fetch_result()

    monkeypatch.setattr(loader_cmd, "SingleInstanceLock", _NoopLock)
    monkeypatch.setattr(loader_cmd, "_configure_bronze_start_bounds", _fake_configure_bronze_start_bounds)
    monkeypatch.setattr(loader_cmd, "_current_runtime_bounds_context", _fake_current_runtime_bounds_context)
    monkeypatch.setattr(loader_cmd, "_fetch_all_task_groups", _fake_fetch_all_task_groups)
    monkeypatch.setattr(loader_cmd, "ensure_bronze_sidecars", lambda **kwargs: [])

    args = argparse.Namespace(
        exchange="deribit",
        exchanges=None,
        market=["spot_ohlcv"],
        symbols=["BTCUSDT"],
        perp_trade_symbols=["BTC"],
        options_trade_symbols=["BTC"],
        save_parquet_lake=False,
        lake_root="lake/bronze",
        no_json_output=True,
        tail_delta_only=True,
    )
    logger = logging.getLogger("test_compat_bounds_monkeypatch")
    loader_cmd.run_bronze_build(args=args, logger=logger)

    assert len(configure_calls) == 1
    assert configure_calls[0] is args


def test_run_bronze_build_raises_when_no_valid_symbols(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    monkeypatch.setattr(loader_cmd, "SingleInstanceLock", _NoopLock)

    args = argparse.Namespace(
        exchange="deribit",
        exchanges=None,
        market=["spot_ohlcv"],
        symbols=[None, "", "  "],
        perp_trade_symbols=["BTC"],
        options_trade_symbols=["BTC"],
        save_parquet_lake=False,
        lake_root="lake/bronze",
        no_json_output=True,
        tail_delta_only=True,
    )
    logger = logging.getLogger("test_symbol_sanitization_empty")
    with pytest.raises(ValueError, match="No valid symbols configured"):
        loader_cmd.run_bronze_build(args=args, logger=logger)


def test_run_bronze_build_uses_symbols_for_trade_tasks(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    scheduled_trade_tasks: list[tuple[str, str, str]] = []

    def _fake_fetch_all_task_groups(**kwargs: object):  # type: ignore[no-untyped-def]
        scheduled_trade_tasks.extend(cast(Any, kwargs["trade_tasks"]))
        return _empty_fetch_result()

    monkeypatch.setattr(loader_cmd, "SingleInstanceLock", _NoopLock)
    monkeypatch.setattr(loader_cmd, "_fetch_all_task_groups", _fake_fetch_all_task_groups)
    monkeypatch.setattr(loader_cmd, "ensure_bronze_sidecars", lambda **kwargs: [])

    args = argparse.Namespace(
        exchange="deribit",
        exchanges=None,
        market=["perps_trades", "options_trades"],
        symbols=["BTCUSDT", "ETHUSDT"],
        save_parquet_lake=False,
        lake_root="lake/bronze",
        no_json_output=True,
        tail_delta_only=True,
    )
    logger = logging.getLogger("test_trade_symbols")
    loader_cmd.run_bronze_build(args=args, logger=logger)

    assert ("deribit", "perp", "BTCUSDT") in scheduled_trade_tasks
    assert ("deribit", "perp", "ETHUSDT") in scheduled_trade_tasks
    assert ("deribit", "option", "BTCUSDT") in scheduled_trade_tasks
    assert ("deribit", "option", "ETHUSDT") in scheduled_trade_tasks


def test_sanitize_symbols_requires_list_input() -> None:
    with pytest.raises(ValueError, match="Symbols must be provided as a list"):
        loader_cmd._sanitize_symbols("BTC", logger=logging.getLogger("test"))  # type: ignore[arg-type]


def test_resolved_symbol_groups_sanitizes_and_sorts() -> None:
    args = argparse.Namespace(
        symbols=["BTC", " ", None, "ETH"],
    )

    ohlcv_symbols, perp_trade_symbols, options_trade_symbols = loader_cmd._resolved_symbol_groups(
        args=args,
        logger=logging.getLogger("test"),
    )

    assert ohlcv_symbols == ["BTC", "ETH"]
    assert perp_trade_symbols == ["BTC", "ETH"]
    assert options_trade_symbols == ["BTC", "ETH"]


def test_build_bronze_fetch_plan_is_deterministic_and_sorted() -> None:
    args = argparse.Namespace(
        exchange="deribit",
        exchanges=["deribit"],
        market=["funding", "spot_ohlcv", "open_interest", "perps_ohlcv"],
        symbols=["ETH", "BTC"],
        perp_trade_symbols=["ETH", "BTC"],
        options_trade_symbols=["SOL", "BTC"],
    )

    plan = loader_cmd._build_bronze_fetch_plan(args=args, logger=logging.getLogger("test"))

    assert plan.data_types == ["funding", "open_interest", "perps_ohlcv", "spot_ohlcv"]
    assert plan.symbols == ["BTC", "ETH"]
    assert plan.candle_tasks == [
        ("deribit", "perp", "BTC", "1m"),
        ("deribit", "spot_ohlcv", "BTC", "1m"),
        ("deribit", "perp", "ETH", "1m"),
        ("deribit", "spot_ohlcv", "ETH", "1m"),
    ]
    assert plan.open_interest_tasks == [("deribit", "BTC", "1m"), ("deribit", "ETH", "1m")]
    assert plan.funding_tasks == [("deribit", "BTC", "1m"), ("deribit", "ETH", "1m")]
    assert [(task.dataset_type, task.instrument_type) for task in plan.dataset_tasks] == [
        ("perps_ohlcv", "perp"),
        ("spot_ohlcv", "spot_ohlcv"),
        ("perps_ohlcv", "perp"),
        ("spot_ohlcv", "spot_ohlcv"),
        ("funding", "perp"),
        ("funding", "perp"),
        ("open_interest", "perp"),
        ("open_interest", "perp"),
    ]


def test_dataset_task_key_maps_use_registry_checkpoint_keys() -> None:
    args = argparse.Namespace(
        exchange="deribit",
        exchanges=["deribit"],
        market=["spot_ohlcv", "open_interest", "perps_trades"],
        symbols=["BTC"],
        perp_trade_symbols=["BTC"],
        options_trade_symbols=["BTC"],
    )
    plan = loader_cmd._build_bronze_fetch_plan(args=args, logger=logging.getLogger("test"))
    candle_map, open_interest_map, funding_map, trade_map = loader_cmd._dataset_task_key_maps(plan)

    assert candle_map[("deribit", "spot_ohlcv", "BTC", "1m")] == "deribit|spot_ohlcv|spot_ohlcv|BTC|1m|spot_ohlcv"
    assert open_interest_map[("deribit", "BTC", "1m")] == "deribit|open_interest|perp|BTC|1m|perp"
    assert ("deribit", "BTC", "1m") not in funding_map
    assert trade_map[("deribit", "perp", "BTC")] == "deribit|perps_trades|perp|BTC|tick|perp"


def test_run_bronze_build_resumes_from_checkpoint_and_clears_on_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(loader_cmd, "SingleInstanceLock", _NoopLock)
    monkeypatch.setattr(loader_cmd, "ensure_bronze_sidecars", lambda **kwargs: [])
    scheduled: list[tuple[str, str, str, str]] = []

    def _fake_fetch_all_task_groups(**kwargs: object):  # type: ignore[no-untyped-def]
        candle_tasks = cast(list[tuple[str, str, str, str]], kwargs["candle_tasks"])
        scheduled.extend(candle_tasks)
        rows = {task: [] for task in candle_tasks}
        return _empty_fetch_result(candle_results=rows)

    monkeypatch.setattr(loader_cmd, "_fetch_all_task_groups", _fake_fetch_all_task_groups)

    args = argparse.Namespace(
        exchange="deribit",
        exchanges=None,
        market=["spot_ohlcv"],
        symbols=["BTC", "ETH"],
        perp_trade_symbols=["BTC"],
        options_trade_symbols=["BTC"],
        save_parquet_lake=False,
        lake_root="lake/bronze",
        no_json_output=True,
        tail_delta_only=True,
    )
    plan = loader_cmd._build_bronze_fetch_plan(args=args, logger=logging.getLogger("test"))
    fingerprint = loader_cmd._bronze_checkpoint_fingerprint(args=args, plan=plan)
    checkpoint_path = loader_cmd._bronze_checkpoint_path()
    loader_cmd._write_bronze_checkpoint(
        checkpoint_path,
        fingerprint=fingerprint,
        completed={
            "candle": {"deribit|spot_ohlcv|BTC|1m"},
            "open_interest": set(),
            "funding": set(),
            "volatility_index_data": set(),
            "trade": set(),
        },
    )

    loader_cmd.run_bronze_build(args=args, logger=logging.getLogger("test"))

    assert ("deribit", "spot_ohlcv", "BTC", "1m") not in scheduled
    assert ("deribit", "spot_ohlcv", "ETH", "1m") in scheduled
    assert not checkpoint_path.exists()


def test_run_bronze_build_full_gap_fill_ignores_checkpoint_and_rescans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(loader_cmd, "SingleInstanceLock", _NoopLock)
    monkeypatch.setattr(loader_cmd, "ensure_bronze_sidecars", lambda **kwargs: [])
    scheduled: list[tuple[str, str, str, str]] = []

    def _fake_fetch_all_task_groups(**kwargs: object):  # type: ignore[no-untyped-def]
        candle_tasks = cast(list[tuple[str, str, str, str]], kwargs["candle_tasks"])
        scheduled.extend(candle_tasks)
        rows = {task: [] for task in candle_tasks}
        return _empty_fetch_result(candle_results=rows)

    monkeypatch.setattr(loader_cmd, "_fetch_all_task_groups", _fake_fetch_all_task_groups)

    args = argparse.Namespace(
        exchange="deribit",
        exchanges=None,
        market=["spot_ohlcv"],
        symbols=["BTC", "ETH"],
        perp_trade_symbols=["BTC"],
        options_trade_symbols=["BTC"],
        save_parquet_lake=False,
        lake_root="lake/bronze",
        no_json_output=True,
        tail_delta_only=False,
    )
    plan = loader_cmd._build_bronze_fetch_plan(args=args, logger=logging.getLogger("test"))
    fingerprint = loader_cmd._bronze_checkpoint_fingerprint(args=args, plan=plan)
    checkpoint_path = loader_cmd._bronze_checkpoint_path()
    loader_cmd._write_bronze_checkpoint(
        checkpoint_path,
        fingerprint=fingerprint,
        completed={
            "candle": {"deribit|spot_ohlcv|BTC|1m"},
            "open_interest": set(),
            "funding": set(),
            "volatility_index_data": set(),
            "trade": set(),
        },
    )

    loader_cmd.run_bronze_build(args=args, logger=logging.getLogger("test"))

    assert ("deribit", "spot_ohlcv", "BTC", "1m") in scheduled
    assert ("deribit", "spot_ohlcv", "ETH", "1m") in scheduled
    assert not checkpoint_path.exists()
