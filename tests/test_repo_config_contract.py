"""Repository-level config contract guards.

These tests validate the committed runtime configuration shape used by the
CLI and medallion pipeline script. They should fail fast when `config.yaml`
is accidentally replaced by unrelated local content.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _repo_config_path() -> Path:
    return _repo_root() / "config.example.yaml"


def _load_repo_config() -> dict[str, Any]:
    yaml = pytest.importorskip("yaml")
    config_path = _repo_config_path()
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict), "config.yaml must contain a top-level mapping"
    return loaded


def _load_pipeline_module():
    script_path = _repo_root() / "scripts" / "run_medallion_pipeline.py"
    spec = importlib.util.spec_from_file_location("run_medallion_pipeline", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_repo_config_has_required_top_level_sections() -> None:
    config = _load_repo_config()
    required = {"env", "export-descriptive-stats", "bronze-build", "medallion-pipeline"}
    missing = sorted(required.difference(config))
    assert not missing, f"config.yaml missing required section(s): {', '.join(missing)}"


def test_repo_config_caps_polars_threads() -> None:
    config = _load_repo_config()
    env_config = config["env"]
    assert isinstance(env_config, dict)
    assert env_config["POLARS_MAX_THREADS"] == 4


def test_repo_config_uses_only_symbol_start_dates() -> None:
    config = _load_repo_config()
    assert "global" not in config

    bronze_cfg = config["bronze-build"]
    assert "start_date" not in bronze_cfg
    assert "exchange_symbol_start_dates" not in bronze_cfg
    assert bronze_cfg["symbol_start_dates"] == [
        "BTC=2018-08-14",
        "ETH=2019-03-14",
        "SOL=2022-03-15",
    ]


def test_repo_config_medallion_pipeline_contract() -> None:
    config = _load_repo_config()
    pipeline_cfg = config["medallion-pipeline"]
    assert isinstance(pipeline_cfg, dict)
    order = pipeline_cfg.get("execution_order")
    assert isinstance(order, list) and order, "medallion-pipeline.execution_order must be a non-empty list"

    allowed = {"bronze", "silver", "gold"}
    unknown = [str(name) for name in order if str(name) not in allowed]
    assert not unknown, f"Unsupported medallion-pipeline layer(s): {unknown}"

    for layer in order:
        layer_name = str(layer)
        layer_cfg = pipeline_cfg.get(layer_name)
        assert isinstance(layer_cfg, dict), f"medallion-pipeline.{layer_name} must be a mapping"
        if not bool(layer_cfg.get("enabled", True)):
            continue
        command = layer_cfg.get("command")
        assert isinstance(command, str) and command.strip(), f"medallion-pipeline.{layer_name}.command is required"
        cli_args = layer_cfg.get("cli_args", [])
        assert isinstance(cli_args, list), f"medallion-pipeline.{layer_name}.cli_args must be a list"


def test_repo_config_builds_pipeline_steps() -> None:
    module = _load_pipeline_module()
    config = _load_repo_config()
    main_path = _repo_root() / "main.py"
    config_path = _repo_config_path()
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=config)
    assert steps, "Expected at least one enabled medallion pipeline step"
    for step in steps:
        assert step.args, f"Pipeline step '{step.name}' must include command args"


def test_repo_config_medallion_bronze_inherits_symbol_start_bounds() -> None:
    module = _load_pipeline_module()
    config = _load_repo_config()
    main_path = _repo_root() / "main.py"
    config_path = _repo_config_path()
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=config)
    bronze_step = next(step for step in steps if step.name == "bronze")

    assert "--start-date" not in bronze_step.args
    assert "--exchange-symbol-start-dates" not in bronze_step.args

    symbol_idx = bronze_step.args.index("--symbol-start-dates")
    expected_symbol_dates = config["bronze-build"]["symbol_start_dates"]
    assert bronze_step.args[symbol_idx + 1 : symbol_idx + 1 + len(expected_symbol_dates)] == expected_symbol_dates


def test_repo_config_medallion_bronze_uses_tail_delta_mode() -> None:
    module = _load_pipeline_module()
    config = _load_repo_config()
    main_path = _repo_root() / "main.py"
    config_path = _repo_config_path()
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=config)
    bronze_step = next(step for step in steps if step.name == "bronze")

    assert "--tail-delta-only" in bronze_step.args
    assert "--full-gap-fill" not in bronze_step.args


def test_repo_medallion_schedule_builds_only_history_datasets() -> None:
    module = _load_pipeline_module()
    config = _load_repo_config()
    steps = module._build_steps(
        main_path=_repo_root() / "main.py",
        config_path=_repo_config_path(),
        config_data=config,
    )
    silver_step = next(step for step in steps if step.name == "silver")
    gold_step = next(step for step in steps if step.name == "gold")

    silver_dataset_index = silver_step.args.index("--dataset")
    silver_datasets = []
    for value in silver_step.args[silver_dataset_index + 1 :]:
        if value.startswith("--"):
            break
        silver_datasets.append(value)
    live_silver_datasets = {
        "index_price_snapshot_1m",
        "futures_summary_snapshot_1m",
        "options_ticker_snapshot_1m",
        "options_instrument_ticker_snapshot_1m",
        "options_surface_1m_feature",
        "perps_l2_snapshot_1m",
        "options_l2_snapshot_1m",
        "recent_trade_snapshot_1m",
        "instrument_metadata_snapshot_daily",
        "futures_instrument_metadata_snapshot_daily",
    }
    assert live_silver_datasets.isdisjoint(silver_datasets)
    assert {"spot_ohlcv", "perps_ohlcv", "perps_trades", "options_trades"}.issubset(silver_datasets)
    assert "historical_prediction" in silver_datasets

    exclusion_index = gold_step.args.index("--exclude-dataset-id")
    excluded_dataset_ids = []
    for value in gold_step.args[exclusion_index + 1 :]:
        if value.startswith("--"):
            break
        excluded_dataset_ids.append(value)
    assert "gold.history.extended_full.m1" in excluded_dataset_ids
    history_dataset_ids = {
        "gold.history.extended.m1",
        "gold.history.extended.m5",
        "gold.history.extended.m30",
        "gold.history.extended.h1",
    }
    assert history_dataset_ids.isdisjoint(excluded_dataset_ids)
    live_dataset_ids = {
        "gold.live.full.m1",
        "gold.live.full.m5",
        "gold.live.full.m30",
        "gold.live.full.h1",
        "gold.live.extended.m1",
        "gold.live.extended.m5",
        "gold.live.extended.m30",
        "gold.live.extended.h1",
    }
    assert live_dataset_ids.issubset(excluded_dataset_ids)
