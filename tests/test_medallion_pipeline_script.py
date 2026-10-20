"""Integration-like tests for medallion pipeline script config behavior."""

from __future__ import annotations

import importlib.util
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest


def _load_pipeline_module() -> Any:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_medallion_pipeline.py"
    spec = importlib.util.spec_from_file_location("run_medallion_pipeline", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_steps_uses_configured_market_args_with_trades(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "medallion-pipeline": {
            "execution_order": ["bronze", "silver", "gold"],
            "bronze": {
                "enabled": True,
                "command": "bronze-build",
                "cli_args": ["--dataset", "spot_ohlcv", "perps_ohlcv", "open_interest", "funding", "perps_trades"],
            },
            "silver": {"enabled": False, "command": "silver-build", "cli_args": []},
            "gold": {"enabled": False, "command": "gold-build", "cli_args": []},
        }
    }
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=cfg)
    assert len(steps) == 1
    args = steps[0].args
    assert "--dataset" in args
    assert "perps_trades" in args
    assert "volatility_index_data" in args
    assert "--tail-delta-only" not in args


def test_build_steps_preserves_bronze_delta_mode_from_config(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "medallion-pipeline": {
            "execution_order": ["bronze"],
            "bronze": {
                "enabled": True,
                "command": "bronze-build",
                "cli_args": ["--dataset", "spot_ohlcv", "--tail-delta-only"],
            },
        }
    }

    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=cfg)
    args = steps[0].args

    assert "--tail-delta-only" in args
    assert "--full-gap-fill" not in args


def test_build_steps_splits_trade_tail_mode_into_minute_gap_step(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "medallion-pipeline": {
            "execution_order": ["bronze"],
            "bronze": {
                "enabled": True,
                "command": "bronze-build",
                "cli_args": [
                    "--dataset",
                    "spot_ohlcv",
                    "perps_trades",
                    "options_trades",
                    "--symbols",
                    "BTC",
                    "--tail-delta-only",
                    "--no-json-output",
                ],
            },
        }
    }

    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=cfg)

    assert [step.name for step in steps] == ["bronze-trades-minute-gap", "bronze"]
    gap_args = steps[0].args
    bronze_args = steps[1].args
    gap_dataset_idx = gap_args.index("--dataset")
    bronze_dataset_idx = bronze_args.index("--dataset")
    assert gap_args[gap_dataset_idx + 1 : gap_args.index("--symbols")] == ["perps_trades", "options_trades"]
    assert "--full-gap-fill" in gap_args
    assert "--tail-delta-only" not in gap_args
    bronze_datasets = bronze_args[bronze_dataset_idx + 1 : bronze_args.index("--symbols")]
    assert "perps_trades" not in bronze_datasets
    assert "options_trades" not in bronze_datasets
    assert "--tail-delta-only" in bronze_args


def test_build_steps_splits_options_only_tail_mode_into_minute_gap_step(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "medallion-pipeline": {
            "execution_order": ["bronze"],
            "bronze": {
                "enabled": True,
                "command": "bronze-build",
                "cli_args": [
                    "--dataset",
                    "options_trades",
                    "--symbols",
                    "BTC",
                    "--tail-delta-only",
                    "--no-json-output",
                ],
            },
        }
    }

    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=cfg)

    assert [step.name for step in steps] == ["bronze-trades-minute-gap", "bronze"]
    gap_args = steps[0].args
    bronze_args = steps[1].args
    gap_dataset_idx = gap_args.index("--dataset")
    bronze_dataset_idx = bronze_args.index("--dataset")
    assert gap_args[gap_dataset_idx + 1 : gap_args.index("--symbols")] == ["options_trades"]
    assert "--full-gap-fill" in gap_args
    assert "--tail-delta-only" not in gap_args
    assert bronze_args[bronze_dataset_idx + 1 : bronze_args.index("--symbols")] == ["volatility_index_data"]
    assert "--tail-delta-only" in bronze_args


def test_build_steps_adds_volatility_to_silver_dataset_args(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "medallion-pipeline": {
            "execution_order": ["silver"],
            "reuse-existing-inputs": {"enabled": True, "max_age_seconds": 60},
            "silver": {
                "enabled": True,
                "command": "silver-build",
                "cli_args": ["--dataset", "spot_ohlcv", "perps_ohlcv", "open_interest", "funding"],
            },
        }
    }
    steps = module._build_steps(
        main_path=main_path,
        config_path=config_path,
        config_data=cfg,
        freshness_checker=lambda _layer, _root, _max_age: True,
    )
    assert len(steps) == 1
    assert "volatility_index_data" in steps[0].args


def test_build_steps_bronze_inherits_symbol_start_bounds_from_bronze_config(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "bronze-build": {
            "symbol_start_dates": ["BTC=2018-08-14", "ETH=2019-03-14", "SOL=2022-03-15"],
        },
        "medallion-pipeline": {
            "execution_order": ["bronze"],
            "bronze": {
                "enabled": True,
                "command": "bronze-build",
                "cli_args": ["--dataset", "spot_ohlcv", "perps_ohlcv"],
            },
        },
    }
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=cfg)
    assert len(steps) == 1
    assert "--start-date" not in steps[0].args
    assert "--exchange-symbol-start-dates" not in steps[0].args
    sym_idx = steps[0].args.index("--symbol-start-dates")
    assert steps[0].args[sym_idx + 1 : sym_idx + 4] == [
        "BTC=2018-08-14",
        "ETH=2019-03-14",
        "SOL=2022-03-15",
    ]


def test_build_steps_bronze_preserves_explicit_medallion_start_bounds(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "bronze-build": {
            "start_date": "2018-08-14",
            "symbol_start_dates": ["BTC=2018-08-14"],
            "exchange_symbol_start_dates": ["deribit:SOL=2022-03-15"],
        },
        "medallion-pipeline": {
            "execution_order": ["bronze"],
            "bronze": {
                "enabled": True,
                "command": "bronze-build",
                "cli_args": [
                    "--start-date",
                    "2020-01-01",
                    "--symbol-start-dates",
                    "BTC=2021-01-01",
                    "ETH=2026-05-10",
                    "--exchange-symbol-start-dates",
                    "deribit:SOL=2020-01-01",
                ],
            },
        },
    }
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=cfg)
    args = steps[0].args
    start_idx = args.index("--start-date")
    assert args[start_idx + 1] == "2020-01-01"
    sym_idx = args.index("--symbol-start-dates")
    assert args[sym_idx + 1] == "BTC=2021-01-01"
    assert args[sym_idx + 2] == "ETH=2026-05-10"
    ex_idx = args.index("--exchange-symbol-start-dates")
    assert args[ex_idx + 1] == "deribit:SOL=2020-01-01"


def test_log_path_from_config_prefers_explicit_log_file(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    cfg = {"env": {"DEPTH_SYNC_LOG_FILE": str(tmp_path / "logs" / "pipeline.log")}}
    out = module._log_path_from_config(config_data=cfg, repo_root=tmp_path)
    assert out == (tmp_path / "logs" / "run-medallion-pipeline.log").resolve()


def test_build_steps_validation_errors(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="config missing required section"):
        module._build_steps(main_path=main_path, config_path=config_path, config_data={})
    with pytest.raises(ValueError, match="execution_order must be a non-empty list"):
        module._build_steps(main_path=main_path, config_path=config_path, config_data={"medallion-pipeline": {}})
    with pytest.raises(ValueError, match="Unsupported pipeline layer"):
        module._build_steps(
            main_path=main_path,
            config_path=config_path,
            config_data={"medallion-pipeline": {"execution_order": ["x"]}},
        )


@pytest.mark.parametrize(
    "execution_order",
    [["gold", "bronze"], ["silver", "bronze", "gold"], ["bronze", "bronze", "silver"]],
)
def test_build_steps_rejects_invalid_dependency_order(tmp_path: Path, execution_order: list[str]) -> None:
    module = _load_pipeline_module()
    pipeline = {
        "execution_order": execution_order,
        "bronze": {"enabled": True, "command": "bronze-build", "cli_args": []},
        "silver": {"enabled": True, "command": "silver-build", "cli_args": []},
        "gold": {"enabled": True, "command": "gold-build", "cli_args": []},
    }

    with pytest.raises(ValueError, match="duplicate|bronze -> silver -> gold"):
        module._build_steps(
            main_path=tmp_path / "main.py",
            config_path=tmp_path / "config.yaml",
            config_data={"medallion-pipeline": pipeline},
        )


def test_build_steps_requires_verified_reuse_for_disabled_prerequisite(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    pipeline = {
        "execution_order": ["bronze", "silver"],
        "bronze": {"enabled": False, "command": "bronze-build", "cli_args": []},
        "silver": {"enabled": True, "command": "silver-build", "cli_args": []},
    }

    with pytest.raises(ValueError, match="reuse-existing-inputs"):
        module._build_steps(
            main_path=tmp_path / "main.py",
            config_path=tmp_path / "config.yaml",
            config_data={"medallion-pipeline": pipeline},
        )

    pipeline["reuse-existing-inputs"] = {"enabled": True, "max_age_seconds": 60}
    with pytest.raises(ValueError, match="missing or stale"):
        module._build_steps(
            main_path=tmp_path / "main.py",
            config_path=tmp_path / "config.yaml",
            config_data={"medallion-pipeline": pipeline},
            freshness_checker=lambda _layer, _root, _max_age: False,
        )

    steps = module._build_steps(
        main_path=tmp_path / "main.py",
        config_path=tmp_path / "config.yaml",
        config_data={"medallion-pipeline": pipeline},
        freshness_checker=lambda _layer, _root, _max_age: True,
    )
    assert [step.name for step in steps] == ["silver"]


def test_build_steps_layer_validation_errors(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    base = {
        "medallion-pipeline": {
            "execution_order": ["bronze"],
            "bronze": {"enabled": True, "command": "bronze-build", "cli_args": []},
        }
    }

    bad_layer = {"medallion-pipeline": {"execution_order": ["bronze"], "bronze": []}}
    with pytest.raises(ValueError, match="must be a mapping"):
        module._build_steps(main_path=main_path, config_path=config_path, config_data=bad_layer)

    bad_cmd = {"medallion-pipeline": {"execution_order": ["bronze"], "bronze": {"enabled": True, "cli_args": []}}}
    with pytest.raises(ValueError, match="command is required"):
        module._build_steps(main_path=main_path, config_path=config_path, config_data=bad_cmd)

    bad_args = {
        "medallion-pipeline": {
            "execution_order": ["bronze"],
            "bronze": {"enabled": True, "command": "x", "cli_args": "bad"},
        }
    }
    with pytest.raises(ValueError, match="cli_args must be a list"):
        module._build_steps(main_path=main_path, config_path=config_path, config_data=bad_args)

    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=base)
    assert len(steps) == 1


def test_log_path_from_config_dir_and_default(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    cfg = {"env": {"DEPTH_SYNC_LOG_DIR": str(tmp_path / "xlogs")}}
    out = module._log_path_from_config(config_data=cfg, repo_root=tmp_path)
    assert out == (tmp_path / "xlogs" / "crypto-loader.log").resolve()
    out_default = module._log_path_from_config(config_data={}, repo_root=tmp_path)
    assert out_default == (tmp_path / ".run" / "logs" / "crypto-loader.log").resolve()


def test_log_path_from_config_anchors_relative_dir_to_repo_root(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    cfg = {"env": {"DEPTH_SYNC_LOG_DIR": ".logs"}}

    out = module._log_path_from_config(config_data=cfg, repo_root=tmp_path)

    assert out == (tmp_path / ".logs" / "crypto-loader.log").resolve()


def test_default_config_path_prefers_runtime_cron_config(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    runtime_config = tmp_path / ".run" / "cron-config.yaml"
    runtime_config.parent.mkdir()
    runtime_config.write_text("x: 1\n", encoding="utf-8")

    assert module._default_config_path(tmp_path) == runtime_config


def test_default_config_path_falls_back_to_repo_config(tmp_path: Path) -> None:
    module = _load_pipeline_module()

    assert module._default_config_path(tmp_path) == tmp_path / "config.yaml"


def test_apply_env_from_config_exports_postgres_runtime_values(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_pipeline_module()
    for key in ("PGHOST", "PGPORT", "PGUSER", "PGDATABASE", "PGPASSWORD"):
        monkeypatch.delenv(key, raising=False)

    module._apply_env_from_config(
        {
            "env": {
                "PGHOST": "10.10.1.3",
                "PGPORT": 54321,
                "PGUSER": "crypto-loader",
                "PGDATABASE": "postgres",
                "PGPASSWORD": "local-only",
            }
        }
    )

    assert module.os.environ["PGHOST"] == "10.10.1.3"
    assert module.os.environ["PGPORT"] == "54321"
    assert module.os.environ["PGUSER"] == "crypto-loader"
    assert module.os.environ["PGDATABASE"] == "postgres"
    assert module.os.environ["PGPASSWORD"] == "local-only"


def test_apply_env_from_config_falls_back_to_protected_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_pipeline_module()
    fallback_config = tmp_path / "config.yaml"
    fallback_config.write_text(
        "env:\n"
        "  PGHOST: 10.10.1.3\n"
        "  PGPORT: 54321\n"
        "  PGUSER: crypto-loader\n"
        "  PGDATABASE: postgres\n"
        "  PGPASSWORD: protected-secret\n",
        encoding="utf-8",
    )
    for key in ("PGHOST", "PGPORT", "PGUSER", "PGDATABASE", "PGPASSWORD"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("PGUSER", "shell-user")

    module._apply_env_from_config(
        {"env": {"DEPTH_SYNC_LOG_DIR": ".logs"}},
        fallback_config_path=fallback_config,
    )

    assert module.os.environ["PGHOST"] == "10.10.1.3"
    assert module.os.environ["PGPORT"] == "54321"
    assert module.os.environ["PGUSER"] == "shell-user"
    assert module.os.environ["PGDATABASE"] == "postgres"
    assert module.os.environ["PGPASSWORD"] == "protected-secret"


def test_lock_acquire_and_release(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    lock_file = tmp_path / "lock" / "x.lock"
    fd = module._acquire_nonblocking_lock(lock_file)
    assert isinstance(fd, int)
    try:
        assert module._acquire_nonblocking_lock(lock_file) is None
    finally:
        module._release_lock(fd)
    assert module._acquire_nonblocking_lock(lock_file) is not None


def test_rotate_pipeline_log_rotates_and_prunes(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    log_path = tmp_path / "pipeline.log"
    log_path.write_text("a\n", encoding="utf-8")
    today = datetime.now(UTC).date()
    rotated_date = today.fromordinal(today.toordinal() - 1)
    archived_date = today.fromordinal(today.toordinal() - 6)
    stale_date = today.fromordinal(today.toordinal() - 120)
    os.utime(log_path, (datetime.combine(rotated_date, datetime.min.time(), UTC).timestamp(),) * 2)
    for days_ago in range(2, 7):
        daily_date = today.fromordinal(today.toordinal() - days_ago)
        (tmp_path / f"pipeline.log.{daily_date.isoformat()}").write_text("x\n", encoding="utf-8")
    stale = tmp_path / f"pipeline.log.{stale_date.isoformat()}.gz"
    stale.write_bytes(b"stale")
    module._rotate_pipeline_log(log_path)
    assert not log_path.exists()
    assert (tmp_path / f"pipeline.log.{rotated_date.isoformat()}").exists()
    assert (tmp_path / f"pipeline.log.{archived_date.isoformat()}.gz").exists()
    assert not stale.exists()


def test_main_missing_paths_returns_2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_pipeline_module()
    config_path = tmp_path / "config.yaml"
    main_path = tmp_path / "main.py"

    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "repo_root": str(tmp_path),
                "config": str(config_path),
                "main_path": str(main_path),
                "lock_file": str(tmp_path / "lock" / "x.lock"),
                "python_bin": sys.executable,
                "log_file": None,
            },
        )(),
    )
    assert module.main() == 2
    config_path.write_text("medallion-pipeline: {}\n", encoding="utf-8")
    assert module.main() == 2


def test_main_lock_already_running_returns_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_pipeline_module()
    config_path = tmp_path / "config.yaml"
    main_path = tmp_path / "main.py"
    config_path.write_text(
        "medallion-pipeline:\n  execution_order: [bronze]\n  bronze:\n    enabled: false\n", encoding="utf-8"
    )
    main_path.write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "repo_root": str(tmp_path),
                "config": str(config_path),
                "main_path": str(main_path),
                "lock_file": str(tmp_path / "lock" / "x.lock"),
                "python_bin": sys.executable,
                "log_file": None,
            },
        )(),
    )
    monkeypatch.setattr(module, "_acquire_nonblocking_lock", lambda _p: None)
    assert module.main() == 1


def test_main_success_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_pipeline_module()
    config_path = tmp_path / "config.yaml"
    main_path = tmp_path / "main.py"
    log_path = tmp_path / "custom.log"
    lock_file = tmp_path / "lock" / "x.lock"
    config_path.write_text(
        "medallion-pipeline:\n  execution_order: [bronze]\n  bronze:\n    enabled: false\n",
        encoding="utf-8",
    )
    main_path.write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "repo_root": str(tmp_path),
                "config": str(config_path),
                "main_path": str(main_path),
                "lock_file": str(lock_file),
                "python_bin": sys.executable,
                "log_file": str(log_path),
            },
        )(),
    )
    monkeypatch.setattr(module, "_acquire_nonblocking_lock", lambda _p: 123)
    monkeypatch.setattr(module, "_release_lock", lambda _fd: None)
    monkeypatch.setattr(module, "_run_pipeline", lambda **kwargs: None)
    assert module.main() == 0
    assert log_path.exists()
