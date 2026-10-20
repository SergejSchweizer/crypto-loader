"""Run Bronze, Silver, Gold, and PostgreSQL Gold sync sequentially for cron automation."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from application.services.medallion_freshness import audit_gold_history_freshness
from application.services.runtime_service import LOG_ARCHIVE_RETENTION_DAYS, enforce_log_retention

_POSTGRES_ENV_KEYS = ("PGHOST", "PGPORT", "PGUSER", "PGDATABASE", "PGPASSWORD")
_POSTGRES_CONFIG_ALIASES = {
    "host": "PGHOST",
    "port": "PGPORT",
    "user": "PGUSER",
    "database": "PGDATABASE",
    "password": "PGPASSWORD",
}


@dataclass(frozen=True)
class PipelineStep:
    """One CLI command step in the medallion pipeline."""

    name: str
    args: list[str]


def _utc_ts() -> str:
    """Return current UTC timestamp in ISO-like format."""

    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log_line(message: str) -> None:
    """Write one timestamped log line."""

    print(f"[{_utc_ts()}] {message}", flush=True)


def _default_repo_root() -> Path:
    """Return repository root inferred from this script location."""

    return Path(__file__).resolve().parents[1]


def _default_config_path(repo_root: Path) -> Path:
    """Return the safest default config path for unattended pipeline runs."""

    runtime_config = repo_root / ".run" / "cron-config.yaml"
    if runtime_config.exists():
        return runtime_config
    return repo_root / "config.yaml"


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML mapping from config path."""

    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required to load pipeline config.") from exc

    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError("config file must contain a top-level mapping")
    return loaded


def _apply_env_from_config(
    config_data: dict[str, Any],
    *,
    fallback_config_path: Path | None = None,
) -> None:
    """Export configured runtime values, using protected config.yaml for missing PG values."""

    env_config = config_data.get("env")
    if isinstance(env_config, dict):
        for raw_key, value in env_config.items():
            if value is not None:
                os.environ[str(raw_key)] = str(value)

    if fallback_config_path is None or not fallback_config_path.is_file():
        return

    fallback_config = _load_yaml(fallback_config_path)
    fallback_env = fallback_config.get("env")
    postgres_values = {
        key: str(fallback_env[key])
        for key in _POSTGRES_ENV_KEYS
        if isinstance(fallback_env, dict) and fallback_env.get(key) is not None and str(fallback_env[key]).strip()
    }
    postgres_config = fallback_config.get("postgres-sync")
    if isinstance(postgres_config, dict):
        for raw_key, env_key in _POSTGRES_CONFIG_ALIASES.items():
            value = postgres_config.get(raw_key)
            if env_key not in postgres_values and value is not None and str(value).strip():
                postgres_values[env_key] = str(value)

    for key, value in postgres_values.items():
        if not os.environ.get(key, "").strip():
            os.environ[key] = value


def _build_steps(
    *,
    main_path: Path,
    config_path: Path,
    config_data: dict[str, Any],
    freshness_checker: Callable[[str, Path, int], bool] | None = None,
) -> list[PipelineStep]:
    """Build pipeline command sequence and append sync directly after enabled Gold."""

    pipeline_cfg = config_data.get("medallion-pipeline")
    if not isinstance(pipeline_cfg, dict):
        raise ValueError("config missing required section: medallion-pipeline")

    order_raw = pipeline_cfg.get("execution_order")
    if not isinstance(order_raw, list) or not order_raw:
        raise ValueError("medallion-pipeline.execution_order must be a non-empty list")
    execution_order = [str(name).strip() for name in order_raw if str(name).strip()]
    valid_layers = {"bronze", "silver", "gold"}
    invalid = [name for name in execution_order if name not in valid_layers]
    if invalid:
        raise ValueError(
            f"Unsupported pipeline layer(s) in execution_order: {invalid}. Allowed: {sorted(valid_layers)}"
        )
    if execution_order.count("gold") > 1:
        raise ValueError("medallion-pipeline.execution_order may contain Gold at most once")
    if len(execution_order) != len(set(execution_order)):
        raise ValueError("medallion-pipeline.execution_order must not contain duplicate layers")
    canonical_order = {"bronze": 0, "silver": 1, "gold": 2}
    if execution_order != sorted(execution_order, key=canonical_order.__getitem__):
        raise ValueError("medallion-pipeline.execution_order must preserve bronze -> silver -> gold")

    checker = freshness_checker or _inputs_are_fresh
    _validate_disabled_prerequisites(pipeline_cfg, execution_order, main_path.parent, checker)

    postgres_cfg = pipeline_cfg.get("postgres-sync", {})
    if not isinstance(postgres_cfg, dict):
        raise ValueError("medallion-pipeline.postgres-sync must be a mapping")
    serving_deprecation_policy = str(postgres_cfg.get("serving_deprecation_policy", "retain")).strip()
    if serving_deprecation_policy != "retain":
        raise ValueError(
            "medallion-pipeline.postgres-sync.serving_deprecation_policy must be 'retain'; "
            "serving deletion requires a separately implemented explicit policy"
        )
    publication_result_value = str(postgres_cfg.get("publication_result", ".run/gold-publication-result.json")).strip()
    if not publication_result_value:
        raise ValueError("medallion-pipeline.postgres-sync.publication_result must not be empty")
    publication_result = Path(publication_result_value)
    if not publication_result.is_absolute():
        publication_result = main_path.parent / publication_result

    steps: list[PipelineStep] = []
    for layer_name in execution_order:
        layer_cfg = pipeline_cfg.get(layer_name)
        if not isinstance(layer_cfg, dict):
            raise ValueError(f"medallion-pipeline.{layer_name} must be a mapping")
        enabled = bool(layer_cfg.get("enabled", True))
        if not enabled:
            continue

        command = str(layer_cfg.get("command", "")).strip()
        if not command:
            raise ValueError(f"medallion-pipeline.{layer_name}.command is required")

        cli_args_raw = layer_cfg.get("cli_args", [])
        if not isinstance(cli_args_raw, list):
            raise ValueError(f"medallion-pipeline.{layer_name}.cli_args must be a list")
        cli_args = [str(token) for token in cli_args_raw]
        skip_layer_step = False
        if layer_name == "bronze" and command == "bronze-build":
            cli_args = _apply_bronze_start_defaults(cli_args=cli_args, config_data=config_data)
            cli_args = _ensure_volatility_dataset_arg(cli_args)
            trade_gap_args, cli_args = _split_trade_minute_gap_args(cli_args)
            if trade_gap_args:
                cmd = [str(main_path), "--config", str(config_path), command, *trade_gap_args]
                steps.append(PipelineStep(name="bronze-trades-minute-gap", args=cmd))
                skip_layer_step = not cli_args
        if layer_name == "silver" and command == "silver-build":
            cli_args = _ensure_volatility_dataset_arg(cli_args)
        if layer_name == "gold" and command == "gold-build" and "--publication-result" not in cli_args:
            cli_args.extend(["--publication-result", str(publication_result)])

        if not skip_layer_step:
            cmd = [str(main_path), "--config", str(config_path), command, *cli_args]
            steps.append(PipelineStep(name=layer_name, args=cmd))
            if layer_name == "gold":
                sync_cmd = [
                    str(main_path),
                    "--config",
                    str(config_path),
                    "gold-sync-postgres",
                    "--gold-root",
                    "lake/gold",
                    "--publication-result",
                    str(publication_result),
                ]
                steps.append(PipelineStep(name="postgres-gold-sync", args=sync_cmd))
    return steps


def _validate_disabled_prerequisites(
    pipeline_cfg: dict[str, Any],
    execution_order: list[str],
    repo_root: Path,
    freshness_checker: Callable[[str, Path, int], bool],
) -> None:
    """Require explicit and fresh existing inputs when a prerequisite layer is disabled."""

    enabled = {
        layer: bool(pipeline_cfg.get(layer, {}).get("enabled", True))
        for layer in ("bronze", "silver", "gold")
        if isinstance(pipeline_cfg.get(layer), dict)
    }
    required_reuse: list[str] = []
    if enabled.get("silver", False) and not enabled.get("bronze", False):
        required_reuse.append("bronze")
    if enabled.get("gold", False) and not enabled.get("silver", False):
        required_reuse.append("silver")
    if not required_reuse:
        return

    reuse_cfg = pipeline_cfg.get("reuse-existing-inputs")
    if not isinstance(reuse_cfg, dict) or not bool(reuse_cfg.get("enabled", False)):
        raise ValueError("disabled prerequisite layers require reuse-existing-inputs.enabled=true")
    max_age_seconds = reuse_cfg.get("max_age_seconds")
    if not isinstance(max_age_seconds, int) or isinstance(max_age_seconds, bool) or max_age_seconds <= 0:
        raise ValueError("reuse-existing-inputs.max_age_seconds must be a positive integer")
    for layer in required_reuse:
        root_value = reuse_cfg.get(f"{layer}_root", f"lake/{layer}")
        layer_root = Path(str(root_value))
        if not layer_root.is_absolute():
            layer_root = repo_root / layer_root
        if not freshness_checker(layer, layer_root, max_age_seconds):
            raise ValueError(f"reused {layer} inputs are missing or stale")


def _inputs_are_fresh(layer: str, root: Path, max_age_seconds: int) -> bool:
    """Return whether a layer has a recently modified Parquet artifact."""

    del layer
    if not root.is_dir():
        return False
    newest_mtime = max((path.stat().st_mtime for path in root.rglob("*.parquet")), default=None)
    if newest_mtime is None:
        return False
    return datetime.now(UTC).timestamp() - newest_mtime <= max_age_seconds


def _has_option(cli_args: list[str], option_name: str) -> bool:
    """Return whether a CLI option is already present."""

    return option_name in cli_args


def _dataset_values(cli_args: list[str]) -> list[str]:
    """Return values belonging to the ``--dataset`` option."""

    if "--dataset" not in cli_args:
        return []
    dataset_idx = cli_args.index("--dataset")
    values: list[str] = []
    cursor = dataset_idx + 1
    while cursor < len(cli_args) and not cli_args[cursor].startswith("--"):
        values.append(cli_args[cursor])
        cursor += 1
    return values


def _option_values(cli_args: list[str], option_name: str) -> list[str]:
    """Return the positional value block following one CLI option."""

    if option_name not in cli_args:
        return []
    option_idx = cli_args.index(option_name)
    return [token for token in cli_args[option_idx + 1 :] if not token.startswith("--")]


def _replace_dataset_values(cli_args: list[str], dataset_values: list[str]) -> list[str]:
    """Return CLI args with the ``--dataset`` value block replaced."""

    if "--dataset" not in cli_args:
        return cli_args
    dataset_idx = cli_args.index("--dataset")
    cursor = dataset_idx + 1
    while cursor < len(cli_args) and not cli_args[cursor].startswith("--"):
        cursor += 1
    return [*cli_args[: dataset_idx + 1], *dataset_values, *cli_args[cursor:]]


def _without_option(cli_args: list[str], option_name: str) -> list[str]:
    """Return CLI args without a boolean option token."""

    return [token for token in cli_args if token != option_name]


def _split_trade_minute_gap_args(cli_args: list[str]) -> tuple[list[str] | None, list[str]]:
    """Split Medallion Bronze args so tick trades can run minute-level gap fill.

    Daily cron still uses tail-delta mode for normal market datasets, while
    trade tick datasets get a separate full-gap-fill pass that inspects Bronze
    minute coverage. Successful zero-row Deribit responses are stored in
    ``empty_minutes.parquet`` sidecars so legitimately quiet minutes are not
    re-checked on later full-gap runs.
    """

    dataset_values = _dataset_values(cli_args)
    trade_dataset_values = [value for value in dataset_values if value in {"perps_trades", "options_trades"}]
    if not trade_dataset_values or "--tail-delta-only" not in cli_args:
        return None, cli_args

    remaining_datasets = [value for value in dataset_values if value not in trade_dataset_values]
    gap_args = _replace_dataset_values(cli_args, trade_dataset_values)
    gap_args = _without_option(gap_args, "--tail-delta-only")
    if "--full-gap-fill" not in gap_args:
        gap_args.append("--full-gap-fill")

    if not remaining_datasets:
        return gap_args, []
    remaining_args = _replace_dataset_values(cli_args, remaining_datasets)
    return gap_args, remaining_args


def _ensure_volatility_dataset_arg(cli_args: list[str]) -> list[str]:
    """Include volatility index data in Bronze medallion dataset schedules."""

    if "--dataset" not in cli_args or "volatility_index_data" in cli_args:
        return cli_args
    rewritten = list(cli_args)
    dataset_idx = rewritten.index("--dataset")
    insert_idx = dataset_idx + 1
    while insert_idx < len(rewritten) and not rewritten[insert_idx].startswith("--"):
        insert_idx += 1
    rewritten.insert(insert_idx, "volatility_index_data")
    return rewritten


def _string_list(value: object) -> list[str]:
    """Return a clean string list from config values."""

    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _apply_bronze_start_defaults(*, cli_args: list[str], config_data: dict[str, Any]) -> list[str]:
    """Append Bronze symbol start-bound defaults from ``bronze-build``.

    Medallion runs should use the same symbol-specific historical boundaries
    as direct ``bronze-build`` runs unless the step explicitly overrides them.
    """

    bronze_cfg = config_data.get("bronze-build")
    if not isinstance(bronze_cfg, dict):
        return cli_args

    rewritten = list(cli_args)
    symbol_start_dates = _string_list(bronze_cfg.get("symbol_start_dates"))
    if symbol_start_dates and not _has_option(rewritten, "--symbol-start-dates"):
        rewritten.append("--symbol-start-dates")
        rewritten.extend(symbol_start_dates)

    return rewritten


def _resolve_configured_log_path(value: str, *, repo_root: Path) -> Path:
    """Resolve a configured log path, anchoring relative values to the repo root."""

    path = Path(value.strip())
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def _log_path_from_config(*, config_data: dict[str, Any], repo_root: Path) -> Path:
    """Resolve shared log file path from config env mapping."""

    env_cfg = config_data.get("env")
    if isinstance(env_cfg, dict):
        configured_file = env_cfg.get("DEPTH_SYNC_LOG_FILE")
        if isinstance(configured_file, str) and configured_file.strip():
            return _resolve_configured_log_path(configured_file, repo_root=repo_root).with_name(
                "run-medallion-pipeline.log"
            )
        configured_dir = env_cfg.get("DEPTH_SYNC_LOG_DIR")
        if isinstance(configured_dir, str) and configured_dir.strip():
            return _resolve_configured_log_path(configured_dir, repo_root=repo_root) / "crypto-loader.log"
    return (repo_root / ".run" / "logs" / "crypto-loader.log").resolve()


def _run_pipeline(
    *,
    python_bin: str,
    steps: list[PipelineStep],
    repo_root: Path,
    env: dict[str, str],
) -> None:
    """Run all steps sequentially, stopping on first failure."""

    for step in steps:
        _log_line(f"ACTIVE_STEP={step.name}")
        _log_line(f"START {step.name}")
        try:
            completed = subprocess.run(
                [python_bin, *step.args],
                cwd=str(repo_root),
                env=env,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            signal_name = f" signal={-exc.returncode}" if exc.returncode < 0 else ""
            _log_line(f"FAILED {step.name} returncode={exc.returncode}{signal_name}")
            raise
        _log_line(f"EXIT {step.name} returncode={completed.returncode}")
        _log_line(f"DONE {step.name}")


@contextmanager
def _redirect_output_to(log_path: Path):
    """Redirect stdout/stderr to log file for the pipeline scope."""

    with log_path.open("a", encoding="utf-8") as log_handle:
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        try:
            sys.stdout = log_handle
            sys.stderr = log_handle
            yield
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr


def _acquire_nonblocking_lock(lock_file: Path) -> int | None:
    """Acquire non-blocking lock. Returns fd on success, ``None`` when locked."""

    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_file, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        return None


def _release_lock(fd: int) -> None:
    """Release file lock and close descriptor."""

    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


def _rotate_pipeline_log(log_path: Path, *, retention_days: int = LOG_ARCHIVE_RETENTION_DAYS) -> None:
    """Rotate shared pipeline log by day and apply repository log retention."""

    if not log_path.exists():
        return
    today = datetime.now(UTC).date()
    modified_date = datetime.fromtimestamp(log_path.stat().st_mtime, tz=UTC).date()
    if modified_date < today:
        rotated_path = log_path.with_name(f"{log_path.name}.{modified_date.isoformat()}")
        if rotated_path.exists():
            suffix = datetime.now(UTC).strftime("%H%M%S")
            rotated_path = log_path.with_name(f"{log_path.name}.{modified_date.isoformat()}.{suffix}")
        log_path.rename(rotated_path)

    enforce_log_retention(log_path, archive_retention_days=retention_days)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""

    repo_root = _default_repo_root()
    default_lock_file = repo_root / ".run" / "full-pipeline.lock"

    parser = argparse.ArgumentParser(description="Run bronze, silver, and gold builders as one pipeline.")
    parser.add_argument("--repo-root", default=str(repo_root), help="Repository root path")
    parser.add_argument("--config", default=str(_default_config_path(repo_root)), help="Path to config.yaml")
    parser.add_argument("--python-bin", default=sys.executable, help="Python executable used for builder commands")
    parser.add_argument("--main-path", default=str(repo_root / "main.py"), help="Path to main.py entrypoint")
    parser.add_argument("--lock-file", default=str(default_lock_file), help="Non-blocking lock file path")
    parser.add_argument("--log-file", help="Single append-only pipeline log file (overrides config)")
    parser.add_argument("--dry-run", action="store_true", help="Print the deterministic pipeline plan without writing")
    return parser.parse_args()


def main() -> int:
    """Entrypoint for cron-friendly medallion pipeline execution."""

    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    config_path = Path(args.config).resolve()
    main_path = Path(args.main_path).resolve()
    lock_file = Path(args.lock_file).resolve()

    if not config_path.exists():
        print(f"missing required config file: {config_path}", file=sys.stderr)
        return 2
    if not main_path.exists():
        print(f"missing main entrypoint: {main_path}", file=sys.stderr)
        return 2

    config_data = _load_yaml(config_path)
    _apply_env_from_config(config_data, fallback_config_path=main_path.parent / "config.yaml")
    steps = _build_steps(main_path=main_path, config_path=config_path, config_data=config_data)
    if getattr(args, "dry_run", False):
        bronze_step = next((step for step in steps if step.name == "bronze"), None)
        symbols = _option_values(bronze_step.args, "--symbols") if bronze_step is not None else []
        gold_root = repo_root / "lake" / "gold"
        freshness = audit_gold_history_freshness(gold_root=gold_root, exchange="deribit", symbols=symbols)
        print(
            json.dumps(
                {
                    "mode": "dry-run",
                    "steps": [{"name": step.name, "args": step.args} for step in steps],
                    "gold_freshness": freshness,
                },
                indent=2,
            )
        )
        return 0
    config_log_path = _log_path_from_config(config_data=config_data, repo_root=repo_root)
    log_path = Path(args.log_file).resolve() if args.log_file else config_log_path

    log_path.parent.mkdir(parents=True, exist_ok=True)
    _rotate_pipeline_log(log_path)

    lock_fd = _acquire_nonblocking_lock(lock_file)
    if lock_fd is None:
        print("pipeline already running", file=sys.stderr)
        return 1
    try:
        with _redirect_output_to(log_path):
            _log_line("PIPELINE START script=run_medallion_pipeline.py")
            env = dict(os.environ)
            _log_line(f"SCHEDULED_STEPS={','.join(step.name for step in steps)}")
            _run_pipeline(
                python_bin=str(args.python_bin),
                steps=steps,
                repo_root=repo_root,
                env=env,
            )
            _log_line("PIPELINE DONE")
    except subprocess.CalledProcessError as exc:
        with _redirect_output_to(log_path):
            _log_line(f"PIPELINE FAILED returncode={exc.returncode}")
        return 1
    except Exception as exc:
        with _redirect_output_to(log_path):
            _log_line(f"PIPELINE FAILED exception={type(exc).__name__}: {exc}")
        return 1
    finally:
        _release_lock(lock_fd)

    print(f"pipeline completed: log={log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
