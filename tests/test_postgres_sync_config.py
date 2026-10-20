from __future__ import annotations

import pytest

from application.postgres_sync.config import PostgresSyncConfig


def _runtime() -> dict[str, object]:
    return {
        "PGHOST": "10.10.1.3",
        "PGPORT": "54321",
        "PGUSER": "crypto-loader",
        "PGDATABASE": "market_data",
        "PGPASSWORD": "fake-runtime-secret",
    }


def test_runtime_only_configuration() -> None:
    config = PostgresSyncConfig.from_sources(environment={}, runtime_values=_runtime())
    assert config.host == "10.10.1.3"
    assert config.port == 54321
    assert config.user == "crypto-loader"
    assert config.database == "market_data"
    assert config.as_env() == {key: str(value) for key, value in _runtime().items()}
    assert config.statement_timeout_ms == 3_600_000
    assert config.idle_in_transaction_session_timeout_ms == 3_600_000
    assert config.max_parallel_lineages == 2
    assert "fake-runtime-secret" not in repr(config)
    assert config.safe_dict()["password"] == "<redacted>"


def test_environment_overrides_runtime_values() -> None:
    runtime = _runtime()
    runtime["PGDATABASE"] = "runtime_db"
    config = PostgresSyncConfig.from_sources(environment={"PGDATABASE": "env_db"}, runtime_values=runtime)
    assert config.database == "env_db"


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("PGHOST", "localhost"),
        ("PGPORT", "5432"),
        ("PGUSER", "postgres"),
        ("PGDATABASE", ""),
        ("PGPASSWORD", ""),
    ],
)
def test_invalid_identity_or_missing_value_fails(key: str, value: str) -> None:
    runtime = _runtime()
    runtime[key] = value
    with pytest.raises(ValueError):
        PostgresSyncConfig.from_sources(environment={}, runtime_values=runtime)


def test_shell_special_password_is_not_rendered() -> None:
    runtime = _runtime()
    runtime["PGPASSWORD"] = "fake-$()'\" secret"
    config = PostgresSyncConfig.from_sources(environment={}, runtime_values=runtime)
    assert config.as_env()["PGPASSWORD"] == "fake-$()'\" secret"
    assert "fake-$()" not in repr(config)


def test_timeout_configuration_defaults_and_overrides() -> None:
    runtime = _runtime()
    runtime.update(
        {
            "PGCONNECT_TIMEOUT_S": "7",
            "PGLOCK_TIMEOUT_MS": "800",
            "PGSTATEMENT_TIMEOUT_MS": "900",
            "PGIDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS": "1000",
        }
    )
    config = PostgresSyncConfig.from_sources(environment={}, runtime_values=runtime)
    assert (
        config.connect_timeout_s,
        config.lock_timeout_ms,
        config.statement_timeout_ms,
        config.idle_in_transaction_session_timeout_ms,
    ) == (7, 800, 900, 1000)


@pytest.mark.parametrize(("value", "expected"), [("1", 1), ("2", 2)])
def test_parallel_lineage_worker_configuration(value: str, expected: int) -> None:
    config = PostgresSyncConfig.from_sources(environment={"PGSYNC_MAX_WORKERS": value}, runtime_values=_runtime())
    assert config.max_parallel_lineages == expected


@pytest.mark.parametrize("value", ["0", "3", "bad"])
def test_parallel_lineage_worker_configuration_is_bounded(value: str) -> None:
    with pytest.raises(ValueError, match="PGSYNC_MAX_WORKERS"):
        PostgresSyncConfig.from_sources(environment={"PGSYNC_MAX_WORKERS": value}, runtime_values=_runtime())


@pytest.mark.parametrize("key", ["PGCONNECT_TIMEOUT_S", "PGLOCK_TIMEOUT_MS", "PGSTATEMENT_TIMEOUT_MS"])
@pytest.mark.parametrize("value", ["0", "-1", "bad"])
def test_timeout_configuration_rejects_non_positive_values(key: str, value: str) -> None:
    runtime = _runtime()
    runtime[key] = value
    with pytest.raises(ValueError, match=key):
        PostgresSyncConfig.from_sources(environment={}, runtime_values=runtime)
