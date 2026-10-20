"""Typed protected runtime configuration for PostgreSQL Gold synchronization."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

POSTGRES_HOST = "10.10.1.3"
POSTGRES_PORT = 54321
POSTGRES_ROLE = "crypto-loader"
_REQUIRED_KEYS = ("PGHOST", "PGPORT", "PGUSER", "PGDATABASE", "PGPASSWORD")
_TIMEOUT_DEFAULTS = {
    "PGCONNECT_TIMEOUT_S": 10,
    "PGLOCK_TIMEOUT_MS": 5_000,
    "PGSTATEMENT_TIMEOUT_MS": 3_600_000,
    "PGIDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS": 3_600_000,
}
_MAX_PARALLEL_LINEAGES = 2


@dataclass(frozen=True, slots=True)
class PostgresSyncConfig:
    """Validated application connection settings with redacted password representation."""

    host: str
    port: int
    user: str
    database: str
    password: str = field(repr=False)
    connect_timeout_s: int = _TIMEOUT_DEFAULTS["PGCONNECT_TIMEOUT_S"]
    lock_timeout_ms: int = _TIMEOUT_DEFAULTS["PGLOCK_TIMEOUT_MS"]
    statement_timeout_ms: int = _TIMEOUT_DEFAULTS["PGSTATEMENT_TIMEOUT_MS"]
    idle_in_transaction_session_timeout_ms: int = _TIMEOUT_DEFAULTS["PGIDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS"]
    max_parallel_lineages: int = _MAX_PARALLEL_LINEAGES

    def __post_init__(self) -> None:
        if self.host != POSTGRES_HOST:
            raise ValueError(f"PGHOST must be {POSTGRES_HOST}")
        if self.port != POSTGRES_PORT:
            raise ValueError(f"PGPORT must be {POSTGRES_PORT}")
        if self.user != POSTGRES_ROLE:
            raise ValueError(f"PGUSER must be {POSTGRES_ROLE}")
        if not self.database.strip():
            raise ValueError("PGDATABASE must not be empty")
        if not self.password:
            raise ValueError("PGPASSWORD must not be empty")
        for name, value in (
            ("PGCONNECT_TIMEOUT_S", self.connect_timeout_s),
            ("PGLOCK_TIMEOUT_MS", self.lock_timeout_ms),
            ("PGSTATEMENT_TIMEOUT_MS", self.statement_timeout_ms),
            ("PGIDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS", self.idle_in_transaction_session_timeout_ms),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if (
            not isinstance(self.max_parallel_lineages, int)
            or isinstance(self.max_parallel_lineages, bool)
            or self.max_parallel_lineages not in (1, _MAX_PARALLEL_LINEAGES)
        ):
            raise ValueError("PGSYNC_MAX_WORKERS must be 1 or 2")

    @classmethod
    def from_sources(
        cls,
        *,
        environment: Mapping[str, str] | None = None,
        runtime_values: Mapping[str, object] | None = None,
    ) -> PostgresSyncConfig:
        """Resolve runtime config with explicit environment values taking precedence."""

        env = os.environ if environment is None else environment
        runtime = {} if runtime_values is None else runtime_values
        resolved: dict[str, str] = {}
        for key in _REQUIRED_KEYS:
            env_value = env.get(key)
            if env_value is not None and str(env_value).strip():
                resolved[key] = str(env_value)
                continue
            runtime_value = runtime.get(key)
            if runtime_value is not None and str(runtime_value).strip():
                resolved[key] = str(runtime_value)
        missing = [key for key in _REQUIRED_KEYS if key not in resolved]
        if missing:
            raise ValueError(f"missing PostgreSQL runtime setting(s): {', '.join(missing)}")
        try:
            port = int(resolved["PGPORT"])
        except ValueError as exc:
            raise ValueError("PGPORT must be an integer") from exc
        timeout_values: dict[str, int] = {}
        for key, default in _TIMEOUT_DEFAULTS.items():
            raw_timeout = env.get(key, runtime.get(key, default))
            try:
                timeout_values[key] = int(str(raw_timeout).strip())
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{key} must be a positive integer") from exc
        try:
            raw_workers = env.get("PGSYNC_MAX_WORKERS", runtime.get("PGSYNC_MAX_WORKERS", _MAX_PARALLEL_LINEAGES))
            max_parallel_lineages = int(str(raw_workers).strip())
        except (TypeError, ValueError) as exc:
            raise ValueError("PGSYNC_MAX_WORKERS must be 1 or 2") from exc
        return cls(
            host=resolved["PGHOST"].strip(),
            port=port,
            user=resolved["PGUSER"].strip(),
            database=resolved["PGDATABASE"].strip(),
            password=resolved["PGPASSWORD"],
            connect_timeout_s=timeout_values["PGCONNECT_TIMEOUT_S"],
            lock_timeout_ms=timeout_values["PGLOCK_TIMEOUT_MS"],
            statement_timeout_ms=timeout_values["PGSTATEMENT_TIMEOUT_MS"],
            idle_in_transaction_session_timeout_ms=timeout_values["PGIDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS"],
            max_parallel_lineages=max_parallel_lineages,
        )

    @classmethod
    def from_env(cls) -> PostgresSyncConfig:
        """Resolve all settings from the current process environment."""

        return cls.from_sources(environment=os.environ)

    def as_env(self) -> dict[str, str]:
        """Return exactly the five standard application PG variables."""

        return {
            "PGHOST": self.host,
            "PGPORT": str(self.port),
            "PGUSER": self.user,
            "PGDATABASE": self.database,
            "PGPASSWORD": self.password,
        }

    def safe_dict(self) -> dict[str, object]:
        """Return a serialization-safe view that never includes the password."""

        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "database": self.database,
            "password": "<redacted>",
            "max_parallel_lineages": self.max_parallel_lineages,
        }
