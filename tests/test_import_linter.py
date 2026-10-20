"""Architecture boundary tests using import-linter."""

from __future__ import annotations

import configparser
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_import_linter_contracts_pass() -> None:
    """Validate import-linter contracts for package layering."""

    executable = Path(sys.executable).parent / "lint-imports"
    process = subprocess.run(
        [sys.executable, str(executable), "--no-cache", "--config", str(REPO_ROOT / ".importlinter")],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert process.returncode == 0, process.stdout + process.stderr


def test_import_linter_includes_acyclic_root_package_contract() -> None:
    """Root packages should remain guarded against circular sibling dependencies."""

    config = configparser.ConfigParser()
    config.read(REPO_ROOT / ".importlinter")

    section = "importlinter:contract:acyclic_root_packages"
    assert section in config
    assert config[section]["type"] == "acyclic_siblings"
    assert set(config[section]["ancestors"].split()) == {"api", "application", "ingestion"}


def test_api_does_not_import_lake_adapter_directly() -> None:
    """API modules should reach lake persistence through application-facing services."""

    config = configparser.ConfigParser()
    config.read(REPO_ROOT / ".importlinter")

    section = "importlinter:contract:no_api_to_lake_persistence_internals"
    assert section in config
    assert config[section]["type"] == "forbidden"
    assert config[section].getboolean("allow_indirect_imports") is True
    assert set(config[section]["source_modules"].split()) == {"api"}
    assert set(config[section]["forbidden_modules"].split()) == {
        "ingestion.lake",
        "ingestion.lake_bronze_writes",
        "ingestion.lake_dataframe",
        "ingestion.lake_queries",
        "ingestion.lake_reads",
        "ingestion.lake_records",
        "ingestion.lake_sidecars",
        "ingestion.lake_writes",
    }
