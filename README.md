# CRYPTO-HISTORY-LOADER

Quant research data platform for historical crypto market ingestion, normalization, feature engineering, and model-ready dataset generation.

Author: Sergej Schweizer

## Documentation

| Document | Scope |
|---|---|
| [`DATASETS.md`](DATASETS.md) | Authoritative Gold dataset catalog: every dataset ID, source contract, feature, feature meaning, null policy, lineage rule, physical status, and build example |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Package boundaries, medallion data flow, side effects, storage ownership, and architectural update rules |
| [`AGENTS.md`](AGENTS.md) | Repository operating and engineering policy |
| [`BACKLOG.md`](BACKLOG.md) | Authoritative work-item backlog, scope, and acceptance criteria |
| [`docs/dataset_inventory.md`](docs/dataset_inventory.md) | Generated physical and contracted dataset inventory when present |

Gold dataset definitions must not be duplicated in this README. Update `DATASETS.md` together with `application/dataset_contracts.py` whenever a Gold contract changes.

## System overview

The repository implements a deterministic medallion pipeline:

```text
Deribit APIs
    |
    v
Bronze: normalized append-oriented source records
    |
    v
Silver: validated, time-aligned feature datasets
    |
    v
Gold: versioned model-ready datasets
```

Core properties:

- deterministic and idempotent ingestion
- explicit typed dataset contracts
- restart-safe Bronze checkpoints
- canonical one-minute Silver and Gold alignment
- anti-leakage backward as-of joins
- schema-stable Parquet outputs
- manifests and source lineage
- reproducible quality gates

Current exchange support: Deribit.

Primary symbols: BTC, ETH, SOL.

## Supported Bronze domains

Bronze domains are defined by `DATASET_REGISTRY` in `application/datasets.py`.

| CLI dataset | Bronze `dataset_type` | Instrument type | Native cadence | Description |
|---|---|---|---|---|
| `spot_ohlcv` | `spot_ohlcv` | spot | `1m` | Physical spot OHLCV candles |
| `perps_ohlcv` | `perps_ohlcv` | perpetual | `1m` | Perpetual futures OHLCV candles |
| `open_interest` | `open_interest` | perpetual | `1m` | Open-interest observations |
| `funding` | `funding` | perpetual | `8h` | Funding-rate observations |
| `perps_trades` | `perps_trades` | perpetual | tick | Historical perpetual executions |
| `options_trades` | `options_trades` | option | tick | Historical option executions |
| `volatility_index_data` | `volatility_index_data` | volatility index | `1m` | Historical Deribit volatility-index OHLC observations |

Live datasets are supplied by the companion [`crypto-live-loader`](https://github.com/SergejSchweizer/crypto-live-loader)
repository. Its live market snapshots are populated into the Bronze layer of this repository as
live-origin source datasets, where this project applies the Silver normalization and publishes the
supported Gold live families. The physical lake may therefore contain live-origin snapshots mounted
or copied from `crypto-live-loader`; use the inventory command for the current source-of-truth list,
schemas, periods, file counts, row counts, and missing days.

## Repository structure

```text
api/                 CLI entrypoints
application/         orchestration, services, and typed dataset contracts
ingestion/           exchange adapters, parsing, and lake IO
scripts/             pipeline runners, validation, and maintenance tools
lake/                local Bronze, Silver, and Gold storage roots
docs/                generated inventories and documentation assets
tests/               unit, integration, and regression tests
config.example.yaml  versioned runtime configuration template
config.yaml          local runtime configuration (ignored; create from the template)
main.py               Python CLI entrypoint
DATASETS.md           authoritative Gold dataset catalog
ARCHITECTURE.md       architecture contract
AGENTS.md             repository operating policy
BACKLOG.md            authoritative ticket backlog
```

New Bronze datasets start with a `DatasetSpec` in `application/datasets.py`. Silver and Gold output contracts are centralized in `application/dataset_contracts.py`.

## Installation

System prerequisites:

```bash
sudo apt update
sudo apt install -y git gh
```

Install the Python environment and development quality gates:

```bash
uv sync --extra dev
```

Runtime configuration uses the ignored local `config.yaml`. Create it from the safe template and set the
PostgreSQL password only in the local file:

```bash
cp config.example.yaml config.yaml
# edit config.yaml and set env.PGPASSWORD
chmod 600 config.yaml
```

## Pipeline commands

Run the full configured pipeline:

```bash
uv run python scripts/run_medallion_pipeline.py --config config.yaml
```

The scheduled Medallion configuration builds historical Silver inputs and the canonical and extended
Gold history datasets (`m1`, `m5`, `m30`, `h1`) only. Live snapshot/L2 Silver datasets and all
`gold.live.*` outputs are excluded; the following PostgreSQL sync publishes the Gold artifacts from
that history-only build.

Build Bronze:

```bash
uv run python main.py bronze-build \
  --exchange deribit \
  --dataset spot_ohlcv perps_ohlcv open_interest funding perps_trades options_trades volatility_index_data \
  --symbols BTC ETH SOL \
  --full-gap-fill \
  --save-parquet-lake \
  --no-json-output
```

Build Silver:

```bash
uv run python main.py silver-build \
  --bronze-root lake/bronze \
  --silver-root lake/silver \
  --exchange deribit \
  --dataset spot_ohlcv perps_ohlcv open_interest funding perps_trades options_trades historical_prediction \
  --timeframe 1m \
  --maxprocesses 4
```

`historical_prediction` creates trailing ex-ante Silver predictors for historical IV/RV and regime research. It excludes forward-looking labels and volatility-index or IV/RV-derived inputs.

Build only the historical prediction features:

```bash
uv run python main.py silver-build \
  --bronze-root lake/bronze \
  --silver-root lake/silver \
  --exchange deribit \
  --dataset historical_prediction \
  --timeframe 1m \
  --maxprocesses 4
```

Build the canonical historical Gold dataset:

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --maxprocesses 4 \
  --dataset-id gold.history.full.m1
```

Build the extended historical Gold dataset, which keeps the canonical schema and adds
history-prediction features:

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --maxprocesses 4 \
  --dataset-id gold.history.extended.m1
```

Gold datasets are always named as standalone contracts; canonical and extended datasets get
distinct IDs instead of overloading one another.

The supported historical Gold family is:

- `gold.history.full.m1`: canonical history-only market data;
- `gold.history.extended.m1`: canonical historical data plus trailing historical-prediction features;
- `gold.history.extended_full.m1`: compatibility variant of the extended historical contract;
- `gold.history.full.m5`, `gold.history.full.m30`, and `gold.history.full.h1`: derived from
  `gold.history.full.m1`;
- `gold.history.extended.m5`, `gold.history.extended.m30`, and `gold.history.extended.h1`: derived
  from `gold.history.extended.m1`.

`gold.history.extended.m1` and `gold.history.extended_full.m1` keep the canonical
`gold.history.full.m1` minute schema and add historical-prediction features.

Build the extended live Gold dataset, which keeps the live-full snapshot schema and adds
deterministic live-derived features:

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --maxprocesses 4 \
  --dataset-id gold.live.extended.m1
```

`gold.live.extended.m1` is built directly from the live snapshot Silver sources. It keeps the
canonical `gold.live.full.m1` surface and adds causal, same-row live-derived features; it is not
read back from a previously materialized `gold.live.full.m1` artifact.
`gold.live.extended.m5`, `gold.live.extended.m30`, and `gold.live.extended.h1`
are derived from the canonical `gold.live.extended.m1` artifact.

Contract-level live option ticker snapshots are aggregated to one minute-level
surface row before Gold joins. Tick and daily metadata sources are likewise
deduplicated to one `(timestamp_m1, exchange, symbol)` row, preventing duplicate-key
joins and keeping live Gold builds bounded in memory.
Raw Silver L2 snapshots are normalized from their source `timestamp` contract into the
Gold minute-level L2 feature contract; perpetual snapshots remain one row per minute,
while option snapshots are aggregated across contracts before the join.

The canonical live family is `gold.live.full.m1` with derived `m5`, `m30`, and `h1` datasets.
The extended live family is `gold.live.extended.m1` with derived `m5`, `m30`, and `h1` datasets.
When several Gold datasets are requested together, the CLI completes each source dataset before
starting its derived children while keeping symbols parallel within a dataset.

See [`DATASETS.md`](DATASETS.md) for every supported Gold dataset ID and its complete feature contract.

## Inventory

Generate a read-only inventory of physical and contracted datasets:

```bash
uv run python main.py dataset-inventory \
  --bronze-root lake/bronze \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --format markdown \
  --output docs/dataset_inventory.md \
  --no-json-output
```

The inventory reports physical datasets, contracted but unmaterialized outputs, schemas, row/file counts, per-series date spans, missing calendar days, and source lineage.

## Operational guarantees

Bronze checkpoint:

```text
.run/checkpoints/bronze-build.json
```

- completed tasks are recorded incrementally
- interrupted runs resume when the effective plan is unchanged
- `--full-gap-fill` rescans internal, head, and tail gaps
- successful runs remove the checkpoint

Gold behavior:

- source variants normalize to canonical symbols
- the newest matching upstream artifact is selected
- event-driven trade activity is not forward-filled
- only the latest three versions are retained per dataset/exchange/symbol lineage
- live Gold datasets never fill gaps from historical datasets

Gold mirror:

- every successful `gold-build` mirrors `lake/gold` to `/volume1/Temp/gold`
- the mirror uses `rsync -a --delete`, copying only changed files and removing stale destination artifacts
- use `--no-mirror-gold-to-temp` only when a successful build must not update the NAS staging copy
- a separate Sunday 21:00 server-local cron job copies every `*.parquet` file, preserving the Gold directory structure under `/volume1/Temp/Quotes`
- this weekly copy is additive: it updates changed Parquet files and leaves non-Parquet or stale destination files untouched

## Quality gates

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv run pyright --level error
uv run ty check
uv run lint-imports --config .importlinter
uv run python scripts/validate_config_with_pydantic.py --config config.example.yaml
uv run python scripts/validate_readme_inventory.py
uv run --extra dev pytest
```

`validate_readme_inventory.py` retains its legacy name for compatibility but validates the authoritative Gold catalog in `DATASETS.md`.

## Roadmap

1. Maintain complete historical coverage for BTC, ETH, and SOL.
2. Strengthen automated data-quality and continuity controls.
3. Add multi-exchange coverage and reconciliation.
4. Harden Silver and Gold schema contracts with regression tests.
5. Make model readiness measurable through recurring coverage, freshness, and quality reports.
