# PostgreSQL Gold Sync Runbook

## Security boundary

`scripts/provision_postgres_sync_role.py` is the only operational entrypoint that
creates or migrates the `crypto_loader` and `crypto_loader_sync` schemas and their
tables. Run it with a PostgreSQL administrator identity. The schemas and tables
remain owned by that administrator.

Normal `gold-sync-postgres` execution connects only as `crypto-loader`. It validates
the PR-85 catalog metadata and performs DML; it never creates, alters, or drops a
schema or table.

The runtime role is `LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
NOREPLICATION NOBYPASSRLS`. It has:

- `USAGE` on `crypto_loader` and `crypto_loader_sync`, without `CREATE`.
- `SELECT, INSERT, UPDATE, DELETE` on registered consumer tables.
- `SELECT, INSERT, UPDATE, DELETE` on `crypto_loader_sync.gold_row_hashes`.
- `SELECT, INSERT, UPDATE` on `crypto_loader_sync.gold_sync_state`.

No runtime role membership or `PUBLIC` table grant is permitted in the two owned
schemas.

## Runtime timeouts

`gold-sync-postgres` requires positive bounded timeout settings. The defaults are
`PGCONNECT_TIMEOUT_S=10`, `PGLOCK_TIMEOUT_MS=5000`, `PGSTATEMENT_TIMEOUT_MS=3600000`,
and `PGIDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS=3600000`. The connection timeout is
applied before connecting; the remaining settings are applied in the UTC session
before catalog reads or DML. Invalid or non-positive values fail configuration.

## Parallelism and memory

Independent Gold lineages are reconciled with at most two workers by default.
Set `PGSYNC_MAX_WORKERS=1` to force serial execution; values above `2` are rejected.
To cap peak memory, a lineage with an estimated source working set above 2 GiB is
always processed alone. The estimate uses Parquet size and row/column counts. Source
Parquet is read in streaming batches, and delta planning uses sorted merge scans
instead of full-size key dictionaries. Each lineage remains its own atomic
transaction; after a failure, rerunning safely resumes from committed checkpoints.

## Administrator bootstrap

Export the protected settings in the administrator's process environment:

```bash
export PGHOST=10.10.1.3
export PGPORT=54321
export PGDATABASE=market_data
export PGADMINUSER=postgres-admin
export PGADMINPASSWORD
export PGPASSWORD
export GOLD_ROOT=/absolute/path/to/lake/gold
python scripts/provision_postgres_sync_role.py
```

`PGADMINPASSWORD` authenticates the administrator connection. `PGPASSWORD` is the
runtime role password to install; neither value is accepted as a command argument,
rendered in logs, or stored in migration metadata. `GOLD_ROOT` must contain the
certified current registered Gold artifacts used to generate PR-85 bootstrap
metadata.

Provisioning is transactional and idempotent. It transfers legacy schemas owned by
`crypto-loader` to the connected administrator, applies deterministic table DDL,
replaces runtime grants, and reads the role, ownership, and grants back from the
catalog before commit. Unexpected ownership, role membership, or grants fail closed.

Run administrator provisioning after a registered Gold schema change and before
starting runtime synchronization. A runtime catalog mismatch is a migration-required
error; do not grant `CREATE` or move DDL into `gold-sync-postgres` as a workaround.

## Permission verification

The real permission probes require isolated test credentials:

```bash
export TEST_POSTGRES_ADMIN_DSN
export TEST_POSTGRES_RUNTIME_DSN
python -m pytest --no-cov -q tests/test_postgres_runtime_privileges.py
```

The runtime DSN must authenticate exactly as `crypto-loader`. The probes create an
administrator-owned temporary consumer table, verify runtime insert/update/select/
delete, prove runtime create/alter/drop failures, and remove the probe table. They
also read back exact schema ownership, role attributes, and table grants.

## Production Reconstruction Or Certification

Run `postgres-live-conformance` first and retain its sanitized PR-101 report. Then
run the guarded command with the certified current Gold root:

```bash
python main.py postgres-production-reconstruction \
	--current-report artifacts/acceptance/postgres-live-conformance-v2.json \
	--gold-root /absolute/path/to/lake/gold \
	--evidence-file artifacts/acceptance/postgres-production-reconstruction-v2.json
```

A `PASS` report chooses `no-op-certification`; no operator adapter, maintenance
window, backup, bootstrap, replay, or schema mutation is used. The command performs
an independent live conformance check and writes only sanitized PR-102 evidence.

For a report that fails only `owned-catalog` or `lineage:*` checks in the `catalog`
or `data` categories, provide an operator-reviewed adapter factory in
`module:callable` form. It must implement the `ReconstructionAdapter` protocol in
`application.services.postgres_reconstruction` and may reconstruct only
`crypto_loader` and `crypto_loader_sync`.

```bash
python main.py postgres-production-reconstruction \
	--current-report artifacts/acceptance/postgres-live-conformance-v2.json \
	--gold-root /absolute/path/to/lake/gold \
	--adapter-factory operations.postgres_reconstruction:create_adapter
```

The adapter disables scheduling; obtains host and PostgreSQL exclusion; validates
the configured endpoint/database; creates and verifies a timestamped backup of the
two owned schemas; reconstructs schemas and DML-only runtime access; bootstraps
certified Gold; and runs a zero-mutation replay. The scheduler is restored only
after the independent verifier passes and replay completes. Permission, timeout,
configuration, endpoint, source, temporal, or unrelated-object failures hard-stop.
