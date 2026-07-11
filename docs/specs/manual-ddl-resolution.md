# Manual DDL Resolution

`stream-binlog` treats source schema-changing `QueryEvent` records as an operator-controlled migration boundary. The stream contract is described here; [the DDL resolution runbook](../ddl-resolution.md) describes the ledger and operator procedure.

## What it must do

### Stream safety

- [x] Never auto-execute a source schema-changing `QueryEvent` on the target.
- [x] Flush all earlier grouped DML and their checkpoints before handling the DDL boundary.
- [x] Record an unseen DDL boundary as `pending` in the target DDL ledger, keyed by base source incarnation identity plus event server ID, binlog file, and event start position.
- [x] Stop at a pending DDL boundary without checkpointing past it.
- [x] Keep the exact source SQL, source server identity, schema, and event end position in the ledger record.
- [x] Enforce pending-only inserts with a validated target trigger and reject runtime credentials that can update, delete, alter, drop, trigger, or role-bypass the ledger.
- [x] Keep `cdc_stream` without `TRIGGER`; grant it only `EXECUTE` on the exact `<table>_trigger_inventory` `SQL SECURITY DEFINER` routine used for trigger inspection.
- [x] Have bootstrap/resolver credentials independently inspect the actual trigger rows and `SHOW CREATE` routine definition; runtime never reads `information_schema.triggers` directly.
- [x] Call the exact inventory routine during startup validation and fail closed when the routine is missing, fails, or returns missing/malformed trigger metadata.
- [x] Enforce immutable event identity/coordinates/raw SQL and a single one-way `pending` to `resolved` transition with a validated `BEFORE UPDATE` trigger.
- [x] Scope durable stream checkpoint rows to the base source identity so a replaced source cannot consume an earlier incarnation's coordinate.

### Manual resolution and restart

- [x] Resume past a DDL boundary only when its existing ledger row is `resolved`.
- [x] Require the ledger row's raw SQL to exactly equal the replayed source SQL before advancing.
- [x] Advance the checkpoint to the DDL event end position without executing the DDL again.
- [x] Invalidate cached target schema state after a resolved boundary.
- [x] Never treat target errors such as object already exists or object missing as resolution. They cannot resolve or checkpoint a DDL boundary; an operator must apply and validate the exact DDL, then explicitly resolve the ledger row.

### Operator interface

- [x] Default the ledger table to `cdc.ddl_events` and allow replacement with `--ddl-ledger-table TABLE`.
- [x] Provide an operator-readable runbook for inspection, target application/validation, resolution, restart, and pending-ledger monitoring.

## How it works

- [DDL resolution runbook](../ddl-resolution.md)
- [Checkpoints](../checkpoints.md)
- [Statement events](../statement-events.md)

## Implementation inventory

- `src/live/structured_stream.rs` — detects schema-changing query events, flushes prior DML, gates progress on the ledger, and checkpoints resolved boundaries.
- `src/live/ddl_ledger.rs` — validates the ledger and runtime grants, calls the exact derived `<table>_trigger_inventory` routine, validates returned trigger metadata, and records pending events.
- `src/live.rs` — owns the default ledger table and validates its configuration.
- `src/main.rs` — exposes `--ddl-ledger-table`.
- `src/statement.rs` — classifies source schema-changing statements.

## Tests asserting this spec

- `src/live/structured_stream/tests.rs` — pending DDL does not execute SQL or checkpoint; resolved DDL advances only to the end position without executing SQL.
- `src/live/ddl_ledger.rs` — ledger schema, immutable coordinate lookup, and pending insert behavior.
- `src/tests.rs` — parses `--ddl-ledger-table` into stream configuration.

## Known gaps (current cycle)

- [ ] Add an end-to-end startup test proving a real target with a missing or mismatched ledger guard fails before source replication begins.
- [x] Add a test that a resolved ledger row with non-identical raw SQL fails without checkpointing.
- [ ] Add an end-to-end test proving a real target DDL error cannot turn a pending row into `resolved`.
- [x] Document the inspect-apply-validate-resolve-restart sequence with immutable-coordinate and exact-SQL guards.

## Out of scope

- Automatically translating, applying, or retrying source DDL on the target.
- Declaring a DDL safe from a generic target error such as already-exists or missing-object.
- Resolving target schema divergence caused by an operator marking a row resolved before applying and validating the DDL.
