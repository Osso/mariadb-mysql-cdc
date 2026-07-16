# DDL Replay and Manual Resolution

`stream-binlog` automatically replays only compatible, unqualified application-schema
DDL and uses an operator-controlled ledger for DDL that cannot be replayed safely.
The [DDL resolution runbook](../ddl-resolution.md) describes the ledger and operator
procedure.

## What it must do

### Automatic replay

- [x] Automatically execute source schema-changing `QueryEvent` SQL only when the default database is the configured application schema, the SQL is unqualified, and the compatibility policy accepts it.
- [x] Automatically replay full application-schema DDL: table, index, view, routine, event, trigger, `RENAME TABLE`, `TRUNCATE TABLE`, and `DROP` operations accepted by the compatibility policy.
- [x] Automatically replay compatible `ALTER TABLE` statements, including multiple `ADD COLUMN` clauses and ordinary `DEFAULT`, `COMMENT`, and `AFTER` clauses.
- [x] Grant the runtime exact schema-scoped DML plus `CREATE`, `ALTER`, `DROP`, `INDEX`, `REFERENCES`, `CREATE VIEW`, `SHOW VIEW`, `CREATE ROUTINE`, `ALTER ROUTINE`, `EXECUTE`, `EVENT`, and `TRIGGER` privileges on the application schema only; never grant global DDL, `ALL`, `GRANT OPTION`, account, role, server, resource-group, or tablespace administration.
- [x] Route database/schema DDL through manual resolution because MySQL requires global privileges for those operations.
- [x] Route qualified or cross-schema DDL, unsafe `DEFINER`/`SQL SECURITY DEFINER` DDL, MariaDB-only syntax, and disallowed multi-statement DDL through manual resolution.
- [x] Flush grouped DML before automatic DDL, execute it, then persist its event end position in a separate checkpoint transaction, invalidate cached target schema state, and require no DDL ledger row; DDL and checkpoint persistence are not atomic as one operation.
- [x] Do not advance the checkpoint when automatic DDL execution fails.
- [x] Keep statement DML forbidden in production `ROW`/`FULL` streaming even though compatible DDL is replayed.

### Manual-resolution safety

- [x] Require manual resolution when a recognized schema-changing statement is rejected by compatibility policy because it is unsafe, MariaDB-only, or contains a disallowed multi-statement sequence.
- [x] Require manual resolution when the event cannot be safely associated with the configured source schema, including every qualified or cross-schema identifier, not only ambiguous ones.
- [x] Flush all earlier grouped DML and their checkpoints before handling a manual DDL boundary.
- [x] Record an unseen manual DDL boundary as `pending` in the target DDL ledger, keyed by base source incarnation identity plus event server ID, binlog file, and event start position.
- [x] Stop at a pending manual DDL boundary without checkpointing past it.
- [x] Keep the exact source SQL, source server identity, schema, and event end position in the ledger record.
- [x] Enforce pending-only inserts with a validated target trigger and reject runtime credentials that can update, delete, alter, drop, trigger, or role-bypass the ledger.
- [x] Keep `cdc_stream` without ledger `UPDATE`, `DELETE`, `ALTER`, `DROP`, or `TRIGGER` mutation privileges; grant it only `EXECUTE` on the exact `<table>_trigger_inventory` `SQL SECURITY DEFINER` routine used for ledger-trigger inspection. Application-schema `TRIGGER` remains allowed for automatic source trigger replay.
- [x] Have bootstrap/resolver credentials independently inspect the actual trigger rows and `SHOW CREATE` routine definition; runtime never reads `information_schema.triggers` directly.
- [x] Call the exact inventory routine during startup validation and fail closed when the routine is missing, fails, or returns missing/malformed trigger metadata.
- [x] Enforce immutable event identity/coordinates/raw SQL and a single one-way `pending` to `resolved` transition with a validated `BEFORE UPDATE` trigger.
- [x] Scope durable stream checkpoint rows to the base source identity so a replaced source cannot consume an earlier incarnation's coordinate.

### Manual resolution and restart

- [x] Resume past a manual DDL boundary only when its existing ledger row is `resolved`.
- [x] Require the ledger row's raw SQL to exactly equal the replayed source SQL before advancing.
- [x] Advance the checkpoint to the manually resolved DDL event end position without executing the DDL again.
- [x] Invalidate cached target schema state after a resolved boundary.
- [x] Never treat target errors such as object already exists or object missing as manual resolution.

### Operator interface

- [x] Default the ledger table to `cdc.ddl_events` and allow replacement with `--ddl-ledger-table TABLE`.
- [x] Provide an operator-readable runbook for inspection, target application/validation, resolution, restart, and pending-ledger monitoring.

## How it works

- [DDL resolution runbook](../ddl-resolution.md)
- [Checkpoints](../checkpoints.md)
- [Statement events](../statement-events.md)

## Implementation inventory

- `src/live/structured_stream.rs` — routes compatible, unqualified application DDL to automatic replay and database/schema, qualified, unsafe, MariaDB-only, or otherwise rejected DDL to the manual ledger.
- `src/live/ddl_ledger.rs` — validates the ledger and runtime grants, validates trigger metadata, and records pending events.
- `src/live.rs` — owns the default ledger table and validates its configuration.
- `src/main.rs` — exposes `--ddl-ledger-table`.
- `src/statement.rs` — classifies automatically replayable and manually resolved schema changes.

## Tests asserting this spec

- `src/live/structured_stream/tests.rs` — compatible DDL bypasses manual resolution; automatic replay failure does not checkpoint; unsafe, database/schema, and qualified DDL remains pending; resolved manual DDL enforces exact SQL and checkpoint monotonicity before advancing without re-execution.
- `src/statement.rs` — application-object automatic DDL coverage, database/schema manual boundaries, runtime grant contract, and compatible/unsafe/MariaDB-only classification.
- `src/live/ddl_ledger.rs` — ledger schema, immutable coordinate lookup, and pending insert behavior.
- `src/tests.rs` — parses `--ddl-ledger-table` into stream configuration.

## Known gaps (current cycle)

- [ ] Add an end-to-end startup test proving a real target with a missing or mismatched ledger guard fails before source replication begins.
- [ ] Add an end-to-end test proving a real target DDL error cannot turn a pending row into `resolved`.

## Out of scope

- Automatically translating SQL that the compatibility policy rejects.
- Treating target errors as proof that a manual DDL boundary was applied correctly.
- Resolving target schema divergence caused by an operator marking a row resolved before applying and validating the DDL.
