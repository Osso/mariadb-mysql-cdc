# Row Conflict Handling

The structured stream applies MariaDB ROW/FULL events by source primary key. A
secondary-unique conflict must not mutate the target row that owns the
conflicting secondary key.

## Current behavior

- [x] Build plain `INSERT` statements with the explicit source primary key.
- [x] Never use `ON DUPLICATE KEY UPDATE` for source inserts.
- [x] Classify error 1062 plus admitted NOT NULL, foreign-key, and CHECK
      constraint failures as durable repair debt.
- [x] Persist conflict evidence on the independent control-plane connection,
      then fail the row event so every earlier target mutation in the same
      source transaction rolls back.
- [x] Leave the stream checkpoint unchanged when the target transaction rolls
      back.
- [x] Emit parseable `cdc_row_conflict_skipped` output with operation, table,
      source coordinate, and source primary key.
- [x] Replay the same source event into the same identity and increment its
      attempt count; a different source primary key creates a separate identity.
- [x] Keep generated columns out of row writes.
- [x] Provide a durable conflict-record schema/library contract containing source
      identity/server/file/start/end, schema/table/operation, source PK,
      duplicate index/owner when available, error code/text, first/last observed
      times, attempt count, unresolved/resolved status, repair run ID, and
      resolution evidence.
- [x] Provide duplicate classification for same-primary, secondary-unique owner
      mismatch, and malformed duplicate errors in the repair library/tests.

## Durable conflict control plane

`cdc.row_conflicts` is bootstrapped by the admin DDL file and is never created by
runtime code. Startup validates the exact columns, nullability/defaults,
ASCII SHA-256 `conflict_identity` primary key, unresolved/resolved status CHECK,
insert/update guards, and effective privileges before opening the source stream.
Trigger metadata is read only through the SQL SECURITY DEFINER, READS SQL DATA
procedure `cdc.row_conflicts_trigger_inventory`; runtime calls that exact
procedure and validates its returned trigger rows before streaming. Admin/resolver
bootstrap separately reviews `SHOW CREATE PROCEDURE` and the direct trigger rows.
The identity is lowercase SHA-256 over an ordered, length-prefixed tuple of
`source_identity`, server ID, binlog file, start position, schema, table,
operation, and the complete source primary-key JSON. All identity fields remain
stored. Duplicate-key upserts compare every hashed field; a mismatch fails via
the immutable-identity guard instead of merging a theoretical hash collision.

The runtime grant is exact table scope: `SELECT, INSERT, UPDATE` only, plus
EXECUTE on the exact inventory procedure. DELETE, ALTER, DROP, other CDC
EXECUTE/schema scopes, schema-wide/global/admin/role/grant-option access is
rejected.
Observations use a guarded UPSERT that increments unresolved attempts but never
downgrades a resolved record. Resolution updates only an unresolved record after
verified source/target equality and requires non-empty repair evidence.

It is forbidden to convert an insert into an update of the secondary-key owner
or to select a target row by the secondary key.

## Live wiring and remaining proof

The structured live stream persists row-conflict observations to this durable
ledger before returning the row failure. The target data transaction and its
checkpoint then roll back together, while the independently persisted evidence
survives. Startup validates the ledger schema, guards, trigger inventory, and
exact grants before source replication. `repair-drift` resolves rows only after
verified equality and records the run ID plus evidence. The Docker harness proves
multi-row rollback, durable idempotent evidence, different-primary-key isolation,
unchanged checkpoints, and zero unresolved debt for repaired scope.

- [ ] Schedule recurring repair from unresolved records.
- [ ] Prove the live deployed path and repeated convergence before cutover.

See [Table Sync Repair](table-sync-repair.md) and
[Catchup Workflow](../catchup.md).
