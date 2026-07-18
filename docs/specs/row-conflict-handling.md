# Row Conflict Handling

The structured stream applies MariaDB ROW/FULL events by source primary key. A
secondary-unique conflict must not mutate the target row that owns the
conflicting secondary key.

## Current behavior

- [x] Build plain `INSERT` statements with the explicit source primary key.
- [x] Never use `ON DUPLICATE KEY UPDATE` for source inserts.
- [x] Classify native ROW `1062` as durable repair debt unless
      `ignore-duplicate` proves exact source/target row equality; admitted NOT
      NULL, foreign-key, and CHECK constraint failures are also durable debt.
- [x] Under `ignore-duplicate`, skip a native ROW `INSERT` `1062` only when the
      target row fetched by source primary key exactly equals the source row;
      divergent `ROW INSERT` values and every non-`INSERT` `1062` unique conflict
      persist evidence and abort. Only equal `ROW INSERT` duplicates continue;
      the default `error` policy fails native row duplicates.
- [x] Persist supported constraint-conflict evidence on the independent
      control-plane connection, then fail the row event so every earlier target
      mutation in the same source transaction rolls back.
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

## Insert conflict policy boundary

`--insert-conflict-policy ignore-duplicate` applies to both generic target
execution and native ROW changes. The generic target executor treats MySQL
`1062` as success for statements beginning with `INSERT INTO`; native ROW
`INSERT` changes do so only after the target row fetched by source primary key
exactly equals the source row. A divergent or otherwise non-equal row is a
constraint conflict: it is persisted as repair debt, fails the row event, and
leaves the target transaction/checkpoint uncommitted. Every non-`INSERT` `1062`
unique conflict likewise persists evidence and aborts. Only equal native ROW
`INSERT` duplicates under `ignore-duplicate` continue without a ledger record;
with the default `error` policy, native row duplicates fail and leave the
transaction/checkpoint uncommitted. Supported non-duplicate constraint conflicts
still use the durable conflict path regardless of policy.
Snapshot/catchup writes and normal range repairs use explicit `INSERT IGNORE`
independently of the flag; the `sync-table --updated-since` path uses an upsert.

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
stored. Conflict-observation UPSERTs compare every hashed field; a mismatch fails via
the immutable-identity guard instead of merging a theoretical hash collision.

The runtime grant is exact table scope: `SELECT, INSERT, UPDATE` only, plus
`EXECUTE` on the exact inventory procedure. Separately, the reviewed application
schema grant includes `SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP,
INDEX, REFERENCES, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE,
EXECUTE, EVENT, TRIGGER`; application `EXECUTE` is required, while control-plane
or global/admin mutation is rejected. DELETE, ALTER, DROP, other CDC
EXECUTE/schema scopes, schema-wide/global/admin/role/grant-option access is
rejected.
Observations use a guarded UPSERT that increments unresolved attempts but never
downgrades a resolved record. Resolution updates only an unresolved record after
verified source/target equality and requires non-empty repair evidence.

It is forbidden to convert an insert into an update of the secondary-key owner
or to select a target row by the secondary key.

## Live wiring and remaining proof

The structured live stream persists supported constraint-conflict observations
to this durable ledger before returning the row failure. The target data
transaction rolls back and the live target checkpoint does not advance, while
the independently persisted evidence survives. Equal native ROW `INSERT`
duplicates are logged and applied without ledger persistence or rollback;
divergent native ROW `INSERT` duplicates follow the durable conflict path.
Replaying
the same source identity is idempotent: it updates attempt evidence rather than
creating a second row; a different source primary key remains a distinct
identity. Startup validates the ledger schema, guards, trigger inventory, and
exact grants before source replication. `repair-drift` resolves rows only after
its non-mutating Verify phase proves full-scope equality, then records the run ID
plus evidence. The Docker harness `row-conflict-rollback` scenario passes
`--insert-conflict-policy ignore-duplicate` and proves equal-duplicate
continuation/checkpointing, constraint-conflict rollback, durable idempotent
evidence, different-primary-key isolation, and zero unresolved debt for repaired
scope.

- [ ] Schedule recurring repair from unresolved records.
- [ ] Prove the live deployed path and repeated convergence before cutover.

See [Table Sync Repair](table-sync-repair.md) and
[Catchup Workflow](../catchup.md).
