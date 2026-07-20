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
- [x] Successful equal native ROW `INSERT` no-ops and successful
      `replace-divergent-pk` replacements never create a new ledger row; they
      stage resolution of an already-recorded unresolved row only.
- [x] Staged success resolution matches only `source_identity`, schema, table,
      and the canonical source primary-key JSON used by observation; source and
      schema records remain isolated.
- [x] Resolution is finalized only after the target transaction and its
      checkpoint commit; rollback or commit/checkpoint failure leaves the
      existing conflict unresolved.
- [x] Under the explicit `replace-divergent-pk` policy, an unequal native ROW
      `INSERT` duplicate is replaceable only when MySQL identifies `PRIMARY`:
      read exactly one target row by source primary key, update every writable
      source-image column by that primary-key predicate, and require exactly one
      matched target row. Missing/multiple PK rows or any other update count persist
      conflict evidence and abort without checkpoint advancement.
      Secondary-unique, foreign-key, CHECK, and replacement-update conflicts never
      use this path and remain durable aborting conflicts. The accepted policy risk
      is overwriting the divergent target row. Replacement keeps applying rows and
      may checkpoint; if its enclosing target transaction later rolls back, the
      independent ledger evidence survives while the replacement itself rolls back.
- [x] Stage supported constraint-conflict observations within the source
      transaction; at its XID, finalize their source-transaction end
      coordinates, roll back the target transaction, persist the unresolved
      observations through the independent control-plane connection, and retry
      from the unchanged checkpoint with bounded in-process backoff. Successful
      replay resolves the matching evidence row.
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

`--insert-conflict-policy` has three values: `error`, `ignore-duplicate`, and
`replace-divergent-pk`. `ignore-duplicate` keeps its equality-only native ROW
behavior: a duplicate continues without ledger evidence only when the target row
fetched by source primary key exactly equals the source row. `replace-divergent-pk`
is native ROW-only and replaces unequal rows only for a `PRIMARY` duplicate using
a primary-key UPDATE of the source image; it records durable evidence only when
a matching unresolved conflict already exists, then continues so the target
transaction/checkpoint can commit. Successful no-op/replacement events never
create ledger rows. Secondary-unique, foreign-key, CHECK, and replacement-update
conflicts always persist evidence and abort. The accepted overwrite risk is
explicit. Resolution is staged until target commit/checkpoint success; rollback
leaves existing evidence unresolved. Generic statement execution does not gain an
unsafe replacement fallback. Snapshot/catchup writes and normal range repairs use
explicit `INSERT IGNORE` independently of the flag; the `sync-table
--updated-since` path uses an upsert.

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

The structured live stream stages supported constraint-conflict observations
within the source transaction. At the source XID, it finalizes their
source-transaction end coordinates, rolls back the target transaction, then
persists the unresolved observations through the independent durable ledger
before returning the row failure. The failed transaction and later coordinates
are not checkpointed, while the independently persisted evidence survives.
Equal native ROW `INSERT` duplicates are logged and applied without ledger
persistence or rollback; divergent native ROW `INSERT` duplicates follow the
durable conflict path.
Replaying
the same source identity is idempotent: existing conflict evidence is resolved
only after successful target commit/checkpoint, and a different source primary
key remains a distinct identity. Startup validates the ledger schema, guards, trigger inventory, and
exact grants before source replication. `repair-drift` resolves rows only after
its non-mutating Verify phase proves full-scope equality, then records the run ID
plus evidence. The Docker harness `replace-divergent-pk` scenario proves a real ROW transaction's
XID target commit and checkpoint advancement, successful replacement without
creating a ledger row, and a replacement CHECK failure rolling back target DML
without advancing the checkpoint. The harness does not inject a
crash between target commit and process completion; that crash boundary remains
unproven. The `row-conflict-rollback` scenario passes
`--insert-conflict-policy ignore-duplicate` for an equal same-primary-key
`ROW INSERT`, and asserts that the target transaction succeeds, the checkpoint
advances to the event end, and no unresolved ledger row exists for that source
primary key. It then asserts rollback, unchanged checkpoints, and durable
idempotent evidence for a divergent secondary-unique conflict, different
primary-key isolation, and a CHECK conflict. The structured-stream transaction
tests separately assert the same rollback/evidence boundary for a foreign-key
conflict; the harness's FK scenarios cover repair ordering and cycle blocking.

- [ ] Schedule recurring repair from unresolved records.
- [ ] Prove the live deployed path and repeated convergence before cutover.

See [Table Sync Repair](table-sync-repair.md) and
[Catchup Workflow](../catchup.md).
