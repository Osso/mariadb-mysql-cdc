# Table Sync Repair

`sync-table` reconciles one source table against one target table in primary-key
chunks. It is the child operation used by the run-scoped `repair-drift`
orchestration; live conflict observations persist in `cdc.row_conflicts` and
are resolved only after verified equality. The current table-sync recovery contract is implemented by commits `fa018af..3fe8b17`.

## What it must do

- [x] Compare rows by configured primary-key columns.
- [x] Report missing source rows, divergent rows, and target extras.
- [x] Apply divergent rows in bounded primary-key update batches, with at most
      128 rows per SQL statement, further split to stay under MySQL's
      prepared-statement placeholder limit; verify every updated row exactly
      afterward and persist run progress only after that verification succeeds,
      leaving a failed chunk uncheckpointed for retry.
- [x] Apply missing rows in strict batched `INSERT` statements. Table-sync apply
      and missing-primary-key modes do not use `INSERT IGNORE`; `--updated-since`
      remains the explicit upsert path. Batch success is not inferred from the
      planned insert count.
- [x] Require explicit `--max-deletes` for apply-mode orphan deletion and preflight
      the ceiling before mutation. Persist successful preflight completion on the
      immutable run row before repair starts, so reconnects skip the completed
      scan; failed or incomplete preflights remain unmarked and rerun.
- [x] Require `--run-id`; direct `sync-table` resumes only the exact interrupted
      run and rejects a completed ID. Apply-mode `repair-drift` InsertMissing may
      atomically claim one specification-identical failed missing-PK run within
      the table and immutable-specification scope; compatibility and uniqueness
      are revalidated in a per-transaction `REPEATABLE READ` selection
      transaction with `FOR UPDATE` candidate reads before marking it running,
      and ambiguity fails closed without reclaiming a run.
- [x] Persist run-scoped progress in `cdc.table_sync_runs` by default and reject
      concurrent use of the same run ID with a target named lock.
- [x] For an existing run-progress table, validate the full 16-column contract
      and `run_id` primary key through `information_schema`. Migrate only the
      exact legacy 15-column contract by adding `delete_preflight_complete`
      with default false, preserving existing runs as requiring preflight;
      reject malformed tables without modifying them. After migration,
      least-privilege runtime use requires only `SELECT, INSERT, UPDATE` on the
      prebootstrapped table; absent or legacy tables require DDL grants.
- [x] Keep `cdc.table_sync_progress` as catchup-only legacy state.
- [x] Keep shared MySQL TCP liveness bounds on persistent source, target, and
      progress connections: 10-second TCP connect timeout and TCP keepalive
      beginning after 10 seconds idle; on Linux, probes run every 5 seconds for
      3 attempts with a 30-second TCP user timeout. Apply 30-second read/write
      operation timeouts to sync-table work only; live CDC/DDL connections use
      the shared liveness bounds without those operation timeouts.
- [x] In `missing-primary-keys` mode, retry transient connection failures up to
      five attempts total, resuming each retry from durable run progress.
- [x] With `replace-divergent-pk`, repair only exact one-hop parent displacement
      from one stable source chunk: lock both identities, restore the displaced
      owner, insert the missing owner, verify affected children unchanged, and
      commit run progress in the same transaction; roll back all state on failure.
- [x] For production table-sync inserts, build FK edges from source and target
      schema inventories, retain local edges for the configured schema, and
      discover exact parent identities from each child row's FK columns.
- [x] On a foreign-key insert failure, recursively repair each exact source
      parent before retrying the original child batch. An absent target parent
      is inserted; a divergent target parent is updated; an equal target parent
      is left unchanged; repeated identities are repaired once per batch.
- [x] Treat nullable FK values as having no parent to repair. Missing source
      parents, malformed identities, ambiguous target identities, and dependency
      cycles remain explicit repair errors.
- [x] On foreign-key failures from insert or divergent-update batches, recursively
      repair each exact source parent before retrying the same child batch. Apply
      this to the affected schema-dependent writer subbatch (capped at 128
      rows and reduced by placeholder capacity) so earlier committed subbatches
      are not replayed. Parent repairs require exact post-write
      rereads; concurrent duplicate writes are accepted only when rereads prove
      complete source/target equality, and divergent owners fail closed.
- [x] After a child insert, parent-retry batch, or divergent update batch, reread
      every affected child row by its source primary key and require exactly one
      target row with equal values. A chunk may advance durable progress only
      after this verification returns successfully.
- [x] Keep `--updated-since` retries restartable from the beginning under the same
      immutable run specification.
- [x] In apply and missing-primary-key modes, retry bounded recoverable read,
      duplicate, verification, progress, network, deadlock, and lock-timeout
      failures up to five attempts. Retry from unchanged durable progress and
      leave recoverable failures in `running` state rather than recording terminal
      error state.
- [x] For FK-aware apply runs, perform a final zero-drift scan over the full
      configured range before marking the durable run `complete`; any missing,
      divergent, or extra row keeps completion blocked.
- [x] Provide a durable conflict schema/SQL contract and resolve rows only after
      verified source/target equality.
- [ ] Complete the remaining FK-aware phased repair work for `repair-drift`:
      canonical child/parent columns, cross-engine rule normalization, and
      resumable per-operation state remain separate from table-sync's runtime
      parent repair.
- [x] Build read-only and repair inputs from the full `plan.tables` union. Cumulative
      DeleteExtras preflight and child-first deletes cover every childward table;
      parentward inserts/updates retain their directional scope. Verification uses
      observed phase outcomes: insert/update scope receives full equality, while
      delete-only descendants verify only that target extras are gone.

## Remaining boundaries

- [x] The live stream writes supported constraint conflicts to the durable
      ledger through the row-event conflict context; only equal native ROW
      `INSERT` duplicates under `ignore-duplicate` continue without ledger
      records. The explicit `replace-divergent-pk` policy may replace only
      divergent `PRIMARY` duplicates using a source-image primary-key UPDATE and
      durable audit evidence; secondary-unique, foreign-key, CHECK, and
      replacement-update conflicts persist evidence and abort. The accepted
      overwrite risk is documented in the row-conflict spec.
- [x] `repair-drift` creates a fresh orchestration ID, derives FK-safe phases, and
      passes immutable child run IDs to `sync-table`.
- [ ] No recurring conflict-to-repair scheduler exists; operators must invoke a
      fresh bounded repair run.
- [ ] Cross-schema FK edges are not auto-repaired. Parent repair requires the
      parent table and exact source row to exist in the configured source
      inventory; cycles and ambiguous identities fail closed.
- [ ] Parent repair has no separate durable per-parent operation ledger or
      unbounded internal retry policy; a repair error leaves the chunk
      uncheckpointed for the bounded surrounding run retry/orchestration.

## Required phased behavior before completion

1. Inventory source/target schemas and canonical enforced FK edges; for a
   selected-table repair, derive parentward ancestor scope for inserts/updates
   and childward descendant scope for delete safety independently. Never
   alternate directions through shared nodes into siblings.

For direct table-sync insert batches, runtime FK recovery uses the merged local
source/target FK metadata to discover only the exact parent rows referenced by
that batch. It does not resync an entire parent table.
2. Hash the immutable run plan and filtered directional inventories; fail closed
   on drift. Disconnected FK cycles are outside the hash and do not block the run,
   while a cycle in either required phase scope blocks before mutation.
3. Preflight the cumulative delete ceiling across every table in the childward
   scope, and preflight cycles, before any mutation.
4. Delete reviewed extras child-first.
5. Insert missing rows parent-first.
6. Update divergent rows after blockers are removed; handle FK/unique key changes
   explicitly.
7. Run non-mutating verification over the union of both directional scopes:
   reread full equality for selected/parentward insert-update tables, and reread
   only target extras for delete-only descendants. Fail on any property that the
   corresponding phase repaired before recording conflict resolution evidence.
8. Run a fresh second pass and require zero actionable mismatches and unresolved
   conflict/manual/journal debt.

Lexical order is only a tie-breaker among proven independent tables. It is not a
replacement for FK dependency analysis.

## How it works

- [Target writer](../target-writer.md) — DML modes, strict table-sync inserts,
  and target-side error boundaries.
- [Schema inventory](../schema-inventory.md) — source/target metadata and FK
  edge acquisition.
- [Table catalog sync](table-catalog-sync.md) — broader phased dependency
  scheduling for catalog and `repair-drift` workflows.

## Implementation inventory

- `src/table_sync/fk_parent_repair.rs` — recursive exact-parent discovery,
  nullable-FK handling, cycle detection, and child-batch retry orchestration.
- `src/table_sync/mysql.rs` — exact inventory-row reads for source and target
  parent/child verification.
- `src/table_sync/target.rs` — strict batched writes, merged local FK edges,
  parent insert/update repair, and post-write child verification.
- `src/table_sync/run.rs` — source/target inventory loading and repair-target
  construction.
- `src/table_sync/range.rs` — chunk progress persistence after repair returns.

## Tests asserting this spec

- `src/table_sync/tests.rs`
  — strict batch inserts, missing-parent repair before child retry and progress
  advance, and unresolved post-insert verification.
- `src/table_sync/fk_parent_repair.rs`
  — recursive ordering, equal/divergent parent handling, nullable FK skipping,
  cycle detection, exact parent post-write verification, concurrent duplicate
  reconciliation, and structured source-read errors.
- `src/table_sync/tests_support.rs` — observable batch and progress fixtures.
- `scripts/cdc-integration-harness.py` and
  `tests/cdc_eventual_consistency.rs` — containerized MariaDB/MySQL `1452`
  parent-repair boundary.

## Known gaps (current cycle)

- [ ] Cross-schema parents, missing source parents, ambiguous identities, and
      dependency cycles fail closed instead of being auto-repaired.
- [ ] Parent repairs have no separate durable per-parent operation ledger or
      unbounded internal retry policy.

## Out of scope

- Automatic repair of non-FK unique, CHECK, or arbitrary target constraints.
- Full parent-table resynchronization when only exact referenced rows are needed.
- Deployment or production execution; those require a separately verified image.

See [Catchup Workflow](../catchup.md) and
[Row Conflict Handling](row-conflict-handling.md).
