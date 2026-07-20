# Table Sync Repair

`sync-table` reconciles one source table against one target table in primary-key
chunks. It is the child operation used by the run-scoped `repair-drift`
orchestration; live conflict observations persist in `cdc.row_conflicts` and
are resolved only after verified equality.

## Current behavior

- [x] Compare rows by configured primary-key columns.
- [x] Report missing source rows, divergent rows, and target extras.
- [x] Require explicit `--max-deletes` for apply-mode orphan deletion and preflight
      the ceiling before mutation.
- [x] Require `--run-id`; direct `sync-table` resumes only the exact interrupted
      run and rejects a completed ID. Apply-mode `repair-drift` InsertMissing may
      atomically claim one specification-identical failed missing-PK run within
      the table and immutable-specification scope; compatibility and uniqueness
      are revalidated in a per-transaction `REPEATABLE READ` selection
      transaction with `FOR UPDATE` candidate reads before marking it running,
      and ambiguity fails closed without reclaiming a run.
- [x] Persist run-scoped progress in `cdc.table_sync_runs` by default and reject
      concurrent use of the same run ID with a target named lock.
- [x] Keep `cdc.table_sync_progress` as catchup-only legacy state.
- [x] Use bounded MySQL network timeouts: 10 seconds for TCP connect and 30 seconds
      for reads and writes.
- [x] In `missing-primary-keys` mode, retry transient connection failures up to
      five attempts total, resuming each retry from durable run progress.
- [x] With `replace-divergent-pk`, repair only exact one-hop parent displacement
      from one stable source chunk: lock both identities, restore the displaced
      owner, insert the missing owner, verify affected children unchanged, and
      commit run progress in the same transaction; roll back all state on failure.
- [x] Keep `--updated-since` retries restartable from the beginning under the same
      immutable run specification.
- [x] Provide a durable conflict schema/SQL contract and resolve rows only after
      verified source/target equality.
- [x] Provide and wire an FK-aware phased repair path with canonical child/parent
      columns, cross-engine rule normalization, inventory/plan hashes, global
      delete preflight, child-first deletes, parent-first inserts, resumable
      per-operation state, cycle/schema-mismatch blocking, PK-window bounds, a
      non-mutating full-scope Verify equality phase, and real Docker proof.

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

## Required phased behavior before completion

1. Inventory source/target schemas and canonical enforced FK edges.
2. Hash the immutable run plan and inventories; fail closed on drift.
3. Preflight all delete ceilings and cycles before any mutation.
4. Delete reviewed extras child-first.
5. Insert missing rows parent-first.
6. Update divergent rows after blockers are removed; handle FK/unique key changes
   explicitly.
7. Run the real Verify phase: reread the full configured scope, make no target
   mutations, and fail on any missing, extra, or divergent row before recording
   conflict resolution evidence.
8. Run a fresh second pass and require zero actionable mismatches and unresolved
   conflict/manual/journal debt.

Lexical order is only a tie-breaker among proven independent tables. It is not a
replacement for FK dependency analysis.

See [Catchup Workflow](../catchup.md) and
[Row Conflict Handling](row-conflict-handling.md).
