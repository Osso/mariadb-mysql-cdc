# Table Sync Repair

`sync-table` reconciles one source table against one target table in primary-key
chunks. It is the child operation used by the run-scoped `repair-drift`
orchestration; live conflict observations persist in `cdc.row_conflicts` and
are resolved only after verified equality.

## Current behavior

- [x] Compare rows by configured primary-key columns.
- [x] Report missing source rows, divergent rows, and target extras.
- [x] Apply divergent rows in bounded primary-key update batches, further split to
      stay under MySQL's prepared-statement placeholder limit; persist run progress
      only after the source chunk's update batch succeeds, leaving a failed chunk
      uncheckpointed for retry.
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
- [x] For an existing run-progress table, validate the full 15-column contract
      and `run_id` primary key through `information_schema` without issuing
      `CREATE` or `ALTER`; reject malformed existing tables without modifying
      them. Least-privilege runtime use therefore requires an admin-
      prebootstrapped table with only `SELECT, INSERT, UPDATE` on that table;
      an absent table still follows the creation path and requires DDL grants.
- [x] Keep `cdc.table_sync_progress` as catchup-only legacy state.
- [x] Bound persistent source, target, and progress MySQL connections with a
      10-second TCP connect timeout, 30-second read/write timeouts, and TCP
      keepalive beginning after 10 seconds idle; on Linux, probes run every
      5 seconds for 3 attempts with a 30-second TCP user timeout.
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
- [ ] Complete the remaining FK-aware phased repair work: canonical
      child/parent columns, cross-engine rule normalization, resumable
      per-operation state, and selected-scope cycle/schema-mismatch blocking.
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

## Required phased behavior before completion

1. Inventory source/target schemas and canonical enforced FK edges; for a
   selected-table repair, derive parentward ancestor scope for inserts/updates
   and childward descendant scope for delete safety independently. Never
   alternate directions through shared nodes into siblings.
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

See [Catchup Workflow](../catchup.md) and
[Row Conflict Handling](row-conflict-handling.md).
