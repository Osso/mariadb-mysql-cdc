# FK-scoped convergence and strict CDC runtime cutover plan

Date: 2026-08-18

Status: **plan only**. No code, database, registry, GitOps, Kubernetes, grant, deployment, rollback, or repair action is authorized or executed by this document.

## Objective

Run one bounded source-authoritative convergence over the tables that can cause enforced foreign-key failures, then replace the live `05c2345` stream with strict image `ffa244d`.

This is a **CDC runtime cutover**, not an application endpoint/database cutover. It does not claim full-catalog parity.

## Current state

- CDC repository `master`: `2cae00f3eb4e14723b6e44aa77e068f8f3a81f1b`.
- Strict runtime code commit/image tag: `ffa244d`.
- Strict image digest: `sha256:7c22fbd43e09b054a8efaf1b0850a7f9f0fd443ecdc2839906d0c88d62398ca0`.
- Ops repository `master`: `426a5131ec419ddb60629609ed20f0031dc5ebfb`; the relevant CDC stream/sync files remain unchanged from rollback commit `6acd084`.
- Live stream image: `registry.digitalocean.com/globalcomix/mariadb-mysql-cdc:05c2345`.
- Strict startup previously reached row application and stopped at `mysqld-bin.002873:212894218` on `globalcomix.users_search_queries_history`, primary key `69685999`, with MySQL `1452` because the referenced `guests` row was absent.
- The restored `05c2345` stream later advanced beyond that coordinate. This proves service recovery, not target convergence for strict behavior.
- Production still needs the dedicated `cdc_sync` identity and durable `cdc.sync_runs` bootstrap.
- The legacy `cdc_stream` grants are currently restored. The tracked one-time grant transition must run while the stream is stopped immediately before strict-image startup.

## Locked decisions

- [x] Keep `05c2345` running during bulk convergence.
- [x] Use one dedicated `cdc_sync` target identity; do not reuse `cdc_stream`.
- [x] Use one uniquely named, reviewed Flux Job and one immutable run ID.
- [x] Include both child and parent endpoints of every same-schema enforced FK edge, recursively.
- [x] Include every explicitly evidenced non-`1062` blocker table.
- [x] Exclude only explicitly reviewed high-volume FK-isolated tables; retain other otherwise-syncable tables.
- [x] Keep `MySqlConflictLedger`/`cdc.row_conflicts` as independent historical evidence. Do not restore live conflict resolution or couple the sync run to it.
- [x] Preserve strict live behavior: ignore only plain INSERT `1062`; every other row error rolls back the source transaction and prevents checkpoint advancement.
- [x] On final cutover failure: preserve evidence, stop, report, and wait. No automatic rollback, grant reversal, data repair, progress mutation, or retry.

## Scope contract

### FK graph

Build one graph from the union of current source and target FK inventories:

```text
child --foreign key--> parent
```

A table is FK-connected when it appears as either endpoint of a same-schema FK edge. Self-referencing FKs count as connected. Source-only and target-only same-schema edges both count.

Both endpoints are required:

- Parent convergence prevents MySQL `1452` when a child INSERT/UPDATE references a missing target parent.
- Child convergence prevents MySQL `1451` when stale target children block a source-authoritative parent DELETE/UPDATE.

Parents-only selection is rejected.

### Deterministic selected set

1. Read fresh source and target schema inventories and the ordinary syncable/non-syncable catalog without row mutation.
2. Build the union FK graph before filtering tables.
3. Include every otherwise-syncable table in any same-schema FK edge, across every connected component.
4. Include every recorded non-`1062` blocker table even when FK-isolated.
5. Expand every explicit inclusion through required same-schema parent closure.
6. Retain otherwise-syncable FK-isolated tables unless the reviewed policy explicitly excludes them as high-volume.
7. Permit an exclusion only when the table has zero incoming and zero outgoing same-schema FK edges in both current inventories and is not an explicit blocker.
8. Reject the scope if a connected component contains a missing, non-syncable, cross-schema, or unresolved cyclic member. Do not silently omit one endpoint.
9. Produce deterministic selected-catalog and scope-report artifacts plus a SHA-256 scope hash.

High-volume selection is an explicit reviewed table list, not a name heuristic or an unreliable automatic threshold. Current `information_schema.TABLE_ROWS` estimates nominate candidates; bounded primary-key-window measurements may confirm the largest candidates without a full `COUNT(*)` scan.

### Scope claim

Successful completion means:

> Source-authoritative convergence completed for the immutable selected table set and its recorded FK graph.

It does **not** mean:

- full-catalog convergence;
- parity for excluded page-view/log/stat tables;
- application readiness to serve from the target;
- lost-binlog recovery proof;
- elimination of all historical duplicate debt.

Excluded tables remain explicit residual drift. They do not block this CDC runtime cutover unless fresh evidence shows a non-`1062` failure or an FK edge.

## Phase 1 — implementation changes

No implementation starts without separate authorization.

### CDC repository

- [ ] Extend `table-catalog` with an optional explicit scope-policy input and deterministic selected-catalog/scope-report outputs while source and target inventories are still available.
- [ ] Model the policy as required blocker tables plus explicitly excluded high-volume FK-isolated tables. No implicit exclusions or fallback selection path.
- [ ] Record in the scope report: source/target inventory hashes, union FK edges and origin, selected tables, excluded tables and reasons, blocker tables, estimated rows, and selected/excluded totals.
- [ ] Reject exclusion of an FK endpoint, explicit blocker, required parent, non-syncable dependency, cross-schema dependency, or unresolved cycle.
- [ ] Preserve the existing full syncable and non-syncable catalogs as evidence; policy-skipped tables remain technically syncable and must not be mislabeled as schema-incompatible.
- [ ] Extend `sync-catalog` with explicit bounded `--parallelism` and exactly one immutable `--run-id` or `--run-id-prefix`; remove the fixed operational assumption of 16 workers while keeping 16 as the hard maximum.
- [ ] Keep `SyncRunSpec` binding the exact sorted selected table definitions, chunk size, parallelism, endpoints, and progress table. Put the first 12 scope-hash characters in the exact run ID and Job/ConfigMap names.
- [ ] Do not change live row apply, INSERT `1062`, checkpoint, DDL journal, conflict-ledger, reconnect, or transaction-ordering behavior.

### Behavioral tests

- [ ] Large FK-isolated table is excluded only when named in policy.
- [ ] Small/unlisted FK-isolated table remains selected.
- [ ] FK parent and child are both selected.
- [ ] Transitive parent/child component is complete.
- [ ] Self-referencing FK table is selected.
- [ ] Target-only FK edge is included.
- [ ] Explicit blocker cannot be excluded.
- [ ] Selected child without parent is rejected.
- [ ] Exclusion of either FK endpoint is rejected.
- [ ] Non-syncable or cross-schema member blocks the connected component.
- [ ] Selected catalog, report ordering, and scope hash are deterministic.
- [ ] A different selected set changes the immutable run identity.
- [ ] `sync-catalog --parallelism 4 --run-id <id>` maps exactly to one unified run; it rejects zero or values above 16 and rejects missing or conflicting run-identity options.
- [ ] Disposable MariaDB/MySQL proof reproduces both `1452` missing-parent and `1451` blocking-child cases, converges the complete component, and then permits strict CDC to advance.
- [ ] Disposable proof confirms an excluded isolated table receives no schema/row progress entry and remains unchanged.
- [ ] Disposable proof confirms `cdc.row_conflicts` is neither read nor written by the run.

### Documentation and verification

- [ ] Update `docs/specs/table-catalog-sync.md` and `docs/specs/unified-sync.md` with the bounded scope contract.
- [ ] Update ops sync spec/runbook with the selected-catalog, dedicated identity, exact Job, and evidence requirements.
- [ ] Do not weaken all-table `recover-lost-binlog` proof. This policy applies only to the reviewed one-off convergence Job.
- [ ] Do not reuse `docs/cutover.md`; that document governs application traffic cutover, not this stream-image replacement.
- [ ] Run targeted tests, disposable endpoint tests, `cargo fmt`, full repository tests once after the final relevant change, Clippy with warnings denied, Rust readability audit, ops tests, and independent verification.

## Phase 2 — immutable scope and Job preparation

No production mutation occurs in this phase until separately authorized.

### Read-only scope evidence

- [ ] Generate fresh full syncable/non-syncable catalogs from current source and target inventories.
- [ ] Rebuild the union source/target FK graph.
- [ ] Collect every known non-`1062` blocker from prior strict-run logs; include at least `users_search_queries_history` and its complete FK component.
- [ ] Rank FK-isolated candidates by estimated rows and confirm the intended high-volume exclusions. Expected candidates include page-view/log/stat tables, but names alone are not evidence.
- [ ] Review selected and excluded table lists, FK edges, non-syncable components, estimated work reduction, and scope hash.
- [ ] Stop planning if any excluded table is now FK-connected or any connected component cannot be fully synchronized.

### Target bootstrap

Use `mysql-gc -s prod-rw` for the eventual authorized migration.

- [ ] Create/provision `cdc_sync` with `REQUIRE SSL`; never place its password in Git, argv, logs, or the plan artifact.
- [ ] Grant the tested application privileges required by schema convergence and locked row synchronization:
  `SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX, REFERENCES, LOCK TABLES, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, EXECUTE, EVENT, TRIGGER` on the target application schema.
- [ ] Grant `CREATE` on `cdc.*` because the current progress adapter executes idempotent schema/table creation.
- [ ] Grant only `SELECT, INSERT, UPDATE` on `cdc.sync_runs` for progress data. Do not grant access to `cdc.row_conflicts` or its inventory procedure.
- [ ] Create/verify the exact `cdc.sync_runs` schema before admitting the Job.
- [ ] Add a dedicated sealed-secret key such as `SYNC_TARGET_PASSWORD`; the Job uses `--target-user cdc_sync` and `--target-password-env SYNC_TARGET_PASSWORD` with no fallback to `TARGET_PASSWORD`/`cdc_stream`.
- [ ] Capture `SHOW GRANTS FOR 'cdc_sync'@'%'` and exact `cdc.sync_runs` DDL as prerequisite evidence.

### One-off Flux Job

- [ ] Create one manifest named `mariadb-mysql-cdc-fk-sync-YYYYMMDD-<scopehash12>` and one ConfigMap containing the reviewed selected catalog and scope report.
- [ ] Pin the Job image by tag and digest:
  `registry.digitalocean.com/globalcomix/mariadb-mysql-cdc:ffa244d@sha256:7c22fbd43e09b054a8efaf1b0850a7f9f0fd443ecdc2839906d0c88d62398ca0`.
- [ ] Label the pod `app: mariadb-mysql-cdc-sync`; existing Kubernetes and Cilium policies already admit this label.
- [ ] Use `cdc_reader` for the source and dedicated `cdc_sync` for the target.
- [ ] Use `--chunk-size 10000`, `--parallelism 4`, `--progress-table cdc.sync_runs`, and exact run ID `fk-sync-YYYYMMDD-<scopehash12>`.
- [ ] Set `backoffLimit: 0` and `restartPolicy: Never`; omit automatic retry, deadline, and TTL cleanup.
- [ ] Initial resource envelope: request `1` CPU and `1Gi` memory; limit `4` CPU and `8Gi` memory. Any change is reviewed before execution.
- [ ] Keep the completed/failed Job and ConfigMap until durable evidence and final cutover results are retained.
- [ ] Add the exact Job/ConfigMap to ops kustomization through Git review. Never create or patch it directly with `kubectl`.

## Phase 3 — targeted bulk convergence

Requires separate explicit authorization.

1. [ ] Record the CDC/ops Git revisions, Job manifest hash, scope hash, image digest, exact run ID, selected tables, excluded tables, grants, and initial live-stream identity.
2. [ ] Confirm the only live stream is healthy on `05c2345`, one Ready pod, zero restarts, and an advancing checkpoint.
3. [ ] Revalidate the source/target FK graph immediately before admitting the Job. Stop if it differs from the reviewed scope.
4. [ ] Reconcile the one-off Job while leaving the `05c2345` Deployment unchanged.
5. [ ] Observe Job logs and `cdc.sync_runs` externally. Do not enter the pod, mutate progress, or start a second Job.
6. [ ] Allow transient stream lag caused by per-table `WRITE` locks; require the old stream to catch up after the Job releases them.
7. [ ] If the Job fails, preserve the pod/logs/progress and stop. An identical-run resume or any changed scope requires explicit authorization.

### Bulk acceptance gate

All conditions are mandatory:

- [ ] The running/completed Job image ID matches the pinned strict digest.
- [x] Superseded (unexecuted stale plan): the former requirement that every `run_spec_json` row match the reviewed catalog, chunk size, parallelism, endpoints, and progress table is not a live gate. Durable progress is identified only by run ID; the physical column is ignored legacy evidence, and resume needs no authorization or run-spec migration.
- [ ] The expected progress key set is exact: one row per selected table for each of `prerequisite_schema`, `rows`, and `final_constraints`; no missing, unexpected, duplicate, stale `running`, or `error` row exists.
- [ ] Every expected progress row is `complete` and has no `last_error`.
- [ ] Final selected-scope source/target schema fingerprints match and every selected FK constraint is restored.
- [ ] No coercion, CHECK, FK, schema, generated-column, mapping, or unique-key blocker remains in the selected scope.
- [ ] The fresh post-run FK graph still matches the reviewed complete selection.
- [ ] Bounded primary-key-window checks pass for the original blocker table and representative parent/child tables; source writes after the boundary are excluded from the comparison.
- [ ] The live `05c2345` stream returns to Ready, advances past its pre-Job checkpoint, and reports no unresolved non-`1062` row error.
- [ ] Excluded tables remain listed as intentional residual drift; no full-catalog claim is made.

## Phase 4 — strict CDC runtime cutover

Requires a new explicit authorization after the bulk acceptance gate passes.

### Pre-stop evidence

- [ ] Confirm the strict image digest still matches the reviewed digest.
- [ ] Confirm the targeted Job remains terminal-successful and its durable progress still passes the exact acceptance query.
- [ ] Confirm the current stream is `05c2345`, Ready, zero restarts, and advancing.
- [ ] Capture Deployment generation, ReplicaSet/pod identity, image ID, checkpoint row, current source binlog coordinate, DDL-journal blockers, grants, logs, metrics, and error-monitoring baseline.
- [ ] Prepare two separate reviewed GitOps changes. The start change must not reach Flux before the stop change is proven reconciled.

### Exact sequence

1. [ ] Reconcile a stop-only ops change setting the stream Deployment to zero replicas. Do not change grants or image in this step.
2. [ ] Prove zero live stream pods and capture the final old-stream checkpoint.
3. [ ] Capture a source binlog coordinate after the old pod is stopped; this is the strict-start catch-up fence.
4. [ ] Apply `docs/live-stream-runtime-grants-migration-20260818.sql` once with `mysql-gc -s prod-rw`.
5. [ ] Verify `cdc_stream` retains only application, checkpoint, DDL-journal, and DDL-journal inventory-procedure access; verify the obsolete `cdc.row_conflicts`, conflict inventory procedure, and `cdc.table_sync_runs` grants are absent.
6. [ ] Reconcile the start change: pin the stream to the strict `ffa244d` digest and restore one replica. Do not alter checkpoint identity, source identity, transaction ordering, or unrelated arguments.
7. [ ] Verify the new pod image ID/digest, Ready state, zero restarts, startup control-plane validation, stream lease, and checkpoint continuity; verify effective grants externally because stream startup does not inspect them.
8. [ ] Require the strict checkpoint to reach or pass the post-stop source fence and then continue advancing.
9. [ ] Capture three read-only health samples five minutes apart: Ready `1/1`, zero restarts, advancing checkpoint/acceptable lag, no active DDL barrier, and no non-`1062` row error.
10. [ ] Inventory every live CDC runtime and image. No live runtime may remain on `05c2345` or another superseded image.
11. [ ] Retain the Job, scope report, progress query, grant output, checkpoint samples, workload identities, logs, metrics, and error-monitoring results as the cutover evidence bundle.

### Cutover acceptance gate

- [ ] Exactly one strict stream pod is Ready on the pinned `ffa244d` digest.
- [ ] No superseded CDC runtime remains live.
- [ ] Startup control-plane validation passes; effective grants are verified externally because stream startup does not inspect them.
- [ ] Checkpoint continuity is preserved and the strict stream advances beyond the cutover fence.
- [ ] No non-INSERT-`1062` row error, DDL barrier, transaction rollback loop, checkpoint stall, or restart occurs during the observation window.
- [ ] INSERT `1062`, if naturally encountered, is ignored without target reads, conflict-ledger writes, replacement, or repair.
- [ ] `cdc.row_conflicts` remains unchanged by live streaming.
- [ ] Monitoring shows no new CDC regression.

## Stop conditions and authority

Stop immediately and wait for explicit direction if any of these occurs:

- scope/FK graph differs from the reviewed artifact;
- any selected component is incomplete or non-syncable;
- Job or durable stage progress fails;
- schema/final-constraint convergence is incomplete;
- resource exhaustion, OOM, database saturation, or persistent lock failure occurs;
- old stream fails to recover after Job locks release;
- grant migration output differs from the reviewed strict set;
- strict startup fails, restarts, stalls, hits a DDL barrier, or encounters any non-`1062` row error;
- checkpoint continuity or image identity cannot be proven.

Not authorized automatically:

- restoring `05c2345`;
- reversing grants;
- editing `cdc.sync_runs` or checkpoints;
- repairing rows;
- rerunning/resuming the Job;
- changing scope, chunk size, parallelism, identity, or image;
- deleting evidence;
- switching application traffic.

Monitoring and diagnosis do not grant rollback or repair authority.

## Completion evidence

The work is complete only after the evidence bundle contains:

- implementation and ops commits plus passing verification;
- immutable full, selected, excluded, and FK-edge scope artifacts with hash;
- exact `cdc_sync` and strict `cdc_stream` grants;
- exact `cdc.sync_runs` schema and terminal progress rows;
- Job/pod image IDs and resource configuration;
- pre/post checkpoint and source-fence coordinates;
- selected-scope schema/FK convergence proof and bounded blocker-table checks;
- post-cutover workload inventory, logs, metrics, and error-monitoring proof;
- an explicit statement that excluded tables remain outside the convergence claim.
