# Catchup Workflow

Catchup uses the pragmatic migration path chosen for rehearsal:

1. Record a global binlog start coordinate `t0`.
2. Snapshot source tables into the target in primary-key chunks.
3. Replay all CDC events from `t0` globally, in binlog order.
4. Continue streaming after replay reaches the live tail.
5. Validate counts, checksums, and row samples.

This is not a perfectly consistent snapshot. Chunk reads happen after `t0`, so
some rows copied by the snapshot may already include changes that also appear in
the replay stream. During catchup, duplicate-key INSERT replay is therefore
treated as expected and can be ignored with:

```bash
--insert-conflict-policy ignore-duplicate
```

Updates and deletes still replay normally. Unsupported SQL is still quarantined.

This keeps the workflow simple: no staging tables, no per-row touched-key
overlay, and no assumption that source statements stay inside chunk boundaries.

## Resumable Snapshot Catchup

Use `catchup-snapshot` for the source-to-target backfill. It reads the source
schema inventory, copies rows in primary-key chunks, writes target rows with
`INSERT IGNORE`, and persists chunk progress to the target CDC progress table
and the local progress file.

```bash
mariadb-mysql-cdc catchup-snapshot \
  --source-host 192.0.2.10 \
  --source-user cdc_reader \
  --source-password-env SOURCE_PASSWORD \
  --source-database globalcomix \
  --target-host target-mysql.example \
  --target-port 25060 \
  --target-user target_user \
  --target-password-env TARGET_PASSWORD \
  --target-database globalcomix \
  --progress-file /var/lib/mariadb-mysql-cdc/snapshot-progress.json \
  --chunk-size 10000 \
  --parallel-workers 4
```

`--parallel-workers 4` splits a table into disjoint primary-key ranges. Each
worker opens its own persistent source, target, and progress database
connections, and writes range-scoped checkpoints such as
`table_name#range2` to the CDC progress table. Leave it at `1` for a sequential
single-worker catchup.

For a single-table retry or rehearsal:

```bash
mariadb-mysql-cdc catchup-snapshot ... --table activity_tracking
```

Progress can be checked without reading pod logs:

```bash
mariadb-mysql-cdc catchup-progress \
  --progress-file /var/lib/mariadb-mysql-cdc/snapshot-progress.json
```

The durable progress state supports safe restart after a failed pod. Sequential
catchup resumes from the table checkpoint; parallel catchup resumes from each
worker range checkpoint.

## Table Repair Runs

Use `sync-table` after catchup or validation identifies a table-level drift. Every
invocation requires `--run-id`; choose a new descriptive ID for each recurring
repair. Reuse an ID only to resume the exact interrupted invocation. A completed
ID is terminal, and changing source/target endpoint or database, target write
policy, table shape, mode, chunk size, bounds, `--max-deletes`, or
`--updated-since` requires a new ID. A target-side named lock rejects concurrent
processes using the same ID.

```bash
mariadb-mysql-cdc sync-table \
  --source-host 192.0.2.10 \
  --source-user cdc_reader \
  --source-password-env SOURCE_PASSWORD \
  --source-database globalcomix \
  --target-host target-mysql.example \
  --target-port 25060 \
  --target-user target_user \
  --target-password-env TARGET_PASSWORD \
  --target-database globalcomix \
  --table releases \
  --primary-key id \
  --columns id,slug,title,updated_at \
  --mode apply \
  --run-id releases-repair-20260710-01
```

Run state defaults to `cdc.table_sync_runs`. It is separate from legacy
`cdc.table_sync_progress`, which remains the catchup checkpoint store. Inspect a
specific repair run with:

```bash
mariadb-mysql-cdc sync-progress ... \
  --progress-table cdc.table_sync_runs \
  --run-id releases-repair-20260710-01
```

For a recent-update accelerator, add both `--updated-at-column` and
`--updated-since`. An interrupted retry with the same run ID restarts from the
beginning rather than resuming by primary key, because a source row can become
newly eligible behind a saved key. Idempotent upserts make that restart safe.
These runs never delete target orphans and cannot be combined with primary-key
range bounds.

```bash
mariadb-mysql-cdc sync-table ... \
  --table releases --primary-key id --columns id,slug,title,updated_at \
  --mode apply --run-id releases-updated-20260710-01 \
  --updated-at-column updated_at --updated-since '2026-07-01 00:00:00'
```

Normal range repair reports extra target rows. Deletion requires `--mode apply`
and an explicit nonzero `--max-deletes`; otherwise the default limit is zero.
