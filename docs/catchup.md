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
