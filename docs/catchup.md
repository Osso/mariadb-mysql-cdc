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
