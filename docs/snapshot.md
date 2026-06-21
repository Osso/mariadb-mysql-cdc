# Snapshot

Snapshot copies source tables before CDC apply starts.

The snapshot core is storage-agnostic:

- `SnapshotTable` names the table, primary-key columns, and selected columns.
- `ChunkRequest` requests a deterministic primary-key ordered chunk.
- `SnapshotSource` reads chunks from the source.
- `SnapshotTarget` writes rows to the target.
- `SnapshotProgressStore` persists per-table progress.

Progress is tracked per table:

- last copied primary-key value
- copied row count
- completion flag

This lets a snapshot resume from the last successfully imported chunk instead
of restarting a table.

The file-backed progress store writes JSON through a temporary file and rename.

