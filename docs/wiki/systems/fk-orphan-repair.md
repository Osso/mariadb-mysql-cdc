# FK orphan repair

Normative requirements: [bounded FK orphan repair](../../specs/fk-orphan-repair.md).

`repair-fk-orphans` repairs one exact allowlisted FK case per invocation. It reads
source and target, requires `--expected-orphans` to match the bounded target set,
and uses a target transaction. It never writes source rows or CDC progress,
checkpoints, journals, or run specifications.

## Cases

The original four cases repair or delete only child rows. They require an existing,
source-identical target parent and never restore a parent.

The ten `comics-langs-*` and `releases-*` cases may restore the complete source
`comics` row before their child. Parent and child are write-locked, constraints stay
enabled, parent-first mutations and any FK cascades share the batch transaction, and
exact source/target rereads precede commit. The slug case validates its source slug
relationship while resolving the parent by `comic_id`; it does not look up a parent
by slug.

## Operational boundary

Run selected cases sequentially. Before a live run, re-read the exact count and
confirm the selected target FK remains absent. Keep terminal evidence showing zero
remaining identities. Disposable proof covers the ten comics-parent selectors and
unchanged CDC control-plane data; it is not production authorization.
