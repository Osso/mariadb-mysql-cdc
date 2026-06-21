# Rehearsal Workflow

Rehearsal runs the migration path against a target that is explicitly not
serving application traffic.

## Sequence

1. Assert the target endpoint is not serving traffic.
2. Snapshot every planned table in deterministic primary-key chunks.
3. Apply CDC changes from the recorded binlog position.
4. Validate source and target table counts.
5. Validate deterministic sampled checksums.
6. Produce paged row-level divergence reports.

## Pass Criteria

A rehearsal passes only when:

- every count comparison matches
- sampled checksums have no differences
- row divergence reports are empty
- CDC apply leaves zero quarantined events

The target remains a rehearsal artifact until these gates pass. Cutover is a
separate workflow.
