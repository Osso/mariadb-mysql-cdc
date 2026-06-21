# Cutover Workflow

Cutover is the first workflow allowed to move application traffic to the target.
It is deliberately stricter than rehearsal.

## Sequence

1. Stop application writes to the source.
2. Drain CDC lag to the configured threshold.
3. Reject cutover if any CDC event remains quarantined.
4. Validate source and target counts, sampled checksums, and row divergences.
5. Switch the application endpoint to the target.
6. Resume application writes.

## Failure Behavior

If cutover fails after writes are stopped and before the endpoint is switched,
the workflow attempts to resume writes and returns the original failure. If
resuming writes also fails, both errors are reported.

The endpoint is switched only after lag and validation gates pass.
