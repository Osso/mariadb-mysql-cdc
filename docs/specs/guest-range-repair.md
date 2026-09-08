# Dedicated guest range repair

A bounded, insert-only repair for an explicitly supplied inclusive `guests.guest_id` range. Core implementation: `src/sync/guest_range_repair.rs`. This command is separate from [FK orphan repair](fk-orphan-repair.md); that tool's limits remain unchanged.

## What it must do

### Scope and input
- [ ] Accept source/target connection options, required `--start-guest-id`, `--end-guest-id`, `--expected-rows`, and optional `--batch-size` (default 100, maximum 1000).
- [ ] Reject reversed/empty/overflowing ranges, expected counts unequal to inclusive range width, duplicate range options, unknown options, and invalid batch sizes before connecting.
- [ ] Operate only on `guests`; no arbitrary table, recursive repair, source mutation, session mutation, or control-plane writes.

### Preconditions and consistency
- [ ] Require exact source count/minimum/maximum before target writes. Read count and bounded keyset pages from one read-only repeatable-read source snapshot; copied values represent that snapshot, not necessarily the latest concurrently changed source values.
- [ ] Require source/target InnoDB base tables with matching typed row metadata, integer PK exactly `guest_id`, and no generated columns. Require compatible `utms` metadata and PK exactly `id`.
- [ ] Validate exactly one enforced canonical FK `guests(utm_id)` → local `utms(id)`, with RESTRICT update/delete and NONE match. Accept the canonical source name and its normal mapped target name respectively.
- [ ] Require each non-null referenced target UTM before copying its guest; retain its shared row lock through the target batch commit. Null UTM references need no parent repair.

### Mutations and failure
- [ ] Insert complete source row values using strict inserts. Equal existing target rows are unchanged; differing rows fail closed without updates, deletes, ignore, upsert, or secondary-key reconciliation.
- [ ] Bound source pages and target transactions by batch size. Verify contiguous numeric keyset coverage, lock target identities, and compare complete target readback with source snapshot before commit.
- [ ] Roll back the entire current target batch on failure, including readback/commit errors; report rollback errors alongside the original failure. Prior committed batches remain and exact equal rows make reruns idempotent. Lost commit acknowledgement may leave a committed batch; do not automatically retry mutations.

## How it works
- [Unified sync context](unified-sync.md)
- [Target writer contract](../target-writer.md)

## Implementation inventory
- `src/sync/guest_range_repair.rs`: config/parser, command entry, bounded orchestration, transactional backend seam.
- `src/sync/guest_range_repair/mysql_backend.rs`: scoped metadata, source snapshot, typed reads, locked target checks and strict writes.
- `src/sync/guest_range_repair/tests.rs`: concrete row-state seam tests.

## Tests asserting this spec
- `src/sync/guest_range_repair/tests.rs`: multi-page full-value insert/rerun, source count mismatch, differing existing rows, missing parent, corrupt readback, commit failure, invalid bounds/batch limits.

## Known gaps (current cycle)
- [ ] Parent integration owns CLI wiring and database-backed proof of SQL, transaction/lock semantics, canonical metadata, and post-repair child FK validation.
- [ ] Core seam GREEN evidence pending initial implementation commit.

## Out of scope
- Production identifiers, hardcoded repair ranges, deployment, broad verification, and broker operations.
- Updating existing guests, repairing UTM ancestors, modifying sessions, or validating unrelated tables.
- Whole-run atomic rollback or guaranteeing latest source values during concurrent source changes.
