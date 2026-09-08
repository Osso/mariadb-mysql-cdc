# Bounded foreign-key orphan repair

`repair-fk-orphans` is a source-authoritative, target-only repair command for the
FK orphan sets explicitly allowlisted below. It is an out-of-band
one-shot tool, not part of staged sync progress or live-stream checkpointing. Operational
implementation details belong in the [FK orphan repair wiki](../wiki/systems/fk-orphan-repair.md).

## What it must do

### Command and scope

- [ ] Expose `repair-fk-orphans` with source and target connection options, `--case`, `--expected-orphans`, optional `--batch-size`, and optional `--limit`.
- [ ] Accept exactly these cases and no arbitrary table, constraint, or identifier input:
  - `artists-favorites`: `artists_favorites_ibfk_2`, child `artists_favorites(user_id,user_username)`, parent `users(id,name)`.
  - `comics`: `comics_ibfk_5`, child `comics(artist_id,artist_name)`, parent `artists(id,name)`.
  - `phrases-suggestions`: `phrases_suggestions_ibfk_1`, child `phrases_suggestions(author_id,author_username)`, parent `users(id,name)`.
  - `forums-replies`: `forums_replies_ibfk_2`, child `forums_replies(author_id,author_username)`, parent `users(id,name)`.
  - `comics-langs-category`: `ibfk_accl_category`, child `comics_langs(comic_id,comic_category_id)`, parent `comics(id,section_id)`.
  - `comics-langs-type`: `ibfk_accl_type`, child `comics_langs(comic_id,comic_type_id)`, parent `comics(id,comic_type_id)`.
  - `releases-name`: `releases_ibfk_1`, child `releases(comic_id,comic_name)`, parent `comics(id,name)`.
  - `releases-type`: `releases_ibfk_10`, child `releases(comic_id,comic_type_id)`, parent `comics(id,comic_type_id)`.
  - `releases-category`: `releases_ibfk_2`, child `releases(comic_id,comic_category_id)`, parent `comics(id,section_id)`.
  - `releases-visibility`: `releases_ibfk_3`, child `releases(comic_id,comic_is_visible)`, parent `comics(id,is_visible)`.
  - `releases-id`: `releases_ibfk_6`, child `releases(comic_id)`, parent `comics(id)`.
  - `releases-slug`: `releases_ibfk_7`, child `releases(comic_slug)`, parent `comics(slug)`.
  - `releases-show-in-list`: `releases_ibfk_9`, child `releases(comic_id,comic_show_in_list)`, parent `comics(id,show_in_list)`.
  - `releases-format`: `releases_ibfk_format`, child `releases(comic_id,comic_format_id)`, parent `comics(id,comic_format_id)`.
- The comics-parent cases use child and parent primary key `id`, update `CASCADE`, delete `RESTRICT`. Each relation remains independently discoverable; copying a complete child can resolve overlapping cases, so re-count before subsequent repairs rather than applying redundant mutations. The slug relation does not itself supply parent primary key `id`; backend integration must resolve that identity before mutation.
- [ ] Require `--expected-orphans` to equal the exact bounded target orphan count observed before mutation; abort when the count changes.
- [ ] Default `--batch-size` to 50 and reject values above 100; default `--limit` to 1000 and reject values above 1000. Reject an expected count greater than the limit.

### Fail-closed validation

- [ ] Validate the exact source table, column, primary-key, composite-FK, update-rule, delete-rule, and enforcement metadata for the selected allowlisted case.
- [ ] Validate that the target child/parent tables match the source writable metadata and that the allowlisted FK is absent from the target before mutation.
- [ ] Abort without target mutation when source or target metadata, parent identity, or source/target FK state is unexpected; retain a production-shaped live-endpoint proof for every failure boundary.
- [ ] Re-read each candidate by exact child primary key and skip candidates that are no longer target orphans; fail closed if an expected child, parent, or identity changes during repair.

### Source-authoritative child repair

- [x] Lock only the selected target child and parent tables for each bounded batch, then commit or roll back that batch before releasing the locks.
- [x] When the source child exists, require the source FK relationship to be valid and the target parent to have the exact source parent identity before copying the complete source child row to the target.
- [x] When the source child is absent, delete only the exact target child primary-key row; never create or mutate parent rows.
- [ ] Verify source stability, target child state, target parent identity, and zero remaining selected orphan identities after mutation.
- [x] Roll back and unlock on batch-start, mutation, verification, commit, or cleanup failure.

### Durable-state and operations boundary

- [ ] Never write `cdc.sync_runs`, progress cursors, row counters, stream checkpoints, DDL journals, run specifications, or any other CDC control-plane state.
- [ ] Run at most one repair process at a time and execute selected cases sequentially, with evidence and a terminal result captured before starting the next case.
- [ ] Keep Kubernetes Job creation, image publication, cleanup, and deployment ownership in the ops repository; this spec defines only the CLI contract and repair safety boundary.

## How it works

- [FK orphan repair](../wiki/systems/fk-orphan-repair.md) — runtime flow and database interaction details.
- [Unified synchronization](unified-sync.md) — the separate staged sync and durable progress contract that this command must not modify.

## Implementation inventory

- `src/main.rs` — command dispatch and user-facing usage text.
- `src/sync/fk_orphan_repair.rs` — allowlist, CLI parsing, bounded orchestration, validation, source-authoritative repair, and report formatting.
- `src/sync/fk_orphan_repair/mysql_backend.rs` — source/target metadata reads, orphan queries, target locks, exact reads, mutations, verification, and transaction cleanup.
- `src/sync/fk_orphan_repair/tests.rs` — focused allowlist, repair, fail-closed, rollback, cleanup, and idempotence coverage.
- `tests/sync_cli.rs` — top-level help coverage for the command.

## Tests asserting this spec

- `src/sync/fk_orphan_repair/tests.rs` — exact allowlisted identities, source-present update, source-absent delete, invalid source relationship refusal, failed batch cleanup, and zero-orphan idempotence.
- `src/sync/fk_orphan_repair/mysql_backend.rs` — bounded orphan query, exact allowlisted join, and selected-table lock SQL coverage.
- `tests/sync_cli.rs` — command help and description.

## Known gaps (current cycle)

- [ ] Prove all allowlisted cases against disposable production-shaped MariaDB/MySQL endpoints with the exact expected counts and target metadata.
- [ ] Record production one-shot evidence for sequential execution and zero remaining orphan identities.

## Out of scope

- Generic FK repair, arbitrary table or constraint selection, parent-row repair, full-table synchronization, parity sweeps, or fallback mutation engines.
- Durable progress, checkpoint, DDL-journal, run-spec, or live-stream state changes.
- Kubernetes manifests, Flux ownership, image publication, and deployment procedures; those belong to the ops repository.
