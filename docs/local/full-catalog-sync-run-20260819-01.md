# Full-catalog sync runtime evidence — 2026-08-19

**Status: ACTIVE RECOVERY; NOT COMPLETE.** This is runtime evidence, not completion proof. The reviewed secondary-unique repair is deployed in immutable image `6047e38@sha256:aa132d5104560522679089965ca9e2f41521abc6662fb529e3e691c32d4a30da`; Flux Job `mariadb-mysql-cdc-sync-full-20260822-resume-03` is actively resuming the durable run. Terminal resume-02 remains retained until final pruning.

Scope evidence: [full-catalog-sync-scope-20260819.md](full-catalog-sync-scope-20260819.md)

## Run identity and initial invocation

Durable progress is identified only by run ID `full-catalog-sync-20260819-01`. The values below describe the initial invocation, not immutable run state. On resume, source/target endpoint, address or domain, current table scope/definitions, chunk size, parallelism, and other invocation settings may change without authorization or run-spec migration. The physical `run_spec_json` column is ignored legacy evidence; historical distinct-spec counts are observations, not completion gates.

Commit `e2d1fa5` implements this run-ID-only behavior. At this documentation revision it has not yet been built, deployed, or independently verified.

- Run ID: `full-catalog-sync-20260819-01`
- Job: `ops/mariadb-mysql-cdc-sync-full-20260819-01`
- Initial scope: 461 included tables; 6 excluded; 467 catalog tables.
- Scope artifact SHA-256: `b6504e8d8ad5009133055fc179e4e3395e632f5bf0cd45c07a3996f43bcdae5a`
- Initial ordered included-name SHA-256: `56f48fbbc283ed051ce0558004d0549f427c4d39acc2b5a0539eeb2d3c6413c7`
- Initial invocation settings: `--chunk-size 1000`, `--parallelism 1`, `--progress-table cdc.sync_runs`.
- Image: `86f6e28@sha256:aca9c631540e654d7acd58d5e71361658743149c171a618fca4ed769c8e7f1d6`.
- Resources: request `500m CPU / 512Mi`; limit `2 CPU / 2Gi`.
- Deadline: 604800 seconds (7 days); `backoffLimit: 0`; `restartPolicy: Never`.
- Target CA is read-only mounted. Passwords are supplied through the existing Secret by env-var name; no secret values appear in logs or evidence.

## Preflight and Flux

- Catalog preflight Job succeeded at `05:16:42Z`; zero restarts/failures.
- Preflight produced 461 FK-closed, acyclic, PK-complete included tables and six generated-column exclusions.
- No preflight `cdc.sync_runs` rows were created; no active or failed prior sync existed.
- Flux fetched `master@sha1:68ff75161fcb25462008ff9315de748830fcdf35` and applied the full Job.
- The completed catalog Job was pruned when the full Job appeared.
- `infra-ops` uses `wait: true` with a 60-second health timeout. While the long-running Job is `InProgress`, this expected timeout leaves `infra-ops` NotReady/Unhealthy; it is not evidence of Job failure. Flux continues fetching/applying newer revisions.

## Full Job and schema stage

- Created: `2026-08-19T05:44:45Z`.
- Pod was Running/Ready with zero restarts at observed samples.
- Schema-stage result: `461/461` tables converged.
- Schema log summary: 461 table summaries; 130 DDL statements executed across 85 tables; 0 failed, skipped, or blocked statements.
- Runtime log snapshot: `/tmp/claude/mariadb-mysql-cdc-sync-full-20260819-01.log`, 101,001 bytes, SHA-256 `d4c0522426dfa72743eb5888a35b1ec1b1d28378e40ceec3c280ca38199436ee`.

## Row progress through 06:00 UTC

| Sample | Durable row state |
|---|---|
| `05:50:35Z` | 13 tables complete, 1 running; 7,833 chunks; 7,811,221 rows scanned; 0 errors. |
| `05:51:22Z–05:59:51Z` | Current table `activity_tracking`; 20,236 chunks / 20,236,000 rows; 13 tables complete, 1 running; 0 errors. |
| `06:19:36Z` | 71 tables complete, `comics_assets` running; 45,080 chunks; 44,967,854 rows scanned; 598,333 inserts, 399,389 updates, 0 deletes; 0 errors. |
| `06:25:39Z` | 75 tables complete, `comics_assets_files` running; 55,779 chunks; 55,660,031 rows scanned; 598,333 inserts, 400,215 updates, 0 deletes; 0 errors. |
| `06:35:24Z` | 79 tables complete, `comics_assets_files_suggested_fragments` running; 69,279 chunks; 69,154,329 rows scanned; 598,333 inserts, 400,295 updates, 0 deletes; 0 errors. |
| `06:36:44Z` | 79 tables complete, `comics_assets_files_suggested_fragments` running; 71,166 chunks; 71,041,329 rows scanned; 598,333 inserts, 400,295 updates, 0 deletes; 0 errors or stale progress rows. Durable state had 541 rows, 461 distinct tables, 2 stages, and 1 run specification. |
| `06:49:04Z` | 82 tables complete, `comics_assets_history` running; 93,078 chunks; 92,938,894 rows scanned; 598,333 inserts, 400,456 updates, 0 deletes; 0 errors or stale progress rows. Durable state had 544 rows, 461 distinct tables, 2 stages, and 1 run specification. |
| `07:03:58Z` | 108 tables complete, `comics_releases_fragments_stats` running; 131,480 chunks; 131,307,296 rows scanned; 623,570 inserts, 409,555 updates, 0 deletes; 0 errors or stale progress rows. Durable state had 570 rows, 461 distinct tables, 2 stages, and 1 run specification. |
| `07:19:02Z` | 108 tables complete, `comics_releases_fragments_stats` running; 170,195 chunks; 170,022,296 rows scanned; 623,570 inserts, 409,555 updates, 0 deletes; 0 errors or stale progress rows. Durable state remained 570 rows, 461 distinct tables, 2 stages, and 1 run specification. |
| `07:34:01Z` | 109 tables complete, `comics_releases_fragments_views` running; 207,342 chunks; 207,167,554 rows scanned; 623,570 inserts, 409,555 updates, 0 deletes; 0 errors or stale progress rows. Durable state had 571 rows, 461 distinct tables, 2 stages, and 1 run specification. |
| `07:49:00Z` | 109 tables complete, `comics_releases_fragments_views` running; 242,695 chunks; 242,520,554 rows scanned; 623,570 inserts, 409,555 updates, 0 deletes; 0 errors or stale progress rows. Durable state remained 571 rows, 461 distinct tables, 2 stages, and 1 run specification. |
| `08:04:00Z` | 109 tables complete, `comics_releases_fragments_views` running; 278,995 chunks; 278,820,554 rows scanned; 623,570 inserts, 409,555 updates, 0 deletes; 0 errors or stale progress rows. Durable state remained 571 rows, 461 distinct tables, 2 stages, and 1 run specification. |
| `08:18:58Z` | 109 tables complete, `comics_releases_fragments_views` running; 311,545 chunks; 311,370,554 rows scanned; 623,570 inserts, 409,555 updates, 0 deletes; 0 errors or stale progress rows. Durable state remained 571 rows, 461 distinct tables, 2 stages, and 1 run specification. |

First ~15-minute row throughput was approximately **23.4k rows/s**. Rough elapsed-time estimate was **25–62 hours**, not an authoritative completion forecast; source churn, locks, retries, and table-size distribution can change it.

## CDC and load samples

- CDC remained Ready with zero restarts and no error, failure, panic, rollback, or timeout lines.
- `05:51:51Z–06:00:31Z`: checkpoint advanced `mysqld-bin.002879:159925383` → `mysqld-bin.002879:181976336`; applied statements `137,989` → `163,640`; source master reached `mysqld-bin.002879:204805924`; quarantined remained `0`.
- Sync load: approximately `559–580m CPU / 66Mi`.
- CDC load: approximately `14–15m CPU / 52Mi`.
- Target: 9 connected / 2 running. Source: 134 connected / 5 running.
- Lock sample: 0 row/table/data-lock waits and no pending metadata locks.
- No Job-specific warnings, errors, restarts, or secret-value matches observed.
- `06:36Z`: CDC reached `mysqld-bin.002879:465116258` with 231,484 applied statements and zero quarantines; source master was `mysqld-bin.002879:465400519` (about 284 KiB ahead).
- `06:36Z`: target had 7 connected / 2 running threads, with zero row, table, data-lock, or pending metadata-lock waits. Job and CDC pods were Ready with zero restarts; the last 300 sync log lines contained no suspicious runtime event.
- Flux continued fetching/applying revision `7451dfe042c5f3b667c872adf18fd316a5454b61`; `infra-ops` remained in the expected long-Job health wait.
- `06:49Z`: CDC reached `mysqld-bin.002879:487248836` with 245,631 applied statements and zero quarantines; source master was `mysqld-bin.002879:487574020` (about 325 KiB ahead). Target had 8 connected / 2 running threads and zero row, table, data-lock, or pending metadata-lock waits. Both pods were Ready with zero restarts; no Job warnings or suspicious sync/CDC log lines. Flux was attempting revision `71ddb2ed4527cff005fcff7b01f280cb167c886a` under the expected InProgress health wait.
- `07:04Z`: CDC reached `mysqld-bin.002879:578858281` with 286,317 applied statements and zero quarantines; source master was `mysqld-bin.002879:579202660` (about 344 KiB ahead). Target had 7 connected / 2 running threads and zero row, table, data-lock, or pending metadata-lock waits. Both pods were Ready with zero restarts; no Job warnings or suspicious sync/CDC log lines. Flux remained on the expected long-Job health timeout for revision `71ddb2ed4527cff005fcff7b01f280cb167c886a`.
- `07:19Z`: CDC reached `mysqld-bin.002879:978456292` with 351,841 applied statements and zero quarantines; source master was `mysqld-bin.002879:991219453` (about 12.8 MiB ahead) while CDC continued advancing. Target had 8 connected / 2 running threads and zero row, table, data-lock, or pending metadata-lock waits. Both pods were Ready with zero restarts; no Job warnings or suspicious sync/CDC log lines. Flux remained on the expected long-Job health timeout for revision `3272af84cd553f7f32af31fa3bb29801ce425968`.
- `07:34Z`: CDC advanced across the binlog rollover to `mysqld-bin.002880:283820073` with 716,274 applied statements and zero quarantines; source master was `mysqld-bin.002880:409434730` (about 125.6 MiB ahead) while CDC continued advancing. Target had 8 connected / 4 running threads and zero row, table, data-lock, or pending metadata-lock waits. Both pods were Ready with zero restarts; no Job warnings or suspicious sync/CDC log lines. Flux remained on the expected long-Job health wait for revision `3272af84cd553f7f32af31fa3bb29801ce425968`.
- `07:49Z`: CDC caught back up to `mysqld-bin.002880:428607709` with 811,668 applied statements and zero quarantines; source master was `mysqld-bin.002880:429309672` (about 702 KiB ahead). Target had 9 connected / 3 running threads and zero row, table, data-lock, or pending metadata-lock waits. Both pods were Ready with zero restarts; no Job warnings or suspicious sync/CDC log lines. Flux remained on the expected long-Job health wait for revision `5f952373b3ba97d1abd9dc3906d5994626835316`.
- `08:04Z`: CDC reached `mysqld-bin.002880:514288942` with 844,579 applied statements and zero quarantines; source master was `mysqld-bin.002880:514364461` (about 76 KiB ahead). Target had 8 connected / 3 running threads and zero row, table, or data-lock waits. One sync metadata lock briefly waited behind a granted CDC write lock on `comics_releases_fragments_views`, then cleared within 10 seconds; no sustained pressure. Both pods were Ready with zero restarts; no Job warnings or suspicious sync/CDC log lines. Flux remained on the expected long-Job health wait for revision `76fbc794f647ce9c81ee0c2b8caea4f9a2f033aa`.
- `08:19Z`: CDC caught up to `mysqld-bin.002880:1014973934` with 930,068 applied statements and zero quarantines; source master was `mysqld-bin.002880:1015035233` (about 61 KiB ahead). Target had 8 connected / 3 running threads and zero row, table, data-lock, or pending metadata-lock waits. Both pods were Ready with zero restarts; no Job warnings or suspicious sync/CDC log lines. Flux remained on the expected long-Job health timeout for revision `5a5af35079ff04c8fc6a927f8cae1ff754277d20`.

## Failure incident

The run terminated before completion:

- Pod `mariadb-mysql-cdc-sync-full-20260819-01-l8drf` exited with code 1 at `2026-08-19T14:06:35Z`; the Job became `Failed=True` with reason `BackoffLimitExceeded` at `14:06:38Z`. It had zero restarts, no OOM/eviction, and no Job warning events.
- Exact runtime error: `sync table comics_releases_user_reads failed: connect progress store ... CodecError { IO error: Resource temporarily unavailable (os error 11) }`.
- Code-path review found the failure occurred in `Conn::new` while opening the target progress-store connection, before progress SQL or row SQL ran for `comics_releases_user_reads`. Evidence does not distinguish local socket/file-descriptor pressure from target, TLS, or network resource pressure.
- At `2026-08-21T18:50:33Z`, durable state contained 461 complete `prerequisite_schema` rows, 115 complete `rows` rows, and no `final_constraints` rows: 576 rows total across 461 tables, two stages, and one run specification. No row was running/error and no `last_error` was stored because the failed connection could not record one.
- Latest durable progress was `comics_releases_stats`, complete at `2026-08-19T14:06:04.870288Z`, with 319,071 chunks and 319,069,144 rows scanned.
- Flux source remained healthy, but `infra-ops` was `Ready=False` solely because the failed Job remained in inventory.
- At `2026-08-21T18:48:28Z–18:51:23Z`, serial CDC was Kubernetes Ready with zero restarts and zero quarantines, but functionally stalled at `mysqld-bin.002893:7899284` on repeated `DDL_blocked: DDL_translator_unavailable` reconnects. Target had no row, data, table, or pending metadata-lock pressure.
- Read-only DDL investigation: first occurrence was `2026-08-21T04:54:56.667386208Z` after checkpoint `mysqld-bin.002893:7899284`. Target journal row records exact event `7899326–7899792`, `ALTER TABLE content_sections_events_raw ADD COLUMN IF NOT EXISTS direct_seen_at ... ADD COLUMN IF NOT EXISTS sync_seen_at ... ALGORITHM=INSTANT`, status `translation_pending`, transformation `translator-unavailable`, created `04:54:56`. The same row remains pending; logs reached reconnect attempt 8,677 at `18:56:43Z`, so state has not recovered.
- Code review shows the live translator admits modeled index DDL, supported drop/rename column forms, exact routine forms, and exact convergence CREATE handling; this two-column `ADD COLUMN IF NOT EXISTS ... ALGORITHM=INSTANT` statement is outside the admission policy. `handle_untranslated_ddl_event` durably journals it and intentionally leaves the checkpoint blocked; reconnect retries the same coordinate indefinitely.
- Remediation was implemented in CDC commits `e12170a` and `073990d`: exact admission models both source `IF NOT EXISTS` guards, emits valid MySQL 8.4 unguarded atomic target SQL only after fenced pre-state proof, suppresses execution when both exact columns already exist, and blocks partial/divergent state. Exact and near-miss replay tests passed 8/8; target parse-only `PREPARE`/`DEALLOCATE` of the generated SQL exited 0 without executing DDL.
- Immutable image `b4486da@sha256:99683197cfe6e323f4ad855c53da4386e175a059e4195c00226f6684060a260e` rolled out through ops commit `e887795cc`, contained in deployed clean ops revision `668c5908aae8a724266da8a8991b9f8a6ac3d427`. Deployment and pod were 1/1 Ready on that exact digest with zero restarts.
- The existing journal row recovered without journal or checkpoint edits: `translation_pending` became `prepared` at `2026-08-21T19:38:31Z`, then `checkpointed` with transformation `mariadb-mysql8-v1` at `19:46:03Z`. Both target columns now exist with exact nullable TIMESTAMP definitions and comments.
- CDC advanced from `mysqld-bin.002893:7899284` to `mysqld-bin.002893:101440845` by `19:48:44Z`; applied statements reached at least 37,751, quarantines remained zero, and pending metadata locks were zero. Source master was `mysqld-bin.002896:945694884`, so the stream was still behind but actively catching up.

Captured evidence lives under `/tmp/claude/cdc-full-sync-failure-20260819/`. The full Job log is 123,609 bytes with SHA-256 `39c44d53fd3a7ed1f2c28cfbb77cc6b4b72d1dcd04a4f491b98fee8aaf97d7cc`.

Stop containment followed the reviewed GitOps path. Ops commit `76eee82d1aecdc56abc60603582e11751f1508dd` removed the failed one-off Job manifest and its Kustomization entry, then was pushed to `origin/master`. Flux fetched and applied that revision at `2026-08-21T18:54:13Z`; `infra-ops` returned to `Ready=True` / `Healthy=True`, and the failed Job and pod were pruned. No `cdc.sync_runs` row or CDC checkpoint was modified.

Tracked cleanup documentation and tests were updated in ops commit `4bf42b999535dc73457f6fda166e52b0e26f3d25`. Independent verification at `2026-08-21T19:01:35Z` proved a clean pushed checkout, 6/6 focused tests, a valid Kustomize render with zero failed-Job references, exact Flux application with `Ready=True` / `Healthy=True`, Job NotFound, zero owned pods, and no Job inventory entry.

The connection failure was fixed in commit `6c05696`, published as `6c05696@sha256:4f3b5c759dcb2d7afad1d08ffbc3ab328eedd21629811d2824259f10689e922b`, and rolled out through ops commit `c2d9ffdc7`. The stream remained Ready with zero restarts and zero quarantines.

A reviewed exact-argument resume Job, `mariadb-mysql-cdc-sync-full-20260821-resume-01`, was deployed through ops commit `6d527d694`. Preflight found no conflicting Job, 576 durable rows with one run specification, zero running/error rows, and advancing CDC. The Job failed before schema or row mutation at `2026-08-21T20:08:03Z` with `sync prerequisite_schema progress run specification mismatch for table access_tokens_countries`. Evidence is retained under `/tmp/claude/cdc-full-sync-resume-failure-20260821/`; the Job log SHA-256 is `d6f8d775a71ff715b064cad44eb0ba8df60e706dc4a5b431e6c5019b78231f00`.

The mismatch was caused by one source-inventory delta since August 19: `content_sections_events_raw` appended writable nullable columns `direct_seen_at` and `sync_seen_at`. Primary keys and PK ordering did not change. The persisted run specification SHA-256 was `d71eea35d0b667a9088272e7d8de719fdf2a2d40b6b88067b20aca41b5d5fda0`. Under the former implementation, recent source rows containing non-NULL `direct_seen_at` were treated as unsafe to resume with the old row projection; `e2d1fa5` supersedes that run-spec gate.

Ops commit `06aaf6874` pruned the terminal resume Job through Flux after evidence retention. Flux returned Ready; the Job and pod were absent; durable progress remained 461 complete prerequisite rows and 115 complete row rows with one run specification and no errors. No progress, checkpoint, or journal row was edited.

## Historical additive run-spec recovery record (superseded by `e2d1fa5`)

The following records the former authorization-based recovery model and its observed execution. It is preserved as historical evidence, not a current requirement: `e2d1fa5` makes run ID the only durable progress identity and removes run-spec authorization/migration.

An explicit atomic migration was authorized for this exact run from persisted SHA-256 `d71eea35d0b667a9088272e7d8de719fdf2a2d40b6b88067b20aca41b5d5fda0` to the current additive specification. Authorization preserved the run ID, 576 durable rows, endpoints, settings, ordered 461-table scope, primary keys, primary-key ordering, retained-column ordering, and failure evidence. It permitted only the two added writable nullable columns on row-unstarted `content_sections_events_raw`.

CDC commits `79dc356`, `2d19cf1`, `82e3efb`, and `8d6c1de` implement additive planning, exact hash authorization, serializable exact-run locking/update, and runtime wiring. Commit `f3155b9` adds the disposable MariaDB 11.4 to MySQL 8 proof. The refactored scenario passed on August 21, 2026 with wrong-hash no-write behavior, three-row atomic migration, preserved progress metadata, resumed data convergence, one current terminal specification, a no-write idempotent retry, and changed-table row-progress rejection. Evidence: `/tmp/claude/cdc-sync-authorized-additive-spec-migration-refactored-8d6c1de.log`.

Image `da5e5e0@sha256:415ba388e9e53b09947c50ddef9dc6b981da9abe5460306e5ed041e3cdf8ce10` passed 11 runtime checks and a Trivy HIGH/CRITICAL scan with zero findings, then replaced the live CDC pod through Flux. The new pod was Ready with zero restarts; its checkpoint advanced 396,656 bytes in 15 seconds and logs showed zero quarantines or suspicious errors.

Ops commits `973433e1d` and `0aada47fb` added and clarified Flux-owned Job `mariadb-mysql-cdc-sync-full-20260821-resume-02`. At `2026-08-21T22:18:09Z`, its serializable transaction locked and migrated all 576 rows from old SHA-256 `d71eea35d0b667a9088272e7d8de719fdf2a2d40b6b88067b20aca41b5d5fda0` to current SHA-256 `c11dbfffee7bda5e4be1ba9206e66ec60adc63ad0c9375d38dfa6d9f2a884618`. The emitted audit recorded exactly `content_sections_events_raw + direct_seen_at,sync_seen_at`, `affected_row_count=576`, and `locked_row_count=576`. No progress, checkpoint, or journal row was edited manually.

At `2026-08-21T22:23:50Z`, durable state had 461 complete prerequisite rows, 115 complete row rows, and `comics_releases_user_reads` running with 6,998 chunks / 6,998,000 rows scanned. All 577 rows had one current specification, with zero errors or stored errors. The Job pod was Running/Ready on the exact image with zero restarts. Flux had applied the revision but reported the expected one-minute health timeout while the Job remained `InProgress`.

## Secondary-unique owner failure and recovery proof

The latest Job failed at `2026-08-22T08:58:06Z` with exit code 1 and `BackoffLimitExceeded`. The exact error was a strict insert into `paid_subscriptions_users_pages`: MySQL `1062`, index `uidx_user_access_token_page`, duplicate value `614623-2181834`.

Durable state remained resumable and was not manually changed: 461 `prerequisite_schema` rows complete, 320 `rows` rows complete, one `rows` table running, 782 total rows, one current run specification, last committed primary key `172054507`, 171,849 chunks, 171,849,000 rows scanned, and 9,695 updates. No progress, checkpoint, or DDL-journal edits were performed. Failure artifacts: Job log SHA-256 `72cc1a576b9c810c14191ebdf36065bf5dde49df33c634b45f9db226f1cf54d5`, Job JSON SHA-256 `3a4153602a99b75ce7d38174a399d49ed4e26dfcae3b525fa1baa399ac3ff636`, and pod JSON SHA-256 `f357af4f664e254df3b9d9211151fb5ad806be4d5131127a3775e09506c4039c`.

Source owns intended row `id=172054899`. Target had a wrong-primary-key owner `id=172079103` carrying that row's secondary-unique identity; source `id=172079103` legitimately owns a different identity. This is the importer-omitted-ID corruption shape documented in [`brief-2026-07-26.md`](brief-2026-07-26.md), where later primary-key-addressed updates can leave a misfiled owner with mixed values.

The current unified-sync repair is strict and fail-closed. It preserves plain batched `INSERT`; only a named full non-`PRIMARY` secondary unique `1062` enters repair. Under the existing target `WRITE` lock and transaction it exact-reads the owner, reads the owner primary key from current source, updates the target owner to the complete source row or deletes it when source-absent, verifies, retries the failed plus remaining insert rows, guards repeated conflicts, verifies intended rows, commits, and then emits a secret-free audit. `PRIMARY`, prefixed, expression, NULL, absent, ambiguous, repeated, and source-legitimate-owner evidence remains a hard failure. Counters remain planned source operations; run identity/specification and live CDC duplicate handling are unchanged. Implementation commits are `81fb4be`, `f64e566`, and `6047e38`.

Disposable scenario `sync-unique-owner-rollback-resume` passed against MariaDB 11.4 and MySQL 8: actual 128-row first batch, actual 2-row `1062` batch, injected retry failure rolled back all target changes with row-stage progress absent, identical run resumed, 131 rows converged, progress completed at `["200"]` with `chunks=3`, `rows=131`, `inserts=130`, `updates=0`, `deletes=0`, both attempts recorded `first,second,second`, and exactly one post-commit audit was emitted. The corrected ordered proof also asserted transaction start, table lock, owner update, retry, rollback/commit. Evidence: `/tmp/claude/cdc-sync-unique-owner-real-6047e38.log`, SHA-256 `1828230532025e48ff5f1cedbaa0c7e45607ce4026c59f8744ad2a7444a415c5`.
## Active resume and completion gates

The repaired runtime was published and verified as `registry.digitalocean.com/globalcomix/mariadb-mysql-cdc:6047e38@sha256:aa132d5104560522679089965ca9e2f41521abc6662fb529e3e691c32d4a30da`: 11 runtime checks passed and the pinned Trivy HIGH/CRITICAL scan found zero vulnerabilities. Stream rollout through ops commit `984d7b165` left CDC Ready 1/1 with zero restarts, advancing checkpoint, and zero quarantines.

Flux resume-03 was launched through ops commits `f730398a3` and `a3e0b0287` at `2026-08-22T21:20:10Z` on the exact image; its pod was Running/Ready with zero restarts. At `21:26Z`, durable state had one current specification, 781 complete rows and one running row. `paid_subscriptions_users_pages` advanced from PK `172054507` to `173581935`; 1,407 committed reconciliation audits were present with no suspicious log lines. Flux Ready=False is attributable to retained terminal resume-02, not resume-03.

The run is not complete. Completion still requires all of the following:

1. Exactly **1,383** durable `cdc.sync_runs` rows (`461 tables × 3 stages`) are `complete` for this run ID, with zero running or error rows.
2. Current source drift is acceptable; live source/target parity is not a completion gate and must not trigger rollback, resync, or repair.
3. Post-run CDC health proves Ready, an advancing checkpoint, zero quarantines, and no new error-like lines.
4. Flux removes the completed full-sync Job manifest after evidence retention.

No completion or full-catalog parity claim is made here.
