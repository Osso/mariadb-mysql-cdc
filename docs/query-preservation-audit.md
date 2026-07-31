# Query preservation audit

verified: 2026-07-30
scope: live `stream-binlog` QueryEvent handling plus the shared `StatementApplier` used by `apply-binlog`

This matrix records every current path that removes, ignores, normalizes, or rejects source query text. “Blocked” means no checkpoint advance. Durable automatic-DDL blocks persist a journal barrier and retry the same source coordinate in-process without consuming the ordinary transport retry budget. “Exit” means the error is not reconnect-eligible, reaches `run_stream_binlog_command`, and exits the process with status 1; generic non-DDL mapping/quarantine failures still use that fatal path.

## Routing before statement execution

| Source condition | Code path | Text/result handling | Target write | Runtime consequence |
|---|---|---|---|---|
| Statement DML QueryEvent that may target the source schema | `structured_stream/event.rs:query_event_precheck` | Whole query rejected as a ROW/FULL contract violation; text appears only in error context | None | Blocked; mapping error exits |
| QueryEvent whose default database is outside the configured source schema | `structured_stream/event.rs:query_event_precheck` | Whole query ignored | None | `EventPolicy::Ignore`; no statement execution |
| `BEGIN`, `COMMIT`, or `ROLLBACK`, after outer trim and removal of trailing semicolons | `structured_stream/event.rs:is_transaction_control_query` | Whole query ignored | None | `EventPolicy::Ignore` |
| Query with a qualified identifier when qualification is ambiguous | `structured_stream/event.rs:reject_ambiguous_query_database` | Whole query rejected | None | Blocked; mapping error exits |
| Query with pending UserVar events | `structured_stream/event.rs:apply_query_context` | Whole query rejected; variable names reported | None | Blocked; mapping error exits |
| Query with an IntVar other than `INSERT_ID` | `structured_stream/event.rs:apply_intvar` | Whole query rejected | None | Blocked; mapping error exits |
| RowsQuery annotation | `structured_stream/event.rs:apply_structured_event` | Annotation text ignored | None | `EventPolicy::IgnoreAnnotation` |
| Unknown/non-modeled binlog event | `structured_stream/event.rs:apply_structured_event` | Whole event ignored | None | `EventPolicy::Ignore` |

## Shared statement normalization and policy

`StatementApplier::apply` normalizes a copy of `event.sql` for classification. When classification returns `Replay`, execution receives the exact original `event.sql`; `Skip` and quarantine decisions remain based on the normalized copy.

| Source piece/condition | Code path | Current action | Runtime consequence |
|---|---|---|---|
| Leading whitespace | `statement.rs:normalize_statement` | Removed only from the classification copy | Replay executes the exact source SQL |
| Any leading `--...` line, including `--` without MySQL’s required following whitespace | `statement.rs:strip_one_leading_comment` | Entire comment removed only from the classification copy; repeated leading comments remain in replay | Replay executes the exact source SQL |
| Leading `#...` line | `statement.rs:strip_one_leading_comment` | Entire comment removed only from the classification copy; repeated leading comments remain in replay | Replay executes the exact source SQL |
| Leading ordinary `/*...*/` | `statement.rs:strip_one_leading_comment` | Entire comment removed only from the classification copy | Replay executes the exact source SQL |
| Leading MariaDB executable `/*M!...*/` | `statement.rs:strip_one_leading_comment` | Entire semantic comment removed only from the classification copy because only `/*!` is exempted | If classification replays, execution receives the exact source SQL; otherwise the normalized copy is quarantined/skipped as before |
| Leading optimizer hint `/*+...*/` | `statement.rs:strip_one_leading_comment` | Entire semantic hint removed only from the classification copy because only `/*!` is exempted | If classification replays, execution receives the exact source SQL; otherwise the normalized copy is quarantined/skipped as before |
| Leading MySQL executable `/*!...*/` | `statement.rs:strip_one_leading_comment` | Preserved by the classification copy | Usually fails prefix classification and is quarantined; no replay occurs |
| Outer trailing whitespace | `statement.rs:normalize_statement` | Removed only from the classification copy | Replay executes the exact source SQL |
| Every trailing semicolon after trim | `statement.rs:normalize_statement` | Removed only from the classification copy by `trim_end_matches(';')` | Replay executes the exact source SQL |
| Semicolon inside `'...'` or `"..."` | `statement.rs:contains_multi_statement` | Preserved and ignored for multi-statement detection | No loss from this check; replay still receives exact source SQL |
| Text inside `--` or `/*...*/` while detecting multiple statements | `statement.rs:skip_line_comment`, `skip_block_comment` | Ignored only for classification | Replay still receives exact source SQL |
| More than one statement outside recognized compound routine/event/trigger bodies | `statement.rs:classify_statement` | Whole normalized query quarantined | Live stream converts quarantine to fatal `ApplyBinlogError::Quarantined`; blocked; exits |
| `RETURNING`, `SEQUENCE`, `SYSTEM VERSIONING`, `VERSIONING`, `DELETE HISTORY`, `INSERT DELAYED` outside string literals | `statement.rs:find_mariadb_only_pattern` | Whole normalized query quarantined | Blocked; exits in live stream |
| `LOAD_FILE(`, `INTO OUTFILE`, `INTO DUMPFILE`, `LOAD DATA`, `DEFINER`, `SQL SECURITY DEFINER` outside string literals | `statement.rs:find_unsafe_pattern` | Whole normalized query quarantined | Blocked; exits in live stream |
| `IF EXISTS`/`IF NOT EXISTS` in guarded ALTER/index DDL | `statement.rs:find_ddl_if_exists_pattern` | Whole normalized query quarantined | Structured DDL routing normally records `translation_pending`; otherwise quarantine exits |
| Unknown first statement keyword | `statement.rs:classify_statement` | Whole normalized query quarantined | Blocked; exits in live stream |
| `GRANT` or `REVOKE` | `statement.rs:is_skipped_administrative_ddl` | Whole normalized query skipped with no target statement | Counted as applied/commit by `StatementApplier` callers |
| Other administrative CREATE/ALTER/DROP/RENAME forms in the skip list | `statement.rs:is_skipped_administrative_ddl` plus structured DDL routing | `apply-binlog` skips them; live `stream-binlog` normally intercepts them as untranslated schema changes | Batch mode can checkpoint without target write; live mode records `translation_pending`, leaves the checkpoint unchanged, and retries the same coordinate in-process indefinitely without raw execution |

## DDL tokenization and generated SQL

| DDL path | Code path | Pieces omitted or normalized | Rejection boundary |
|---|---|---|---|
| Generic DDL tokenization | `ddl_semantics/tokenizer.rs:tokenize_comment`, `tokenize_quoted` | All `--`, `#`, `/*...*/`, including executable comments and hints, become no token. Identifier quote characters are removed. String literals become `<string>` for syntax parsing. | Unterminated block comments/quotes error; callers decide whether omission is only classification or affects emission |
| DDL operation identity | `ddl_semantics/parser.rs:parse_ddl_operation` | Uses comment-free tokens for family/object identity; `OR REPLACE`, `UNIQUE`, and table/view `IF [NOT] EXISTS` affect indexes but are not retained in `DdlOperation` | Unsupported command/object/qualification returns error |
| Streamed simple CREATE/DROP INDEX | `ddl_semantics/parser.rs:parse_index_ddl`, `ddl_semantics.rs:translate_ddl_with_provenance` | Accepted SQL retains internal text but loses outer whitespace and trailing semicolons | Any comment, double-quoted identifier, generated name, FULLTEXT/SPATIAL/unsupported UNIQUE, non-BTREE type, IF EXISTS, qualification, or unmodeled option blocks automatic handling |
| Modeled planner CREATE INDEX | `ddl_semantics/transform.rs:render_modeled_index_ddl` | Reconstructed SQL: identifier quoting/order normalized, `USING BTREE` always emitted, default ASC omitted, modeled visibility/comment/key options re-emitted | Non-create or non-BTREE model rejected |
| Generated planner schema DDL | `ddl_semantics/transform.rs:generated_schema_tokens`, `transform_generated_schema_ddl` | Leading ordinary comments are removed only for validation; emitted SQL preserves them but loses outer whitespace and trailing semicolons | Remaining comments, double quotes, unsupported family/action/type/modifier/options reject generation |
| Fixture/exact production CREATE TABLE | `ddl_semantics/transform.rs:parse_fixture_create_table`, `transform_fixture_create_table_ast` | All leading ordinary `--`, `#`, `/*...*/` comments removed. Generated SQL drops `IF NOT EXISTS`, original formatting/casing, integer display widths, inline PRIMARY KEY placement, and original `CHARSET` spelling; it re-quotes identifiers and canonicalizes defaults/options. | Any remaining comment (including executable comment/hint), double quote, unsupported type/engine/trailing option/shape blocks |
| Production ADD COLUMN/ADD KEY ALTER | `ddl_semantics/transform.rs:production_alter_sql`, `render_production_alter_table` | Exactly one leading ordinary MySQL `-- ` comment is stripped for parsing and reattached verbatim to rendered SQL, including its source line ending. Output drops source formatting/casing; `ADD INDEX` becomes `ADD KEY`; column null/default form is canonicalized; identifiers and string literals are re-quoted. | Any remaining comment, unsupported clause/type/option/quote/shape blocks |
| `DROP COLUMN IF EXISTS` ALTER | `ddl_semantics/transform.rs:transform_drop_columns_if_exists` | Leading comment behavior above; `IF EXISTS` removed; absent target columns and repeated case variants are omitted; entire target SQL becomes `None` when no target column remains | Mixed/unsupported clauses block |
| `RENAME COLUMN IF EXISTS` ALTER | `ddl_semantics/transform.rs:transform_rename_columns_if_exists` | `tokenize_ddl` removes every comment form anywhere, including executable comments/hints. Output removes `IF EXISTS`, formatting, and non-executable absent clauses; can become `None`. | Ambiguous target state or unsupported shape blocks |
| DROP PROCEDURE | `ddl_semantics/transform.rs:parse_supported_drop_procedure`, `transform_drop_procedure` | Output removes `IF EXISTS`, source formatting, and source spelling in favor of matched target spelling; becomes `None` if target procedure is absent | Any comment, double quote, qualification, extra token, or unsupported plain name blocks |
| Exact source-only CREATE PROCEDURE | `ddl_semantics/transform.rs:transform_source_only_release_move_procedure_create` | Entire source statement intentionally emits no target SQL (`target_sql=None`) after exact raw hash admission | Any other hash/name/body blocks |
| Automatic semantic no-op/recovery | `ddl_semantics/canonical.rs`, `structured_stream/ddl.rs:execute_transformed_ddl` | A proven `target_sql=None` executes no source text | Checkpoint occurs only after evidence/post-state proof |

## Runtime failure consequences

| Condition | Durable state | Target/checkpoint | Process consequence |
|---|---|---|---|
| Untranslated schema-changing query | Exact `raw_sql` inserted as `translation_pending` | No target write; no checkpoint | Durable `DdlBlocked`; stream reconnects in-process at the unchanged coordinate indefinitely without consuming transport retry budget |
| Modeled transformation returns error | `translation_pending` | No target write; no checkpoint | Same durable DDL-block loop; no statement skip or raw execution |
| Evidence capture unavailable | `translation_pending` | No target write; no checkpoint | Same durable DDL-block loop; no statement skip or raw execution |
| Transformed target SQL fails | Usually remains `prepared` | Target result uncertain; no checkpoint | Fatal target failure; restart enters prepared reconciliation |
| Post-state differs from expected | `blocked` | Target may already have changed; no checkpoint | Durable `DdlBlocked`; same coordinate retries in-process until reviewed resolution |
| Existing blocked row remains divergent | `blocked` | No additional target write/checkpoint | Durable `DdlBlocked`; process remains alive and retries without raw execution |
| Prepared row cannot be proven applied | Transitioned to `blocked` | No checkpoint | Durable DDL-block loop; no checkpoint advancement or statement skip |
| Generic statement quarantine | Only in-memory `RecordingQuarantine` in live path | No target write/checkpoint | Fatal `Quarantined` error; exits |
| Generic target execution error | No DDL journal state in generic path | No checkpoint | Fatal `Statement` error; exits |
| Retryable source transport error | No query mutation | Resume from durable checkpoint | Internal bounded/unbounded reconnect; process stays alive |

## Production-fidelity preservation test

Use the exact previously observed event, because it proves a source component was silently removed on the production automatic-DDL path:

```rust
#[test]
fn observed_alter_preserves_its_leading_comment_in_generated_sql() {
    let source_sql = "-- The serve-time blacklist check resolves a blacklisted artist's imprints.\r\n\
ALTER TABLE `artists_imprints`\r\n\
    ADD KEY `idx_artist_id` (`artist_id`)";

    let transformation = transform_production_alter_table(source_sql)
        .expect("the observed ALTER is modeled");

    assert!(transformation
        .target_sql
        .as_deref()
        .is_some_and(|sql| sql.starts_with("-- The serve-time blacklist check resolves a blacklisted artist's imprints.\r\n")));
}
```

At commit `f9b35d5`, the supported production ALTER renderer preserves the exact leading ordinary MySQL `-- ` comment prefix, including its source line ending, while still rendering the parsed ALTER body deterministically. This behavior is limited to this production ALTER path; other comment forms and query paths retain their separately documented behavior.

The independent RED test `unsupported_ddl_keeps_replicator_alive_at_unchanged_checkpoint` now proves the production boundary: the journal records `translation_pending`, no target SQL or checkpoint write occurs, and the reconnect loop retries from the unchanged coordinate instead of terminating the process. This durable DDL-block path never skips the statement or falls back to raw source SQL.
