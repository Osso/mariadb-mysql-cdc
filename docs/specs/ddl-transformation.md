# MariaDB to MySQL 8 DDL Transformation

The CDC stream must convert production MariaDB DDL syntax into deterministic
MySQL 8 DDL syntax. This translator is not responsible for reconciling
preexisting source/target schema or data differences. This is the authoritative
DDL transformation contract. Journal and checkpoint mechanics are described in
[DDL resolution and recovery](../ddl-resolution.md), but they must serve this
transformation pipeline rather than restrict automatic handling to a small DDL
allowlist.

## What it must do

### Transformation contract

- [ ] Parse every production MariaDB DDL event into a canonical representation
      before target execution.
- [ ] Transform MariaDB syntax, defaults, identifiers, data types, collations,
      indexes, constraints, generated columns, table options, partitioning,
      views, routines, triggers, and events into MySQL 8-compatible DDL.
- [ ] Preserve the meaning expressed by the parsed DDL statement while converting
      syntax; reject unsupported clauses instead of dropping or approximating
      them.
- [ ] Preserve object qualification and dependency relationships without
      allowing writes outside the configured application schema.
- [ ] Produce deterministic MySQL 8 SQL from the parsed source statement.
- [ ] Make transformations observable by persisting source SQL, canonical input,
      generated MySQL SQL, transformation version, source coordinate, pre-state,
      expected post-state, and observed post-state.

### Current implemented slice

#### Basic common DDL expansion — implementation in progress

This branch replaces the former one-observed-shape-at-a-time direction for basic
column DDL with the bounded common family below. It does **not** claim all DDL,
real-database integration completion, deployment, or live recovery proof. The
production-specific records below remain applicable where they add a narrower
exception; their narrower type exclusions are superseded by this matrix.

| Area | Accepted basic family | Safety boundary |
|---|---|---|
| Shared column types (`CREATE TABLE`, `ADD COLUMN`) | Signed/unsigned `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`/`INTEGER`, `BIGINT`; `BOOL`/`BOOLEAN` as `TINYINT`; `DECIMAL`/`NUMERIC`; `FLOAT`; `DOUBLE`/`DOUBLE PRECISION`; `CHAR`, `VARCHAR`, `BINARY`, `VARBINARY`; `TINYTEXT`/`TEXT`/`MEDIUMTEXT`/`LONGTEXT`; `TINYBLOB`/`BLOB`/`MEDIUMBLOB`/`LONGBLOB`; `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `YEAR`, and `JSON`. | Type keywords and numeric parameters are unquoted; lengths/precisions are canonical and bounded by the parser. `REAL`, `ZEROFILL`, and other mode-dependent qualifiers remain blocked. |
| Definitions and defaults | `NULL`/`NOT NULL`; `DEFAULT NULL`; bounded, range-checked numeric literals for numeric types; printable unescaped string defaults for character/text types, including `VARCHAR DEFAULT ''`; `COMMENT` and `AFTER` for ALTER column clauses; existing modeled character-set/collation handling. | Contradictory `NOT NULL DEFAULT NULL`, out-of-range/non-finite defaults, arbitrary expressions, and JSON literal defaults remain blocked. Text literal defaults retain their MySQL expression rendering. |
| `CREATE TABLE` | Ordinary unqualified `CREATE TABLE` and `CREATE TABLE IF NOT EXISTS` use the shared observed grammar and basic types, subject to modeled table options, primary/ordinary/unique index forms, charset/collation evidence, and target-absent proof. When both table charset and collation are omitted, stable target-database defaults are inherited and recorded as `inherited_database_defaults`. | Existing target is not a converged no-op. The removed fixture/table-specific parser is not an alternate path; only the legacy exact-hash `assistant_reply_reports` no-op admission remains outside the shared grammar. Unmodeled table, index, constraint, comment, qualification, or option grammar remains pending. |
| `ALTER TABLE` columns | `ADD COLUMN` and ordinary unguarded `MODIFY COLUMN` share the definitions above. `MODIFY` applies only to an existing ordinary non-JSON, non-generated, non-`AUTO_INCREMENT` column and preserves its target position/index references while replacing modeled attributes. Ordinary `RENAME COLUMN old TO new`, `DROP COLUMN`, and their existing conditional forms are admitted from fenced target evidence. | `CHANGE COLUMN`, `ALTER COLUMN SET/DROP DEFAULT`, `FIRST`, generated/JSON/`AUTO_INCREMENT` MODIFY, and unmodeled clause combinations remain blocked. Rename and drop fail closed on ambiguous target state. |
| Indexes | Existing admitted named ordinary/unique key additions and strict standalone `CREATE INDEX`/`DROP INDEX` rules remain available; ordinary `DROP INDEX` in ALTER is derived from target evidence. | New index/constraint grammar, FK-dependent index changes, and unmodeled key parts/options remain blocked. |


- [x] Token-parse the production-observed unqualified multi-clause `ALTER TABLE` form with `ADD COLUMN`, named `ADD KEY`, MariaDB-syntax `ADD INDEX` normalized to the same AST, and named `ADD UNIQUE KEY` clauses; preserve clause order and render deterministic MySQL 8 SQL with source `ADD INDEX` emitted as target `ADD KEY`.
- [x] Admit the exact production event at `mysqld-bin.002778:750897987-750898224` (150 raw bytes with CRLF line endings; SHA-256 `ea9f789b158dca0146715bafe9f2712b5945b9c6626411b382347e60e52eb85f`) when its otherwise-supported ALTER has exactly one leading ordinary MySQL `-- ` line comment. Strip that comment only for parsing, then preserve its exact source prefix, including the source line ending, in generated SQL. Modeled ADD COLUMN and MODIFY clauses also admit ordinary block and inline line comments; executable/version comments and optimizer hints remain rejected.
- [x] Convert MariaDB `ALTER TABLE ... DROP COLUMN IF EXISTS ...` into MySQL 8 `DROP COLUMN` clauses by matching target identifiers ASCII-case-insensitively, emitting each matched target spelling once, and treating absent or repeated case-variant clauses as proven no-ops. A leading ordinary client block comment is admitted only for this modeled conditional-drop form and is removed before deterministic target rendering; embedded ordinary comments, executable/version comments, and optimizer hints remain rejected.
- [x] Transform the generic exact unqualified, unquoted `DROP PROCEDURE IF EXISTS <identifier>` form and the exact unqualified, unquoted plain `DROP PROCEDURE apply_release_move_purchase_repair` form using target-local routine inventory. An existing target routine emits deterministic MySQL `DROP PROCEDURE` with the target spelling backtick-quoted; an absent target records a proven no-op. Qualified, quoted, commented, and other plain-name variants remain `translation_pending` barriers.
- [x] Transform only the exact raw, unqualified, unquoted, comment-free `DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives` form, with an optional trailing semicolon. Stable target trigger evidence matches the name case-insensitively; a present target emits deterministic quoted MySQL `DROP TRIGGER`, while an absent target records `generated_sql = NULL` as a proven no-op. Qualified, quoted, commented, differently named, extra-token, and all other trigger forms remain `translation_pending` barriers.
- [x] Admit the source-only `CREATE PROCEDURE` form only when the complete statement matches one of two private exact hashes for the exact unqualified routine identity `apply_release_move_purchase_repair`. The exact admitted bodies are tracked as `fixtures/ddl/create-apply-release-move-purchase-repair.sql` and `fixtures/ddl/create-apply-release-move-purchase-repair-95.sql`; fixture tests exercise both bodies, and adding a comment or changing any body text remains rejected. Admission precedes generic qualified-identifier rejection because the admitted statements contain qualified tokens. Require the target routine to be absent before and after evidence capture, execute no target SQL, and record a proven no-op. The body is never executed; data effects may arrive only through subsequent source ROW/FULL events in source order. An existing `translation_pending` row promotes automatically after exact-hash admission. Every other body, name, and routine DDL remains a `translation_pending` barrier. Raw production procedure bodies, `DEFINER` hosts, and event coordinates are intentionally excluded from public documentation.
- [x] Transform the production-observed unqualified multi-clause `ALTER TABLE ... RENAME COLUMN IF EXISTS ...` form from target column pre-state into deterministic MySQL 8 SQL. Exactly one leading ordinary MySQL `-- ` line comment is removed only for parsing and reattached verbatim to executable generated SQL, including its source prefix and line ending. Any remaining or embedded comment form is rejected into the durable `translation_pending`/`blocked` DDL path. Absent rename clauses remain proven no-ops and emit no target SQL.
- [x] Transform the general observed `ADD COLUMN` forms only under the exact unquoted type grammar `CHAR(canonical decimal length 1..255)`, `VARCHAR(positive canonical decimal length)`, `DATETIME`, `SMALLINT UNSIGNED`, or `FLOAT UNSIGNED`. The first four retain the observed `DEFAULT NULL`, explicit `NULL`, `COMMENT`, and `AFTER` options; `FLOAT UNSIGNED` additionally admits the observed `NOT NULL DEFAULT 0` form. Expected post-state for added character columns records the table-inherited character set and collation so live inventory comparison matches MySQL metadata. Type keywords, `CHAR`/`VARCHAR` parentheses and length, and `UNSIGNED` must be unquoted; `DATETIME` precision, `SMALLINT` display width, `FLOAT` parameters, and other numeric defaults remain unsupported.
- [x] Admit only the exact production `content_sections_events_raw` shape with two ordered `ADD COLUMN IF NOT EXISTS` clauses for nullable `TIMESTAMP DEFAULT NULL` columns `direct_seen_at` and `sync_seen_at`, their exact comments, and a final `ALGORITHM=INSTANT`. Model both source existence guards in the AST, but emit valid MySQL 8 SQL as one atomic two-column ALTER without `IF NOT EXISTS` after proving both columns are absent. When both exact columns are already present, suppress target SQL as a proven no-op. Partial presence or any divergent definition fails closed before target execution. `ALGORITHM=INPLACE` and every other table, column, comment, type, clause count/order, or algorithm variant remain `translation_pending` with no target execution or checkpoint advance.
- [x] Admit the production-observed signed `TINYINT(1) NOT NULL DEFAULT 0` and unsigned `TINYINT(1) UNSIGNED NOT NULL DEFAULT 0` `ADD COLUMN` forms. Emit deterministic MySQL 8 `TINYINT` or `TINYINT UNSIGNED`, respectively, preserving signed range, nullability, default, and requested column position. When the target already contains the exact column definition at the requested position, promote the same `translation_pending` journal row as a proven no-op with `generated_sql = NULL`; any definition or position mismatch remains blocked.
- [x] Admit the production-observed `reader_memory` ALTER forms: `ADD COLUMN <name> TEXT|MEDIUMTEXT NOT NULL DEFAULT '<literal>'` whose literal is non-empty printable ASCII without quotes or backslashes, rendered as the MySQL 8 expression default `(_utf8mb4'<literal>')` because MySQL rejects literal TEXT defaults; the expected post-state records `COLUMN_DEFAULT` as `_utf8mb4\'<literal>\'` with `DEFAULT_GENERATED`. A required TEXT column without a modeled default remains blocked. `ADD COLUMN CHAR(n) CHARACTER SET <charset> COLLATE <charset>_<suffix>` records the explicit column encoding in the AST and expected post-state. `ADD CONSTRAINT <name> CHECK (...)` admits only the bounded CHECK grammar: predicates `<column> IS NULL`, `JSON_VALID(<column>)`, `OCTET_LENGTH(<column>) <= <positive integer>`, and `<column> IN ('<[A-Za-z0-9_]+>', ...)`, joined only by `OR`. CHECK constraints are outside the compared schema inventory; the expected post-state proves each referenced column exists after preceding clauses apply, and the real-database harness proves MySQL's stored `CHECK_CLAUSE` text and enforcement.
- [x] Admit the production-observed guarded `reader_memory_profiles` ALTER at `mysqld-bin.003062:888678037-888678577` (`fixtures/ddl/alter-reader-memory-profiles-suggestions.sql`): generic `ADD COLUMN IF NOT EXISTS` for every modeled column type, `ADD INDEX IF NOT EXISTS` for named non-unique composite keys, `DATETIME(6)` columns (other precisions and `TIMESTAMP` precision remain blocked), and `CHAR`/`VARCHAR` `NOT NULL DEFAULT '<literal>'` string defaults rendered as quoted MySQL literals (a required string column without a modeled default remains blocked). MySQL 8 has no `IF NOT EXISTS` for these clauses, so the AST records `if_not_exists` on each guarded column and key, rendered SQL drops the guards, and the expected post-state executes them only when every guarded object is absent from the fenced target pre-state. When every guarded object already exists with its exact definition the expected post-state equals the pre-state, so the event is a proven no-op with `generated_sql = NULL` and a normal checkpoint. Partial presence or a divergent existing column/index definition fails closed as `translation_pending`. The exact `content_sections_events_raw` admission remains as documented above.
- [x] Admit nullable `ADD COLUMN [IF NOT EXISTS] <name> JSON DEFAULT NULL [AFTER <column>]` using MariaDB's `LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin` alias plus an enforced `JSON_VALID` CHECK with a MySQL-assigned table-specific name, avoiding MariaDB/MySQL constraint-name scope differences. Preserve JSON text bytes, SQL NULL, defaults and column order; never substitute native MySQL JSON. Validate the CHECK before checkpointing, including an existing-column guarded no-op. Non-NULL JSON defaults, required JSON columns and explicit JSON encodings remain unsupported.
- [x] Recover only the exact second source-layout JSON ALTER on `releases_pages_history` and the exact `home_feed_contributor_cards` CREATE at `mysqld-bin.003091:322779514` (`fixtures/ddl/create-home-feed-contributor-cards.sql`) from the legacy named-CHECK translation: recorded generated SQL must differ solely by explicit CHECK naming, recorded canonical/pre/post evidence must match the corrected translation, and the target must match recorded pre-state or verified post-state. Execute corrected SQL only from proven pre-state, preserve journal event identity, reject divergence, and recover a post-DDL crash without reexecuting the ALTER. This is not generic retry permission for prepared or blocked DDL.
- [x] Reject quoted type keywords, quoted `VARCHAR` lengths, and quoted `UNSIGNED` forms as unsupported syntax. These variants remain `translation_pending` with no target DDL or checkpoint advance.
- [x] Transform named composite `ADD KEY`, MariaDB-syntax `ADD INDEX`, and `ADD UNIQUE KEY` clauses over ordinary columns as BTREE indexes; multiple admitted clauses remain ordered, source `ADD INDEX` emits as target `ADD KEY`, and broader index and clause options remain outside this slice.
- [x] Admit only the observed `releases` `DROP INDEX idx_downloads_sort`, replacement eight-part `ADD INDEX idx_downloads_sort` with `published_time DESC`, final `ALGORITHM=INPLACE, LOCK=NONE` shape. Preserve typed drop/add index clauses, algorithm, lock, key-part direction, and fenced target pre/post-state; every table, index, key list/order, algorithm, lock, or clause-order variation remains `translation_pending`.
- [x] Encode a canonical typed clause AST: `add_column` records name/type/nullability/default/comment/position, records `if_not_exists` only for the admitted guarded form, and records `character_set`/`collation` only when the source names them; `add_key` records the typed index AST and ordered key parts; `add_check` records the constraint name and ordered typed predicates; the exact instant ALTER records `algorithm=instant`. Statements without these optional parts keep their previous canonical encoding byte for byte.
- [x] Record expected target object state for crash/replay verification without treating that evidence as source/target reconciliation.
- [x] Fail closed as `translation_pending` before target execution when syntax, context, dependencies, or semantics fall outside that explicit slice; the stream checkpoint and later-event barrier must remain unchanged, and the durable DDL block retries in-process without skipping or executing raw source SQL.
- [x] Carry `TIMESTAMP` column types across unchanged. The former unconditional `TIMESTAMP` to `DATETIME` rewrite is removed: MySQL rejects values past 2038-01-19 that MariaDB 11 accepts, but no source column holds one, so the rewrite bought nothing and would have required rebuilding 384 tables and about 864 GB with `ALGORITHM=COPY`.
- [x] Emit deterministic MySQL 8 SQL and record transformation version `mariadb-mysql8-v1`.
- [x] Set journal `transformation_version` and nullable `generated_sql` from the actual transformation before `prepared`; proven no-ops persist `generated_sql = NULL`.
- [x] Execute generated SQL in the automatic stream path instead of the MariaDB source SQL.
- [x] Keep unsupported or semantically blocked DDL durable and observable: persist the journal barrier, leave the checkpoint unchanged, and retry in-process indefinitely without consuming transport retry budget, skipping the event, or falling back to raw source SQL.

This is a production-derived ALTER TABLE slice plus bounded typed CREATE TABLE
admissions, one exact production `assistant_reply_reports` CREATE recovery, one
identity-scoped source-only CREATE PROCEDURE form, two exact procedure-drop
admissions, and one exact trigger-drop admission. It is not full ALTER TABLE,
generic CREATE TABLE, general routine/trigger DDL, or the full
MariaDB-to-MySQL 8 transformation pipeline. The translator may use only
semantics represented by the admitted event AST, captured QueryEvent context,
and fenced target pre-state; it must not infer historical source state from the
current source schema.

Unsupported or ambiguous DDL syntax enters the durable journal as
`translation_pending` with sentinel/no execution evidence. It performs no target
DDL, does not advance the stream checkpoint, and blocks later events from
overtaking it. After that barrier is durable, the live reconnect loop retries the
same source coordinate in-process indefinitely without consuming the ordinary
transport retry budget. It never skips the statement or executes raw source SQL.
When translator code later supports the exact syntax, the same row may promote
once to `prepared`, fill immutable evidence, execute generated SQL, and
checkpoint automatically. An exact target pre-state that already equals the
modeled post-state is a proven no-op; divergent preexisting target schema or
data remains an execution/reconciliation failure, not a translator-unavailable
event.
The retired manual ledger is absent from runtime, configuration, bootstrap,
grants, and harness behavior. This contract remains deployment-blocked by the
broader DDL coverage and operational proof gaps listed below.

### Fixture-backed CREATE TABLE boundary

- [x] The strict unqualified fixture `CREATE TABLE` grammar (the harness
      exercises `accounts`) accepts identifiers matching
      `[A-Za-z_][A-Za-z0-9_]*` after tokenization, with backtick quoting allowed,
      comments/double quotes/qualification rejected, one or more `BIGINT` or
      `VARCHAR(positive canonical decimal length)` `NOT NULL` columns with at
      least one inline `PRIMARY KEY`, zero or more one-column named ordinary
      `KEY` items, and `ENGINE=InnoDB` with an optional semicolon. It records a
      typed AST and deterministic MySQL 8 SQL.
- [x] The exact production-observed unqualified
      `CREATE TABLE IF NOT EXISTS home_feed_artist_blacklist` form is admitted
      with its observed `INT`/`MEDIUMINT`/`VARCHAR`/`TIMESTAMP` columns,
      nullability/defaults, auto-increment inline primary key, unique artist
      index, `ENGINE=InnoDB`, and `utf8mb4` charset/collation. This is an exact
      modeled form, not generalized CREATE TABLE support.
- [x] Leading ordinary `--`, `#`, and `/* ... */` comments are stripped before
      that exact production CREATE admission. Executable comments, MariaDB
      executable comments, optimizer hints, embedded comments, and all other
      commented or unmodeled CREATE forms remain rejected.
- [x] Production `LiveDdlSemanticInventory` captures source schema
      charset/collation only between fences whose before/after source master
      coordinate exactly equals the event file/end position; the target
      inventory proves the table is absent before and after capture.
- [x] The evidence persists source `character_set` and `collation`, renders
      explicit `DEFAULT CHARACTER SET ... COLLATE ...` SQL, and derives a
      deterministic expected post-state from the typed AST and captured defaults;
      canonical table evidence sorts indexes by index name.
- [x] Runtime admission executes an admitted grammar form only after the evidence
      gates, validates the exact observed post-state, and checkpoints it.
      Unsupported `CREATE TABLE` variants remain `translation_pending` with zero
      target DDL and zero checkpoint execution.
- [x] The observed generic `CREATE TABLE IF NOT EXISTS` family accepts ordinary
      leading block comments and inline `--` comments; `MEDIUMINT`, `SMALLINT`,
      and `TINYINT UNSIGNED`; canonical `VARCHAR(n)`; exactly `DECIMAL(4,3)`;
      unquoted nonnegative `DECIMAL(4,3)` defaults with one integer digit and
      exactly three fractional digits, preserved without floating-point conversion;
      named single-column same-schema foreign keys with `ON DELETE CASCADE`
      and implicit update restriction, requiring an explicit supporting index;
      observed foreign-key identity and referential actions must match before checkpointing;
      `TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`;
      a composite primary key; named composite ordinary keys; `ENGINE=InnoDB`;
      and `DEFAULT CHARSET=utf8mb4`. It preserves the event definition, including
      historical `VARCHAR(80)`, rather than reading a later source definition.
- [x] The observed storefront CREATE additionally models `INT UNSIGNED` with non-null `AUTO_INCREMENT`, signed `TINYINT(1)`, `ENUM` members containing only ASCII letters/digits/underscores, nullable columns, explicit `DEFAULT NULL`, integer `DEFAULT 0`, signed-tinyint `DEFAULT 1`, optional timestamp default/on-update clauses, and named composite `UNIQUE KEY`. Enum value spelling is retained; strings with backslash escapes and unmodeled defaults/options remain blocked. Historical charset evidence and target-absent preconditions are unchanged. The concrete parser fixture is `fixtures/ddl/create-storefront-chips.sql`; integration/deployment proof is recorded separately.
- [x] The storefront pending-replay harness reproduces the historical CREATE, durable pending-journal promotion, and following ENUM row event. MariaDB emits that ENUM in `TABLE_MAP` as raw `STRING` with metadata high byte `ENUM`; its optional ENUM-label metadata binds to that column exactly as for a direct raw `ENUM`, so the subsequent ordinal is applied as its declared label. The harness separately proves that metadata for an omitted non-null ENUM default remains `NULL` while an omitted INSERT uses the first label. No other `STRING` reinterpretation or metadata family is admitted by this requirement.
- [x] For a charset-only CREATE, runtime decodes MariaDB QueryEvent status
      variables `Q_CHARACTER_SET_COLLATIONS` and resolves the historical
      `utf8mb4` collation through the source collation-ID catalog. The canonical
      AST records that context and target SQL renders an explicit MySQL-compatible
      collation. An absent, malformed, or unsupported context remains
      `translation_pending`. When both table charset and collation are omitted,
      MariaDB's CREATE-in-database behavior is modeled by stable target-database
      defaults, captured twice around the fenced target pre-state and recorded as
      `inherited_database_defaults`; no source-head query or source-coordinate
      charset/default fence is required. Source/target default drift is out of
      scope.
- [x] Parser/admission proof covers the observed `kg_comic_facets` CREATE at
      `mysqld-bin.002994:1005806835-1005808327`: 98 targeted tests and a real
      historical `VARCHAR(80)` crash/restart/row-replay harness passed. Production
      deployment remains pending; its target pre-state must be absent. Existing-target
      `CREATE TABLE IF NOT EXISTS` is not admitted as a no-op.
- [x] The observed `reader_memory` CREATEs at `mysqld-bin.003058:312414813-312418004` extend the generic `CREATE TABLE IF NOT EXISTS` family with: an inline `NOT NULL PRIMARY KEY` column (exactly one primary key definition, inline or table-level); `BIGINT UNSIGNED`; `DATETIME` and `DATETIME(6)` with `DEFAULT CURRENT_TIMESTAMP(6)` and `ON UPDATE CURRENT_TIMESTAMP(6)` whose precision must match the column; `TEXT` and `MEDIUMTEXT`, whose literal default renders as the MySQL 8 expression default `(_utf8mb4'<literal>')` and whose expected post-state records `_utf8mb4\'<literal>\'` with `DEFAULT_GENERATED`; `CHAR(1..255)`; quoted string defaults for `CHAR`/`VARCHAR`; integer defaults `0`/`1` for every unsigned integer kind and `TINYINT(1)`; per-column `CHARACTER SET <charset> COLLATE <charset>_<suffix>` on character types; table-level `CONSTRAINT <name> CHECK (...)` under the bounded CHECK grammar with distinct names and known columns; and an explicit `COLLATE=utf8mb4_<suffix>` table option, which makes the charset/collation evidence explicit so no source fence or QueryEvent charset context is required. Table definitions after the columns may appear in any order. The concrete fixtures are `fixtures/ddl/create-reader-memory-{profiles,items,operations}.sql`.
- [x] The observed `sales_placements` CREATE at `mysqld-bin.003100:246478320-246479621` admits `LONGTEXT NOT NULL` without a default and preserves inline comments, nullable columns, numeric and timestamp defaults, the primary key, three ordinary indexes, and explicit utf8mb4 collation. Other unmodeled LONGTEXT options remain blocked; runtime evidence gates are unchanged. Fixture: `fixtures/ddl/create-sales-placements.sql`.
- [x] The observed `home_feed_mantle_spotlights` CREATE form additionally admits non-null `BIGINT UNSIGNED AUTO_INCREMENT` columns. It preserves nullable `JSON DEFAULT NULL` as MariaDB's `LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL` plus a `JSON_VALID(column)` CHECK; it does not substitute native MySQL `JSON`. Other JSON defaults and unmodeled JSON options remain blocked. This does not claim a general CREATE grammar.
- [x] The production `home_feed_mantle_spotlights` event at `mysqld-bin.003082:611189718-611194695` was faithfully replayed from a persisted `translation_pending` row and checkpointed on 2026-09-22. The real MariaDB → MySQL harness proves all 39 columns, five JSON aliases and their validation/text preservation, keys, defaults, collation, unsigned identities above 32-bit range, timestamp update behavior, durable journal identity/promotion, and subsequent DML. This proof applies only to the observed form; it does not claim general CREATE coverage.
- [x] The observed `assistant_quality` migration at `mysqld-bin.003089:956542848-957585602` (`fixtures/ddl/{create-assistant-quality-runs,create-assistant-quality-verdicts,alter-assistant-quality-runs-in-flight-lock,alter-assistant-quality-verdicts-conversation-key,alter-assistant-quality-verdicts-conversation-fk}.sql`) extends the generic CREATE family with canonical positive integer display widths on unsigned `INT`/`MEDIUMINT`/`SMALLINT`/`BIGINT` (dropped; MySQL 8 has none), a trailing column `COMMENT '<literal>'` of 1..1024 printable ASCII characters without quotes or backslashes (rendered and recorded in the expected post-state), and CREATE foreign keys with `ON DELETE RESTRICT` as well as `CASCADE`. The `assistant_reply_reports` CREATE stays on its exact-hash admission. CREATE renders JSON alias CHECKs anonymously because MySQL CHECK names are schema-wide; user-named CHECKs keep their names. Production ALTER admits:
      - `ADD COLUMN <name> TINYINT[(1)] UNSIGNED [GENERATED ALWAYS] AS (IF(<column> = <operand> [AND ...], <0..255>, NULL)) PERSISTENT|STORED`, operands being canonical unsigned integers or `[A-Za-z0-9_]+` string literals, followed only by `COMMENT`/`AFTER`. It renders as a MySQL `STORED` generated column whose string literals carry an explicit `_utf8mb4` introducer; the expected post-state records `STORED GENERATED`, nullable, no default, and the exact MySQL 8.4 `GENERATION_EXPRESSION` (`if((<p>),v,NULL)` for one predicate, `if(((<p1>) and (<p2>)…),v,NULL)` otherwise). References must name existing ordinary columns; generated or `AUTO_INCREMENT` references (which MySQL rejects) block. Keys may cover STORED generated columns; VIRTUAL remains blocked.
      - `ADD CONSTRAINT <name> FOREIGN KEY (<column>) REFERENCES <table> (<column>) ON DELETE CASCADE|RESTRICT`, same schema, single column, only when an existing index or the primary key leads with the child column (MySQL would otherwise create an unmodeled index) and the name is unused. The observed target state verifies the key's rules once present.
      The real MariaDB 11.4 → MySQL 8.4 harness scenario `assistant-quality-pending-replay`, with the previously deployed `35c6f88` binary persisting the barrier, proves promotion of the same journal row, checkpointing of all five events, exact column/comment/generation metadata, index parity, FK rules, post-ALTER DML with computed `in_flight_lock`, unique-slot rejection, RESTRICT rejection, and CASCADE deletion.
- [ ] The bounded `MODIFY COLUMN <name> VARCHAR(positive canonical n) NOT NULL`
      renderer preserves target column order and existing indexes/FKs without drops.
      Unit/renderer proof exists, but the historical CREATE harness's later widening
      still needs its real integration gate; no deployment is claimed.

The exact production `assistant_reply_reports` CREATE event is a bounded
convergence recovery, not generic `CREATE TABLE` support. Its target table must
be provisioned out of band from the recorded source definition before replay is
retried; runtime emits no CREATE. The raw event hash is the admission boundary,
and a stable source inventory must exactly match the target table, indexes, and
foreign-key metadata. Equality records a proven no-op with `generated_sql = NULL`
and permits the normal journal/checkpoint sequence. A changed statement, absent
target, moving source fence, or schema mismatch remains `translation_pending`
with no checkpoint advance; operator-authored SQL and manual journal mutation
are not resolution paths.

Beyond the basic matrix and separately listed observed extensions, unsupported
`CREATE TABLE` syntax remains a durable barrier. This includes executable/version
comments, optimizer hints, unmodeled table/index/constraint grammar, CHECK
predicates outside the bounded grammar, string defaults with quotes or
backslashes, cross-schema forms, and an existing target table.

### Execution and recovery

- [x] Execute transformed DDL through the durable replay journal before advancing
      the stream checkpoint.
- [x] Reconcile crashes after prepare, target implicit commit, journal update, and
      checkpoint update without blind duplicate execution; ambiguous evidence
      becomes a durable barrier.
- [x] Block checkpoint advancement when required syntax transformation is
      unsupported or ambiguous, or when target execution/recovery fails.
- [x] When translator code becomes available, automatically promote the same
      `translation_pending` event to `prepared`, fill evidence, execute generated
      SQL, and checkpoint without operator-authored SQL or status transition.
- [x] Prevent later row or statement events from overtaking a
      `translation_pending`, `prepared`, or `blocked` DDL event.
- [ ] Keep runtime grants exact: application DML and required application DDL
      privileges only, exact CDC control-plane table privileges, and no global
      administration or grant delegation.

### Compatibility proof

- [ ] Maintain a production-derived MariaDB DDL corpus covering every observed
      DDL family and MariaDB-specific construct.
- [ ] Run each corpus case against real MariaDB and MySQL 8 instances and compare
      canonical schema objects and behavior after transformation.
- [ ] Cover versioned comments, SQL modes, quoted and qualified identifiers,
      implicit defaults, definers/security context, composite constraints,
      expression indexes, generated columns, partition clauses, and engine or
      charset differences.
- [ ] Prove retry, crash, mismatch, dependency, and unsupported-transformation
      behavior at real database boundaries.

## How it works

- [DDL recovery journal and upgrade runbook](../ddl-resolution.md)
- [Checkpoint ordering](../checkpoints.md)
- [Schema inventory](../schema-inventory.md)
- [System design](../design.md)

## Implementation inventory

- `src/live/structured_stream.rs` — reads ordered QueryEvents and enforces the
  checkpoint barrier.
- `src/live/ddl_semantics.rs` — dispatches current DDL transformations and
  captures semantic evidence.
- `src/live/query_charset_context.rs` — fail-closed MariaDB QueryEvent status
  variable decoder for historical charset/collation context.
- `src/live/ddl_semantics/transform.rs` — production-derived `ADD COLUMN`,
  the exact guarded two-column `TIMESTAMP ... ALGORITHM=INSTANT` admission,
  `ADD KEY`/MariaDB `ADD INDEX`, `ADD UNIQUE KEY`, generic and exact `DROP PROCEDURE`,
  exact `DROP TRIGGER IF EXISTS`, and `RENAME COLUMN IF EXISTS` translators,
  including deterministic SQL emission.
- `src/live/ddl_semantics/canonical.rs` — typed ALTER clause AST encoding and
  expected post-state derivation from the fenced target pre-state.
- `src/live/structured_stream/ddl.rs` — prepares the journal, executes generated
  target SQL, and preserves checkpoint ordering.
- `src/live/ddl_replay_journal.rs` — durable evidence, crash reconciliation, and
  checkpoint ordering.
- `scripts/cdc-integration-harness.py` — real MariaDB/MySQL compatibility and
  crash matrix.

## Tests asserting this spec

The current slice is covered by:

- [x] `src/live/ddl_semantics/tests.rs` — deterministic production `ADD COLUMN`,
      `ADD KEY`/MariaDB `ADD INDEX`, `ADD UNIQUE KEY`, generic and exact `DROP PROCEDURE`,
      and exact `DROP TRIGGER IF EXISTS` SQL/no-op behavior, plus the exact-hash source-only
      `CREATE PROCEDURE apply_release_move_purchase_repair` form, target-absence
      evidence, and proven no-op behavior,
      typed ALTER AST/post-state behavior and rename boundaries, plus the shared
      observed CREATE TABLE grammar/typed AST/rendering, historical QueryEvent
      charset-context decoding where a table charset is explicit, inherited
      target-database defaults recorded as `inherited_database_defaults` when
      both table defaults are omitted, explicit charset/collation SQL,
      deterministic post-state with sorted indexes, exact-grammar rejection, and
      runtime-admission contract.
- [x] `src/live/structured_stream/tests/ddl_replay.rs` — the stream executes
      generated SQL and preserves journal/checkpoint ordering for supported
      fixtures; the exact `content_sections_events_raw` barrier emits one unguarded
      atomic ALTER when both columns are absent, suppresses SQL when both exact
      columns are present, and blocks partial or divergent pre-state; its
      `ALGORITHM=INPLACE` and other near-misses remain pending; unsupported CREATE
      remains pending without target/checkpoint
      execution, and `unsupported_ddl_keeps_replicator_alive_at_unchanged_checkpoint`
      proves the durable block loop retries from the unchanged checkpoint.
- [x] `production_tinyint_unsigned_add_column_normalizes_display_width`,
      `already_present_tinyint_add_column_has_equal_pre_and_post_state`,
      `divergent_existing_tinyint_add_column_remains_blocked`, and
      `existing_translation_pending_tinyint_add_column_is_proven_and_checkpointed`
      assert production translation, exact converged-target proof, divergent
      definition/position rejection, and same-barrier checkpoint recovery.
- [ ] `scripts/cdc-integration-harness.py --scenario create-facets-historical-crash-restart` —
      pending final integration proof for the recorded `kg_comic_facets` CREATE:
      a historical `VARCHAR(80)` despite a later source `VARCHAR(128)`, prepared
      crash/restart, exact journal-coordinate promotion, target CREATE once, and
      later DDL in source order.
- [x] `scripts/cdc-integration-harness.py --scenario create-table-crash-restart` —
      real differing-default MariaDB/MySQL fixture admission, target-absence
      evidence, explicit charset/collation SQL, exact observed post-state,
      post-DDL/pre-applied crash, prepared-state restart, exact checkpoint, and
      idempotent replay with one target CREATE execution.
- [x] `scripts/cdc-integration-harness.py --scenario production-alter-table` —
      real MariaDB/MySQL replay of five checkpointed ALTER events, including
      VARCHAR/DATETIME/SMALLINT column parity, comments, non-unique and unique
      composite-index metadata, duplicate-row rejection parity, translated
      column removal and its absent-column no-op, journal evidence/version, and
      final supported-event checkpoint; an unsupported
      unique-prefix option remains `translation_pending` with zero target
      execution and unchanged checkpoint.
- [x] `scripts/cdc-integration-harness.py --scenario curated-strip-sale-alter-pending-replay` —
      real MariaDB 11.4/MySQL 8 pending-row promotion with immutable identity,
      crash/restart, schema order/default checks, JSON CHECK enforcement, and
      following DML.
- [x] `scripts/cdc-integration-harness.py --scenario basic-scalar-create-add-pending-replay` —
      real MariaDB 11.4/MySQL 8 replay of 32 shared scalar definitions across
      CREATE and ADD, with default/width/fractional-time/binary-text-blob metadata
      and following DML.
- [x] `scripts/cdc-integration-harness.py --scenario basic-column-operations-pending-replay` —
      real MariaDB 11.4/MySQL 8 pending-row promotion through MODIFY, RENAME, and
      DROP column/index operations, crash/restart, persisted journal evidence,
      metadata/default/order/index checks, and following DML.
- [x] `src/live/ddl_semantics/transform/observed_create.rs` tests and the
      `reader_memory_*` tests in `src/live/ddl_semantics/tests.rs` — typed AST for
      the three `reader_memory` CREATE fixtures (inline primary key, `BIGINT
      UNSIGNED`, `DATETIME(6)` defaults/on-update, TEXT expression defaults,
      ascii `CHAR`, quoted string defaults, bounded CHECK predicates, explicit
      collation), exact MySQL 8 SQL for all five fixtures, expected post-state
      matching MySQL's `COLUMN_DEFAULT`/`EXTRA`/encoding metadata, canonical AST
      encoding of checks and column encoding without changing older statements,
      ALTER post-state for the TEXT expression default and ascii column,
      rejection of unknown CHECK columns, and rejection of unmodeled precisions,
      functions, operators, literals, encodings, and duplicate definitions.
- [x] `scripts/cdc-integration-harness.py --scenario reader-memory-create-pending-replay`
      (`real_reader_memory_create_pending_replay_promotes_and_replays_following_ddl`) —
      real MariaDB 11.4/MySQL 8 replay of the durable `translation_pending`
      `reader_memory_profiles` CREATE followed by two CREATEs, two ALTERs, and
      DML: pending-row promotion with immutable identity, five checkpointed
      journal rows with evidence, exact column/key/CHECK/collation metadata,
      identical source and target rows including `DATETIME(6)` microseconds,
      CHECK enforcement on both endpoints, target implicit defaults, and exact
      final checkpoint.
- [x] `reader_memory_guarded_alter_*` tests in `src/live/ddl_semantics/tests.rs` —
      unguarded MySQL 8 SQL for the guarded fixture, absent-target post-state with
      appended `DATETIME(6)`/`VARCHAR` columns and both keys, all-present target
      proven as a no-op (pre-state equals post-state), partial and divergent
      pre-state rejection, and rejection of other precisions, required string
      columns without defaults, quoted defaults, guarded unique keys, and index
      options.
- [x] `scripts/cdc-integration-harness.py --scenario reader-memory-guarded-alter-pending-replay`
      (`real_reader_memory_guarded_alter_replays_then_proves_noop`) — real
      MariaDB 11.4/MySQL 8 promotion of the durable pending guarded ALTER, exact
      column/key metadata, identical rows with `DATETIME(6)` values, then the
      same statement re-run on the source journaled as a checkpointed no-op with
      `generated_sql = NULL` and an advanced checkpoint.
- [x] `src/live/ddl_semantics/tests.rs` and
      `src/live/structured_stream/tests/ddl_replay.rs` — exact `releases`
      `idx_downloads_sort` DROP/ADD directional-index AST, deterministic target
      SQL/evidence, rejection of changed shape, and replay promotion from the
      durable pending row.

Existing proof covers earlier observed ALTER/CREATE slices and narrow DDL paths.
The basic common DDL matrix now has bounded real MariaDB 11.4/MySQL 8 replay and
crash/restart proof for sale ALTER, 32 scalar CREATE/ADD definitions, and column
operations. Deployment, final integration gating, and live-stream proof remain
open. This does not prove full `ALTER TABLE`, all `CREATE TABLE` syntax, a
complete compatibility matrix, or deployment safety.

## Known gaps (current cycle)

- [ ] Complete final integration and deployment gates for the basic common DDL
      matrix before treating it as deployment-ready.
- [x] Remove runtime/config/bootstrap/grant/harness/test dependencies on the
      retired manual DDL ledger without restoring manual replay.
- [ ] Restore the pre-existing `production-alter-table` harness path, then run
      its new exact `releases` directional-index case against disposable
      MariaDB/MySQL. Current unit/structured-stream proof passes; the integration
      extension times out before its new assertions and proves neither deployment
      nor recovery.
- [ ] Build the broader production-derived DDL corpus and real MariaDB/MySQL 8
      parity matrix; the current five-event ALTER scenario plus one exact CREATE
      fixture crash/restart scenario remains only a slice proof.
- [ ] Define transformation-version compatibility after the first production
      deployment establishes a real schema upgrade boundary.
- [ ] Extend beyond the basic matrix only with a bounded grammar and proof that
      unsupported variants remain `translation_pending`, execute no target SQL,
      leave the checkpoint unchanged, and cannot be overtaken.

## Out of scope

- Manual target-SQL authoring or operator resolution as a CDC fallback.
- Index-only automatic replay as the target DDL architecture.
- Full `ALTER TABLE` coverage beyond the basic matrix and separately listed
  observed extensions.
- `ALTER TABLE FIRST`, `ALTER COLUMN SET/DROP DEFAULT`, `CHANGE COLUMN`, table
  rename/drop/truncate, and new constraint grammars unless separately listed in
  the implemented slice.
- Mode-dependent `REAL`/`ZEROFILL`, arbitrary expressions, and unmodeled
  `ALGORITHM`/`LOCK` variants.
- Silently dropping, weakening, or approximating parsed DDL clauses.
- Cross-schema mutation outside the configured application schema.
- Detecting or reconciling preexisting source/target schema differences,
  including column type, charset, collation, defaults, or existing indexes.
- Detecting or repairing preexisting source/target data differences, including
  duplicate target rows before `ADD UNIQUE KEY` execution.
- Treating target execution failure caused by schema/data drift as unsupported
  translation; those failures remain observable recovery/reconciliation blocks.
- Coordinate-anchored historical source semantic lineage reconstruction or a
  durable source-model head in the current cycle. Events needing that history
  remain `translation_pending` rather than guessed from current source or target
  state.
