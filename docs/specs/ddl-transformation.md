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

- [x] Token-parse the production-observed unqualified multi-clause `ALTER TABLE` form with `ADD COLUMN`, named `ADD KEY`, MariaDB-syntax `ADD INDEX` normalized to the same AST, and named `ADD UNIQUE KEY` clauses; preserve clause order and render deterministic MySQL 8 SQL with source `ADD INDEX` emitted as target `ADD KEY`.
- [x] Convert MariaDB `ALTER TABLE ... DROP COLUMN IF EXISTS ...` into MySQL 8 `DROP COLUMN` clauses by matching target identifiers ASCII-case-insensitively, emitting each matched target spelling once, and treating absent or repeated case-variant clauses as proven no-ops.
- [x] Transform the generic exact unqualified, unquoted `DROP PROCEDURE IF EXISTS <identifier>` form and the exact unqualified, unquoted plain `DROP PROCEDURE apply_release_move_purchase_repair` form using target-local routine inventory. An existing target routine emits deterministic MySQL `DROP PROCEDURE` with the target spelling backtick-quoted; an absent target records a proven no-op. Qualified, quoted, commented, and other plain-name variants remain `translation_pending` barriers.
- [x] Admit the source-only `CREATE PROCEDURE` form only when the complete statement matches one of two private exact hashes for the exact unqualified routine identity `apply_release_move_purchase_repair`. Admission precedes generic qualified-identifier rejection because the admitted statements contain qualified tokens. Require the target routine to be absent before and after evidence capture, execute no target SQL, and record a proven no-op. The body is never executed; data effects may arrive only through subsequent source ROW/FULL events in source order. An existing `translation_pending` row promotes automatically after exact-hash admission. Every other body, name, and routine DDL remains a `translation_pending` barrier. Raw production procedure bodies, `DEFINER` hosts, and event coordinates are intentionally excluded from public documentation.
- [x] Transform the observed `ADD COLUMN` forms only under the exact unquoted type grammar `VARCHAR(positive canonical decimal length)`, `DATETIME`, or `SMALLINT UNSIGNED`, with the observed `DEFAULT NULL`, explicit `NULL`, `COMMENT`, and `AFTER` options. Expected post-state for added character columns records the table-inherited character set and collation so live inventory comparison matches MySQL metadata. The type keyword, `VARCHAR` parentheses and length, and `UNSIGNED` keyword must be unquoted; `DATETIME` precision and `SMALLINT` display width are unsupported.
- [x] Reject quoted type keywords, quoted `VARCHAR` lengths, and quoted `UNSIGNED` forms as unsupported syntax. These variants remain `translation_pending` with no target DDL or checkpoint advance.
- [x] Transform named composite `ADD KEY`, MariaDB-syntax `ADD INDEX`, and `ADD UNIQUE KEY` clauses over ordinary columns as BTREE indexes; multiple admitted clauses remain ordered, source `ADD INDEX` emits as target `ADD KEY`, and broader index and clause options remain outside this slice.
- [x] Encode a canonical typed clause AST: `add_column` records name/type/nullability/default/comment/position, while `add_key` records the typed index AST and ordered key parts.
- [x] Record expected target object state for crash/replay verification without treating that evidence as source/target reconciliation.
- [x] Fail closed as `translation_pending` before target execution when syntax, context, dependencies, or semantics fall outside that explicit slice; the stream checkpoint and later-event barrier must remain unchanged.
- [x] Carry `TIMESTAMP` column types across unchanged. The former unconditional `TIMESTAMP` to `DATETIME` rewrite is removed: MySQL rejects values past 2038-01-19 that MariaDB 11 accepts, but no source column holds one, so the rewrite bought nothing and would have required rebuilding 384 tables and about 864 GB with `ALGORITHM=COPY`.
- [x] Emit deterministic MySQL 8 SQL and record transformation version `mariadb-mysql8-v1`.
- [x] Set journal `transformation_version` and nullable `generated_sql` from the actual transformation before `prepared`; proven no-ops persist `generated_sql = NULL`.
- [x] Execute generated SQL in the automatic stream path instead of the MariaDB source SQL.

This is a production-derived ALTER TABLE slice plus one exact fixture CREATE
TABLE admission, one identity-scoped source-only CREATE PROCEDURE form, and
two exact procedure-drop admissions, not full ALTER TABLE, generic CREATE TABLE,
general routine DDL, or the full MariaDB-to-MySQL 8 transformation pipeline. Coordinate-anchored reconstruction
of historical source schema lineage is explicitly excluded from the current
cycle. The translator may use only semantics completely represented by the
admitted event AST and fenced target pre-state; it must not infer unmodeled
historical source state.

Unsupported or ambiguous DDL syntax enters the durable journal as
`translation_pending` with sentinel/no execution evidence. It performs no target
DDL, does not advance the stream checkpoint, and blocks later events from
overtaking it. When translator code later supports the exact syntax, the same row
may promote once to `prepared`, fill immutable evidence, execute generated SQL,
and checkpoint automatically. Target execution failures caused by preexisting
target schema or data differences are execution/reconciliation failures, not
translator-unavailable events.
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

No other `CREATE TABLE` syntax is admitted.

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
- `src/live/ddl_semantics/transform.rs` — production-derived `ADD COLUMN`,
  `ADD KEY`/MariaDB `ADD INDEX`, `ADD UNIQUE KEY`, generic and exact `DROP PROCEDURE` translators,
  and `RENAME COLUMN IF EXISTS` translators,
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
      `ADD KEY`/MariaDB `ADD INDEX`, `ADD UNIQUE KEY`, generic and exact `DROP PROCEDURE` SQL/no-op
      behavior, and the exact-hash source-only
      `CREATE PROCEDURE apply_release_move_purchase_repair` form, target-absence
      evidence, and proven no-op behavior,
      typed ALTER AST/post-state behavior and rename boundaries, plus the strict
      fixture `CREATE TABLE` grammar/typed AST/renderer, fenced source-default
      evidence, exact-coordinate rejection, explicit charset/collation SQL,
      deterministic post-state with sorted indexes, exact-grammar rejection, and
      runtime-admission contract.
- [x] `src/live/structured_stream/tests/ddl_replay.rs` — the stream executes
      generated SQL and preserves journal/checkpoint ordering for supported
      fixtures; unsupported CREATE remains pending without target/checkpoint
      execution.
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

These tests prove only the observed ALTER slice, the exact fixture CREATE TABLE
admission, the identity-scoped source-only `CREATE PROCEDURE` form, the generic
and exact `DROP PROCEDURE` admissions, the real differing-default crash/restart
scenario, and existing narrow DDL paths. They do not prove full
ALTER TABLE, generic CREATE TABLE, the broader
transformation contract, a full MariaDB/MySQL matrix, or deployment safety.

## Known gaps (current cycle)

- [ ] Extend the current translator beyond the production-observed
      `ADD COLUMN`/`ADD KEY`/MariaDB `ADD INDEX`/`ADD UNIQUE KEY`/`DROP COLUMN IF EXISTS`, the
      identity-scoped source-only `CREATE PROCEDURE` form, generic and exact
      `DROP PROCEDURE`, and rename slices into the canonical
      MariaDB DDL parser and MySQL 8 transformation pipeline.
- [x] Remove runtime/config/bootstrap/grant/harness/test dependencies on the
      retired manual DDL ledger without restoring manual replay.
- [ ] Build the broader production-derived DDL corpus and real MariaDB/MySQL 8
      parity matrix; the current five-event ALTER scenario plus one exact CREATE
      fixture crash/restart scenario remains only a slice proof.
- [ ] Define transformation-version compatibility after the first production
      deployment establishes a real schema upgrade boundary.
- [ ] Extend the canonical AST and renderer one production-derived unsupported
      ALTER shape at a time; each shape must first prove `translation_pending`,
      no target execution, unchanged checkpoint, and no-overtake behavior.

## Out of scope

- Manual target-SQL authoring or operator resolution as a CDC fallback.
- Index-only automatic replay as the target DDL architecture.
- Full `ALTER TABLE` coverage beyond the observed `ADD COLUMN`, `ADD KEY`/MariaDB
  `ADD INDEX`, `ADD UNIQUE KEY`, and `DROP COLUMN IF EXISTS` forms.
- Additional column types, defaults, clauses, index options, and DDL families not
  listed in the implemented slice.
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
