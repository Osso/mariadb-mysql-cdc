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

- [x] Token-parse the production-observed unqualified multi-clause `ALTER TABLE` form with `ADD COLUMN`, named `ADD KEY`, and named `ADD UNIQUE KEY` clauses.
- [x] Convert MariaDB `ALTER TABLE ... DROP COLUMN IF EXISTS ...` into MySQL 8 `DROP COLUMN` clauses by matching target identifiers ASCII-case-insensitively, emitting each matched target spelling once, and treating absent or repeated case-variant clauses as proven no-ops.
- [x] Transform the observed `ADD COLUMN` forms for `VARCHAR(length)`, `DATETIME`, and `SMALLINT UNSIGNED`, with the observed `DEFAULT NULL`, explicit `NULL`, `COMMENT`, and `AFTER` options.
- [x] Transform named composite `ADD KEY` and `ADD UNIQUE KEY` clauses over ordinary columns as BTREE indexes; broader index and clause options remain outside this slice.
- [x] Encode a canonical typed clause AST: `add_column` records name/type/nullability/default/comment/position, while `add_key` records the typed index AST and ordered key parts.
- [x] Record expected target object state for crash/replay verification without treating that evidence as source/target reconciliation.
- [x] Fail closed as `translation_pending` before target execution when syntax, context, dependencies, or semantics fall outside that explicit slice; the stream checkpoint and later-event barrier must remain unchanged.
- [x] Emit deterministic MySQL 8 SQL and record transformation version `mariadb-mysql8-v1`.
- [x] Set journal `transformation_version` and nullable `generated_sql` from the actual transformation before `prepared`; proven no-ops persist `generated_sql = NULL`.
- [x] Execute generated SQL in the automatic stream path instead of the MariaDB source SQL.

This is a production-derived ALTER TABLE slice, not full ALTER TABLE or the full
MariaDB-to-MySQL 8 transformation pipeline. Coordinate-anchored reconstruction
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

- [x] The exact harness `CREATE TABLE accounts` statement has a test-scoped typed
      AST and deterministic MySQL 8 renderer covering only `BIGINT`,
      `VARCHAR(length)`, `NOT NULL`, inline `PRIMARY KEY`, ordinary named `KEY`,
      and `ENGINE=InnoDB`.
- [x] Production `LiveDdlSemanticInventory` can capture this exact fixture's
      source schema defaults only inside a fence whose before/after source master
      coordinate exactly brackets the event file/end position; the target
      inventory must also prove the table is absent before and after capture.
- [x] The evidence persists source `character_set` and `collation` in the
      canonical AST, emits explicit `DEFAULT CHARACTER SET ... COLLATE ...`
      SQL, and derives a deterministic expected post-state from the typed AST
      and captured defaults.
- [x] Runtime transform/admission remains disabled for this fixture: the stream
      stays `translation_pending` with zero target DDL and zero checkpoint
      execution. This slice does not claim real-engine or deployment coverage;
      any future admission must retain the target-absence and deterministic
      post-state gates.

This fixture-backed contract does not admit other `CREATE TABLE` syntax.

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
  `ADD KEY`, `ADD UNIQUE KEY`, `DROP COLUMN IF EXISTS`, and
  `RENAME COLUMN IF EXISTS` translators,
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
      `ADD KEY`, `ADD UNIQUE KEY`, and `DROP COLUMN IF EXISTS` SQL/no-op behavior,
      typed ALTER AST/post-state behavior and rename boundaries, plus the exact
      harness `CREATE TABLE` typed AST/renderer, fenced source-default evidence,
      exact-coordinate rejection, explicit charset/collation SQL, deterministic
      post-state, and disabled runtime-admission contract.
- [x] `src/live/structured_stream/tests/ddl_replay.rs` — the stream executes
      generated SQL and preserves journal/checkpoint ordering; the fixture
      `CREATE TABLE` delegates to the production transformer and remains pending
      without target/checkpoint execution.
- [x] `scripts/cdc-integration-harness.py --scenario production-alter-table` —
      real MariaDB/MySQL replay of five checkpointed ALTER events, including
      VARCHAR/DATETIME/SMALLINT column parity, comments, non-unique and unique
      composite-index metadata, duplicate-row rejection parity, translated
      column removal and its absent-column no-op, journal evidence/version, and
      final supported-event checkpoint; an unsupported
      unique-prefix option remains `translation_pending` with zero target
      execution and unchanged checkpoint.

These tests prove only the observed ALTER slice, the fixture evidence seam, and
existing narrow DDL paths. They do not prove full ALTER TABLE, the broader
transformation contract, real-engine compatibility, a full MariaDB/MySQL matrix,
or deployment safety.

## Known gaps (current cycle)

- [ ] Extend the current translator beyond the production-observed
      `ADD COLUMN`/`ADD KEY`/`ADD UNIQUE KEY`/`DROP COLUMN IF EXISTS` and rename
      slices into the canonical
      MariaDB DDL parser and MySQL 8 transformation pipeline.
- [x] Remove runtime/config/bootstrap/grant/harness/test dependencies on the
      retired manual DDL ledger without restoring manual replay.
- [ ] Build the broader production-derived DDL corpus and real MariaDB/MySQL 8
      parity matrix; the current five-supported-event plus one pending-event ALTER
      scenario is only a slice proof.
- [ ] Define transformation-version compatibility after the first production
      deployment establishes a real schema upgrade boundary.
- [ ] Extend the canonical AST and renderer one production-derived unsupported
      ALTER shape at a time; each shape must first prove `translation_pending`,
      no target execution, unchanged checkpoint, and no-overtake behavior.

## Out of scope

- Manual target-SQL authoring or operator resolution as a CDC fallback.
- Index-only automatic replay as the target DDL architecture.
- Full `ALTER TABLE` coverage beyond the observed `ADD COLUMN`, `ADD KEY`,
  `ADD UNIQUE KEY`, and `DROP COLUMN IF EXISTS` forms.
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
