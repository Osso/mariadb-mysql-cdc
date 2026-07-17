# MariaDB to MySQL 8 DDL Transformation

The CDC stream must convert production MariaDB DDL into deterministic MySQL 8
DDL while preserving source intent. This is the authoritative DDL behavior
contract. Journal and checkpoint mechanics are described in
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
- [ ] Preserve the source operation's observable schema and database behavior;
      reject silent semantic weakening or approximation.
- [ ] Preserve object qualification and dependency relationships without
      allowing writes outside the configured application schema.
- [ ] Produce deterministic MySQL 8 SQL and canonical expected post-state from
      the source event plus immutable target pre-state.
- [ ] Make transformations observable by persisting source SQL, canonical input,
      generated MySQL SQL, transformation version, source coordinate, pre-state,
      expected post-state, and observed post-state.

### Current implemented slice

- [x] Token-parse the production-observed unqualified multi-clause `ALTER TABLE` form with `ADD COLUMN`, named `ADD KEY`, and named `ADD UNIQUE KEY` clauses.
- [x] Transform the observed `ADD COLUMN` forms for `VARCHAR(length)`, `DATETIME`, and `SMALLINT UNSIGNED`, with the observed `DEFAULT NULL`, explicit `NULL`, `COMMENT`, and `AFTER` options.
- [x] Transform named composite `ADD KEY` and `ADD UNIQUE KEY` clauses over ordinary columns as BTREE indexes; broader index and clause options remain outside this slice.
- [x] Encode a canonical typed clause AST: `add_column` records name/type/nullability/default/comment/position, while `add_key` records the typed index AST and ordered key parts.
- [x] Derive the expected post-state for the explicitly supported ALTER slice by applying the event AST to a fenced target pre-state snapshot.
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

Unsupported or ambiguous DDL enters the durable journal as
`translation_pending` with sentinel/no execution evidence. It performs no target
DDL, does not advance the stream checkpoint, and blocks later events from
overtaking it. When translator code later supports the exact event, the same row
may promote once to `prepared`, fill immutable evidence, execute generated SQL,
and checkpoint automatically. Evidence-capture failure uses the same barrier.
The event handler has no supported manual-ledger workflow, but
config/bootstrap/grant/harness cleanup is still open and this contract is not
deployment-ready.

### Execution and recovery

- [x] Execute transformed DDL through the durable replay journal before advancing
      the stream checkpoint.
- [x] Reconcile crashes after prepare, target implicit commit, journal update, and
      checkpoint update without blind duplicate execution; ambiguous evidence
      becomes a durable barrier.
- [x] Block checkpoint advancement when a required transformation or evidence
      capture is missing, ambiguous, or cannot preserve semantics.
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
  `ADD KEY`, `ADD UNIQUE KEY`, and `RENAME COLUMN IF EXISTS` translators,
  including deterministic SQL emission.
- `src/live/ddl_semantics/canonical.rs` — typed ALTER clause AST encoding and
  expected post-state derivation from the fenced target pre-state.
- `src/live/structured_stream/ddl.rs` — prepares the journal, executes generated
  target SQL, and preserves checkpoint ordering.
- `src/live/ddl_replay_journal.rs` — durable evidence, crash reconciliation, and
  checkpoint ordering.
- `src/live/ddl_ledger.rs` — legacy ledger artifact retained behind test-only
  compilation in this slice; config/parser and harness/test dependencies remain
  open and it is not a supported DDL workflow.
- `scripts/cdc-integration-harness.py` — real MariaDB/MySQL compatibility and
  crash matrix.

## Tests asserting this spec

The current slice is covered by:

- [x] `src/live/ddl_semantics/tests.rs` — deterministic production `ADD COLUMN`,
      `ADD KEY`, and `ADD UNIQUE KEY` SQL, typed AST parsing, post-state
      derivation from fenced target pre-state, plus the existing rename
      boundaries.
- [x] `src/live/structured_stream/tests/ddl_replay.rs` — the stream executes
      generated SQL and preserves journal/checkpoint ordering.
- [x] `scripts/cdc-integration-harness.py --scenario production-alter-table` —
      real MariaDB/MySQL replay of three checkpointed ALTER events, including
      VARCHAR/DATETIME/SMALLINT column parity, comments, non-unique and unique
      composite-index metadata, duplicate-row rejection parity, journal
      evidence/version, and final supported-event checkpoint; an unsupported
      unique-prefix option remains `translation_pending` with zero target
      execution and unchanged checkpoint.

These tests prove only the observed ALTER slice and existing narrow DDL paths.
They do not prove full ALTER TABLE, the broader transformation contract, a full
MariaDB/MySQL matrix, or deployment safety.

## Known gaps (current cycle)

- [ ] Extend the current translator beyond the production-observed
      `ADD COLUMN`/`ADD KEY`/`ADD UNIQUE KEY` and rename slices into the canonical
      MariaDB DDL parser and MySQL 8 transformation pipeline.
- [x] Remove runtime/config/bootstrap/grant/harness/test dependencies on the
      retired manual DDL ledger without restoring manual replay.
- [ ] Build the broader production-derived DDL corpus and real MariaDB/MySQL 8
      parity matrix; the current three-supported-event plus one pending-event
      ALTER scenario is only a slice proof.
- [ ] Define transformation-version compatibility after the first production
      deployment establishes a real schema upgrade boundary.
- [ ] Extend the canonical AST and renderer one production-derived unsupported
      ALTER shape at a time; each shape must first prove `translation_pending`,
      no target execution, unchanged checkpoint, and no-overtake behavior.

## Out of scope

- Manual target-SQL authoring or operator resolution as a CDC fallback.
- Index-only automatic replay as the target DDL architecture.
- Full `ALTER TABLE` coverage beyond the observed `ADD COLUMN`, `ADD KEY`, and
  `ADD UNIQUE KEY` forms.
- Additional column types, defaults, clauses, index options, and DDL families not
  listed in the implemented slice.
- Silently dropping, weakening, or approximating source schema semantics.
- Cross-schema mutation outside the configured application schema.
- Coordinate-anchored historical source semantic lineage reconstruction or a
  durable source-model head in the current cycle. Events needing that history
  remain `translation_pending` rather than guessed from current source or target
  state.
