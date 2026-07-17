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

- [x] Token-parse the production-observed unqualified multi-clause `ALTER TABLE ... RENAME COLUMN IF EXISTS ...` form.
- [x] Select executable rename clauses from immutable target-column pre-state; an absent old column is omitted, while old/new coexistence fails closed.
- [x] Emit deterministic MySQL 8 SQL without `IF EXISTS`, return a proven no-op when no clause is executable, and record transformation version `mariadb-mysql8-v1`.
- [x] Execute generated SQL in the automatic stream path instead of the MariaDB source SQL.

This is one translator slice, not the full MariaDB-to-MySQL 8 transformation
pipeline. Unsupported DDL still follows the existing manual-resolution boundary;
manual-ledger removal and deployment remain future work.

### Execution and recovery

- [ ] Execute transformed DDL through the durable replay journal before advancing
      the stream checkpoint.
- [ ] Reconcile crashes after prepare, target implicit commit, journal update, and
      checkpoint update without duplicate or skipped schema mutations.
- [ ] Block checkpoint advancement when a required transformation is missing,
      ambiguous, or cannot preserve semantics.
- [ ] After the translator is extended, automatically replay the blocked source
      event from its durable journal evidence without operator-authored target SQL
      or a manual-resolution state transition.
- [ ] Prevent later row or statement events from overtaking a blocked DDL event.
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

- [DDL recovery journal](../ddl-resolution.md)
- [Checkpoint ordering](../checkpoints.md)
- [Schema inventory](../schema-inventory.md)
- [System design](../design.md)

## Implementation inventory

- `src/live/structured_stream.rs` — reads ordered QueryEvents and enforces the
  checkpoint barrier.
- `src/live/ddl_semantics.rs` — dispatches current DDL transformations and
  captures semantic evidence.
- `src/live/ddl_semantics/transform.rs` — first `RENAME COLUMN IF EXISTS`
  translator slice, including target pre-state selection and versioned SQL
  emission.
- `src/live/structured_stream/ddl.rs` — prepares the journal, executes generated
  target SQL, and preserves checkpoint ordering.
- `src/live/ddl_replay_journal.rs` — durable evidence, crash reconciliation, and
  checkpoint ordering.
- `src/live/ddl_ledger.rs` — legacy manual-resolution path for unsupported DDL;
  removal is not part of this slice.
- `scripts/cdc-integration-harness.py` — real MariaDB/MySQL compatibility and
  crash matrix.

## Tests asserting this spec

The current slice is covered by:

- [x] `src/live/ddl_semantics/tests.rs` — deterministic multi-clause MySQL 8
      output, proven no-op when old columns are absent, and fail-closed behavior
      when old and new columns coexist.
- [x] `src/live/structured_stream/tests/ddl_replay.rs` — the stream executes
      generated SQL without `IF EXISTS` and leaves the journal prepared when
      target execution fails.

These tests prove only the current rename slice. They do not prove the full
transformation contract, real MariaDB/MySQL compatibility, or deployment safety.

## Known gaps (current cycle)

- [ ] Extend the current translator beyond the production-observed rename slice
      into the canonical MariaDB DDL parser and MySQL 8 transformation pipeline.
- [ ] Remove the manual-resolution runtime, schema, grants, documentation, and
      operational workflow.
- [ ] Build the production-derived DDL corpus and real MariaDB/MySQL 8 parity
      matrix.
- [ ] Define transformation-version compatibility for journal replay after code
      upgrades.
- [ ] Reconcile existing pending/resolved legacy ledger rows into the automatic
      transformation journal without checkpoint loss.

## Out of scope

- Manual target-SQL authoring or operator resolution as a CDC fallback.
- Index-only automatic replay as the target DDL architecture.
- Silently dropping, weakening, or approximating source schema semantics.
- Cross-schema mutation outside the configured application schema.
