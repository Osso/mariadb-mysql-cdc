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
- `src/live/ddl_semantics.rs` — current narrow index parser; must be replaced or
  expanded into the canonical MariaDB DDL transformation layer.
- `src/live/ddl_replay_journal.rs` — durable evidence, crash reconciliation, and
  checkpoint ordering.
- `src/live/ddl_ledger.rs` — legacy manual-resolution path to remove; it is not
  part of the target architecture.
- `scripts/cdc-integration-harness.py` — real MariaDB/MySQL compatibility and
  crash matrix.

## Tests asserting this spec

No test currently proves the full transformation contract. Existing journal,
index, grant, and crash tests cover reusable safety infrastructure only; they do
not satisfy the unchecked transformation requirements above.

## Known gaps (current cycle)

- [ ] Replace index-only admission with the canonical MariaDB DDL parser and
      MySQL 8 transformation pipeline.
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
