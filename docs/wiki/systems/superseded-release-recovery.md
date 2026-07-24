# Superseded release recovery

The live structured stream has one narrow recovery path for historical
`globalcomix.releases` `ROW INSERT` events that fail a composite foreign key
because the release and comic parent changed later in source history. This is
not generic FK repair.

## Approved scope

Only the exact production boundaries below are eligible:

- Category: `releases_ibfk_2`, child `(comic_id,comic_category_id)` referencing
  parent `(id,section_id)`, transaction
  `mysqld-bin.002709:515816736–515824875`.
- Visibility: `releases_ibfk_3`, child `(comic_id,comic_is_visible)` referencing
  parent `(id,is_visible)`, transaction
  `mysqld-bin.002709:531921570–531929925`; candidate release event is at
  `531921789`.

The candidate must be an `INSERT` with MySQL error `1452`, and the error text
must identify the exact approved table, constraint, child columns, and parent
columns. Other coordinates, constraints, tables, operations, or error codes
fail closed.

## Proof and target state

The verifier retains the complete historical release image and reads a later,
consistent source snapshot. It requires:

- exactly one current source release with the same release ID and comic ID;
- a changed current parent value, proving later source history;
- exactly one source parent matching the current release FK identity;
- locked target release and parent reads using the same current identity;
- complete source/target row hashes for the release and parent evidence.

If the target release is absent, recovery installs the complete current source
release row. If it exists, its full row hash must already equal current source.
The parent identity is preserved; recovery never updates or deletes the parent.

## Commit and failure behavior

The remaining rows in the source transaction, conflict observation/resolution
evidence, and XID checkpoint commit atomically. A failed proof, predecessor,
coordinate/FK-scope check, install, or commit rolls back target effects and
checkpoint advancement. Unresolved conflict evidence is persisted through the
independent conflict store. Only the subsequent verified replay resolves the
conflict.
