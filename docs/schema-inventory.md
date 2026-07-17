# Schema Inventory

Schema inventory captures the source database metadata needed before snapshot
and CDC apply work begins.

Captured objects:

- base tables
- table engines and collations
- columns, ordinal positions, column types, defaults, nullability, and extras
- generated columns with expression and virtual/stored kind
- primary key columns in key order
- views and definitions
- triggers with timing, event, table, and statement
- routines with type and definition when available
- scheduled events with status and definition

The inventory module exposes:

- `SchemaInventory`: normalized metadata model.
- `InventoryReader`: trait for reading source metadata.
- `MariaDbInventoryReader`: `mariadb` CLI backed reader using
  `information_schema`.

The CLI reader uses read-only `SELECT` statements against `information_schema`.

## Cross-engine visibility compatibility

The index query returns a compatibility literal, `IS_VISIBLE='YES'`, because
MariaDB does not provide a portable `information_schema.STATISTICS.IS_VISIBLE`
field. The normalized `visible` field therefore means “visible according to the
portable reader,” not proof of MySQL target visibility. Before admitting affected
index DDL automatically, inspect target-native visibility; otherwise the stream
slice should leave the event in the journal's `translation_pending` barrier.
The retired manual-ledger runtime, configuration, bootstrap, grants, and harness
paths have been removed.

Source and target inventory connections use endpoint-specific TLS CA settings
when configured. Missing, unreadable, or invalid CA files fail with a
source/target-specific diagnostic before metadata queries run. Retryable I/O,
codec, TLS, timeout, connection, and packet/setup failures cause a fresh
connection attempt and retry the same metadata query once; an existing
connection is discarded before that attempt, while an initial connection
failure simply opens a new one. Configuration and other non-retryable failures
stop immediately. Logs identify endpoint role, inventory stage, schema, TLS
mode, attempt (`1/2` or `2/2`), reset status, connection age, and both the
original and retry errors.

## Tests asserting this behavior

- `src/inventory/tests/reader.rs` — asserts TLS setup, retry after a packet
  desynchronization, retry after an initial connection failure, immediate
  failure for server SQL errors, and replacement of expired connections.

