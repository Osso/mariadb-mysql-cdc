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
index DDL automatically, inspect target-native visibility; route the operation to
the manual DDL ledger when an invisible target index is possible.

Source and target inventory connections use endpoint-specific TLS CA settings
when configured. Missing, unreadable, or invalid CA files fail with a
source/target-specific diagnostic before metadata queries run.

