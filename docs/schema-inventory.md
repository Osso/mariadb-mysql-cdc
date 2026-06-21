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

