# Schema Inventory

Schema inventory captures the source database metadata needed before staged
`sync` and CDC apply work begins.

Captured objects:

- base tables
- table engines and collations
- columns, ordinal positions, column types, defaults, nullability, and extras
- generated columns with expression and virtual/stored kind
- primary key columns in key order
- foreign keys with child table/columns and referenced schema/table/columns
- views and definitions
- triggers with timing, event, table, and statement
- routines with type and definition when available
- scheduled events with status and definition

The inventory module exposes:

- `SchemaInventory`: normalized metadata model, including foreign-key edges.
- `ForeignKeyInventory`: ordered child and referenced column mappings used by
  table-catalog scheduling and table-sync parent repair.
- `InventoryReader`: trait for reading source metadata.
- `MariaDbInventoryReader`: `mariadb` CLI backed reader using
  `information_schema`.

The CLI reader uses read-only `SELECT` statements against `information_schema`.
Table-sync merges local FK edges from source and target inventories; target-only
local constraints therefore participate in exact parent discovery, while
cross-schema edges are excluded from this runtime repair path.

## Cross-engine visibility compatibility

The index query returns a compatibility literal, `IS_VISIBLE='YES'`, because
MariaDB does not provide a portable `information_schema.STATISTICS.IS_VISIBLE`
field. The normalized `visible` field therefore means “visible according to the
portable reader,” not proof of MySQL target visibility. Before admitting affected
index DDL automatically, inspect target-native visibility; otherwise the stream
slice should leave the event in the journal's `translation_pending` barrier.
The retired manual-ledger runtime, configuration, bootstrap, grants, and harness
paths have been removed.

## Connection policy

The live GlobalComix source MariaDB (`source-mariadb.example` /
`192.0.2.10`) is plaintext-only by accepted operational policy. CDC source
inventory, staged sync, and stream connections must use an explicit plaintext
source mode for this endpoint. Do not require a source CA,
do not attempt opportunistic TLS-to-plaintext fallback, and do not treat
source CA absence as an error for the current source.

Target DigitalOcean MySQL connections are different: every target endpoint
connection must use its configured CA file and validate the certificate chain.
DNS/hostname target endpoints require certificate identity matching. Target
plaintext, invalid-certificate acceptance, and TLS-validation retry fallbacks
are forbidden.

Missing, unreadable, or invalid target CA files fail with a target-specific
diagnostic before target metadata queries or writes run. Retryable target I/O,
codec, timeout, connection, and packet/setup failures may cause a fresh
connection attempt and retry the same metadata query once, but every target
attempt reuses the configured TLS settings and performs the same CA/chain and
hostname validation. Configuration, certificate, identity, and other
non-retryable target failures stop immediately. Logs identify endpoint role,
inventory stage, schema, TLS mode, attempt (`1/2` or `2/2`), reset status,
connection age, and both the original and retry errors.

## Tests asserting this behavior

- `src/inventory/tests/reader.rs` — asserts TLS setup, retry after a packet
  desynchronization, retry after an initial connection failure, immediate
  failure for server SQL errors, and replacement of expired connections.

