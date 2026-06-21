# Validation

Validation is modeled as three read-only commands over source and target
readers. The core logic is trait-backed so the SQL client binding can be added
without changing comparison behavior.

## Table Counts

`validate_table_counts` compares source and target row counts for each table and
returns one `CountComparison` per table.

## Sampled Checksums

`validate_sampled_checksums` asks each reader for deterministic checksum samples
using the same table, primary-key, selected-column, and sample-size request. It
reports only differing or missing samples.

## Row Divergence

`report_row_divergence` reads a bounded primary-key ordered window from source
and target. It reports:

- rows missing from the source
- rows missing from the target
- rows present in both with differing column values

Every request carries the table name, primary-key columns, selected columns,
optional `start_after` primary key, and limit so row-level reports can be paged.
