# Production-Derived Fixtures

These fixtures are sanitized from narrow production binlog windows. They keep
event structure and SQL shape while replacing IDs, text, URLs, hashes, and other
user data with neutral values.

Do not commit raw production binlog output here.

DDL QueryEvents appended on 2026-07-17 were recovered from the retired target
DDL ledger, not guessed from synthetic syntax. Coordinates, timestamps, comments,
and descriptive literals are sanitized while table/column names and clause
ordering are preserved because they define the transformation contract.
