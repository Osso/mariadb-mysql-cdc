# Production-Derived Fixtures

These fixtures are sanitized from narrow production binlog windows. They keep
event structure and SQL shape while replacing IDs, text, URLs, hashes, and other
user data with neutral values.

Do not commit raw production binlog output here.
