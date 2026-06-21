# Checkpoints

Checkpoint state is stored as JSON so rehearsals can be inspected and edited
without a database dependency.

The checkpoint records:

- `source_file`: current source binlog file.
- `source_position`: current source binlog position.
- `gtid`: last known source GTID, when available.
- `event_timestamp`: source event timestamp in Unix seconds.
- `last_event`: last successfully processed event type and description.

Example:

```json
{
  "source_file": "mysql-bin.000001",
  "source_position": 1234,
  "gtid": "0-17-10",
  "event_timestamp": 1782075535,
  "last_event": {
    "event_type": "WriteRowsEvent",
    "description": "fixture_cdc.accounts insert"
  }
}
```

Writes use a temporary file followed by rename so a crash does not leave a
partially written checkpoint at the final path.

