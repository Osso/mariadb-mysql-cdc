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

## Live Stream Checkpoints

`stream-binlog` uses `--checkpoint-file` to persist the last successfully
applied statement's resume coordinate. For statement events this is the
`end_log_pos` reported by `mariadb-binlog`, not the `# at` start position, so a
reconnect starts after the applied event.

```bash
mariadb-mysql-cdc stream-binlog \
  --source-host 192.0.2.10 \
  --source-user cdc_reader \
  --source-password-env SOURCE_PASSWORD \
  --source-database globalcomix \
  --binlog-file mysqld-bin.002524 \
  --start-position 882748822 \
  --target-host target-mysql.example \
  --target-port 25060 \
  --target-user target_user \
  --target-password-env TARGET_PASSWORD \
  --target-database globalcomix \
  --insert-conflict-policy ignore-duplicate \
  --checkpoint-file /var/lib/mariadb-mysql-cdc/stream-checkpoint.json
```

When the checkpoint file exists, `stream-binlog` resumes from it instead of the
static `--binlog-file` and `--start-position` arguments. Transient source stream
loss such as TLS connection reset triggers in-process reconnect with bounded
backoff. Non-transient source errors, target write failures, and quarantined SQL
still fail the process.
