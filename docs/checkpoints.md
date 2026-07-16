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

`stream-binlog` stores the last successfully applied event's resume coordinate
in `cdc.stream_checkpoint` by default under
`checkpoint_name = stream-binlog:<source-identity>`. Admin/bootstrap credentials
must provision the exact checkpoint table schema and source-scoped row before
stream startup; runtime credentials only validate and update them. Startup fails
when the row is absent. A rebuilt or replaced source must use a new identity, so
it cannot consume an earlier incarnation's coordinate. The target-table
checkpoint is written in
the same transaction as grouped target DML. `stream-binlog` rejects
`--checkpoint-file`; the target table is the only checkpoint path. The resume
coordinate is the event end position, so reconnect starts after the applied
event.

Automatic compatible application-schema DDL is processed after grouped DML is
flushed. The DDL executes first; only after successful execution does the stream
save the checkpoint for that QueryEvent's `end_log_pos` in a separate target
transaction and invalidate the target schema cache. Automatic DDL does not create
a ledger row. DDL execution and checkpoint persistence are not one atomic
operation: a failure between them can leave the target DDL applied while the
durable checkpoint remains before the DDL. If target execution fails, the stream
stops and the checkpoint remains before the DDL.

Manual DDL is different: the stream flushes earlier DML, inserts the exact event
and coordinates into `cdc.ddl_events` as `pending`, and stops without advancing
past it. After an operator applies and validates the target change and marks the
same row `resolved`, restart requires an exact raw-SQL match and advances the
checkpoint to the recorded event end position without re-executing the DDL.

```bash
mariadb-mysql-cdc stream-binlog \
  --source-host 192.0.2.10 \
  --source-user cdc_reader \
  --source-password-env SOURCE_PASSWORD \
  --source-database globalcomix \
  --source-tls-ca-file /etc/mariadb-mysql-cdc/source-ca.pem \
  --source-identity production-source \
  --binlog-file mysqld-bin.002524 \
  --start-position 882748822 \
  --target-host target-mysql.example \
  --target-port 25060 \
  --target-user cdc_stream \
  --target-password-env TARGET_PASSWORD \
  --target-database globalcomix \
  --insert-conflict-policy ignore-duplicate \
  --checkpoint-table cdc.stream_checkpoint
```

When the configured checkpoint exists, `stream-binlog` resumes from it instead
of the static `--binlog-file` and `--start-position` arguments. Those initial
coordinates are still required for a new source identity with no checkpoint.
Pass the same `--source-identity` to `sync-progress` when inspecting stream
checkpoint freshness. The source CA file must contain the reviewed MariaDB server certificate/CA. The
native client verifies that chain and pins the certificate fingerprint, but
intentionally skips hostname matching because the current self-signed certificate
identity is `MariaDB Server`, while the stream connects over the private IP. It
never accepts an untrusted certificate. Other catchup source SQL connections are
currently non-TLS.

All target-side MySQL connections request TLS and use
`/etc/mariadb-mysql-cdc/do-ca.pem` when that file exists. A missing file is not a
proof of target trust: the client falls back to the MySQL driver's default TLS
options. Mount and inspect the reviewed DigitalOcean CA bundle before treating a
run as CA-verified; the current catchup manifest does not mount this file.

Transient source stream loss such as TLS connection reset triggers in-process
reconnect with bounded backoff. A stale/purged binlog fails without changing the
checkpoint; an operator must repair the gap explicitly. Non-transient source
errors, target write failures, quarantined SQL, and pending manual DDL resolution
still fail the process. A resolved DDL ledger row advances the checkpoint to the
event end position without executing the source DDL; see [DDL Resolution
Runbook](ddl-resolution.md).

The live stream currently writes file/position checkpoints with `gtid: null`;
GTID persistence and resume are not implemented. A duplicate conflict skipped
under `ignore-duplicate` does not block the event checkpoint, so the affected row
must be reconciled separately.
