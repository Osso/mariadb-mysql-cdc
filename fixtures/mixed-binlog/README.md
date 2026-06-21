# Mixed Binlog Fixture

Captured from a scratch MariaDB server with:

- MariaDB `11.4.12-MariaDB-ubu2404-log`
- `server_id=17`
- `log_bin=mysql-bin`
- `binlog_format=MIXED`
- binlog checksums enabled

The fixture covers:

- query events
- GTID events
- table map events
- row inserts
- row updates
- row deletes
- DDL through `CREATE DATABASE`, `CREATE TABLE`, and `ALTER TABLE`
- binlog rotation from `mysql-bin.000001` to `mysql-bin.000002`

Files:

- `mysql-bin.000001`
- `mysql-bin.000002`
- `mysql-bin.index`

Decode with:

```bash
mariadb-binlog --base64-output=DECODE-ROWS --verbose \
  fixtures/mixed-binlog/mysql-bin.000001 \
  fixtures/mixed-binlog/mysql-bin.000002
```

