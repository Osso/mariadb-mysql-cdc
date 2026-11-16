# MINIMAL Delete Binlog Fixture

Captured from a scratch MariaDB server with:

- MariaDB `11.8.8-MariaDB-ubu2404-log`
- `server_id=17`, `binlog_format=ROW`, global `binlog_row_image=FULL`
- binlog checksums enabled

Contents, all on `fixture_cdc.accounts`:

- FULL-image `CREATE` and two-row insert (ids 1 and 2)
- session `binlog_row_image=MINIMAL` `DELETE ... WHERE id=2` (before image holds only `id`)
- session `binlog_row_image=MINIMAL` `UPDATE ... SET balance=150 WHERE id=1`

Decode with:

```bash
mariadb-binlog --base64-output=DECODE-ROWS --verbose \
  fixtures/minimal-delete-binlog/mysql-bin.000001
```
