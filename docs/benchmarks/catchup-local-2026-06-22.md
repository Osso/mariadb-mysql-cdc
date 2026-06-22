# Catchup Local Benchmark - 2026-06-22

Command:

```bash
scripts/benchmark-catchup-local.sh
```

Environment:

- Source: local Docker `mariadb:11.4`
- Target: local Docker `mysql:8.0`
- Rows: 100,000
- Table shape: `BIGINT` primary key, secondary `(tenant_id, id)` index,
  `VARCHAR(96)` payload, `DATETIME`
- Chunk size: 10,000
- Parallel workers: 4

Result:

```text
catchup_benchmark_result rows=100000 elapsed_seconds=1.290 rows_per_second=77519.38 chunk_size=10000 parallel_workers=4
```

This proves the local catchup implementation can exceed the 20,000 imported
rows/second target on an ordinary indexed table when source and target databases
are healthy and not intentionally throttled.
