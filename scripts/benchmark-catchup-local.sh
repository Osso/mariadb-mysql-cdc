#!/usr/bin/env bash
set -euo pipefail

rows="${ROWS:-100000}"
chunk_size="${CHUNK_SIZE:-10000}"
parallel_workers="${PARALLEL_WORKERS:-4}"
minimum_rows_per_second="${MIN_ROWS_PER_SECOND:-20000}"
source_port="${SOURCE_PORT:-23306}"
target_port="${TARGET_PORT:-23307}"
password="${MYSQL_ROOT_PASSWORD:-benchmark-secret}"
source_container="mariadb-mysql-cdc-bench-source-$$"
target_container="mariadb-mysql-cdc-bench-target-$$"
progress_file="/tmp/mariadb-mysql-cdc-benchmark-progress-$$.json"
log_file="/tmp/mariadb-mysql-cdc-benchmark-$$.log"
mariadb_wrapper="/tmp/mariadb-mysql-cdc-benchmark-mariadb-$$"

cleanup() {
    docker rm -f "$source_container" "$target_container" >/dev/null 2>&1 || true
    rm -f "$progress_file" "$log_file" "$mariadb_wrapper"
}
trap cleanup EXIT

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="${repo_root}/target/release/mariadb-mysql-cdc"

if [[ ! -x "$binary" ]]; then
    echo "benchmark_error binary_missing=$binary"
    echo "Run: cargo build --release"
    exit 2
fi

cat >"$mariadb_wrapper" <<'SH'
#!/usr/bin/env sh
exec mariadb --no-defaults "$@"
SH
chmod +x "$mariadb_wrapper"

docker run -d --name "$source_container" \
    -e MARIADB_ROOT_PASSWORD="$password" \
    -e MARIADB_DATABASE=bench \
    -p "127.0.0.1:${source_port}:3306" \
    mariadb:11.4 >/dev/null

docker run -d --name "$target_container" \
    -e MYSQL_ROOT_PASSWORD="$password" \
    -e MYSQL_DATABASE=bench \
    -p "127.0.0.1:${target_port}:3306" \
    mysql:8.0 >/dev/null

wait_for_mysql() {
    local port="$1"
    echo "benchmark_waiting port=$port"
    for _ in {1..180}; do
        if mariadb --no-defaults --host 127.0.0.1 --port "$port" --user root "--password=${password}" \
            --skip-column-names -e "SELECT 1" >/dev/null 2>&1; then
            echo "benchmark_ready port=$port"
            return 0
        fi
        sleep 1
    done
    echo "benchmark_error mysql_not_ready port=$port"
    exit 2
}

load_schema() {
    local port="$1"
    mariadb --no-defaults --host 127.0.0.1 --port "$port" --user root "--password=${password}" bench <<SQL
DROP TABLE IF EXISTS bench_items;
CREATE TABLE bench_items (
    id BIGINT NOT NULL PRIMARY KEY,
    tenant_id INT NOT NULL,
    payload VARCHAR(96) NOT NULL,
    updated_at DATETIME NOT NULL,
    KEY tenant_id_id (tenant_id, id)
);
SQL
}

load_source_rows() {
    mariadb --no-defaults --host 127.0.0.1 --port "$source_port" --user root "--password=${password}" bench <<SQL
CREATE TEMPORARY TABLE digits (i INT NOT NULL PRIMARY KEY);
INSERT INTO digits VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
INSERT INTO bench_items (id, tenant_id, payload, updated_at)
SELECT n,
       MOD(n, 1000),
       RPAD(CONCAT('payload-', n), 96, 'x'),
       TIMESTAMP('2026-01-01 00:00:00') + INTERVAL MOD(n, 86400) SECOND
FROM (
    SELECT ones.i
         + tens.i * 10
         + hundreds.i * 100
         + thousands.i * 1000
         + ten_thousands.i * 10000
         + 1 AS n
    FROM digits ones
    CROSS JOIN digits tens
    CROSS JOIN digits hundreds
    CROSS JOIN digits thousands
    CROSS JOIN digits ten_thousands
) generated
WHERE n <= ${rows};
SQL
}

wait_for_mysql "$source_port"
wait_for_mysql "$target_port"
load_schema "$source_port"
load_schema "$target_port"
load_source_rows

start_ns="$(date +%s%N)"
SOURCE_PASSWORD="$password" TARGET_PASSWORD="$password" "$binary" catchup-snapshot \
    --source-host 127.0.0.1 \
    --source-port "$source_port" \
    --source-user root \
    --source-password-env SOURCE_PASSWORD \
    --source-database bench \
    --target-host 127.0.0.1 \
    --target-port "$target_port" \
    --target-user root \
    --target-password-env TARGET_PASSWORD \
    --target-database bench \
    --progress-file "$progress_file" \
    --chunk-size "$chunk_size" \
    --parallel-workers "$parallel_workers" \
    --mariadb "$mariadb_wrapper" \
    --table bench_items >"$log_file"
end_ns="$(date +%s%N)"

copied="$(mariadb --no-defaults --host 127.0.0.1 --port "$target_port" --user root "--password=${password}" \
    --skip-column-names bench -e "SELECT COUNT(*) FROM bench_items")"
elapsed_seconds="$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN { printf "%.3f", (end - start) / 1000000000 }')"
rows_per_second="$(awk -v rows="$copied" -v elapsed="$elapsed_seconds" 'BEGIN { printf "%.2f", rows / elapsed }')"

echo "catchup_benchmark_result rows=${copied} elapsed_seconds=${elapsed_seconds} rows_per_second=${rows_per_second} chunk_size=${chunk_size} parallel_workers=${parallel_workers}"

if [[ "$copied" != "$rows" ]]; then
    echo "benchmark_error copied_rows=${copied} expected_rows=${rows}"
    exit 1
fi

awk -v rate="$rows_per_second" -v minimum="$minimum_rows_per_second" 'BEGIN { exit(rate >= minimum ? 0 : 1) }' || {
    echo "benchmark_error rows_per_second=${rows_per_second} minimum=${minimum_rows_per_second}"
    exit 1
}
