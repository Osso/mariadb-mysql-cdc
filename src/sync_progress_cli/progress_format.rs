use super::{SyncProgressConfig, SyncProgressRow, source_count};
use std::collections::BTreeMap;

pub(super) fn format_progress_rows(
    config: &SyncProgressConfig,
    rows: &[SyncProgressRow],
) -> Vec<String> {
    let (running_rows, other_rows): (Vec<_>, Vec<_>) = rows
        .iter()
        .partition(|row| row.status.eq_ignore_ascii_case("running"));
    let mut lines = other_rows
        .iter()
        .map(|row| format_progress_row(config, row))
        .collect::<Vec<_>>();
    if !running_rows.is_empty() {
        lines.push("sync_progress_section name=in_progress".to_string());
        lines.extend(format_running_rows(config, &running_rows));
    }
    lines
}

fn format_running_rows(config: &SyncProgressConfig, rows: &[&SyncProgressRow]) -> Vec<String> {
    let regular_rows = rows
        .iter()
        .copied()
        .filter(|row| range_parent(&row.table).is_none());
    let mut lines = running_table_summaries(config, rows)
        .into_iter()
        .map(|row| format_progress_row(config, &row))
        .collect::<Vec<_>>();
    lines.extend(regular_rows.map(|row| format_progress_row(config, row)));
    lines
}

fn running_table_summaries(
    config: &SyncProgressConfig,
    rows: &[&SyncProgressRow],
) -> Vec<SyncProgressRow> {
    let mut ranges_by_parent: BTreeMap<&str, Vec<&SyncProgressRow>> = BTreeMap::new();
    for row in rows {
        if let Some(parent) = range_parent(&row.table) {
            ranges_by_parent.entry(parent).or_default().push(row);
        }
    }
    ranges_by_parent
        .into_iter()
        .map(|(table, ranges)| aggregate_range_rows(config, table, &ranges))
        .collect()
}

fn aggregate_range_rows(
    config: &SyncProgressConfig,
    table: &str,
    ranges: &[&SyncProgressRow],
) -> SyncProgressRow {
    let rows_scanned = ranges.iter().map(|row| row.rows_scanned).sum();
    let total_rows = range_total_rows(config, ranges);
    let inserts = ranges.iter().map(|row| row.inserts).sum();
    let updates = ranges.iter().map(|row| row.updates).sum();
    let extra_target_rows = ranges.iter().map(|row| row.extra_target_rows).sum();
    let elapsed_seconds = ranges
        .iter()
        .map(|row| row.elapsed_seconds)
        .max()
        .unwrap_or(1);

    SyncProgressRow {
        run_id: String::new(),
        table: table.to_string(),
        rows_scanned,
        total_rows,
        inserts,
        updates,
        extra_target_rows,
        status: "running".to_string(),
        last_primary_key: "-".to_string(),
        elapsed_seconds,
        last_error: aggregate_error(ranges),
    }
}

fn range_total_rows(config: &SyncProgressConfig, ranges: &[&SyncProgressRow]) -> Option<u64> {
    let stored_total = ranges
        .iter()
        .map(|row| row.total_rows)
        .collect::<Option<Vec<_>>>()
        .map(|totals| totals.into_iter().sum());
    stored_total.or_else(|| {
        let parent = range_parent(&ranges.first()?.table)?;
        source_count(config, parent).ok().flatten()
    })
}

fn aggregate_error(ranges: &[&SyncProgressRow]) -> String {
    ranges
        .iter()
        .find_map(|row| (!row.last_error.is_empty()).then(|| row.last_error.clone()))
        .unwrap_or_default()
}

fn range_parent(table: &str) -> Option<&str> {
    table.rsplit_once("#range").and_then(|(parent, suffix)| {
        suffix
            .chars()
            .all(|char| char.is_ascii_digit())
            .then_some(parent)
    })
}

pub(super) fn format_progress_row(config: &SyncProgressConfig, row: &SyncProgressRow) -> String {
    let rows_per_second = rate(row.rows_scanned, row.elapsed_seconds);
    let inserts_per_second = rate(row.inserts, row.elapsed_seconds);
    let total_rows = row
        .total_rows
        .or_else(|| source_count(config, &row.table).ok().flatten());
    let remaining = total_rows.map(|total| total.saturating_sub(row.rows_scanned));
    let eta_seconds = remaining.and_then(|remaining| eta(remaining, rows_per_second));

    format!(
        "table={} status={} run_id={} rows_scanned={} total_rows={} progress={} rows_per_second={:.2} inserts_per_second={:.2} eta={} last_pk={} inserts={} updates={} extras={} error={}",
        row.table,
        row.status,
        display_run_id(&row.run_id),
        row.rows_scanned,
        display_optional_u64(total_rows),
        display_percent(row.rows_scanned, total_rows),
        rows_per_second,
        inserts_per_second,
        display_duration(eta_seconds),
        display_last_primary_key(&row.last_primary_key),
        row.inserts,
        row.updates,
        row.extra_target_rows,
        display_error(&row.last_error)
    )
}

fn display_run_id(run_id: &str) -> &str {
    if run_id.is_empty() { "-" } else { run_id }
}

pub(super) fn rate(count: u64, seconds: u64) -> f64 {
    count as f64 / seconds.max(1) as f64
}

pub(super) fn eta(remaining: u64, rows_per_second: f64) -> Option<u64> {
    if rows_per_second <= 0.0 {
        None
    } else {
        Some((remaining as f64 / rows_per_second).ceil() as u64)
    }
}

pub(super) fn display_percent(done: u64, total: Option<u64>) -> String {
    match total {
        Some(0) => "100.00%".to_string(),
        Some(total) => format!("{:.2}%", (done as f64 / total as f64) * 100.0),
        None => "-".to_string(),
    }
}

pub(super) fn display_duration(seconds: Option<u64>) -> String {
    let Some(seconds) = seconds else {
        return "-".to_string();
    };
    let minutes = seconds / 60;
    let seconds = seconds % 60;
    format!("{minutes}m{seconds:02}s")
}

fn display_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn display_last_primary_key(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn display_error(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}
