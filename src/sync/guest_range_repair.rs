mod mysql_backend;

use crate::database_row::DatabaseRow;
use crate::live::TargetMySqlConfig;
use crate::mysql_config::MySqlConnectionConfig;

const MAX_BATCH_SIZE: usize = 1000;

#[derive(Clone, Debug)]
pub(crate) struct GuestRangeRepairConfig {
    pub(crate) source: MySqlConnectionConfig,
    pub(crate) target: TargetMySqlConfig,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) expected_rows: u64,
    pub(crate) batch_size: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct GuestRangeRepairReport {
    pub(crate) scanned: u64,
    pub(crate) inserted: u64,
    pub(crate) unchanged: u64,
    pub(crate) batches: u64,
}

pub(crate) fn run_guest_range_repair_command(args: Vec<String>, usage: &str) {
    let config = match GuestRangeRepairConfig::from_args(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{usage}");
            std::process::exit(2);
        }
    };
    match run_guest_range_repair(&config) {
        Ok(report) => println!(
            "guest_range_repair scanned={} inserted={} unchanged={} batches={}",
            report.scanned, report.inserted, report.unchanged, report.batches
        ),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

impl GuestRangeRepairConfig {
    pub(crate) fn from_args(args: Vec<String>) -> Result<Self, String> {
        let mut source = MySqlConnectionConfig::default();
        let mut target = TargetMySqlConfig::default();
        let mut options = std::collections::BTreeMap::new();
        let mut pairs = args.chunks_exact(2);
        for pair in &mut pairs {
            let (flag, value) = (pair[0].as_str(), pair[1].as_str());
            if crate::sync_cli::apply_source_option(&mut source, flag, value)?
                || crate::sync_cli::apply_target_option(&mut target, flag, value)?
            {
                continue;
            }
            if !matches!(
                flag,
                "--start-guest-id" | "--end-guest-id" | "--expected-rows" | "--batch-size"
            ) {
                return Err(format!("unknown repair-guest-range option: {flag}"));
            }
            let number = value
                .parse::<u64>()
                .map_err(|_| format!("invalid {flag}: {value}"))?;
            if options.insert(flag, number).is_some() {
                return Err(format!("{flag} may be specified only once"));
            }
        }
        if let Some(flag) = pairs.remainder().first() {
            return Err(format!("{flag} needs a value"));
        }
        let required = |flag| {
            options
                .get(flag)
                .copied()
                .ok_or_else(|| format!("{flag} is required"))
        };
        let config = Self {
            source,
            target,
            start: required("--start-guest-id")?,
            end: required("--end-guest-id")?,
            expected_rows: required("--expected-rows")?,
            batch_size: usize::try_from(options.get("--batch-size").copied().unwrap_or(100))
                .map_err(|_| "batch size overflow")?,
        };
        validate_config(&config)?;
        Ok(config)
    }
}

fn validate_bounds(config: &GuestRangeRepairConfig) -> Result<(), String> {
    let count = config
        .end
        .checked_sub(config.start)
        .and_then(|n| n.checked_add(1));
    if count != Some(config.expected_rows) || config.expected_rows == 0 {
        return Err("expected rows must equal the nonempty inclusive guest range width".into());
    }
    if config.batch_size == 0 || config.batch_size > MAX_BATCH_SIZE {
        return Err(format!("batch size must be between 1 and {MAX_BATCH_SIZE}"));
    }
    Ok(())
}

fn validate_config(config: &GuestRangeRepairConfig) -> Result<(), String> {
    validate_bounds(config)?;
    for (name, value) in [
        ("source host", &config.source.host),
        ("source user", &config.source.user),
        ("source password", &config.source.password),
        ("source database", &config.source.database),
        ("target host", &config.target.host),
        ("target user", &config.target.user),
        ("target password", &config.target.password),
        ("target database", &config.target.database),
        ("target TLS CA file", &config.target.tls_ca_file),
    ] {
        if value.trim().is_empty() {
            return Err(format!("{name} is required"));
        }
    }
    Ok(())
}

pub(crate) fn run_guest_range_repair(
    config: &GuestRangeRepairConfig,
) -> Result<GuestRangeRepairReport, String> {
    validate_config(config)?;
    let mut backend = mysql_backend::MySqlGuestRangeBackend::connect(config)?;
    repair_with_backend(config, &mut backend)
}

trait GuestRangeBackend {
    fn preflight(&mut self, config: &GuestRangeRepairConfig) -> Result<(u64, u64, u64), String>;
    fn read_page(
        &mut self,
        config: &GuestRangeRepairConfig,
        after: Option<u64>,
    ) -> Result<Vec<DatabaseRow>, String>;
    fn begin_batch(&mut self) -> Result<(), String>;
    fn require_parent(&mut self, row: &DatabaseRow) -> Result<(), String>;
    fn read_target(&mut self, row: &DatabaseRow) -> Result<Option<DatabaseRow>, String>;
    fn insert(&mut self, row: &DatabaseRow) -> Result<(), String>;
    fn commit(&mut self) -> Result<(), String>;
    fn rollback(&mut self) -> Result<(), String>;
    fn finish_source(&mut self) -> Result<(), String>;
}

fn repair_with_backend(
    config: &GuestRangeRepairConfig,
    backend: &mut impl GuestRangeBackend,
) -> Result<GuestRangeRepairReport, String> {
    validate_bounds(config)?;
    let result = repair_range(config, backend);
    combine_cleanup(result, backend.finish_source(), "close source snapshot")
}

fn combine_cleanup<T>(
    result: Result<T, String>,
    cleanup: Result<(), String>,
    operation: &str,
) -> Result<T, String> {
    match (result, cleanup) {
        (result, Ok(())) => result,
        (Ok(_), Err(error)) => Err(format!("{operation}: {error}")),
        (Err(error), Err(cleanup)) => Err(format!("{error}; {operation}: {cleanup}")),
    }
}

fn repair_range(
    config: &GuestRangeRepairConfig,
    backend: &mut impl GuestRangeBackend,
) -> Result<GuestRangeRepairReport, String> {
    let actual = backend.preflight(config)?;
    if actual != (config.expected_rows, config.start, config.end) {
        return Err(format!(
            "source range mismatch: expected ({}, {}, {}), found {actual:?}",
            config.expected_rows, config.start, config.end
        ));
    }
    let mut report = GuestRangeRepairReport::default();
    let mut after = None;
    while report.scanned < config.expected_rows {
        let rows = backend.read_page(config, after)?;
        let last = validate_page(config, after, &rows)?;
        let batch = repair_batch(backend, &rows)?;
        report.scanned += rows.len() as u64;
        report.inserted += batch.inserted;
        report.unchanged += batch.unchanged;
        report.batches += 1;
        after = Some(last);
    }
    if report.scanned != config.expected_rows || after != Some(config.end) {
        return Err("source range coverage differs from expected rows".into());
    }
    Ok(report)
}

fn guest_id(row: &DatabaseRow) -> Result<u64, String> {
    let [id] = row.primary_key.as_slice() else {
        return Err("guest row requires one primary key".into());
    };
    if row.values.get("guest_id") != Some(&Some(id.clone())) {
        return Err("guest_id differs from row primary key".into());
    }
    id.parse()
        .map_err(|_| "guest_id is not an unsigned integer".into())
}

fn validate_page(
    config: &GuestRangeRepairConfig,
    after: Option<u64>,
    rows: &[DatabaseRow],
) -> Result<u64, String> {
    if rows.is_empty() || rows.len() > config.batch_size {
        return Err("source page is empty or exceeds batch size".into());
    }
    let mut expected = after.map_or(Some(config.start), |id| id.checked_add(1));
    let mut last = config.start;
    for row in rows {
        last = guest_id(row)?;
        if Some(last) != expected || last > config.end {
            return Err("source page is not the exact contiguous guest range".into());
        }
        expected = last.checked_add(1);
    }
    Ok(last)
}

fn repair_batch(
    backend: &mut impl GuestRangeBackend,
    rows: &[DatabaseRow],
) -> Result<GuestRangeRepairReport, String> {
    let result = backend
        .begin_batch()
        .and_then(|()| insert_batch(backend, rows))
        .and_then(|report| backend.commit().map(|()| report));
    match result {
        Ok(report) => Ok(report),
        Err(error) => combine_cleanup(Err(error), backend.rollback(), "rollback target batch"),
    }
}

fn insert_batch(
    backend: &mut impl GuestRangeBackend,
    rows: &[DatabaseRow],
) -> Result<GuestRangeRepairReport, String> {
    let mut report = GuestRangeRepairReport::default();
    for row in rows {
        backend.require_parent(row)?;
        match backend.read_target(row)? {
            Some(existing) if existing == *row => report.unchanged += 1,
            Some(_) => {
                return Err(format!(
                    "target guest {:?} differs from source snapshot",
                    row.primary_key
                ));
            }
            None => {
                backend.insert(row)?;
                report.inserted += 1;
            }
        }
    }
    for row in rows {
        if backend.read_target(row)?.as_ref() != Some(row) {
            return Err(format!(
                "target readback differs for guest {:?}",
                row.primary_key
            ));
        }
    }
    Ok(report)
}

#[cfg(test)]
#[path = "guest_range_repair/tests.rs"]
mod tests;
