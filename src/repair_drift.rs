use crate::drift_check::{self, DriftCheckConfig, DriftComparison};
use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, SchemaInventory, TableInventory, build_inventory,
};
use crate::live::TargetMySqlConfig;
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::mysql_support::{SOURCE_TLS_CA_FILE, TARGET_TLS_CA_FILE};
use crate::table_sync::{self, SyncMode, SyncTable, SyncTableConfig};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static RUN_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct RepairDriftConfig {
    pub source: MySqlConnectionConfig,
    pub target: TargetMySqlConfig,
    pub tables: Vec<String>,
    pub parent_first: Vec<String>,
    pub content_check: bool,
    pub mode: SyncMode,
    pub chunk_size: usize,
    pub progress_table: String,
    pub max_deletes: Option<u64>,
    pub max_deletes_explicit: bool,
    pub run_id_prefix: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairDriftTableReport {
    pub table: String,
    pub run_id: String,
    pub source_count: u64,
    pub target_count: u64,
    pub sync_report: table_sync::SyncTableReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairDriftSkip {
    pub table: String,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairDriftReport {
    pub run_id: String,
    pub source_tables: usize,
    pub target_tables: usize,
    pub compared_tables: usize,
    pub drifted_tables: usize,
    pub repaired: Vec<RepairDriftTableReport>,
    pub skipped: Vec<RepairDriftSkip>,
}

#[derive(Debug)]
pub enum RepairDriftError {
    Config(String),
    Inventory(String),
    DriftCheck(String),
    Repair(String),
}

impl fmt::Display for RepairDriftError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => formatter.write_str(message),
            Self::Inventory(message) => {
                write!(formatter, "repair drift inventory failed: {message}")
            }
            Self::DriftCheck(message) => {
                write!(formatter, "repair drift count check failed: {message}")
            }
            Self::Repair(message) => {
                write!(formatter, "repair drift table repair failed: {message}")
            }
        }
    }
}

impl std::error::Error for RepairDriftError {}

pub fn run_repair_drift(config: &RepairDriftConfig) -> Result<RepairDriftReport, RepairDriftError> {
    validate_repair_drift_config(config).map_err(RepairDriftError::Config)?;
    let run_id = fresh_run_id(&config.run_id_prefix);
    let source_inventory = build_endpoint_inventory(&config.source, InventoryEndpointRole::Source)
        .map_err(|error| RepairDriftError::Inventory(error.to_string()))?;
    let target_inventory = build_target_inventory(&config.target)
        .map_err(|error| RepairDriftError::Inventory(error.to_string()))?;
    let candidate_tables =
        candidate_table_names(config, &source_inventory).map_err(RepairDriftError::Config)?;
    let ordered_tables = order_table_names(&candidate_tables, &config.parent_first)
        .map_err(RepairDriftError::Config)?;

    if ordered_tables.is_empty() {
        return Ok(RepairDriftReport {
            run_id,
            source_tables: source_inventory.tables.len(),
            target_tables: target_inventory.tables.len(),
            compared_tables: 0,
            drifted_tables: 0,
            repaired: Vec::new(),
            skipped: Vec::new(),
        });
    }

    let drift_report =
        drift_check::run_drift_check(&build_drift_check_config(config, ordered_tables.clone()))
            .map_err(|error| RepairDriftError::DriftCheck(error.to_string()))?;

    let source_by_name = source_inventory
        .tables
        .iter()
        .map(|table| (table.name.as_str(), table))
        .collect::<BTreeMap<_, _>>();
    let target_by_name = target_inventory
        .tables
        .iter()
        .map(|table| (table.name.as_str(), table))
        .collect::<BTreeMap<_, _>>();
    let mut repaired = Vec::new();
    let mut skipped = Vec::new();

    for comparison in &drift_report.comparisons {
        if comparison.matches() {
            continue;
        }
        let Some(source_count) = comparison.source_count else {
            skipped.push(RepairDriftSkip {
                table: comparison.table.clone(),
                reason: "source count unavailable".to_string(),
            });
            continue;
        };
        let Some(target_count) = comparison.target_count else {
            skipped.push(RepairDriftSkip {
                table: comparison.table.clone(),
                reason: "target table is missing from inventory".to_string(),
            });
            continue;
        };
        let Some(source_table) = source_by_name.get(comparison.table.as_str()) else {
            skipped.push(RepairDriftSkip {
                table: comparison.table.clone(),
                reason: "source table is missing from inventory".to_string(),
            });
            continue;
        };
        let Some(target_table) = target_by_name.get(comparison.table.as_str()) else {
            skipped.push(RepairDriftSkip {
                table: comparison.table.clone(),
                reason: "target table is missing from inventory".to_string(),
            });
            continue;
        };
        let Some(table) = compatible_sync_table(source_table, target_table, &mut skipped) else {
            continue;
        };

        let table_run_id = child_run_id(&run_id, &comparison.table);
        let sync_report =
            table_sync::run_sync_table(&sync_config(config, table, table_run_id.clone())).map_err(
                |error| RepairDriftError::Repair(format!("{}: {error}", comparison.table)),
            )?;
        repaired.push(RepairDriftTableReport {
            table: comparison.table.clone(),
            run_id: table_run_id,
            source_count,
            target_count,
            sync_report,
        });
    }

    Ok(RepairDriftReport {
        run_id,
        source_tables: source_inventory.tables.len(),
        target_tables: target_inventory.tables.len(),
        compared_tables: drift_report.comparisons.len(),
        drifted_tables: drifted_table_names(&drift_report.comparisons).len(),
        repaired,
        skipped,
    })
}

pub fn run_repair_drift_command(args: Vec<String>, usage: &str) {
    let config = match parse_repair_drift_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{usage}");
            std::process::exit(2);
        }
    };

    match run_repair_drift(&config) {
        Ok(report) => println!("{}", format_repair_drift_report(&report)),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn format_repair_drift_report(report: &RepairDriftReport) -> String {
    let mut lines = vec![format!(
        "repair_drift run_id={} source_tables={} target_tables={} compared_tables={} drifted_tables={} repaired_tables={} skipped_tables={}",
        report.run_id,
        report.source_tables,
        report.target_tables,
        report.compared_tables,
        report.drifted_tables,
        report.repaired.len(),
        report.skipped.len()
    )];
    lines.extend(report.repaired.iter().map(|table| {
        format!(
            "repair_drift_table table={} run_id={} source_count={} target_count={} inserts={} updates={} extra_target_rows={}",
            table.table,
            table.run_id,
            table.source_count,
            table.target_count,
            table.sync_report.inserts,
            table.sync_report.updates,
            table.sync_report.extra_target_rows
        )
    }));
    lines.extend(report.skipped.iter().map(|table| {
        format!(
            "repair_drift_skipped table={} reason={}",
            table.table,
            serde_json::to_string(&table.reason).expect("serialize skip reason")
        )
    }));
    lines.join("\n")
}

fn parse_repair_drift_config(args: Vec<String>) -> Result<RepairDriftConfig, String> {
    let mut config = default_repair_drift_config();
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value"))?;
        repair_drift_option(&mut config, flag, value)?;
        index += 2;
    }
    validate_repair_drift_config(&config).map_err(|error| error.to_string())?;
    Ok(config)
}

fn default_repair_drift_config() -> RepairDriftConfig {
    RepairDriftConfig {
        source: MySqlConnectionConfig::default(),
        target: TargetMySqlConfig::default(),
        tables: Vec::new(),
        parent_first: Vec::new(),
        content_check: true,
        mode: SyncMode::DryRun,
        chunk_size: 1000,
        progress_table: "cdc.table_sync_runs".to_string(),
        max_deletes: Some(0),
        max_deletes_explicit: false,
        run_id_prefix: "repair-drift".to_string(),
    }
}

fn repair_drift_option(
    config: &mut RepairDriftConfig,
    flag: &str,
    value: &str,
) -> Result<(), String> {
    match flag {
        "--source-host" => config.source.host = value.to_string(),
        "--source-port" => config.source.port = crate::parse_u16(flag, value)?,
        "--source-user" => config.source.user = value.to_string(),
        "--source-password-env" => config.source.password = crate::read_env_password(value)?,
        "--source-database" => config.source.database = value.to_string(),
        "--target-host" => config.target.host = value.to_string(),
        "--target-port" => config.target.port = crate::parse_u16(flag, value)?,
        "--target-user" => config.target.user = value.to_string(),
        "--target-password-env" => config.target.password = crate::read_env_password(value)?,
        "--target-database" => config.target.database = value.to_string(),
        "--table" => config.tables.push(value.to_string()),
        "--parent-first" => config.parent_first.extend(parse_csv(value)),
        "--content-check" => config.content_check = crate::parse_bool(flag, value)?,
        "--mode" => config.mode = parse_sync_mode(value)?,
        "--chunk-size" => config.chunk_size = crate::parse_usize(flag, value)?,
        "--progress-table" => config.progress_table = value.to_string(),
        "--max-deletes" => {
            config.max_deletes = Some(crate::parse_u64(flag, value)?);
            config.max_deletes_explicit = true;
        }
        "--run-id-prefix" => config.run_id_prefix = value.to_string(),
        other => return Err(format!("unknown repair-drift option: {other}")),
    }
    Ok(())
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_sync_mode(value: &str) -> Result<SyncMode, String> {
    match value {
        "dry-run" => Ok(SyncMode::DryRun),
        "apply" => Ok(SyncMode::Apply),
        _ => Err(format!("unknown mode: {value}; expected dry-run or apply")),
    }
}

fn validate_repair_drift_config(config: &RepairDriftConfig) -> Result<(), String> {
    if config.source.host.is_empty() {
        return Err("source host is required".to_string());
    }
    if config.source.user.is_empty() {
        return Err("source user is required".to_string());
    }
    if config.source.password.is_empty() {
        return Err("source password is required".to_string());
    }
    if config.source.database.is_empty() {
        return Err("source database is required".to_string());
    }
    if config.target.host.is_empty() {
        return Err("target host is required".to_string());
    }
    if config.target.user.is_empty() {
        return Err("target user is required".to_string());
    }
    if config.target.password.is_empty() {
        return Err("target password is required".to_string());
    }
    if config.target.database.is_empty() {
        return Err("target database is required".to_string());
    }
    if config.chunk_size == 0 {
        return Err("chunk size must be greater than zero".to_string());
    }
    if config.progress_table.is_empty() {
        return Err("progress table is required".to_string());
    }
    if config.run_id_prefix.is_empty() {
        return Err("run id prefix is required".to_string());
    }
    if config.mode == SyncMode::Apply
        && (!config.max_deletes_explicit || config.max_deletes.is_none())
    {
        return Err("--max-deletes is required in apply mode".to_string());
    }
    Ok(())
}

pub fn order_table_names(
    all_tables: &[String],
    parent_first: &[String],
) -> Result<Vec<String>, String> {
    let available = all_tables.iter().collect::<BTreeSet<_>>();
    let mut ordered = Vec::new();
    let mut seen = BTreeSet::new();
    for table in parent_first {
        if !available.contains(table) {
            return Err(format!(
                "parent-first table `{table}` is not in the repair inventory"
            ));
        }
        if seen.insert(table) {
            ordered.push(table.clone());
        }
    }
    let mut remaining = all_tables
        .iter()
        .filter(|table| !seen.contains(table))
        .cloned()
        .collect::<Vec<_>>();
    remaining.sort();
    ordered.extend(remaining);
    Ok(ordered)
}

pub fn drifted_table_names(comparisons: &[DriftComparison]) -> Vec<String> {
    comparisons
        .iter()
        .filter(|comparison| !comparison.matches())
        .map(|comparison| comparison.table.clone())
        .collect()
}

fn candidate_table_names(
    config: &RepairDriftConfig,
    source: &SchemaInventory,
) -> Result<Vec<String>, String> {
    let source_names = source
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<BTreeSet<_>>();
    if config.tables.is_empty() {
        return Ok(source_names.into_iter().map(str::to_string).collect());
    }

    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();
    for table in &config.tables {
        if !source_names.contains(table.as_str()) {
            return Err(format!("table `{table}` is not in the source inventory"));
        }
        if seen.insert(table.as_str()) {
            selected.push(table.clone());
        }
    }
    Ok(selected)
}

fn compatible_sync_table(
    source: &TableInventory,
    target: &TableInventory,
    skipped: &mut Vec<RepairDriftSkip>,
) -> Option<SyncTable> {
    if source.primary_key.is_empty() {
        skipped.push(RepairDriftSkip {
            table: source.name.clone(),
            reason: "source table has no primary key".to_string(),
        });
        return None;
    }
    if source.primary_key != target.primary_key {
        skipped.push(RepairDriftSkip {
            table: source.name.clone(),
            reason: "source and target primary keys differ".to_string(),
        });
        return None;
    }
    let target_columns = target
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    let columns = source
        .columns
        .iter()
        .filter(|column| column.generated.is_none())
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let missing = columns
        .iter()
        .filter(|column| !target_columns.contains(column.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        skipped.push(RepairDriftSkip {
            table: source.name.clone(),
            reason: format!(
                "target table is missing source columns: {}",
                missing.join(", ")
            ),
        });
        return None;
    }
    Some(SyncTable {
        name: source.name.clone(),
        primary_key: source.primary_key.clone(),
        columns,
    })
}

fn build_drift_check_config(config: &RepairDriftConfig, tables: Vec<String>) -> DriftCheckConfig {
    DriftCheckConfig {
        source: config.source.clone(),
        target: config.target.clone(),
        tables,
        content_check: config.content_check,
        chunk_size: config.chunk_size,
    }
}

fn sync_config(config: &RepairDriftConfig, table: SyncTable, run_id: String) -> SyncTableConfig {
    SyncTableConfig {
        source: config.source.clone(),
        target: config.target.clone(),
        table,
        chunk_size: config.chunk_size,
        mode: config.mode,
        progress_table: config.progress_table.clone(),
        run_id,
        start_after: None,
        end_at: None,
        max_deletes: config.max_deletes,
        updated_since: None,
    }
}

fn build_endpoint_inventory(
    source: &MySqlConnectionConfig,
    endpoint_role: InventoryEndpointRole,
) -> Result<SchemaInventory, crate::inventory::InventoryError> {
    let reader = crate::inventory::MariaDbInventoryReader::new(InventoryConfig {
        host: source.host.clone(),
        port: source.port,
        user: source.user.clone(),
        password: source.password.clone(),
        endpoint_role,
        use_tls: true,
        tls_ca_file: Some(SOURCE_TLS_CA_FILE.to_string()),
        ..InventoryConfig::default()
    });
    build_inventory(&source.database, &reader)
}

fn build_target_inventory(
    target: &TargetMySqlConfig,
) -> Result<SchemaInventory, crate::inventory::InventoryError> {
    let reader = crate::inventory::MariaDbInventoryReader::new(InventoryConfig {
        host: target.host.clone(),
        port: target.port,
        user: target.user.clone(),
        password: target.password.clone(),
        endpoint_role: InventoryEndpointRole::Target,
        use_tls: true,
        tls_ca_file: Some(TARGET_TLS_CA_FILE.to_string()),
        ..InventoryConfig::default()
    });
    build_inventory(&target.database, &reader)
}

fn fresh_run_id(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let sequence = RUN_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{millis}-{}-{sequence}", std::process::id())
}

fn child_run_id(run_id: &str, table: &str) -> String {
    let slug = table
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("{run_id}-{slug}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drift_check::DriftComparison;

    #[test]
    fn orders_explicit_parent_first_tables_then_remaining_tables_lexically() {
        let all = vec![
            "children".to_string(),
            "accounts".to_string(),
            "releases".to_string(),
            "authors".to_string(),
        ];

        assert_eq!(
            order_table_names(&all, &["accounts".to_string(), "authors".to_string()])
                .expect("order"),
            vec!["accounts", "authors", "children", "releases"]
        );
    }

    #[test]
    fn rejects_parent_first_table_missing_from_inventory() {
        let error = order_table_names(&["accounts".to_string()], &["missing".to_string()])
            .expect_err("missing parent");

        assert_eq!(
            error,
            "parent-first table `missing` is not in the repair inventory"
        );
    }

    #[test]
    fn selects_count_or_content_drifted_tables() {
        let comparisons = vec![
            DriftComparison {
                table: "accounts".to_string(),
                source_count: Some(10),
                target_count: Some(10),
                content: None,
            },
            DriftComparison {
                table: "children".to_string(),
                source_count: Some(10),
                target_count: Some(9),
                content: None,
            },
            DriftComparison {
                table: "releases".to_string(),
                source_count: Some(10),
                target_count: Some(10),
                content: Some(crate::drift_check::ContentDriftSummary {
                    mismatched_chunks: 1,
                    ..Default::default()
                }),
            },
            DriftComparison {
                table: "missing".to_string(),
                source_count: Some(10),
                target_count: None,
                content: None,
            },
        ];

        assert_eq!(
            drifted_table_names(&comparisons),
            vec![
                "children".to_string(),
                "releases".to_string(),
                "missing".to_string()
            ]
        );
    }

    #[test]
    fn passes_content_check_to_drift_check_config() {
        let mut config = default_repair_drift_config();
        config.content_check = false;

        let drift_config = build_drift_check_config(&config, vec!["accounts".to_string()]);

        assert!(!drift_config.content_check);
        assert_eq!(drift_config.tables, vec!["accounts"]);
    }

    #[test]
    fn apply_requires_explicit_max_deletes() {
        let mut config = default_repair_drift_config();
        config.source.host = "source".to_string();
        config.source.user = "user".to_string();
        config.source.password = "password".to_string();
        config.source.database = "database".to_string();
        config.target.host = "target".to_string();
        config.target.user = "user".to_string();
        config.target.password = "password".to_string();
        config.target.database = "database".to_string();
        config.mode = SyncMode::Apply;
        config.max_deletes = Some(0);
        config.max_deletes_explicit = false;

        let error = validate_repair_drift_config(&config).expect_err("max deletes");
        assert_eq!(error, "--max-deletes is required in apply mode");
    }

    #[test]
    fn parses_repeated_tables_parent_first_prefix_and_content_check() {
        let mut config = default_repair_drift_config();
        assert!(config.content_check);
        repair_drift_option(&mut config, "--table", "children").expect("table");
        repair_drift_option(&mut config, "--table", "accounts").expect("table");
        repair_drift_option(&mut config, "--parent-first", "accounts,authors").expect("order");
        repair_drift_option(&mut config, "--content-check", "false").expect("content check");
        repair_drift_option(&mut config, "--max-deletes", "7").expect("deletes");

        assert_eq!(config.tables, vec!["children", "accounts"]);
        assert_eq!(config.parent_first, vec!["accounts", "authors"]);
        assert!(!config.content_check);
        assert_eq!(config.max_deletes, Some(7));
        assert!(config.max_deletes_explicit);
    }

    #[test]
    fn fresh_run_ids_are_unique_within_one_process() {
        let first = fresh_run_id("repair-drift");
        let second = fresh_run_id("repair-drift");

        assert_ne!(first, second);
        assert!(first.starts_with("repair-drift-"));
        assert!(second.starts_with("repair-drift-"));
    }
}
