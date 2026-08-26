mod mysql_backend;

use super::model::SyncTable;
use crate::database_row::DatabaseRow;
use crate::live::TargetMySqlConfig;
use crate::mysql_config::MySqlConnectionConfig;
#[cfg(test)]
use std::collections::BTreeMap;

const DEFAULT_BATCH_SIZE: usize = 50;
const MAX_BATCH_SIZE: usize = 100;
const DEFAULT_LIMIT: usize = 1000;
const MAX_LIMIT: usize = 1000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FkOrphanRepairCase {
    ArtistsFavorites,
    Comics,
    PhrasesSuggestions,
    ForumsReplies,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RepairCaseSpec {
    pub(super) name: &'static str,
    pub(super) constraint_name: &'static str,
    pub(super) child_table: &'static str,
    pub(super) child_primary_key: &'static [&'static str],
    pub(super) child_foreign_key: &'static [&'static str],
    pub(super) parent_table: &'static str,
    pub(super) parent_primary_key: &'static [&'static str],
    pub(super) parent_key: &'static [&'static str],
    pub(super) update_rule: &'static str,
    pub(super) delete_rule: &'static str,
}

impl FkOrphanRepairCase {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "artists-favorites" => Ok(Self::ArtistsFavorites),
            "comics" => Ok(Self::Comics),
            "phrases-suggestions" => Ok(Self::PhrasesSuggestions),
            "forums-replies" => Ok(Self::ForumsReplies),
            _ => Err(format!("unknown FK orphan repair case: {value}")),
        }
    }

    pub(super) fn spec(self) -> RepairCaseSpec {
        match self {
            Self::ArtistsFavorites => RepairCaseSpec {
                name: "artists-favorites",
                constraint_name: "artists_favorites_ibfk_2",
                child_table: "artists_favorites",
                child_primary_key: &["id"],
                child_foreign_key: &["user_id", "user_username"],
                parent_table: "users",
                parent_primary_key: &["id"],
                parent_key: &["id", "name"],
                update_rule: "CASCADE",
                delete_rule: "RESTRICT",
            },
            Self::Comics => RepairCaseSpec {
                name: "comics",
                constraint_name: "comics_ibfk_5",
                child_table: "comics",
                child_primary_key: &["id"],
                child_foreign_key: &["artist_id", "artist_name"],
                parent_table: "artists",
                parent_primary_key: &["id"],
                parent_key: &["id", "name"],
                update_rule: "CASCADE",
                delete_rule: "RESTRICT",
            },
            Self::PhrasesSuggestions => RepairCaseSpec {
                name: "phrases-suggestions",
                constraint_name: "phrases_suggestions_ibfk_1",
                child_table: "phrases_suggestions",
                child_primary_key: &["id", "lang", "author_id"],
                child_foreign_key: &["author_id", "author_username"],
                parent_table: "users",
                parent_primary_key: &["id"],
                parent_key: &["id", "name"],
                update_rule: "CASCADE",
                delete_rule: "CASCADE",
            },
            Self::ForumsReplies => RepairCaseSpec {
                name: "forums-replies",
                constraint_name: "forums_replies_ibfk_2",
                child_table: "forums_replies",
                child_primary_key: &["id"],
                child_foreign_key: &["author_id", "author_username"],
                parent_table: "users",
                parent_primary_key: &["id"],
                parent_key: &["id", "name"],
                update_rule: "CASCADE",
                delete_rule: "RESTRICT",
            },
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FkOrphanRepairConfig {
    pub(crate) source: MySqlConnectionConfig,
    pub(crate) target: TargetMySqlConfig,
    pub(crate) case: FkOrphanRepairCase,
    pub(crate) expected_orphans: usize,
    pub(crate) batch_size: usize,
    pub(crate) limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RepairMetadata {
    pub(super) source_child: SyncTable,
    pub(super) target_child: SyncTable,
    pub(super) source_parent: SyncTable,
    pub(super) target_parent: SyncTable,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct FkOrphanRepairReport {
    pub(crate) planned: usize,
    pub(crate) examined: usize,
    pub(crate) updated: usize,
    pub(crate) deleted: usize,
    pub(crate) unchanged: usize,
    pub(crate) skipped: usize,
    pub(crate) remaining: usize,
}

pub(super) trait FkOrphanRepairBackend {
    fn validate_case(&mut self, spec: &RepairCaseSpec) -> Result<RepairMetadata, String>;
    fn orphan_keys(
        &mut self,
        spec: &RepairCaseSpec,
        metadata: &RepairMetadata,
        limit: usize,
    ) -> Result<Vec<Vec<String>>, String>;
    fn begin_batch(
        &mut self,
        spec: &RepairCaseSpec,
        metadata: &RepairMetadata,
    ) -> Result<(), String>;
    fn is_target_orphan(
        &mut self,
        spec: &RepairCaseSpec,
        metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<bool, String>;
    fn read_source_child(
        &mut self,
        metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<Option<DatabaseRow>, String>;
    fn read_source_parent(
        &mut self,
        spec: &RepairCaseSpec,
        metadata: &RepairMetadata,
        child: &DatabaseRow,
    ) -> Result<Option<DatabaseRow>, String>;
    fn read_target_child(
        &mut self,
        metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<Option<DatabaseRow>, String>;
    fn read_target_parent(
        &mut self,
        spec: &RepairCaseSpec,
        metadata: &RepairMetadata,
        child: &DatabaseRow,
    ) -> Result<Option<DatabaseRow>, String>;
    fn update_target_child(
        &mut self,
        metadata: &RepairMetadata,
        child: &DatabaseRow,
    ) -> Result<(), String>;
    fn delete_target_child(
        &mut self,
        metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<(), String>;
    fn commit_batch(&mut self) -> Result<(), String>;
    fn rollback_batch(&mut self) -> Result<(), String>;
    fn unlock_batch(&mut self) -> Result<(), String>;
}

pub(crate) fn run_fk_orphan_repair_command(args: Vec<String>, usage: &str) {
    let config = match parse_fk_orphan_repair_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{usage}");
            std::process::exit(2);
        }
    };

    match mysql_backend::run_mysql_fk_orphan_repair(&config) {
        Ok(report) => println!("{}", format_report(config.case.spec(), &report)),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

pub(crate) fn parse_fk_orphan_repair_config(
    args: Vec<String>,
) -> Result<FkOrphanRepairConfig, String> {
    let mut builder = FkOrphanRepairConfigBuilder::default();
    let mut pairs = args.chunks_exact(2);
    for pair in &mut pairs {
        builder.apply(&pair[0], &pair[1])?;
    }
    if let Some(flag) = pairs.remainder().first() {
        return Err(format!("{flag} needs a value"));
    }
    builder.finish()
}

struct FkOrphanRepairConfigBuilder {
    source: MySqlConnectionConfig,
    target: TargetMySqlConfig,
    case: Option<FkOrphanRepairCase>,
    expected_orphans: Option<usize>,
    batch_size: usize,
    limit: usize,
}

impl Default for FkOrphanRepairConfigBuilder {
    fn default() -> Self {
        Self {
            source: MySqlConnectionConfig::default(),
            target: TargetMySqlConfig::default(),
            case: None,
            expected_orphans: None,
            batch_size: DEFAULT_BATCH_SIZE,
            limit: DEFAULT_LIMIT,
        }
    }
}

impl FkOrphanRepairConfigBuilder {
    fn apply(&mut self, flag: &str, value: &str) -> Result<(), String> {
        if crate::sync_cli::apply_source_option(&mut self.source, flag, value)? {
            return Ok(());
        }
        if crate::sync_cli::apply_target_option(&mut self.target, flag, value)? {
            return Ok(());
        }
        match flag {
            "--case" => set_once(&mut self.case, FkOrphanRepairCase::parse(value)?, flag),
            "--expected-orphans" => set_once(
                &mut self.expected_orphans,
                crate::parse_usize(flag, value)?,
                flag,
            ),
            "--batch-size" => {
                self.batch_size = crate::parse_usize(flag, value)?;
                Ok(())
            }
            "--limit" => {
                self.limit = crate::parse_usize(flag, value)?;
                Ok(())
            }
            _ => Err(format!("unknown repair-fk-orphans option: {flag}")),
        }
    }

    fn finish(self) -> Result<FkOrphanRepairConfig, String> {
        let config = FkOrphanRepairConfig {
            source: self.source,
            target: self.target,
            case: self.case.ok_or_else(|| "--case is required".to_string())?,
            expected_orphans: self
                .expected_orphans
                .ok_or_else(|| "--expected-orphans is required".to_string())?,
            batch_size: self.batch_size,
            limit: self.limit,
        };
        validate_fk_orphan_repair_config(&config)?;
        Ok(config)
    }
}

fn set_once<T>(slot: &mut Option<T>, value: T, flag: &str) -> Result<(), String> {
    if slot.is_some() {
        return Err(format!("{flag} may be specified only once"));
    }
    *slot = Some(value);
    Ok(())
}

fn validate_fk_orphan_repair_config(config: &FkOrphanRepairConfig) -> Result<(), String> {
    require_nonempty(&config.source.host, "source host is required")?;
    require_nonempty(&config.source.user, "source user is required")?;
    require_nonempty(&config.source.password, "source password is required")?;
    require_nonempty(&config.source.database, "source database is required")?;
    require_nonempty(&config.target.host, "target host is required")?;
    require_nonempty(&config.target.user, "target user is required")?;
    require_nonempty(&config.target.password, "target password is required")?;
    require_nonempty(&config.target.database, "target database is required")?;
    require_nonempty(&config.target.tls_ca_file, "target TLS CA file is required")?;
    if config.batch_size == 0 || config.batch_size > MAX_BATCH_SIZE {
        return Err(format!("batch size must be between 1 and {MAX_BATCH_SIZE}"));
    }
    if config.limit == 0 || config.limit > MAX_LIMIT {
        return Err(format!("limit must be between 1 and {MAX_LIMIT}"));
    }
    if config.expected_orphans > config.limit {
        return Err(format!(
            "expected orphan count {} exceeds limit {}",
            config.expected_orphans, config.limit
        ));
    }
    Ok(())
}

fn require_nonempty(value: &str, message: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(message.to_string())
    } else {
        Ok(())
    }
}

pub(super) fn repair_with_backend(
    config: &FkOrphanRepairConfig,
    backend: &mut impl FkOrphanRepairBackend,
) -> Result<FkOrphanRepairReport, String> {
    let spec = config.case.spec();
    let metadata = backend.validate_case(&spec)?;
    let planned_keys = backend.orphan_keys(&spec, &metadata, config.limit)?;
    if planned_keys.len() != config.expected_orphans {
        return Err(format!(
            "FK orphan count changed for `{}`: expected {}, found {}",
            spec.name,
            config.expected_orphans,
            planned_keys.len()
        ));
    }

    for primary_key in &planned_keys {
        println!(
            "fk_orphan_repair_candidate case={} primary_key={}",
            spec.name,
            serde_json::to_string(primary_key)
                .map_err(|error| format!("encode repair primary key: {error}"))?
        );
    }

    let mut report = FkOrphanRepairReport {
        planned: planned_keys.len(),
        ..FkOrphanRepairReport::default()
    };
    for batch in planned_keys.chunks(config.batch_size) {
        if let Err(error) = backend.begin_batch(&spec, &metadata) {
            return Err(batch_cleanup_error(error, backend));
        }
        let batch_result = repair_batch(&spec, &metadata, batch, backend, &mut report);
        match batch_result {
            Ok(()) => {
                if let Err(error) = backend.commit_batch() {
                    return Err(batch_cleanup_error(error, backend));
                }
                backend.unlock_batch().map_err(|error| {
                    format!(
                        "unlock committed FK orphan repair batch: {error}; {}",
                        format_report(spec, &report)
                    )
                })?;
            }
            Err(error) => return Err(batch_cleanup_error(error, backend)),
        }
    }

    let remaining_keys = backend.orphan_keys(&spec, &metadata, config.limit)?;
    report.remaining = remaining_keys.len();
    if !remaining_keys.is_empty() {
        return Err(format!(
            "FK orphan identities remain or grew for `{}`: {}; {}; remaining_primary_keys={}",
            spec.name,
            remaining_keys.len(),
            format_report(spec, &report),
            serde_json::to_string(&remaining_keys)
                .map_err(|error| format!("encode remaining repair primary keys: {error}"))?
        ));
    }
    Ok(report)
}

fn repair_batch(
    spec: &RepairCaseSpec,
    metadata: &RepairMetadata,
    primary_keys: &[Vec<String>],
    backend: &mut impl FkOrphanRepairBackend,
    report: &mut FkOrphanRepairReport,
) -> Result<(), String> {
    for primary_key in primary_keys {
        report.examined += 1;
        if !backend.is_target_orphan(spec, metadata, primary_key)? {
            report.skipped += 1;
            continue;
        }
        let target_before = backend
            .read_target_child(metadata, primary_key)?
            .ok_or_else(|| format!("target orphan disappeared for primary key {primary_key:?}"))?;
        let source_before = backend.read_source_child(metadata, primary_key)?;
        match source_before {
            None => repair_source_absent(metadata, primary_key, backend, report)?,
            Some(source_child) => repair_source_present(
                spec,
                metadata,
                primary_key,
                target_before,
                source_child,
                backend,
                report,
            )?,
        }
    }
    Ok(())
}

fn repair_source_absent(
    metadata: &RepairMetadata,
    primary_key: &[String],
    backend: &mut impl FkOrphanRepairBackend,
    report: &mut FkOrphanRepairReport,
) -> Result<(), String> {
    backend.delete_target_child(metadata, primary_key)?;
    if backend.read_source_child(metadata, primary_key)?.is_some() {
        return Err(format!(
            "source child appeared during repair for primary key {primary_key:?}"
        ));
    }
    if backend.read_target_child(metadata, primary_key)?.is_some() {
        return Err(format!(
            "target child remains after delete for primary key {primary_key:?}"
        ));
    }
    report.deleted += 1;
    Ok(())
}

fn repair_source_present(
    spec: &RepairCaseSpec,
    metadata: &RepairMetadata,
    primary_key: &[String],
    target_before: DatabaseRow,
    source_child: DatabaseRow,
    backend: &mut impl FkOrphanRepairBackend,
    report: &mut FkOrphanRepairReport,
) -> Result<(), String> {
    let source_parent =
        validate_repair_parents(spec, metadata, primary_key, &source_child, backend)?;
    apply_source_child(metadata, target_before, &source_child, backend, report)?;
    verify_repaired_relationship(
        spec,
        metadata,
        primary_key,
        &source_child,
        &source_parent,
        backend,
    )
}

fn validate_repair_parents(
    spec: &RepairCaseSpec,
    metadata: &RepairMetadata,
    primary_key: &[String],
    source_child: &DatabaseRow,
    backend: &mut impl FkOrphanRepairBackend,
) -> Result<DatabaseRow, String> {
    let source_parent = backend
        .read_source_parent(spec, metadata, source_child)?
        .ok_or_else(|| format!("source parent is missing for primary key {primary_key:?}"))?;
    validate_child_parent_relationship(spec, source_child, &source_parent, "source")?;
    let target_parent = backend
        .read_target_parent(spec, metadata, source_child)?
        .ok_or_else(|| format!("target parent is missing for primary key {primary_key:?}"))?;
    validate_parent_identity(spec, &source_parent, &target_parent, primary_key)?;
    Ok(source_parent)
}

fn apply_source_child(
    metadata: &RepairMetadata,
    target_before: DatabaseRow,
    source_child: &DatabaseRow,
    backend: &mut impl FkOrphanRepairBackend,
    report: &mut FkOrphanRepairReport,
) -> Result<(), String> {
    if target_before == *source_child {
        report.unchanged += 1;
        return Ok(());
    }
    backend.update_target_child(metadata, source_child)?;
    report.updated += 1;
    Ok(())
}

fn verify_repaired_relationship(
    spec: &RepairCaseSpec,
    metadata: &RepairMetadata,
    primary_key: &[String],
    source_child: &DatabaseRow,
    source_parent: &DatabaseRow,
    backend: &mut impl FkOrphanRepairBackend,
) -> Result<(), String> {
    let source_after = backend.read_source_child(metadata, primary_key)?;
    if source_after.as_ref() != Some(source_child) {
        return Err(format!(
            "source child changed during repair for primary key {primary_key:?}"
        ));
    }
    let source_parent_after = backend
        .read_source_parent(spec, metadata, source_child)?
        .ok_or_else(|| format!("source parent disappeared for primary key {primary_key:?}"))?;
    if source_parent_after != *source_parent {
        return Err(format!(
            "source parent changed during repair for primary key {primary_key:?}"
        ));
    }
    let target_after = backend.read_target_child(metadata, primary_key)?;
    if target_after.as_ref() != Some(source_child) {
        return Err(format!(
            "target verification failed for primary key {primary_key:?}"
        ));
    }
    let target_parent_after = backend
        .read_target_parent(spec, metadata, source_child)?
        .ok_or_else(|| format!("target parent disappeared for primary key {primary_key:?}"))?;
    validate_parent_identity(spec, source_parent, &target_parent_after, primary_key)?;
    validate_child_parent_relationship(spec, source_child, &target_parent_after, "target")
}

fn validate_child_parent_relationship(
    spec: &RepairCaseSpec,
    child: &DatabaseRow,
    parent: &DatabaseRow,
    endpoint: &str,
) -> Result<(), String> {
    for (child_column, parent_column) in spec.child_foreign_key.iter().zip(spec.parent_key.iter()) {
        let child_value = required_row_value(child, child_column, endpoint)?;
        let parent_value = required_row_value(parent, parent_column, endpoint)?;
        if child_value != parent_value {
            return Err(format!(
                "{endpoint} FK relationship is invalid: child `{child_column}` does not match parent `{parent_column}`"
            ));
        }
    }
    Ok(())
}

fn validate_parent_identity(
    spec: &RepairCaseSpec,
    source: &DatabaseRow,
    target: &DatabaseRow,
    child_primary_key: &[String],
) -> Result<(), String> {
    for column in spec.parent_key {
        let source_value = required_row_value(source, column, "source parent")?;
        let target_value = required_row_value(target, column, "target parent")?;
        if source_value != target_value {
            return Err(format!(
                "target parent differs from source for child primary key {child_primary_key:?} column `{column}`"
            ));
        }
    }
    Ok(())
}

pub(super) fn required_row_value<'a>(
    row: &'a DatabaseRow,
    column: &str,
    endpoint: &str,
) -> Result<&'a str, String> {
    row.values
        .get(column)
        .ok_or_else(|| format!("{endpoint} row is missing column `{column}`"))?
        .as_deref()
        .ok_or_else(|| format!("{endpoint} row column `{column}` is NULL"))
}

fn batch_cleanup_error(primary_error: String, backend: &mut impl FkOrphanRepairBackend) -> String {
    let mut error = primary_error;
    if let Err(rollback_error) = backend.rollback_batch() {
        error.push_str(&format!(
            "; additionally rollback repair batch: {rollback_error}"
        ));
    }
    if let Err(unlock_error) = backend.unlock_batch() {
        error.push_str(&format!(
            "; additionally unlock repair batch: {unlock_error}"
        ));
    }
    error
}

fn format_report(spec: RepairCaseSpec, report: &FkOrphanRepairReport) -> String {
    format!(
        "fk_orphan_repair case={} planned={} examined={} updated={} deleted={} unchanged={} skipped={} remaining={}",
        spec.name,
        report.planned,
        report.examined,
        report.updated,
        report.deleted,
        report.unchanged,
        report.skipped,
        report.remaining
    )
}

#[cfg(test)]
mod tests;
