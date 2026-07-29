use crate::conflict_repair::{ConflictStore, MySqlConflictStore};
use crate::live::TargetMySqlConfig;
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::snapshot::SnapshotRow;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, Row, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

const TABLE: &str = "comics_releases_views";
const UTM_COLUMN: &str = "utm_id";
type CanonicalRowsByKey = BTreeMap<Vec<String>, BTreeMap<String, Option<String>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvidenceSide {
    Source,
    Target,
}

pub trait ConflictEvidenceReader {
    fn unresolved_primary_keys(&mut self) -> Result<Vec<Vec<String>>, String>;
    fn read_child_rows(
        &mut self,
        side: EvidenceSide,
        primary_keys: &[Vec<String>],
    ) -> Result<Vec<SnapshotRow>, String>;
    fn read_parent_rows(
        &mut self,
        side: EvidenceSide,
        primary_keys: &[Vec<String>],
    ) -> Result<Vec<SnapshotRow>, String>;
}

pub trait ConflictResolutionWriter {
    fn resolve_if_equal(
        &mut self,
        table: &str,
        primary_key: &[String],
        rows_equal: bool,
        repair_run_id: &str,
        evidence: &str,
    ) -> Result<(), String>;
}

impl<T: ConflictStore> ConflictResolutionWriter for T {
    fn resolve_if_equal(
        &mut self,
        table: &str,
        primary_key: &[String],
        rows_equal: bool,
        repair_run_id: &str,
        evidence: &str,
    ) -> Result<(), String> {
        ConflictStore::resolve_if_equal(
            self,
            table,
            primary_key,
            rows_equal,
            repair_run_id,
            evidence,
        )
    }
}

#[derive(Clone, Debug)]
pub struct TargetedConflictResolutionConfig {
    pub source: MySqlConnectionConfig,
    pub target: TargetMySqlConfig,
    pub source_identity: String,
    pub run_id: String,
    pub batch_size: usize,
}

pub fn parse_targeted_conflict_resolution_config(
    args: Vec<String>,
) -> Result<TargetedConflictResolutionConfig, String> {
    let mut config = TargetedConflictResolutionConfig {
        source: MySqlConnectionConfig::default(),
        target: TargetMySqlConfig::default(),
        source_identity: String::new(),
        run_id: String::new(),
        batch_size: 1000,
    };
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value"))?;
        apply_config_option(&mut config, flag, value)?;
        index += 2;
    }
    validate_config(&config)?;
    Ok(config)
}

fn apply_config_option(
    config: &mut TargetedConflictResolutionConfig,
    flag: &str,
    value: &str,
) -> Result<(), String> {
    if apply_source_option(config, flag, value)?
        || apply_target_option(config, flag, value)?
        || apply_run_option(config, flag, value)?
    {
        return Ok(());
    }
    Err(format!(
        "unknown targeted conflict resolution option: {flag}"
    ))
}

fn apply_source_option(
    config: &mut TargetedConflictResolutionConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--source-host" => config.source.host = value.to_string(),
        "--source-port" => config.source.port = crate::parse_u16(flag, value)?,
        "--source-user" => config.source.user = value.to_string(),
        "--source-password-env" => config.source.password = crate::read_env_password(value)?,
        "--source-database" => config.source.database = value.to_string(),
        "--source-identity" => config.source_identity = value.to_string(),
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_target_option(
    config: &mut TargetedConflictResolutionConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--target-host" => config.target.host = value.to_string(),
        "--target-port" => config.target.port = crate::parse_u16(flag, value)?,
        "--target-user" => config.target.user = value.to_string(),
        "--target-password-env" => config.target.password = crate::read_env_password(value)?,
        "--target-database" => config.target.database = value.to_string(),
        "--target-tls-ca-file" => config.target.tls_ca_file = value.to_string(),
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_run_option(
    config: &mut TargetedConflictResolutionConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--run-id" => config.run_id = value.to_string(),
        "--batch-size" => config.batch_size = crate::parse_usize(flag, value)?,
        _ => return Ok(false),
    }
    Ok(true)
}

fn validate_config(config: &TargetedConflictResolutionConfig) -> Result<(), String> {
    for (name, value) in [
        ("source host", config.source.host.as_str()),
        ("source user", config.source.user.as_str()),
        ("source database", config.source.database.as_str()),
        ("source identity", config.source_identity.as_str()),
        ("target host", config.target.host.as_str()),
        ("target user", config.target.user.as_str()),
        ("target database", config.target.database.as_str()),
        ("run ID", config.run_id.as_str()),
    ] {
        if value.is_empty() {
            return Err(format!("{name} is required"));
        }
    }
    if config.batch_size == 0 {
        return Err("targeted conflict batch size must be positive".to_string());
    }
    crate::mysql_support::validate_target_tls_ca_file(&config.target)
}

pub fn run_targeted_conflict_resolution_command(args: Vec<String>, usage: &str) {
    let config = match parse_targeted_conflict_resolution_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{usage}");
            std::process::exit(2);
        }
    };
    match run_mysql_targeted_conflict_resolution(&config) {
        Ok((report, elapsed_ms)) => println!(
            "targeted_conflict_resolution table={TABLE} examined={} resolved={} elapsed_ms={elapsed_ms}",
            report.examined, report.resolved
        ),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

pub fn run_mysql_targeted_conflict_resolution(
    config: &TargetedConflictResolutionConfig,
) -> Result<(TargetedConflictResolutionReport, u128), String> {
    let started = Instant::now();
    let mut reader = MySqlConflictEvidenceReader::new(config)?;
    let mut store = MySqlConflictStore::new(&config.target, "cdc.row_conflicts")?;
    store.ensure()?;
    let report = resolve_comics_releases_views_conflicts(
        &mut reader,
        &mut store,
        &config.run_id,
        config.batch_size,
    )?;
    Ok((report, started.elapsed().as_millis()))
}

struct MySqlConflictEvidenceReader {
    source: Conn,
    target: Conn,
    source_identity: String,
}

impl MySqlConflictEvidenceReader {
    fn new(config: &TargetedConflictResolutionConfig) -> Result<Self, String> {
        Ok(Self {
            source: Conn::new(source_opts(&config.source))
                .map_err(|error| format!("source evidence connection failed: {error}"))?,
            target: Conn::new(crate::mysql_support::target_mysql_opts(&config.target)?)
                .map_err(|error| format!("target evidence connection failed: {error}"))?,
            source_identity: config.source_identity.clone(),
        })
    }

    fn query_rows(
        &mut self,
        side: EvidenceSide,
        table: &str,
        primary_key: &str,
        primary_keys: &[Vec<String>],
    ) -> Result<Vec<SnapshotRow>, String> {
        let ids = primary_keys
            .iter()
            .map(|key| key[0].as_str())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT * FROM `{table}` WHERE `{primary_key}` IN ({ids}) ORDER BY `{primary_key}`"
        );
        let conn = match side {
            EvidenceSide::Source => &mut self.source,
            EvidenceSide::Target => &mut self.target,
        };
        let rows = conn
            .query::<Row, _>(sql)
            .map_err(|error| format!("{side:?} {table} evidence query failed: {error}"))?;
        rows.into_iter()
            .map(|row| snapshot_row(row, primary_key))
            .collect()
    }
}

impl ConflictEvidenceReader for MySqlConflictEvidenceReader {
    fn unresolved_primary_keys(&mut self) -> Result<Vec<Vec<String>>, String> {
        let sql = format!(
            "SELECT source_primary_key_json FROM cdc.row_conflicts WHERE source_identity={} AND schema_name='globalcomix' AND table_name='{TABLE}' AND status='unresolved' ORDER BY source_primary_key_json",
            quote_sql_literal(&self.source_identity)
        );
        let rows = self
            .target
            .query::<String, _>(sql)
            .map_err(|error| format!("scoped unresolved conflict query failed: {error}"))?;
        rows.into_iter()
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|error| format!("invalid conflict primary key JSON: {error}"))
            })
            .collect()
    }

    fn read_child_rows(
        &mut self,
        side: EvidenceSide,
        primary_keys: &[Vec<String>],
    ) -> Result<Vec<SnapshotRow>, String> {
        self.query_rows(side, TABLE, "view_id", primary_keys)
    }

    fn read_parent_rows(
        &mut self,
        side: EvidenceSide,
        primary_keys: &[Vec<String>],
    ) -> Result<Vec<SnapshotRow>, String> {
        self.query_rows(side, "utms", "id", primary_keys)
    }
}

fn source_opts(source: &MySqlConnectionConfig) -> Opts {
    Opts::from(crate::mysql_support::apply_default_mysql_network_bounds(
        OptsBuilder::default()
            .ip_or_hostname(Some(&source.host))
            .tcp_port(source.port)
            .user(Some(&source.user))
            .pass(Some(&source.password))
            .db_name(Some(&source.database))
            .prefer_socket(false),
    ))
}

fn snapshot_row(row: Row, primary_key: &str) -> Result<SnapshotRow, String> {
    let columns = row
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect::<Vec<_>>();
    let values = row
        .unwrap()
        .into_iter()
        .map(mysql_value_string)
        .collect::<Vec<_>>();
    let values = columns.into_iter().zip(values).collect::<BTreeMap<_, _>>();
    let primary_key = values
        .get(primary_key)
        .and_then(Clone::clone)
        .ok_or_else(|| format!("evidence row is missing primary key `{primary_key}`"))?;
    Ok(SnapshotRow {
        primary_key: vec![primary_key],
        values,
    })
}

fn mysql_value_string(value: Value) -> Option<String> {
    match value {
        Value::NULL => None,
        Value::Bytes(value) => Some(String::from_utf8_lossy(&value).into_owned()),
        Value::Int(value) => Some(value.to_string()),
        Value::UInt(value) => Some(value.to_string()),
        Value::Float(value) => Some(value.to_string()),
        Value::Double(value) => Some(value.to_string()),
        Value::Date(year, month, day, hour, minute, second, micros) => Some(format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"
        )),
        Value::Time(negative, days, hours, minutes, seconds, micros) => Some(format!(
            "{}{days} {hours:02}:{minutes:02}:{seconds:02}.{micros:06}",
            if negative { "-" } else { "" }
        )),
    }
}

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TargetedConflictResolutionReport {
    pub examined: usize,
    pub resolved: usize,
}

pub fn resolve_comics_releases_views_conflicts<R, W>(
    reader: &mut R,
    writer: &mut W,
    repair_run_id: &str,
    batch_size: usize,
) -> Result<TargetedConflictResolutionReport, String>
where
    R: ConflictEvidenceReader,
    W: ConflictResolutionWriter,
{
    if batch_size == 0 {
        return Err("targeted conflict batch size must be positive".to_string());
    }
    let (primary_keys, parent_count) = read_verified_evidence(reader, batch_size)?;
    let evidence = format!(
        "targeted source/target equality for {} conflict rows and {} referenced UTM parents",
        primary_keys.len(),
        parent_count
    );
    for primary_key in &primary_keys {
        writer.resolve_if_equal(TABLE, primary_key, true, repair_run_id, &evidence)?;
    }
    Ok(TargetedConflictResolutionReport {
        examined: primary_keys.len(),
        resolved: primary_keys.len(),
    })
}

fn read_verified_evidence<R: ConflictEvidenceReader>(
    reader: &mut R,
    batch_size: usize,
) -> Result<(Vec<Vec<String>>, usize), String> {
    let primary_keys = reader.unresolved_primary_keys()?;
    validate_primary_keys(&primary_keys)?;
    let source_children =
        read_batched_children(reader, EvidenceSide::Source, &primary_keys, batch_size)?;
    let target_children =
        read_batched_children(reader, EvidenceSide::Target, &primary_keys, batch_size)?;
    validate_equal_rows("child", &primary_keys, &source_children, &target_children)?;
    let parent_keys = referenced_parent_keys(&source_children)?;
    let source_parents =
        read_batched_parents(reader, EvidenceSide::Source, &parent_keys, batch_size)?;
    let target_parents =
        read_batched_parents(reader, EvidenceSide::Target, &parent_keys, batch_size)?;
    validate_equal_rows("UTM parent", &parent_keys, &source_parents, &target_parents)?;
    Ok((primary_keys, parent_keys.len()))
}

fn read_batched_children<R: ConflictEvidenceReader>(
    reader: &mut R,
    side: EvidenceSide,
    primary_keys: &[Vec<String>],
    batch_size: usize,
) -> Result<Vec<SnapshotRow>, String> {
    primary_keys
        .chunks(batch_size)
        .try_fold(Vec::new(), |mut rows, batch| {
            rows.extend(reader.read_child_rows(side, batch)?);
            Ok(rows)
        })
}

fn read_batched_parents<R: ConflictEvidenceReader>(
    reader: &mut R,
    side: EvidenceSide,
    primary_keys: &[Vec<String>],
    batch_size: usize,
) -> Result<Vec<SnapshotRow>, String> {
    primary_keys
        .chunks(batch_size)
        .try_fold(Vec::new(), |mut rows, batch| {
            rows.extend(reader.read_parent_rows(side, batch)?);
            Ok(rows)
        })
}

fn validate_primary_keys(primary_keys: &[Vec<String>]) -> Result<(), String> {
    if primary_keys.iter().any(|key| key.len() != 1) {
        return Err("comics_releases_views conflicts require one-column primary keys".to_string());
    }
    let unique = primary_keys.iter().collect::<BTreeSet<_>>();
    if unique.len() != primary_keys.len() {
        return Err("targeted conflict evidence contains duplicate primary keys".to_string());
    }
    Ok(())
}

fn validate_equal_rows(
    label: &str,
    expected_keys: &[Vec<String>],
    source_rows: &[SnapshotRow],
    target_rows: &[SnapshotRow],
) -> Result<(), String> {
    let source = canonical_rows_by_key(source_rows)?;
    let target = canonical_rows_by_key(target_rows)?;
    let expected = expected_keys.iter().cloned().collect::<BTreeSet<_>>();
    if source.keys().cloned().collect::<BTreeSet<_>>() != expected
        || target.keys().cloned().collect::<BTreeSet<_>>() != expected
    {
        return Err(format!("{label} evidence is missing or stale"));
    }
    if source != target {
        return Err(format!("{label} source/target rows diverge"));
    }
    Ok(())
}

fn canonical_rows_by_key(rows: &[SnapshotRow]) -> Result<CanonicalRowsByKey, String> {
    let mut indexed = BTreeMap::new();
    for row in rows {
        let values = row
            .values
            .iter()
            .map(|(name, value)| (name.clone(), value.as_deref().map(canonical_value)))
            .collect::<BTreeMap<_, _>>();
        if indexed.insert(row.primary_key.clone(), values).is_some() {
            return Err("targeted conflict evidence contains duplicate rows".to_string());
        }
    }
    Ok(indexed)
}

fn canonical_value(value: &str) -> String {
    value.strip_suffix(".000000").unwrap_or(value).to_string()
}

fn referenced_parent_keys(children: &[SnapshotRow]) -> Result<Vec<Vec<String>>, String> {
    let mut parent_ids = BTreeSet::new();
    for child in children {
        let utm_id = child
            .values
            .get(UTM_COLUMN)
            .ok_or_else(|| "child evidence is missing utm_id".to_string())?;
        if let Some(utm_id) = utm_id {
            parent_ids.insert(vec![utm_id.clone()]);
        }
    }
    Ok(parent_ids.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::{
        ConflictEvidenceReader, ConflictResolutionWriter, EvidenceSide,
        parse_targeted_conflict_resolution_config, resolve_comics_releases_views_conflicts,
    };
    use crate::snapshot::SnapshotRow;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct FakeEvidence {
        unresolved: Vec<Vec<String>>,
        source_children: Vec<SnapshotRow>,
        target_children: Vec<SnapshotRow>,
        source_parents: Vec<SnapshotRow>,
        target_parents: Vec<SnapshotRow>,
        child_batches: Vec<Vec<Vec<String>>>,
        parent_batches: Vec<Vec<Vec<String>>>,
    }

    impl ConflictEvidenceReader for FakeEvidence {
        fn unresolved_primary_keys(&mut self) -> Result<Vec<Vec<String>>, String> {
            Ok(self.unresolved.clone())
        }

        fn read_child_rows(
            &mut self,
            side: EvidenceSide,
            primary_keys: &[Vec<String>],
        ) -> Result<Vec<SnapshotRow>, String> {
            self.child_batches.push(primary_keys.to_vec());
            let rows = match side {
                EvidenceSide::Source => &self.source_children,
                EvidenceSide::Target => &self.target_children,
            };
            Ok(select_rows(rows, primary_keys))
        }

        fn read_parent_rows(
            &mut self,
            side: EvidenceSide,
            primary_keys: &[Vec<String>],
        ) -> Result<Vec<SnapshotRow>, String> {
            self.parent_batches.push(primary_keys.to_vec());
            let rows = match side {
                EvidenceSide::Source => &self.source_parents,
                EvidenceSide::Target => &self.target_parents,
            };
            Ok(select_rows(rows, primary_keys))
        }
    }

    #[derive(Default)]
    struct FakeWriter {
        resolutions: Vec<(String, Vec<String>, bool, String, String)>,
    }

    impl ConflictResolutionWriter for FakeWriter {
        fn resolve_if_equal(
            &mut self,
            table: &str,
            primary_key: &[String],
            rows_equal: bool,
            repair_run_id: &str,
            evidence: &str,
        ) -> Result<(), String> {
            self.resolutions.push((
                table.to_string(),
                primary_key.to_vec(),
                rows_equal,
                repair_run_id.to_string(),
                evidence.to_string(),
            ));
            Ok(())
        }
    }

    #[test]
    fn parses_required_targeted_resolution_options() {
        unsafe {
            std::env::set_var("TARGETED_SOURCE_PASSWORD", "source-secret");
            std::env::set_var("TARGETED_TARGET_PASSWORD", "target-secret");
        }
        let ca_file = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem");
        let args = vec![
            "--source-host",
            "source",
            "--source-user",
            "reader",
            "--source-password-env",
            "TARGETED_SOURCE_PASSWORD",
            "--source-database",
            "globalcomix",
            "--source-identity",
            "source-id",
            "--target-host",
            "target",
            "--target-user",
            "writer",
            "--target-password-env",
            "TARGETED_TARGET_PASSWORD",
            "--target-database",
            "globalcomix",
            "--target-tls-ca-file",
            ca_file,
            "--run-id",
            "targeted-run",
            "--batch-size",
            "500",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();

        let config = parse_targeted_conflict_resolution_config(args).expect("valid config");

        assert_eq!(config.source_identity, "source-id");
        assert_eq!(config.run_id, "targeted-run");
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn resolves_only_scoped_matching_children_and_referenced_parents_in_batches() {
        let mut reader = FakeEvidence {
            unresolved: vec![pk("1"), pk("2")],
            source_children: vec![
                child("1", "10", "2026-07-27 10:55:42"),
                child("2", "11", "2026-07-27 10:55:43"),
            ],
            target_children: vec![
                child("1", "10", "2026-07-27 10:55:42.000000"),
                child("2", "11", "2026-07-27 10:55:43.000000"),
            ],
            source_parents: vec![parent("10", "a"), parent("11", "b")],
            target_parents: vec![parent("10", "a"), parent("11", "b")],
            ..Default::default()
        };
        let mut writer = FakeWriter::default();

        let report = resolve_comics_releases_views_conflicts(
            &mut reader,
            &mut writer,
            "targeted-20260729",
            1,
        )
        .expect("matching evidence");

        assert_eq!(report.examined, 2);
        assert_eq!(report.resolved, 2);
        assert_eq!(
            reader.child_batches,
            vec![vec![pk("1")], vec![pk("2")], vec![pk("1")], vec![pk("2")]]
        );
        assert_eq!(writer.resolutions.len(), 2);
        assert!(
            writer
                .resolutions
                .iter()
                .all(|resolution| resolution.0 == "comics_releases_views" && resolution.2)
        );
    }

    #[test]
    fn rejects_missing_divergent_or_stale_evidence_without_writes() {
        for mut reader in [
            FakeEvidence {
                unresolved: vec![pk("1")],
                source_children: vec![child("1", "10", "x")],
                ..Default::default()
            },
            FakeEvidence {
                unresolved: vec![pk("1")],
                source_children: vec![child("1", "10", "x")],
                target_children: vec![child("1", "10", "y")],
                ..Default::default()
            },
            FakeEvidence {
                unresolved: vec![pk("1")],
                source_children: vec![child("2", "10", "x")],
                target_children: vec![child("2", "10", "x")],
                ..Default::default()
            },
        ] {
            let mut writer = FakeWriter::default();
            assert!(
                resolve_comics_releases_views_conflicts(&mut reader, &mut writer, "run", 1000)
                    .is_err()
            );
            assert!(writer.resolutions.is_empty());
        }
    }

    fn select_rows(rows: &[SnapshotRow], primary_keys: &[Vec<String>]) -> Vec<SnapshotRow> {
        rows.iter()
            .filter(|row| primary_keys.contains(&row.primary_key))
            .cloned()
            .collect()
    }

    fn pk(value: &str) -> Vec<String> {
        vec![value.to_string()]
    }

    fn child(view_id: &str, utm_id: &str, post_date: &str) -> SnapshotRow {
        row(
            view_id,
            [
                ("view_id", Some(view_id)),
                ("utm_id", Some(utm_id)),
                ("post_date", Some(post_date)),
            ],
        )
    }

    fn parent(id: &str, value: &str) -> SnapshotRow {
        row(id, [("id", Some(id)), ("utm_source", Some(value))])
    }

    fn row<const N: usize>(primary_key: &str, values: [(&str, Option<&str>); N]) -> SnapshotRow {
        SnapshotRow {
            primary_key: pk(primary_key),
            values: values
                .into_iter()
                .map(|(name, value)| (name.to_string(), value.map(str::to_string)))
                .collect::<BTreeMap<_, _>>(),
        }
    }
}
