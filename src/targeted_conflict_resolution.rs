use crate::conflict_repair::ConflictStore;
use crate::snapshot::SnapshotRow;
use std::collections::{BTreeMap, BTreeSet};

const TABLE: &str = "comics_releases_views";
const UTM_COLUMN: &str = "utm_id";

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

    let evidence = format!(
        "targeted source/target equality for {} conflict rows and {} referenced UTM parents",
        primary_keys.len(),
        parent_keys.len()
    );
    for primary_key in &primary_keys {
        writer.resolve_if_equal(TABLE, primary_key, true, repair_run_id, &evidence)?;
    }
    Ok(TargetedConflictResolutionReport {
        examined: primary_keys.len(),
        resolved: primary_keys.len(),
    })
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

fn canonical_rows_by_key(
    rows: &[SnapshotRow],
) -> Result<BTreeMap<Vec<String>, BTreeMap<String, Option<String>>>, String> {
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
        resolve_comics_releases_views_conflicts,
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
