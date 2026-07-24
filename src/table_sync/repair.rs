use super::*;
use crate::snapshot::SnapshotRow;
use std::collections::BTreeMap;

pub(crate) fn repair_chunk(
    source_rows: &[SnapshotRow],
    target_rows: &[SnapshotRow],
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
    report: &mut SyncTableReport,
    max_deletes: Option<u64>,
    phase: SyncPhase,
) -> Result<(), TableSyncError> {
    let source_by_key = rows_by_key(source_rows);
    let target_by_key = rows_by_key(target_rows);
    if mode == SyncMode::MissingPrimaryKeys {
        repair_missing_rows(&source_by_key, &target_by_key, mode, repair_target, report)?;
        return Ok(());
    }
    if phase == SyncPhase::Verify {
        verify_chunk(&source_by_key, &target_by_key, report);
        return Ok(());
    }
    if phase == SyncPhase::VerifyNoTargetExtras {
        verify_no_target_extras_chunk(&source_by_key, &target_by_key, report);
        return Ok(());
    }
    if matches!(phase, SyncPhase::All | SyncPhase::DeleteExtras) {
        repair_extra_rows(
            &source_by_key,
            &target_by_key,
            mode,
            repair_target,
            report,
            max_deletes,
        )?;
    }
    if matches!(phase, SyncPhase::All | SyncPhase::UpdateDivergent) {
        repair_changed_rows(source_rows, &target_by_key, mode, repair_target, report)?;
    }
    if matches!(phase, SyncPhase::All | SyncPhase::InsertMissing) {
        repair_missing_rows(&source_by_key, &target_by_key, mode, repair_target, report)?;
    }

    Ok(())
}

fn verify_chunk(
    source_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    target_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    report: &mut SyncTableReport,
) {
    report.inserts += source_by_key
        .keys()
        .filter(|primary_key| !target_by_key.contains_key(*primary_key))
        .count() as u64;
    report.updates += source_by_key
        .iter()
        .filter(|(primary_key, source)| {
            target_by_key
                .get(*primary_key)
                .is_some_and(|target| source.values != target.values)
        })
        .count() as u64;
    verify_no_target_extras_chunk(source_by_key, target_by_key, report);
}

fn verify_no_target_extras_chunk(
    source_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    target_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    report: &mut SyncTableReport,
) {
    report.extra_target_rows += target_by_key
        .keys()
        .filter(|primary_key| !source_by_key.contains_key(*primary_key))
        .count() as u64;
}

fn repair_extra_rows(
    source_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    target_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
    report: &mut SyncTableReport,
    max_deletes: Option<u64>,
) -> Result<(), TableSyncError> {
    let extra_primary_keys: Vec<_> = target_by_key
        .keys()
        .filter(|primary_key| !source_by_key.contains_key(*primary_key))
        .collect();
    ensure_delete_allowed(
        report.extra_target_rows + extra_primary_keys.len() as u64,
        max_deletes,
        mode,
    )?;

    for primary_key in extra_primary_keys {
        apply_delete(primary_key, mode, repair_target)?;
        report.extra_target_rows += 1;
    }
    Ok(())
}

fn repair_changed_rows(
    source_rows: &[SnapshotRow],
    target_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
    report: &mut SyncTableReport,
) -> Result<(), TableSyncError> {
    let changed_rows = source_rows
        .iter()
        .filter(|source| {
            target_by_key
                .get(&source.primary_key)
                .is_some_and(|target| source.values != target.values)
        })
        .collect::<Vec<_>>();
    if mode == SyncMode::Apply && !changed_rows.is_empty() {
        repair_target.update_rows(&changed_rows)?;
    }
    report.updates += changed_rows.len() as u64;
    Ok(())
}

fn repair_missing_rows(
    source_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    target_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
    report: &mut SyncTableReport,
) -> Result<(), TableSyncError> {
    let missing_rows = source_by_key
        .iter()
        .filter(|(primary_key, _)| !target_by_key.contains_key(*primary_key))
        .map(|(_, source)| *source)
        .collect::<Vec<_>>();
    apply_inserts(&missing_rows, mode, repair_target)?;
    report.inserts += missing_rows.len() as u64;
    Ok(())
}

pub(crate) fn apply_recent_update_chunk(
    source_rows: &[SnapshotRow],
    mode: SyncMode,
    repair_target: &mut dyn SyncRepairTarget,
    report: &mut SyncTableReport,
) -> Result<(), TableSyncError> {
    report.chunks += 1;
    report.rows_scanned += source_rows.len() as u64;
    report.updates += source_rows.len() as u64;
    if mode == SyncMode::Apply {
        for row in source_rows {
            repair_target.insert_row(row)?;
        }
    }
    Ok(())
}

fn rows_by_key(rows: &[SnapshotRow]) -> BTreeMap<Vec<String>, &SnapshotRow> {
    rows.iter()
        .map(|row| (row.primary_key.clone(), row))
        .collect()
}

pub(crate) fn count_extra_target_rows(
    source_rows: &[SnapshotRow],
    target_rows: &[SnapshotRow],
) -> u64 {
    let source_by_key = rows_by_key(source_rows);
    rows_by_key(target_rows)
        .keys()
        .filter(|primary_key| !source_by_key.contains_key(*primary_key))
        .count() as u64
}

pub(crate) fn ensure_delete_allowed(
    total_deletes: u64,
    max_deletes: Option<u64>,
    mode: SyncMode,
) -> Result<(), TableSyncError> {
    if mode == SyncMode::Apply && max_deletes.is_some_and(|limit| total_deletes > limit) {
        return Err(TableSyncError::Repair(format!(
            "delete safety threshold exceeded: max_deletes={}",
            max_deletes.expect("checked max deletes")
        )));
    }
    Ok(())
}

fn apply_inserts(
    rows: &[&SnapshotRow],
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<(), TableSyncError> {
    if mode != SyncMode::DryRun && !rows.is_empty() {
        repair_target.insert_rows(rows)?;
    }
    Ok(())
}

fn apply_delete(
    primary_key: &[String],
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<(), TableSyncError> {
    if mode == SyncMode::Apply {
        repair_target.delete_row(primary_key)?;
    }
    Ok(())
}
