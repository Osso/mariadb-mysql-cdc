use super::config::SyncConfig;
use super::model::{
    SyncChunkProgress, SyncProgressRow, SyncProgressStatus, SyncRunProgressStore, SyncStage,
};

use super::mysql::{MySqlSyncProgressStore, sync_chunk_progress_from_row};

pub(crate) fn read_recorded_recovery_tables(config: &SyncConfig) -> Result<Vec<String>, String> {
    let run_id = require_explicit_run_id(config.run_id.as_deref())?;
    super::config::validate_sync_config(config)?;
    let mut store = MySqlSyncProgressStore::open_existing(
        &config.target,
        config.progress_table.clone(),
        config.coordinator_session_wait_timeout_seconds,
    )?;
    let prerequisite = store.stage_table_names(run_id, SyncStage::PrerequisiteSchema)?;
    let rows = store.stage_table_names(run_id, SyncStage::Rows)?;
    validate_recorded_recovery_tables(prerequisite, rows)
}

fn validate_recorded_recovery_tables(
    prerequisite: Vec<String>,
    rows: Vec<String>,
) -> Result<Vec<String>, String> {
    let prerequisite: std::collections::BTreeSet<_> = prerequisite.into_iter().collect();
    let rows: std::collections::BTreeSet<_> = rows.into_iter().collect();
    if prerequisite.is_empty() || rows.is_empty() {
        return Err(
            "recorded recovery requires nonempty prerequisite_schema and rows table sets".into(),
        );
    }
    if prerequisite != rows {
        return Err("recorded recovery prerequisite_schema and rows table sets differ".into());
    }
    Ok(rows.into_iter().collect())
}

pub(crate) fn load_completed_recovery_rows(
    config: &SyncConfig,
) -> Result<Vec<SyncChunkProgress>, String> {
    require_explicit_run_id(config.run_id.as_deref())?;
    super::config::validate_sync_config(config)?;
    let mut store = MySqlSyncProgressStore::open_existing(
        &config.target,
        config.progress_table.clone(),
        config.coordinator_session_wait_timeout_seconds,
    )?;
    load_completed_rows_from_store(&mut store, config.run_id.as_deref(), &config.tables)
}

fn require_explicit_run_id(run_id: Option<&str>) -> Result<&str, String> {
    run_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "completed recovery rows require an explicit run_id".to_string())
}

fn load_completed_rows_from_store(
    store: &mut impl SyncRunProgressStore,
    run_id: Option<&str>,
    tables: &[String],
) -> Result<Vec<SyncChunkProgress>, String> {
    let run_id = require_explicit_run_id(run_id)?;
    tables
        .iter()
        .map(|table| {
            load_complete_stage(store, run_id, SyncStage::PrerequisiteSchema, table)?;
            let rows = load_complete_stage(store, run_id, SyncStage::Rows, table)?;
            sync_chunk_progress_from_row(rows)
        })
        .collect()
}

fn load_complete_stage(
    store: &mut impl SyncRunProgressStore,
    run_id: &str,
    stage: SyncStage,
    table: &str,
) -> Result<SyncProgressRow, String> {
    let context = format!(
        "completed recovery run `{run_id}` stage `{}` table `{table}`",
        stage.as_str()
    );
    let row = store
        .load_stage(run_id, stage, table)
        .map_err(|error| format!("{context}: {error}"))?
        .ok_or_else(|| format!("{context}: missing progress"))?;
    if row.run_id != run_id || row.stage != stage || row.table_name != table {
        return Err(format!("{context}: progress identity mismatch"));
    }
    if row.status != SyncProgressStatus::Complete {
        return Err(format!(
            "{context}: expected complete, found {}",
            row.status.as_str()
        ));
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::super::mysql::sync_progress_row_from_chunk;
    use super::*;
    use std::collections::VecDeque;

    struct Store(VecDeque<Result<Option<SyncProgressRow>, String>>);

    impl SyncRunProgressStore for Store {
        fn load_stage(
            &mut self,
            _: &str,
            _: SyncStage,
            _: &str,
        ) -> Result<Option<SyncProgressRow>, String> {
            self.0.pop_front().expect("unexpected progress read")
        }
        fn save_stage(&mut self, _: &SyncProgressRow) -> Result<(), String> {
            panic!("completed recovery must not write progress")
        }
    }

    fn chunk(table: &str) -> SyncChunkProgress {
        SyncChunkProgress {
            run_id: "audit-42".into(),
            table: table.into(),
            last_primary_key: Some(vec!["9007199254740993".into(), "tail".into()]),
            complete: true,
            chunks: 17,
            rows_scanned: 420,
            inserts: 300,
            updates: 80,
            deletes: 40,
        }
    }

    fn rows(table: &str) -> Vec<SyncProgressRow> {
        let row = sync_progress_row_from_chunk(&chunk(table));
        let mut prerequisite = row.clone();
        prerequisite.stage = SyncStage::PrerequisiteSchema;
        vec![prerequisite, row]
    }

    fn store(rows: Vec<SyncProgressRow>) -> Store {
        Store(rows.into_iter().map(|row| Ok(Some(row))).collect())
    }

    #[test]
    fn recorded_tables_require_identical_nonempty_sets_and_sort_names() {
        let names = |values: &[&str]| values.iter().map(|name| (*name).to_string()).collect();
        assert_eq!(
            validate_recorded_recovery_tables(
                names(&["pages", "books"]),
                names(&["books", "pages"])
            )
            .unwrap(),
            names(&["books", "pages"])
        );
        for (schema, rows) in [
            (vec![], vec![]),
            (names(&["books"]), vec![]),
            (vec![], names(&["books"])),
            (names(&["books"]), names(&["pages"])),
            (names(&["books", "pages"]), names(&["books"])),
            (names(&["books"]), names(&["books", "pages"])),
        ] {
            assert!(validate_recorded_recovery_tables(schema, rows).is_err());
        }
    }

    #[test]
    fn preserves_every_completed_table_counter_and_cursor() {
        let mut data = rows("books");
        data.extend(rows("pages"));
        assert_eq!(
            load_completed_rows_from_store(
                &mut store(data),
                Some("audit-42"),
                &["books".into(), "pages".into()]
            )
            .unwrap(),
            vec![chunk("books"), chunk("pages")]
        );
    }

    #[test]
    fn rejects_missing_or_blank_explicit_run_before_reading() {
        for run_id in [None, Some(""), Some("  ")] {
            assert!(
                load_completed_rows_from_store(
                    &mut Store(VecDeque::new()),
                    run_id,
                    &["books".into()]
                )
                .is_err()
            );
        }
    }

    #[test]
    fn rejects_incomplete_or_mismatched_prerequisite_and_rows() {
        for index in 0..2 {
            for mutation in 0..5 {
                let mut data = rows("books");
                match mutation {
                    0 => data[index].status = SyncProgressStatus::Running,
                    1 => data[index].status = SyncProgressStatus::Error,
                    2 => data[index].run_id = "other-run".into(),
                    3 => data[index].table_name = "other-table".into(),
                    _ => data[index].stage = SyncStage::FinalConstraints,
                }
                assert!(
                    load_completed_rows_from_store(
                        &mut store(data),
                        Some("audit-42"),
                        &["books".into()]
                    )
                    .is_err(),
                    "index={index} mutation={mutation}"
                );
            }
        }
    }

    #[test]
    fn rejects_missing_progress_and_propagates_read_failure() {
        for index in 0..2 {
            for failure in [Ok(None), Err("storage unavailable".into())] {
                let mut data = store(rows("books"));
                data.0[index] = failure;
                assert!(
                    load_completed_rows_from_store(&mut data, Some("audit-42"), &["books".into()])
                        .is_err()
                );
            }
        }
    }

    #[test]
    fn rejects_missing_second_table_instead_of_returning_partial_completion() {
        let mut data = store(rows("books"));
        data.0.push_back(Ok(None));
        assert!(
            load_completed_rows_from_store(
                &mut data,
                Some("audit-42"),
                &["books".into(), "pages".into()]
            )
            .is_err()
        );
    }
}
