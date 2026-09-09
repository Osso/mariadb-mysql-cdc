use super::model::{
    SyncChunkProgress, SyncChunkProgressStore, SyncMutationPhase, SyncProgressStatus,
    SyncRunProgressStore, SyncStage,
};

pub(crate) trait SyncPhaseProgressStore {
    fn load_phase(
        &mut self,
        run_id: &str,
        table_name: &str,
        phase: SyncMutationPhase,
    ) -> Result<Option<SyncChunkProgress>, String>;
    fn save_phase(
        &mut self,
        phase: SyncMutationPhase,
        progress: &SyncChunkProgress,
    ) -> Result<(), String>;
}

pub(crate) struct PhaseChunkProgressStore<'a, P, R> {
    phases: &'a mut P,
    legacy: &'a mut R,
    phase: SyncMutationPhase,
}

impl<'a, P: SyncPhaseProgressStore, R: SyncRunProgressStore> PhaseChunkProgressStore<'a, P, R> {
    pub(crate) fn new(
        phases: &'a mut P,
        legacy: &'a mut R,
        phase: SyncMutationPhase,
    ) -> Result<Self, String> {
        phase_name(phase)?;
        Ok(Self {
            phases,
            legacy,
            phase,
        })
    }

    fn load_legacy_complete(
        &mut self,
        run_id: &str,
        table: &str,
    ) -> Result<Option<SyncChunkProgress>, String> {
        let row = self.legacy.load_stage(run_id, SyncStage::Rows, table)?;
        row.filter(|row| row.status == SyncProgressStatus::Complete)
            .map(super::mysql::sync_chunk_progress_from_row)
            .transpose()
    }
}

impl<P: SyncPhaseProgressStore, R: SyncRunProgressStore> SyncChunkProgressStore
    for PhaseChunkProgressStore<'_, P, R>
{
    fn load(&mut self, run_id: &str, table: &str) -> Result<Option<SyncChunkProgress>, String> {
        if let Some(complete) = self.load_legacy_complete(run_id, table)? {
            return Ok(Some(complete));
        }
        self.phases.load_phase(run_id, table, self.phase)
    }
    fn save(&mut self, progress: &SyncChunkProgress) -> Result<(), String> {
        if self
            .load_legacy_complete(&progress.run_id, &progress.table)?
            .is_some()
        {
            return Err(format!(
                "cannot mutate completed legacy Rows progress for run `{}` table `{}`",
                progress.run_id, progress.table
            ));
        }
        self.phases.save_phase(self.phase, progress)
    }
}

fn phase_name(phase: SyncMutationPhase) -> Result<&'static str, String> {
    match phase {
        SyncMutationPhase::InsertMissing => Ok("insert_missing"),
        SyncMutationPhase::UpdateDivergent => Ok("update_divergent"),
        SyncMutationPhase::DeleteExtras => Ok("delete_extras"),
        SyncMutationPhase::All => {
            Err("phase progress requires an explicit mutation phase, not All".into())
        }
    }
}

/// Borrows the caller's session. Construction does not connect, initialize, or create tables.
pub(crate) struct MySqlPhaseProgressStore<'a> {
    conn: &'a mut ::mysql::Conn,
    table: String,
}

impl<'a> MySqlPhaseProgressStore<'a> {
    pub(crate) fn new(conn: &'a mut ::mysql::Conn, phase_table: &str) -> Self {
        Self {
            conn,
            table: crate::mysql_support::quote_identifier_path(phase_table),
        }
    }
}

impl SyncPhaseProgressStore for MySqlPhaseProgressStore<'_> {
    fn load_phase(
        &mut self,
        run_id: &str,
        table_name: &str,
        phase: SyncMutationPhase,
    ) -> Result<Option<SyncChunkProgress>, String> {
        use ::mysql::prelude::Queryable;
        let phase = phase_name(phase)?;
        let sql = format!(
            "SELECT last_primary_key_json, complete, chunks, rows_scanned, inserts, updates, deletes FROM {} WHERE run_id = ? AND table_name = ? AND phase = ?",
            self.table
        );
        let row: Option<StoredPhaseProgress> = self
            .conn
            .exec_first(sql, (run_id, table_name, phase))
            .map_err(|error| {
                format!(
                    "read phase `{phase}` progress for run `{run_id}` table `{table_name}`: {error}"
                )
            })?;
        row.map(|row| decode_phase_progress(run_id, table_name, row))
            .transpose()
    }

    fn save_phase(
        &mut self,
        phase: SyncMutationPhase,
        progress: &SyncChunkProgress,
    ) -> Result<(), String> {
        use ::mysql::prelude::Queryable;
        let phase = phase_name(phase)?;
        let cursor = progress
            .last_primary_key
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| format!("encode phase cursor: {error}"))?;
        let sql = format!(
            "INSERT INTO {} (run_id, table_name, phase, last_primary_key_json, complete, chunks, rows_scanned, inserts, updates, deletes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS new ON DUPLICATE KEY UPDATE last_primary_key_json = new.last_primary_key_json, complete = new.complete, chunks = new.chunks, rows_scanned = new.rows_scanned, inserts = new.inserts, updates = new.updates, deletes = new.deletes",
            self.table
        );
        self.conn
            .exec_drop(
                sql,
                (
                    &progress.run_id,
                    &progress.table,
                    phase,
                    cursor,
                    progress.complete,
                    progress.chunks,
                    progress.rows_scanned,
                    progress.inserts,
                    progress.updates,
                    progress.deletes,
                ),
            )
            .map_err(|error| {
                format!(
                    "write phase `{phase}` progress for run `{}` table `{}`: {error}",
                    progress.run_id, progress.table
                )
            })
    }
}

type StoredPhaseProgress = (Option<String>, u8, u64, u64, u64, u64, u64);

fn decode_phase_progress(
    run_id: &str,
    table: &str,
    row: StoredPhaseProgress,
) -> Result<SyncChunkProgress, String> {
    let (cursor, complete, chunks, rows_scanned, inserts, updates, deletes) = row;
    if complete > 1 {
        return Err(format!(
            "invalid phase completion flag {complete} for run `{run_id}` table `{table}`"
        ));
    }
    let last_primary_key = cursor
        .map(|json| serde_json::from_str::<Vec<String>>(&json))
        .transpose()
        .map_err(|error| {
            format!("invalid phase cursor for run `{run_id}` table `{table}`: {error}")
        })?;
    Ok(SyncChunkProgress {
        run_id: run_id.into(),
        table: table.into(),
        last_primary_key,
        complete: complete == 1,
        chunks,
        rows_scanned,
        inserts,
        updates,
        deletes,
    })
}

#[cfg(test)]
mod tests {
    use super::super::model::SyncProgressRow;
    use super::*;

    #[derive(Default)]
    struct Phases(Vec<(SyncMutationPhase, SyncChunkProgress)>);
    impl SyncPhaseProgressStore for Phases {
        fn load_phase(
            &mut self,
            run: &str,
            table: &str,
            phase: SyncMutationPhase,
        ) -> Result<Option<SyncChunkProgress>, String> {
            Ok(self
                .0
                .iter()
                .find(|(p, row)| *p == phase && row.run_id == run && row.table == table)
                .map(|(_, row)| row.clone()))
        }
        fn save_phase(
            &mut self,
            phase: SyncMutationPhase,
            row: &SyncChunkProgress,
        ) -> Result<(), String> {
            self.0.retain(|(p, old)| {
                *p != phase || old.run_id != row.run_id || old.table != row.table
            });
            self.0.push((phase, row.clone()));
            Ok(())
        }
    }
    #[derive(Default)]
    struct Legacy(Option<SyncProgressRow>);
    impl SyncRunProgressStore for Legacy {
        fn load_stage(
            &mut self,
            run: &str,
            stage: SyncStage,
            table: &str,
        ) -> Result<Option<SyncProgressRow>, String> {
            Ok(self
                .0
                .clone()
                .filter(|row| row.run_id == run && row.stage == stage && row.table_name == table))
        }
        fn save_stage(&mut self, _: &SyncProgressRow) -> Result<(), String> {
            panic!("legacy progress must remain read-only")
        }
    }
    fn progress() -> SyncChunkProgress {
        SyncChunkProgress {
            run_id: "Run/Exact:1".into(),
            table: "Books.Exact".into(),
            last_primary_key: Some(vec!["42".into(), "雪\t\"".into()]),
            complete: false,
            chunks: 3,
            rows_scanned: 91,
            inserts: 7,
            updates: 8,
            deletes: 9,
        }
    }
    fn legacy(row: &SyncChunkProgress) -> Legacy {
        Legacy(Some(super::super::mysql::sync_progress_row_from_chunk(row)))
    }
    #[test]
    fn decodes_persisted_cursor_completion_and_all_counters() {
        let mut expected = progress();
        expected.complete = true;
        let stored = (
            Some(serde_json::to_string(&expected.last_primary_key).unwrap()),
            1,
            3,
            91,
            7,
            8,
            9,
        );
        assert_eq!(
            decode_phase_progress(&expected.run_id, &expected.table, stored).unwrap(),
            expected
        );
        let empty = decode_phase_progress("r", "t", (None, 0, 0, 0, 0, 0, 0)).unwrap();
        assert_eq!(empty.last_primary_key, None);
        assert!(!empty.complete);
    }
    #[test]
    fn rejects_corrupt_persisted_cursor_and_completion() {
        for cursor in ["broken", "[42]", "null"] {
            assert!(
                decode_phase_progress("r", "t", (Some(cursor.into()), 0, 0, 0, 0, 0, 0)).is_err()
            );
        }
        assert!(decode_phase_progress("r", "t", (None, 2, 0, 0, 0, 0, 0)).is_err());
    }
    #[test]
    fn rejects_all() {
        assert!(
            PhaseChunkProgressStore::new(
                &mut Phases::default(),
                &mut Legacy::default(),
                SyncMutationPhase::All
            )
            .is_err()
        );
    }
    #[test]
    fn preserves_ids_and_isolates_phase_cursors_and_counters() {
        let mut phases = Phases::default();
        let mut old = Legacy::default();
        for (index, phase) in [
            SyncMutationPhase::InsertMissing,
            SyncMutationPhase::UpdateDivergent,
            SyncMutationPhase::DeleteExtras,
        ]
        .into_iter()
        .enumerate()
        {
            let mut adapter = PhaseChunkProgressStore::new(&mut phases, &mut old, phase).unwrap();
            assert_eq!(adapter.load("Run/Exact:1", "Books.Exact").unwrap(), None);
            let mut row = progress();
            row.chunks += index as u64;
            row.complete = index == 2;
            adapter.save(&row).unwrap();
            assert_eq!(adapter.load(&row.run_id, &row.table).unwrap(), Some(row));
            assert_eq!(adapter.load("run/exact:1", "Books.Exact").unwrap(), None);
            assert_eq!(adapter.load("Run/Exact:1", "books.exact").unwrap(), None);
        }
        assert_eq!(phases.0.len(), 3);
        assert_eq!(phases.0[0].1, progress());
    }
    #[test]
    fn legacy_complete_wins_unchanged_and_blocks_save_even_without_load() {
        let mut row = progress();
        row.complete = true;
        let mut old = legacy(&row);
        let mut phases = Phases::default();
        let mut adapter =
            PhaseChunkProgressStore::new(&mut phases, &mut old, SyncMutationPhase::UpdateDivergent)
                .unwrap();
        assert!(adapter.save(&progress()).is_err());
        assert_eq!(adapter.load(&row.run_id, &row.table).unwrap(), Some(row));
        assert!(phases.0.is_empty());
    }
    #[test]
    fn incomplete_legacy_cursor_does_not_seed_phase() {
        let row = progress();
        let mut old = legacy(&row);
        let original = old.0.clone();
        let mut phases = Phases::default();
        let mut adapter =
            PhaseChunkProgressStore::new(&mut phases, &mut old, SyncMutationPhase::InsertMissing)
                .unwrap();
        assert_eq!(adapter.load(&row.run_id, &row.table).unwrap(), None);
        adapter.save(&row).unwrap();
        assert_eq!(adapter.load(&row.run_id, &row.table).unwrap(), Some(row));
        assert_eq!(old.0, original);
    }
}
