use super::*;
use std::collections::BTreeMap;

fn row(id: u64, name: &str) -> DatabaseRow {
    DatabaseRow {
        primary_key: vec![id.to_string()],
        values: BTreeMap::from([
            ("guest_id".into(), Some(id.to_string())),
            ("name".into(), Some(name.into())),
            ("utm_id".into(), Some("7".into())),
        ]),
    }
}

fn config() -> GuestRangeRepairConfig {
    GuestRangeRepairConfig {
        source: MySqlConnectionConfig::default(),
        target: TargetMySqlConfig::default(),
        start: 10,
        end: 12,
        expected_rows: 3,
        batch_size: 2,
    }
}

struct MemoryBackend {
    source: Vec<DatabaseRow>,
    target: BTreeMap<Vec<String>, DatabaseRow>,
    saved: BTreeMap<Vec<String>, DatabaseRow>,
    parent_exists: bool,
    corrupt_insert: bool,
    fail_commit: bool,
    finished: bool,
}
impl MemoryBackend {
    fn new() -> Self {
        Self {
            source: vec![row(10, "a"), row(11, "b"), row(12, "c")],
            target: BTreeMap::new(),
            saved: BTreeMap::new(),
            parent_exists: true,
            corrupt_insert: false,
            fail_commit: false,
            finished: false,
        }
    }
}
impl GuestRangeBackend for MemoryBackend {
    fn preflight(&mut self, _: &GuestRangeRepairConfig) -> Result<(u64, u64, u64), String> {
        Ok((self.source.len() as u64, 10, 12))
    }
    fn read_page(
        &mut self,
        c: &GuestRangeRepairConfig,
        after: Option<u64>,
    ) -> Result<Vec<DatabaseRow>, String> {
        Ok(self
            .source
            .iter()
            .filter(|r| after.is_none_or(|a| r.primary_key[0].parse::<u64>().unwrap() > a))
            .take(c.batch_size)
            .cloned()
            .collect())
    }
    fn begin_batch(&mut self) -> Result<(), String> {
        self.saved = self.target.clone();
        Ok(())
    }
    fn require_parent(&mut self, _: &DatabaseRow) -> Result<(), String> {
        if self.parent_exists {
            Ok(())
        } else {
            Err("missing utms".into())
        }
    }
    fn read_target(&mut self, r: &DatabaseRow) -> Result<Option<DatabaseRow>, String> {
        Ok(self.target.get(&r.primary_key).cloned())
    }
    fn insert(&mut self, r: &DatabaseRow) -> Result<(), String> {
        let mut r = r.clone();
        if self.corrupt_insert {
            r.values.insert("name".into(), Some("wrong".into()));
        }
        self.target.insert(r.primary_key.clone(), r);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), String> {
        if self.fail_commit {
            Err("commit failed".into())
        } else {
            Ok(())
        }
    }
    fn rollback(&mut self) -> Result<(), String> {
        self.target = self.saved.clone();
        Ok(())
    }
    fn finish_source(&mut self) -> Result<(), String> {
        self.finished = true;
        Ok(())
    }
}

#[test]
fn inserts_full_rows_over_multiple_pages_and_reruns_without_changes() {
    let mut b = MemoryBackend::new();
    let report = repair_with_backend(&config(), &mut b).unwrap();
    assert_eq!(
        report,
        GuestRangeRepairReport {
            scanned: 3,
            inserted: 3,
            unchanged: 0,
            batches: 2
        }
    );
    assert_eq!(b.target.values().cloned().collect::<Vec<_>>(), b.source);
    assert!(b.finished);
    let report = repair_with_backend(&config(), &mut b).unwrap();
    assert_eq!(report.inserted, 0);
    assert_eq!(report.unchanged, 3);
}
#[test]
fn source_count_mismatch_writes_nothing() {
    let mut b = MemoryBackend::new();
    b.source.pop();
    assert!(
        repair_with_backend(&config(), &mut b)
            .unwrap_err()
            .contains("source range")
    );
    assert!(b.target.is_empty());
    assert!(b.finished);
}
#[test]
fn differing_existing_row_rolls_back_earlier_insert_in_same_batch() {
    let mut b = MemoryBackend::new();
    let wrong = row(11, "wrong");
    b.target.insert(wrong.primary_key.clone(), wrong.clone());
    assert!(
        repair_with_backend(&config(), &mut b)
            .unwrap_err()
            .contains("differs")
    );
    assert_eq!(
        b.target,
        BTreeMap::from([(wrong.primary_key.clone(), wrong)])
    );
}
#[test]
fn missing_parent_and_corrupt_readback_leave_no_inserts() {
    for corrupt in [false, true] {
        let mut b = MemoryBackend::new();
        b.parent_exists = corrupt;
        b.corrupt_insert = corrupt;
        assert!(repair_with_backend(&config(), &mut b).is_err());
        assert!(b.target.is_empty());
    }
}
#[test]
fn commit_failure_rolls_back_batch() {
    let mut b = MemoryBackend::new();
    b.fail_commit = true;
    assert!(repair_with_backend(&config(), &mut b).is_err());
    assert!(b.target.is_empty());
}
#[test]
fn invalid_bounds_and_batch_sizes_fail_before_writes() {
    for (start, end, expected, batch) in [
        (12, 10, 3, 2),
        (10, 12, 2, 2),
        (10, 12, 3, 0),
        (10, 12, 3, 1001),
    ] {
        let mut c = config();
        c.start = start;
        c.end = end;
        c.expected_rows = expected;
        c.batch_size = batch;
        let mut b = MemoryBackend::new();
        assert!(repair_with_backend(&c, &mut b).is_err());
        assert!(b.target.is_empty());
    }
}
