use super::*;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Default)]
struct ExecutionState {
    active: usize,
    maximum: usize,
    starts: usize,
    events: Vec<String>,
}

#[derive(Clone, Default)]
struct ConcurrentExecutor {
    state: Arc<(Mutex<ExecutionState>, Condvar)>,
}

impl SchemaStatementExecutor for ConcurrentExecutor {
    fn execute(&mut self, table: &str, sql: &str) -> Result<(), String> {
        let (lock, wake) = &*self.state;
        let mut state = lock.lock().unwrap();
        state.active += 1;
        state.maximum = state.maximum.max(state.active);
        state.starts += 1;
        state.events.push(format!("start:{table}:{sql}"));
        wake.notify_all();
        if sql == "overlap" {
            state = wake
                .wait_timeout_while(state, Duration::from_secs(2), |state| state.starts < 2)
                .unwrap()
                .0;
        }
        // Release the fixture lock while work runs, as independent SQL sessions do.
        drop(state);
        std::thread::sleep(Duration::from_millis(20));
        let mut state = lock.lock().unwrap();
        state.events.push(format!("end:{table}:{sql}"));
        state.active -= 1;
        wake.notify_all();
        if sql == "fail" {
            Err("statement rejected".to_string())
        } else {
            Ok(())
        }
    }
}

fn table(name: &str, dependencies: &[&str], statements: &[&str]) -> TableSchemaPlan {
    TableSchemaPlan {
        table: name.to_string(),
        source_fingerprint: "source".to_string(),
        target_fingerprint: "target".to_string(),
        status: TableSchemaStatus::Planned,
        dependencies: dependencies.iter().map(|s| s.to_string()).collect(),
        blockers: vec![],
        preflights: vec![],
        statements: statements
            .iter()
            .map(|sql| PlannedSchemaStatement {
                phase: SchemaPhase::Columns,
                sql: sql.to_string(),
                objects: vec![],
                prerequisites: vec![],
            })
            .collect(),
    }
}

fn run_tables(
    tables: Vec<TableSchemaPlan>,
    bound: usize,
    executor: &ConcurrentExecutor,
) -> SchemaConvergenceReport {
    let plan = SchemaConvergencePlan {
        source_fingerprint: "source".to_string(),
        target_fingerprint: "target".to_string(),
        tables,
    };
    execute_sync_schema_stage_plan(plan, bound, &|| Ok(executor.clone()))
}

#[test]
fn independent_tables_overlap_with_configured_bound_and_deterministic_reports() {
    for bound in [1, 2, 3] {
        let executor = ConcurrentExecutor::default();
        let report = run_tables(
            vec![
                table("a", &[], &["overlap"]),
                table("b", &[], &["work"]),
                table("c", &[], &["work"]),
                table("d", &[], &["work"]),
            ],
            bound,
            &executor,
        );
        assert_eq!(report.overall_status, OverallSchemaStatus::Converged);
        assert_eq!(
            report
                .tables
                .iter()
                .map(|r| r.table.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c", "d"]
        );
        assert_eq!(executor.state.0.lock().unwrap().maximum, bound);
    }
}

#[test]
fn child_waits_for_parent_completion_even_when_listed_first() {
    let executor = ConcurrentExecutor::default();
    let report = run_tables(
        vec![
            table("child", &["parent"], &["child-work"]),
            table("parent", &[], &["first", "second"]),
            table("other", &[], &["work"]),
        ],
        2,
        &executor,
    );
    assert_eq!(report.overall_status, OverallSchemaStatus::Converged);
    let state = executor.state.0.lock().unwrap();
    let parent_end = state
        .events
        .iter()
        .position(|e| e == "end:parent:second")
        .unwrap();
    let child_start = state
        .events
        .iter()
        .position(|e| e == "start:child:child-work")
        .unwrap();
    assert!(parent_end < child_start, "{:?}", state.events);
}

#[test]
fn failed_parent_blocks_child_and_descendant_but_not_independent_table() {
    let executor = ConcurrentExecutor::default();
    let report = run_tables(
        vec![
            table("grandchild", &["child"], &["work"]),
            table("child", &["parent"], &["work"]),
            table("parent", &[], &["fail"]),
            table("other", &[], &["work"]),
        ],
        2,
        &executor,
    );
    assert_eq!(report.tables[0].status, TableSchemaStatus::Skipped);
    assert_eq!(report.tables[1].status, TableSchemaStatus::Skipped);
    assert_eq!(report.tables[2].status, TableSchemaStatus::Failed);
    assert_eq!(report.tables[3].status, TableSchemaStatus::Converged);
    assert_eq!(report.tables[1].skipped_dependencies, ["parent"]);
    let state = executor.state.0.lock().unwrap();
    assert!(
        !state
            .events
            .iter()
            .any(|e| e.starts_with("start:child:") || e.starts_with("start:grandchild:"))
    );
}

#[test]
fn all_constraint_drops_finish_before_ordered_table_modifications() {
    let executor = ConcurrentExecutor::default();
    let mut a = table(
        "a",
        &[],
        &["ALTER TABLE a DROP FOREIGN KEY fk_a", "first", "second"],
    );
    a.statements[0].phase = SchemaPhase::Constraints;
    let mut b = table("b", &[], &["ALTER TABLE b DROP FOREIGN KEY fk_b", "work"]);
    b.statements[0].phase = SchemaPhase::Constraints;
    let report = run_tables(vec![a, b], 2, &executor);
    assert_eq!(report.overall_status, OverallSchemaStatus::Converged);
    let state = executor.state.0.lock().unwrap();
    let last_drop = state
        .events
        .iter()
        .rposition(|e| e.starts_with("end:") && e.contains("DROP FOREIGN KEY"))
        .unwrap();
    let first_modification = state
        .events
        .iter()
        .position(|e| e.starts_with("start:") && !e.contains("DROP FOREIGN KEY"))
        .unwrap();
    assert!(last_drop < first_modification, "{:?}", state.events);
    let first_end = state
        .events
        .iter()
        .position(|e| e == "end:a:first")
        .unwrap();
    let second_start = state
        .events
        .iter()
        .position(|e| e == "start:a:second")
        .unwrap();
    assert!(first_end < second_start);
    assert_eq!(
        report.tables[0]
            .executions
            .iter()
            .map(|e| e.sql.as_str())
            .collect::<Vec<_>>(),
        ["ALTER TABLE a DROP FOREIGN KEY fk_a", "first", "second"]
    );
}

#[test]
fn dependency_cycle_rejects_members_without_blocking_independent_work() {
    let executor = ConcurrentExecutor::default();
    let report = run_tables(
        vec![
            table("a", &["b"], &["work"]),
            table("b", &["a"], &["work"]),
            table("other", &[], &["work"]),
        ],
        2,
        &executor,
    );
    assert_eq!(report.tables[0].status, TableSchemaStatus::Failed);
    assert_eq!(report.tables[1].status, TableSchemaStatus::Failed);
    assert_eq!(report.tables[2].status, TableSchemaStatus::Converged);
    assert_eq!(
        executor.state.0.lock().unwrap().events,
        ["start:other:work", "end:other:work"]
    );
}
