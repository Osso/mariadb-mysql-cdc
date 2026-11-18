use super::*;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

const SOURCE_SQL: &str = "CREATE USER 'fixture'@'127.0.0.%' IDENTIFIED VIA mysql_native_password USING '*AB0347385E7793A014B4D9C33308736D4362FB3C' REQUIRE SSL ACCOUNT LOCK";
const SYNTHETIC_SECRET: &str = "*AB0347385E7793A014B4D9C33308736D4362FB3C";
const TARGET_SQL: &str = "CREATE USER 'fixture'@'127.0.0.%' IDENTIFIED WITH mysql_native_password AS '*AB0347385E7793A014B4D9C33308736D4362FB3C' REQUIRE SSL ACCOUNT LOCK";

struct AccountCheckpointStore;

impl StreamCheckpointStore for AccountCheckpointStore {
    fn load_checkpoint(&self) -> Result<Option<crate::checkpoint::Checkpoint>, ApplyBinlogError> {
        Ok(Some(test_checkpoint("mysqld-bin.000777", 4)))
    }

    fn save_checkpoint(
        &self,
        _checkpoint: &crate::checkpoint::Checkpoint,
    ) -> Result<(), ApplyBinlogError> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum CreateUserFailure {
    Uncertain,
    Duplicate,
}

#[derive(Clone)]
struct AccountTarget {
    operations: Rc<RefCell<Vec<&'static str>>>,
    statements: Rc<RefCell<Vec<String>>>,
    failure: Option<CreateUserFailure>,
    locked_account: Rc<Cell<bool>>,
    fail_unlock_once: Rc<Cell<bool>>,
    locked_checkpoint: Option<crate::checkpoint::Checkpoint>,
    saved_positions: Rc<RefCell<Vec<u64>>>,
}

impl AccountTarget {
    fn new(operations: Rc<RefCell<Vec<&'static str>>>) -> Self {
        Self {
            operations,
            statements: Rc::new(RefCell::new(Vec::new())),
            failure: None,
            locked_account: Rc::new(Cell::new(true)),
            fail_unlock_once: Rc::new(Cell::new(false)),
            locked_checkpoint: Some(test_checkpoint("mysqld-bin.000777", 4)),
            saved_positions: Rc::new(RefCell::new(Vec::new())),
        }
    }

    fn failing(failure: CreateUserFailure) -> Self {
        let operations = Rc::new(RefCell::new(Vec::new()));
        Self {
            failure: Some(failure),
            ..Self::new(operations)
        }
    }
}

impl TargetExecutor for AccountTarget {
    fn execute(
        &self,
        statement: &crate::target::SqlStatement,
    ) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("EXEC");
        self.statements.borrow_mut().push(statement.sql.clone());
        if statement.sql.starts_with("CREATE USER") {
            match self.failure {
                Some(CreateUserFailure::Uncertain) => {
                    return Err(crate::target::TargetExecuteError::new(format!(
                        "driver echoed {SYNTHETIC_SECRET}"
                    )));
                }
                Some(CreateUserFailure::Duplicate) => {
                    return Err(crate::target::TargetExecuteError::from_mysql(
                        1396,
                        format!("account already exists: {SYNTHETIC_SECRET}"),
                    ));
                }
                None => {}
            }
        }
        if statement.sql.starts_with("ALTER USER") {
            self.locked_account.set(false);
            if self.fail_unlock_once.replace(false) {
                return Err(crate::target::TargetExecuteError::new(
                    "connection lost after target applied account unlock",
                ));
            }
        }
        Ok(())
    }
}

impl crate::target::TransactionalTargetExecutor for AccountTarget {
    fn acquire_stream_lease(
        &self,
        _lease_name: &str,
    ) -> Result<(), crate::target::TargetExecuteError> {
        Ok(())
    }

    fn begin_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("BEGIN");
        Ok(())
    }

    fn load_transaction_checkpoint_for_update(
        &self,
        _checkpoint_table: &str,
        _checkpoint_name: &str,
    ) -> Result<Option<crate::checkpoint::Checkpoint>, crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("LOCK_CHECKPOINT");
        Ok(self.locked_checkpoint.clone())
    }

    fn save_transaction_checkpoint(
        &self,
        _checkpoint_table: &str,
        _checkpoint_name: &str,
        checkpoint: &crate::checkpoint::Checkpoint,
    ) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("CHECKPOINT");
        self.saved_positions
            .borrow_mut()
            .push(checkpoint.source_position);
        Ok(())
    }

    fn commit_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("COMMIT");
        Ok(())
    }

    fn rollback_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("ROLLBACK");
        Ok(())
    }
}

const UNLOCK_SQL: &str = "ALTER USER 'fixture'@'127.0.0.%' ACCOUNT UNLOCK";

fn unlock_event_key(sql: &str, file: &str, event_start: u64) -> String {
    use sha2::{Digest, Sha256};

    let identity_digest = Sha256::digest(b"production-source#server-id=1");
    let digest = Sha256::digest(sql.as_bytes());
    format!("{identity_digest:x}:{file}:{event_start}:{digest:x}")
}

fn run_account_unlock(
    target: &AccountTarget,
    journal: &RecordingDdlReplayJournal,
    approval: Option<String>,
) -> Result<Option<StructuredEventOutcome>, ApplyBinlogError> {
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("globalcomix".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "globalcomix".to_string(),
        sql_statement: UNLOCK_SQL.to_string(),
    });
    let checkpoint_store = AccountCheckpointStore;
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: Some(&checkpoint_store),
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };

    super::super::account_ddl::handle_account_ddl(
        target,
        journal,
        "production-source",
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
        || approval,
    )
}

fn run_create_user(
    target: &AccountTarget,
    journal: &RecordingDdlReplayJournal,
    sql: &str,
    database_name: &str,
) -> Result<Option<StructuredEventOutcome>, ApplyBinlogError> {
    let mut applier = crate::row::RowApplier::new(target.clone());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("globalcomix".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: database_name.to_string(),
        sql_statement: sql.to_string(),
    });
    let checkpoint_store = AccountCheckpointStore;
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: Some(&checkpoint_store),
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };

    handle_ddl_event(
        &mut applier,
        journal,
        &RecordingSemanticInventory::default(),
        "production-source",
        &mut context,
        &event_header(2, 180),
        &event,
    )
}

#[test]
fn locked_tls_user_is_prepared_executed_and_checkpointed_without_journaling_sql() {
    let operations = Rc::new(RefCell::new(Vec::new()));
    let target = AccountTarget::new(operations.clone());
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());

    let outcome = run_create_user(&target, &journal, SOURCE_SQL, "globalcomix")
        .expect("locked TLS account should replay")
        .expect("account DDL outcome");

    assert_eq!(outcome.policy, EventPolicy::CommitTransaction);
    assert_eq!(outcome.resume_coordinate.unwrap().position, 180);
    assert_eq!(
        target.statements.borrow().as_slice(),
        &[
            TARGET_SQL,
            "UPDATE cdc.ddl_replay_journal SET status='checkpointed'"
        ]
    );
    assert_eq!(
        operations.borrow().as_slice(),
        &[
            "PREPARE",
            "EXEC",
            "APPLIED",
            "BEGIN",
            "LOCK_CHECKPOINT",
            "EXEC",
            "CHECKPOINT",
            "COMMIT"
        ]
    );
    assert_eq!(target.saved_positions.borrow().as_slice(), &[180]);

    let evidence = journal.evidence.borrow();
    let evidence = evidence.as_ref().expect("journal evidence");
    assert_eq!(evidence.generated_sql, None);
    assert!(!evidence.canonical_ast.contains(SYNTHETIC_SECRET));
}

#[test]
fn target_failure_and_duplicate_keep_checkpoint_blocked_and_return_safe_errors() {
    for failure in [CreateUserFailure::Uncertain, CreateUserFailure::Duplicate] {
        let target = AccountTarget::failing(failure);
        let journal = RecordingDdlReplayJournal::with_operations(target.operations.clone());

        let error = run_create_user(&target, &journal, SOURCE_SQL, "globalcomix")
            .expect_err("unconfirmed account creation must block checkpoint advancement");

        assert!(error.to_string().contains("account"), "{error}");
        assert!(!error.to_string().contains(SYNTHETIC_SECRET), "{error}");
        assert_eq!(*journal.status.borrow(), Some(DdlReplayStatus::Prepared));
        assert_eq!(target.operations.borrow().as_slice(), &["PREPARE", "EXEC"]);
        assert!(target.saved_positions.borrow().is_empty());
        assert_eq!(target.statements.borrow().as_slice(), &[TARGET_SQL]);
    }
}

#[test]
fn applied_account_event_restart_checkpoints_without_executing_create_again() {
    let operations = Rc::new(RefCell::new(Vec::new()));
    let target = AccountTarget::new(operations.clone());
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());

    run_create_user(&target, &journal, SOURCE_SQL, "globalcomix")
        .expect("initial replay")
        .expect("initial outcome");
    *journal.status.borrow_mut() = Some(DdlReplayStatus::Applied);
    operations.borrow_mut().clear();
    target.statements.borrow_mut().clear();
    target.saved_positions.borrow_mut().clear();

    run_create_user(&target, &journal, SOURCE_SQL, "globalcomix")
        .expect("applied restart should finalize")
        .expect("restart outcome");

    assert_eq!(
        operations.borrow().as_slice(),
        &["BEGIN", "LOCK_CHECKPOINT", "EXEC", "CHECKPOINT", "COMMIT"]
    );
    assert_eq!(
        target.statements.borrow().as_slice(),
        &["UPDATE cdc.ddl_replay_journal SET status='checkpointed'"]
    );
    assert_eq!(target.saved_positions.borrow().as_slice(), &[180]);
}

#[test]
fn unsupported_create_user_is_left_for_translation_barrier_without_target_execution() {
    let operations = Rc::new(RefCell::new(Vec::new()));
    let target = AccountTarget::new(operations.clone());
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    let unsupported_sql =
        "CREATE USER 'fixture'@'127.0.0.%' IDENTIFIED BY 'not-translated' ACCOUNT LOCK";

    let error = run_create_user(&target, &journal, unsupported_sql, "")
        .expect_err("unsupported account syntax must remain behind the translation barrier");

    assert!(error.to_string().contains("checkpoint remains blocked"));
    assert_eq!(
        *journal.status.borrow(),
        Some(DdlReplayStatus::TranslationPending)
    );
    assert!(target.statements.borrow().is_empty());
    assert!(target.saved_positions.borrow().is_empty());
}

#[test]
fn account_unlock_requires_exact_source_event_approval_before_any_target_effect() {
    let correct_key = unlock_event_key(UNLOCK_SQL, "mysqld-bin.000777", 161);
    for approval in [
        None,
        Some(unlock_event_key(UNLOCK_SQL, "mysqld-bin.000777", 162)),
        Some(unlock_event_key(
            "ALTER USER 'fixture'@'127.0.0.%' ACCOUNT LOCK",
            "mysqld-bin.000777",
            161,
        )),
    ] {
        assert_ne!(approval.as_deref(), Some(correct_key.as_str()));
        let operations = Rc::new(RefCell::new(Vec::new()));
        let target = AccountTarget::new(operations.clone());
        let journal = RecordingDdlReplayJournal::with_operations(operations.clone());

        assert!(
            run_account_unlock(&target, &journal, approval)
                .expect("unapproved event is not handled")
                .is_none()
        );

        assert!(target.locked_account.get());
        assert!(target.statements.borrow().is_empty());
        assert!(target.saved_positions.borrow().is_empty());
        assert!(operations.borrow().is_empty());
        assert_eq!(*journal.status.borrow(), None);
    }
}

#[test]
fn approved_legacy_translation_pending_unlock_is_applied_and_checkpointed() {
    let operations = Rc::new(RefCell::new(Vec::new()));
    let target = AccountTarget::new(operations.clone());
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    *journal.status.borrow_mut() = Some(DdlReplayStatus::TranslationPending);

    let outcome = run_account_unlock(
        &target,
        &journal,
        Some(unlock_event_key(UNLOCK_SQL, "mysqld-bin.000777", 161)),
    )
    .expect("approved pending unlock replays")
    .expect("handled account unlock outcome");

    assert_eq!(outcome.policy, EventPolicy::CommitTransaction);
    assert_eq!(outcome.resume_coordinate.unwrap().position, 180);
    assert!(!target.locked_account.get());
    assert_eq!(
        target.statements.borrow().as_slice(),
        &[
            UNLOCK_SQL,
            "UPDATE cdc.ddl_replay_journal SET status='checkpointed'"
        ]
    );
    assert_eq!(target.saved_positions.borrow().as_slice(), &[180]);
    assert_eq!(*journal.status.borrow(), Some(DdlReplayStatus::Applied));
    assert_eq!(
        operations.borrow().as_slice(),
        &[
            "PROMOTE",
            "EXEC",
            "APPLIED",
            "BEGIN",
            "LOCK_CHECKPOINT",
            "EXEC",
            "CHECKPOINT",
            "COMMIT"
        ]
    );
}

#[test]
fn uncertain_unlock_result_retries_prepared_event_then_checkpoints() {
    let operations = Rc::new(RefCell::new(Vec::new()));
    let target = AccountTarget::new(operations.clone());
    target.fail_unlock_once.set(true);
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    *journal.status.borrow_mut() = Some(DdlReplayStatus::TranslationPending);
    let approval = Some(unlock_event_key(UNLOCK_SQL, "mysqld-bin.000777", 161));

    let error = run_account_unlock(&target, &journal, approval.clone())
        .expect_err("uncertain target acknowledgment must leave checkpoint blocked");
    assert!(
        error
            .to_string()
            .contains("target unlock was not confirmed")
    );
    assert!(!target.locked_account.get());
    assert_eq!(*journal.status.borrow(), Some(DdlReplayStatus::Prepared));
    assert!(target.saved_positions.borrow().is_empty());
    assert_eq!(target.statements.borrow().as_slice(), &[UNLOCK_SQL]);

    operations.borrow_mut().clear();
    target.statements.borrow_mut().clear();
    run_account_unlock(&target, &journal, approval)
        .expect("prepared unlock retry succeeds")
        .expect("handled account unlock outcome");

    assert!(!target.locked_account.get());
    assert_eq!(*journal.status.borrow(), Some(DdlReplayStatus::Applied));
    assert_eq!(target.saved_positions.borrow().as_slice(), &[180]);
    assert_eq!(
        target.statements.borrow().as_slice(),
        &[
            UNLOCK_SQL,
            "UPDATE cdc.ddl_replay_journal SET status='checkpointed'"
        ]
    );
    assert_eq!(
        operations.borrow().as_slice(),
        &[
            "EXEC",
            "APPLIED",
            "BEGIN",
            "LOCK_CHECKPOINT",
            "EXEC",
            "CHECKPOINT",
            "COMMIT"
        ]
    );
}
