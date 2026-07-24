use super::*;
use crate::row::DeferredSupersededInsertCandidate;

#[test]
fn superseded_verification_classifies_only_semantic_rejections_as_retryable() {
    assert!(matches!(
        superseded_verification_error(
            "superseded release insert rejected: TargetParentMismatch".to_string()
        ),
        ApplyBinlogError::SupersededRecoveryFailed(_)
    ));
    assert!(matches!(
        superseded_verification_error(
            "superseded release target evidence failed: permission denied".to_string()
        ),
        ApplyBinlogError::Target(_)
    ));
}

/// A candidate outside the verifier's supported scope is a semantic rejection, not an
/// infrastructure failure. Classifying it as fatal crash-looped the production stream: `releases`
/// 384461 at `mysqld-bin.002710:656283581` hit `releases_ibfk_2` but not the pinned recovery
/// coordinate, and the stream restarted 7 times with `ready=false` instead of persisting a conflict
/// and reconnecting.
#[test]
fn superseded_scope_rejections_are_retryable_not_fatal() {
    for message in [
        "superseded release insert rejected: requires exact production transaction \
         mysqld-bin.002709:515816736-515824875",
        "superseded release insert rejected: requires exact globalcomix.releases releases_ibfk_2 \
         INSERT FK 1452",
        "superseded release insert rejected: historical change must be INSERT",
        "superseded insert rejected: requires globalcomix.users/users.name or \
         globalcomix.comics/comics.slug",
        "superseded insert rejected: requires INSERT",
        "superseded insert rejected: historical change must be INSERT",
    ] {
        assert!(
            matches!(
                superseded_verification_error(message.to_string()),
                ApplyBinlogError::SupersededRecoveryFailed(_)
            ),
            "expected a retryable rejection for {message}"
        );
    }
}

#[test]
fn table_map_and_row_events_do_not_checkpoint_without_transaction_boundary() {
    let table_map = BinlogEvent::TableMapEvent(accounts_table_map_event(5));
    let write = BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id: 18,
        flags: 0,
        columns_number: 5,
        columns_present: vec![true; 5],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(1)),
            Some(MySqlValue::String("alpha".to_string())),
            Some(MySqlValue::Int(100)),
            Some(MySqlValue::String("safe".to_string())),
            Some(MySqlValue::DateTime(DateTime {
                year: 2026,
                month: 6,
                day: 22,
                hour: 12,
                minute: 3,
                second: 4,
                millis: 0,
            })),
        ])],
    });

    assert_eq!(
        classify_event("mysqld-bin.000777", &event_header(19, 200), &table_map).resume_coordinate,
        None
    );
    assert_eq!(
        classify_event("mysqld-bin.000777", &event_header(30, 220), &write).resume_coordinate,
        None
    );
}

#[test]
fn xid_event_checkpoints_after_transaction_rows_are_applied() {
    let event = BinlogEvent::XidEvent(XidEvent { xid: 42 });
    let header = event_header(16, 260);

    let outcome = classify_event("mysqld-bin.000777", &header, &event);

    assert_eq!(outcome.policy, EventPolicy::CommitTransaction);
    assert_eq!(
        outcome.resume_coordinate,
        Some(BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 260,
        })
    );
}

fn guests_table_map_event(table_id: u64) -> MysqlCdcTableMapEvent {
    MysqlCdcTableMapEvent {
        table_id,
        database_name: "fixture_cdc".to_string(),
        table_name: "guests".to_string(),
        column_types: vec![8, 254],
        column_metadata: vec![0, 0],
        null_bitmap: vec![false, false],
        table_metadata: None,
    }
}

fn sessions_table_map_event(table_id: u64) -> MysqlCdcTableMapEvent {
    MysqlCdcTableMapEvent {
        table_id,
        database_name: "fixture_cdc".to_string(),
        table_name: "sessions".to_string(),
        column_types: vec![8, 8, 254],
        column_metadata: vec![0, 0, 0],
        null_bitmap: vec![false, false, false],
        table_metadata: None,
    }
}

fn guest_write_rows_event(table_id: u64) -> BinlogEvent {
    BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id,
        flags: 0,
        columns_number: 2,
        columns_present: vec![true, true],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(78_806_710)),
            Some(MySqlValue::String(
                "02f12400-1020-4c7b-907b-0613c292bcd6MD3X".to_string(),
            )),
        ])],
    })
}

fn sessions_write_rows_event(table_id: u64) -> BinlogEvent {
    BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id,
        flags: 0,
        columns_number: 3,
        columns_present: vec![true, true, true],
        rows: vec![session_row(109_017_694)],
    })
}

fn sessions_write_rows_event_with_conflict_followup(table_id: u64) -> BinlogEvent {
    BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id,
        flags: 0,
        columns_number: 3,
        columns_present: vec![true, true, true],
        rows: vec![
            session_row(109_017_693),
            session_row(109_017_694),
            session_row(109_017_695),
        ],
    })
}

fn session_row(session_id: u32) -> RowData {
    RowData::new(vec![
        Some(MySqlValue::Int(session_id)),
        Some(MySqlValue::Int(78_806_710)),
        Some(MySqlValue::String(
            "02f12400-1020-4c7b-907b-0613c292bcd6MD3X".to_string(),
        )),
    ])
}

#[test]
fn source_xid_boundary_keeps_parent_committed_when_stream_fails_after_child_transaction() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();
    let group_config = TargetTransactionGroupConfig {
        size: 25,
        timeout: Duration::ZERO,
    };
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&NoopCheckpointStore),
                transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
                transaction_checkpoint_name: Some("stream-binlog:test-source"),
                current_file: &mut current_file,
                group_config,
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 215329700),
        BinlogEvent::TableMapEvent(guests_table_map_event(19))
    )
    .expect("guests table map");
    process_event!(
        event_header(19, 215329720),
        BinlogEvent::TableMapEvent(sessions_table_map_event(20))
    )
    .expect("sessions table map");
    process_event!(event_header(30, 215329760), guest_write_rows_event(19))
        .expect("parent write in XID A");
    process_event!(
        event_header(16, 215329780),
        BinlogEvent::XidEvent(XidEvent { xid: 101 })
    )
    .expect("XID A");
    process_event!(event_header(30, 215329892), sessions_write_rows_event(20))
        .expect("child write in XID B");
    process_event!(
        event_header(16, 215329912),
        BinlogEvent::XidEvent(XidEvent { xid: 102 })
    )
    .expect("XID B");

    transaction
        .rollback_if_open(applier.executor())
        .expect("inject stream failure after XID B");

    assert_eq!(
        applier.executor().operations().as_slice(),
        [
            "BEGIN",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "COMMIT",
            "BEGIN",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "COMMIT",
        ]
    );
}

#[test]
fn staged_success_resolution_is_discarded_on_target_rollback() {
    let executor = TransactionRecordingExecutor::default();
    let mut transaction = TargetTransaction::default();
    transaction
        .begin_if_needed(&executor)
        .expect("begin target transaction");
    transaction.pending_conflict_resolutions_mut().push(
        crate::conflict_repair::ConflictResolution {
            source_identity: "source".to_string(),
            schema: "fixture_cdc".to_string(),
            table: "accounts".to_string(),
            source_primary_key: vec!["1".to_string()],
            repair_run_id: "run".to_string(),
            evidence: "successful replay".to_string(),
        },
    );

    transaction
        .rollback_if_open(&executor)
        .expect("rollback target transaction");

    assert!(!transaction.has_pending_conflict_resolutions());
    assert_eq!(executor.operations(), ["BEGIN", "ROLLBACK"]);
}

#[derive(Clone, Copy)]
struct SupersededVerificationFixture {
    verified: bool,
}

impl SupersededInsertVerifier for SupersededVerificationFixture {
    fn verify(
        &mut self,
        _candidate: &DeferredSupersededInsertCandidate,
        _xid_end_position: u64,
    ) -> Result<super::super::superseded_insert::SupersededInsertProof, String> {
        if !self.verified {
            return Err("target transactional re-read changed".to_string());
        }
        Ok(super::super::superseded_insert::SupersededInsertProof {
            source_snapshot: super::super::superseded_insert::BinlogCoordinate {
                file: "mysqld-bin.002740".to_string(),
                position: 1_004_163_590,
            },
            historical_image_hash: "historical-hash".to_string(),
            source_primary_hash: "source-pk-hash".to_string(),
            source_owner_hash: "source-owner-hash".to_string(),
            target_primary_hash: "source-pk-hash".to_string(),
            target_owner_hash: "source-owner-hash".to_string(),
            current_row_install: (_candidate.observation.table == "releases").then(|| {
                crate::target::SqlStatement {
                    sql: "INSERT INTO `globalcomix`.`releases` (`id`,`comic_id`,`comic_category_id`) VALUES (?,?,?)".to_string(),
                    params: vec![
                        mysql::Value::UInt(77),
                        mysql::Value::UInt(12),
                        mysql::Value::UInt(9),
                    ],
                }
            }),
        })
    }
}

fn releases_category_superseded_candidate() -> DeferredSupersededInsertCandidate {
    DeferredSupersededInsertCandidate {
        observation: crate::conflict_repair::ConflictObservation {
            source_identity: "production-source".to_string(),
            source_server_id: 3,
            coordinate: crate::conflict_repair::ConflictCoordinate {
                file: "mysqld-bin.002709".to_string(),
                start_position: 515_816_736,
                end_position: 0,
            },
            schema: "globalcomix".to_string(),
            table: "releases".to_string(),
            operation: crate::conflict_repair::ConflictOperation::Insert,
            source_primary_key: vec!["77".to_string()],
            duplicate_index: None,
            duplicate_owner_primary_key: None,
            error_code: 1452,
            error_text: "Cannot add or update a child row: a foreign key constraint fails (`globalcomix`.`releases`, CONSTRAINT `releases_ibfk_2` FOREIGN KEY (`comic_id`, `comic_category_id`) REFERENCES `comics` (`id`, `section_id`))".to_string(),
            observed_at_ms: 1,
            parent_recovery: None,
        },
        historical_change: crate::target::TargetRowChange {
            statement: crate::target::SqlStatement {
                sql: "INSERT historical release".to_string(),
                params: Vec::new(),
            },
            kind: crate::target::TargetRowChangeKind::Insert,
            table: "globalcomix.releases".to_string(),
            primary_key_columns: vec!["id".to_string()],
            primary_key_values: vec![mysql::Value::UInt(77)],
            writable_columns: vec![
                "id".to_string(),
                "comic_id".to_string(),
                "comic_category_id".to_string(),
            ],
            source_values: vec![
                mysql::Value::UInt(77),
                mysql::Value::UInt(12),
                mysql::Value::UInt(4),
            ],
            set_columns: vec![None, None, None],
        },
    }
}

#[test]
fn verified_superseded_release_installs_current_row_before_evidence_checkpoint_and_commit() {
    let executor = TransactionRecordingExecutor::with_locked_checkpoint(checkpoint_at(
        "mysqld-bin.002709",
        515_816_517,
    ));
    let mut transaction = TargetTransaction::default();
    transaction.begin_if_needed(&executor).expect("begin");
    transaction.defer_superseded_insert(releases_category_superseded_candidate());
    let mut verifier = SupersededVerificationFixture { verified: true };

    transaction
        .verify_deferred_superseded_inserts_at_xid(
            &executor,
            &mut verifier,
            SupersededXidCommitContext {
                xid_end_position: 515_824_875,
                checkpoint_table: "cdc.stream_checkpoint",
                checkpoint_name: "stream-binlog:test-source",
                conflict_table: "cdc.row_conflicts",
                #[cfg(feature = "integration-failpoints")]
                logical_checkpoint_predecessor: None,
            },
        )
        .expect("release recovery commits atomically");

    assert_eq!(
        executor.operations(),
        [
            "BEGIN",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "OBSERVATION",
            "RESOLUTION",
            "COMMIT",
        ]
    );
}

fn users_name_superseded_candidate() -> DeferredSupersededInsertCandidate {
    DeferredSupersededInsertCandidate {
        observation: crate::conflict_repair::ConflictObservation {
            source_identity: "production-source".to_string(),
            source_server_id: 3,
            coordinate: crate::conflict_repair::ConflictCoordinate {
                file: "mysqld-bin.002709".to_string(),
                start_position: 404_034_840,
                end_position: 0,
            },
            schema: "globalcomix".to_string(),
            table: "users".to_string(),
            operation: crate::conflict_repair::ConflictOperation::Insert,
            source_primary_key: vec!["2070980".to_string()],
            duplicate_index: Some("users.name".to_string()),
            duplicate_owner_primary_key: Some(vec!["2071305".to_string()]),
            error_code: 1062,
            error_text: "Duplicate entry '-3572' for key 'users.name'".to_string(),
            observed_at_ms: 1,
            parent_recovery: None,
        },
        historical_change: crate::target::TargetRowChange {
            statement: crate::target::SqlStatement {
                sql: "INSERT INTO `globalcomix`.`users` (`id`,`name`) VALUES (?,?)".to_string(),
                params: vec![
                    mysql::Value::Int(2_070_980),
                    mysql::Value::Bytes(b"-3572".to_vec()),
                ],
            },
            kind: crate::target::TargetRowChangeKind::Insert,
            table: "globalcomix.users".to_string(),
            primary_key_columns: vec!["id".to_string()],
            primary_key_values: vec![mysql::Value::Int(2_070_980)],
            writable_columns: vec!["id".to_string(), "name".to_string()],
            source_values: vec![
                mysql::Value::Int(2_070_980),
                mysql::Value::Bytes(b"-3572".to_vec()),
            ],
            set_columns: vec![None, None],
        },
    }
}

fn comics_slug_superseded_candidate() -> DeferredSupersededInsertCandidate {
    let mut candidate = users_name_superseded_candidate();
    candidate.observation.coordinate.start_position = 531_241_142;
    candidate.observation.table = "comics".to_string();
    candidate.observation.source_primary_key = vec!["48054".to_string()];
    candidate.observation.duplicate_index = Some("comics.slug".to_string());
    candidate.observation.duplicate_owner_primary_key = Some(vec!["48058".to_string()]);
    candidate.observation.error_text = "Duplicate entry 'misc' for key 'comics.slug'".to_string();
    candidate.historical_change.table = "globalcomix.comics".to_string();
    candidate.historical_change.writable_columns = vec!["id".to_string(), "slug".to_string()];
    candidate.historical_change.primary_key_values = vec![mysql::Value::Int(48_054)];
    candidate.historical_change.source_values = vec![
        mysql::Value::Int(48_054),
        mysql::Value::Bytes(b"misc".to_vec()),
    ];
    candidate
}

struct ComicsPredicateVerifier {
    target_owner_primary_key: String,
    target_owner_identity: String,
}

impl ComicsPredicateVerifier {
    fn matching_owner() -> Self {
        Self {
            target_owner_primary_key: "48058".to_string(),
            target_owner_identity: "misc".to_string(),
        }
    }
}

impl SupersededInsertVerifier for ComicsPredicateVerifier {
    fn verify(
        &mut self,
        _candidate: &DeferredSupersededInsertCandidate,
        _xid_end_position: u64,
    ) -> Result<super::super::superseded_insert::SupersededInsertProof, String> {
        let input = super::super::superseded_insert::SupersededInsertVerificationInput {
            schema: "globalcomix".to_string(),
            table: "comics".to_string(),
            operation: crate::conflict_repair::ConflictOperation::Insert,
            duplicate_index: "comics.slug".to_string(),
            candidate_xid: super::super::superseded_insert::BinlogCoordinate {
                file: "mysqld-bin.002709".to_string(),
                position: 531_241_781,
            },
            source_snapshot: super::super::superseded_insert::BinlogCoordinate {
                file: "mysqld-bin.002743".to_string(),
                position: 600_000_000,
            },
            historical_primary_key: "48054".to_string(),
            historical_name: "misc".to_string(),
            historical_image_hash: "historical-hash".to_string(),
            source_primary_row_count: 1,
            source_primary_name: "DELETED_misc".to_string(),
            source_primary_hash: "current-primary-hash".to_string(),
            source_owner_row_count: 1,
            source_owner_primary_key: "48058".to_string(),
            source_owner_hash: "current-owner-hash".to_string(),
            target_rows_read_for_update: true,
            target_primary_row_count: 1,
            target_primary_hash: "current-primary-hash".to_string(),
            target_owner_row_count: 1,
            target_owner_primary_key: self.target_owner_primary_key.clone(),
            target_owner_identity: self.target_owner_identity.clone(),
            target_owner_hash: "lagged-mutable-owner-hash".to_string(),
        };
        super::super::superseded_insert::verify_superseded_insert(&input)
            .map_err(|rejection| format!("superseded insert rejected: {rejection:?}"))
    }
}

fn comics_xid_context() -> SupersededXidCommitContext<'static> {
    SupersededXidCommitContext {
        xid_end_position: 531_241_781,
        checkpoint_table: "cdc.stream_checkpoint",
        checkpoint_name: "stream-binlog:production-source",
        conflict_table: "cdc.row_conflicts",
        #[cfg(feature = "integration-failpoints")]
        logical_checkpoint_predecessor: None,
    }
}

#[test]
fn comics_slug_supersession_commits_resolution_and_checkpoint_atomically() {
    let executor = TransactionRecordingExecutor::with_locked_checkpoint(checkpoint_at(
        "mysqld-bin.002709",
        531_240_959,
    ));
    let mut transaction = TargetTransaction::default();
    transaction.begin_if_needed(&executor).expect("begin");
    transaction.defer_superseded_insert(comics_slug_superseded_candidate());
    let mut verifier = ComicsPredicateVerifier::matching_owner();

    let proof = transaction
        .verify_deferred_superseded_inserts_at_xid(&executor, &mut verifier, comics_xid_context())
        .expect("verified comics transaction commits atomically");

    assert_eq!(proof.checkpoint.source_position, 531_241_781);
    assert_eq!(
        executor.operations(),
        [
            "BEGIN",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "OBSERVATION",
            "RESOLUTION",
            "COMMIT"
        ]
    );
}

fn assert_comics_owner_mismatch_rolls_back(mut verifier: ComicsPredicateVerifier) {
    let executor = TransactionRecordingExecutor::default();
    let mut transaction = TargetTransaction::default();
    transaction.begin_if_needed(&executor).expect("begin");
    transaction.defer_superseded_insert(comics_slug_superseded_candidate());

    transaction
        .verify_deferred_superseded_inserts_at_xid(&executor, &mut verifier, comics_xid_context())
        .expect_err("mismatched owner identity fails closed");

    assert_eq!(executor.operations(), ["BEGIN", "ROLLBACK"]);
}

#[test]
fn comics_slug_owner_primary_key_mismatch_rolls_back_without_checkpoint() {
    let mut verifier = ComicsPredicateVerifier::matching_owner();
    verifier.target_owner_primary_key = "48059".to_string();
    assert_comics_owner_mismatch_rolls_back(verifier);
}

#[test]
fn comics_slug_owner_identity_mismatch_rolls_back_without_checkpoint() {
    let mut verifier = ComicsPredicateVerifier::matching_owner();
    verifier.target_owner_identity = "other".to_string();
    assert_comics_owner_mismatch_rolls_back(verifier);
}

#[test]
fn superseded_insert_verification_failure_rolls_back_later_rows_without_checkpoint() {
    let executor = TransactionRecordingExecutor::default();
    let mut transaction = TargetTransaction::default();
    transaction.begin_if_needed(&executor).expect("begin");
    transaction.defer_superseded_insert(users_name_superseded_candidate());
    executor
        .execute(&crate::target::SqlStatement {
            sql: "INSERT INTO globalcomix.users_profiles (id) VALUES (?)".to_string(),
            params: vec![mysql::Value::Int(2_070_980)],
        })
        .expect("subsequent row executes before XID verification");

    let mut verifier = SupersededVerificationFixture { verified: false };
    let error = transaction
        .verify_deferred_superseded_inserts_at_xid(
            &executor,
            &mut verifier,
            superseded_xid_context("cdc.row_conflicts"),
        )
        .expect_err("failed proof must abort the complete source transaction");

    assert!(
        error
            .to_string()
            .contains("target transactional re-read changed")
    );
    assert_eq!(executor.operations(), ["BEGIN", "EXEC", "ROLLBACK"]);
}

#[test]
fn verified_superseded_insert_commits_later_rows_resolution_and_xid_checkpoint_atomically() {
    let executor = exact_users_predecessor_executor();
    let mut transaction = TargetTransaction::default();
    transaction.begin_if_needed(&executor).expect("begin");
    transaction.defer_superseded_insert(users_name_superseded_candidate());
    executor
        .execute(&crate::target::SqlStatement {
            sql: "INSERT INTO globalcomix.users_profiles (id) VALUES (?)".to_string(),
            params: vec![mysql::Value::Int(2_070_980)],
        })
        .expect("subsequent row executes before XID verification");

    let mut verifier = SupersededVerificationFixture { verified: true };
    let proof = transaction
        .verify_deferred_superseded_inserts_at_xid(
            &executor,
            &mut verifier,
            superseded_xid_context("cdc.row_conflicts"),
        )
        .expect("verified candidate commits atomically");

    assert!(
        proof
            .resolution_evidence
            .contains("mysqld-bin.002740:1004163590")
    );
    assert!(proof.resolution_evidence.contains("source-pk-hash"));
    assert_eq!(
        executor.operations(),
        [
            "BEGIN",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "OBSERVATION",
            "RESOLUTION",
            "COMMIT",
        ]
    );
}

fn execute_users_2070980_followup_effects(executor: &TransactionRecordingExecutor) {
    executor
        .execute(&crate::target::SqlStatement {
            sql: "REPLACE INTO globalcomix.users_profiles (id) VALUES (?)".to_string(),
            params: vec![mysql::Value::Int(2_070_980)],
        })
        .expect("users_profiles effect");
    for setting_group_id in 2..=9 {
        executor
            .execute(&crate::target::SqlStatement {
                sql: "INSERT INTO globalcomix.users_email_settings (user_id,setting_group_id) VALUES (?,?)"
                    .to_string(),
                params: vec![
                    mysql::Value::Int(2_070_980),
                    mysql::Value::Int(setting_group_id),
                ],
            })
            .expect("users_email_settings effect");
    }
}

fn exact_users_2070980_verifier(verified: bool) -> SupersededVerificationFixture {
    SupersededVerificationFixture { verified }
}

fn superseded_xid_context(conflict_table: &str) -> SupersededXidCommitContext<'_> {
    SupersededXidCommitContext {
        xid_end_position: 404_038_011,
        checkpoint_table: "cdc.stream_checkpoint",
        checkpoint_name: "stream-binlog:production-source",
        conflict_table,
        #[cfg(feature = "integration-failpoints")]
        logical_checkpoint_predecessor: None,
    }
}

fn checkpoint_at(file: &str, position: u64) -> crate::checkpoint::Checkpoint {
    crate::checkpoint::Checkpoint {
        source_file: file.to_string(),
        source_position: position,
        gtid: None,
        event_timestamp: 0,
        last_event: crate::checkpoint::LastEvent {
            event_type: "XidEvent".to_string(),
            description: "test predecessor".to_string(),
        },
    }
}

fn exact_users_predecessor_executor() -> TransactionRecordingExecutor {
    TransactionRecordingExecutor::with_locked_checkpoint(checkpoint_at(
        "mysqld-bin.002709",
        404_034_720,
    ))
}

fn ordinary_conflict_observation() -> crate::conflict_repair::ConflictObservation {
    let mut observation = users_name_superseded_candidate().observation;
    observation.table = "accounts".to_string();
    observation.source_primary_key = vec!["99".to_string()];
    observation.coordinate.start_position = 404_035_100;
    observation.error_text = "ordinary conflict".to_string();
    observation
}

#[test]
fn production_404034840_superseded_users_insert_commits_all_followup_effects_at_xid() {
    let executor = exact_users_predecessor_executor();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    transaction
        .begin_if_needed(&executor)
        .expect("begin source transaction");
    transaction.defer_superseded_insert(users_name_superseded_candidate());

    execute_users_2070980_followup_effects(&executor);
    assert_eq!(
        executor.operations(),
        [
            "BEGIN", "EXEC", "EXEC", "EXEC", "EXEC", "EXEC", "EXEC", "EXEC", "EXEC", "EXEC"
        ]
    );

    let mut verifier = exact_users_2070980_verifier(true);
    let proof = transaction
        .verify_deferred_superseded_inserts_at_xid_with_conflicts(
            &executor,
            &mut verifier,
            &mut conflicts,
            superseded_xid_context("cdc.row_conflicts"),
        )
        .expect("exact production transaction commits atomically");

    assert!(
        proof
            .resolution_evidence
            .contains("mysqld-bin.002740:1004163590")
    );
    for full_row_hash in ["historical-hash", "source-pk-hash", "source-owner-hash"] {
        assert!(proof.resolution_evidence.contains(full_row_hash));
    }
    assert_eq!(proof.checkpoint.source_file, "mysqld-bin.002709");
    assert_eq!(proof.checkpoint.source_position, 404_038_011);
    assert_eq!(
        executor.operations(),
        [
            "BEGIN",
            "EXEC",
            "EXEC",
            "EXEC",
            "EXEC",
            "EXEC",
            "EXEC",
            "EXEC",
            "EXEC",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "OBSERVATION",
            "RESOLUTION",
            "COMMIT",
        ]
    );
    assert_eq!(
        conflicts.records()[0].status,
        crate::conflict_repair::ConflictStatus::Resolved
    );
}

#[test]
fn production_404034840_failed_verification_rolls_back_every_followup_effect() {
    let executor = TransactionRecordingExecutor::default();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    transaction
        .begin_if_needed(&executor)
        .expect("begin source transaction");
    transaction.defer_superseded_insert(users_name_superseded_candidate());
    execute_users_2070980_followup_effects(&executor);

    let mut verifier = exact_users_2070980_verifier(false);
    let error = transaction
        .verify_deferred_superseded_inserts_at_xid_with_conflicts(
            &executor,
            &mut verifier,
            &mut conflicts,
            superseded_xid_context("cdc.row_conflicts"),
        )
        .expect_err("failed verification rolls back complete transaction");

    assert!(
        error
            .to_string()
            .contains("target transactional re-read changed")
    );
    assert_eq!(
        executor.operations(),
        [
            "BEGIN", "EXEC", "EXEC", "EXEC", "EXEC", "EXEC", "EXEC", "EXEC", "EXEC", "EXEC",
            "ROLLBACK"
        ]
    );
    assert!(!executor.operations().contains(&"LOCK_CHECKPOINT"));
    assert!(!executor.operations().contains(&"CHECKPOINT"));
    assert!(!executor.operations().contains(&"RESOLUTION"));
    assert!(!executor.operations().contains(&"COMMIT"));
    let records = conflicts.records();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].status,
        crate::conflict_repair::ConflictStatus::Unresolved
    );
    assert_eq!(records[0].key.coordinate.start_position, 404_034_840);
    assert_eq!(records[0].key.coordinate.end_position, 404_038_011);
    assert_eq!(records[0].key.source_primary_key, ["2070980"]);
}

#[test]
fn superseded_xid_rejects_coexisting_ordinary_conflict_and_persists_both() {
    let executor = exact_users_predecessor_executor();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    transaction.begin_if_needed(&executor).expect("begin");
    transaction.defer_superseded_insert(users_name_superseded_candidate());
    transaction
        .pending_conflicts_mut()
        .1
        .push(ordinary_conflict_observation());

    let mut verifier = exact_users_2070980_verifier(true);
    let error = transaction
        .verify_deferred_superseded_inserts_at_xid_with_conflicts(
            &executor,
            &mut verifier,
            &mut conflicts,
            superseded_xid_context("cdc.row_conflicts"),
        )
        .expect_err("ordinary conflict must block superseded commit");

    assert!(error.to_string().contains("ordinary conflict"));
    assert_eq!(executor.operations(), ["BEGIN", "ROLLBACK"]);
    assert_eq!(conflicts.records().len(), 2);
}

#[test]
fn superseded_xid_rejects_multiple_candidates_and_persists_all() {
    let executor = exact_users_predecessor_executor();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    transaction.begin_if_needed(&executor).expect("begin");
    transaction.defer_superseded_insert(users_name_superseded_candidate());
    let mut second = users_name_superseded_candidate();
    second.observation.source_primary_key = vec!["2070981".to_string()];
    second.observation.coordinate.start_position = 404_034_900;
    transaction.defer_superseded_insert(second);

    let mut verifier = exact_users_2070980_verifier(true);
    let error = transaction
        .verify_deferred_superseded_inserts_at_xid_with_conflicts(
            &executor,
            &mut verifier,
            &mut conflicts,
            superseded_xid_context("cdc.row_conflicts"),
        )
        .expect_err("multiple candidates must fail closed");

    assert!(
        error
            .to_string()
            .contains("exactly one deferred superseded insert")
    );
    assert_eq!(executor.operations(), ["BEGIN", "ROLLBACK"]);
    assert_eq!(conflicts.records().len(), 2);
}

#[test]
fn superseded_xid_rejects_invalid_locked_checkpoint_predecessors() {
    let cases = [
        (None, "disappeared"),
        (
            Some(checkpoint_at("mysqld-bin.002708", 999)),
            "file mismatch",
        ),
        (
            Some(checkpoint_at("mysqld-bin.002709", 404_034_840)),
            "concurrently advanced",
        ),
        (
            Some(checkpoint_at("mysqld-bin.002709", 404_038_012)),
            "regression",
        ),
    ];

    for (locked_checkpoint, expected) in cases {
        let executor = TransactionRecordingExecutor {
            locked_checkpoint,
            ..TransactionRecordingExecutor::default()
        };
        let mut transaction = TargetTransaction::default();
        let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
        transaction.begin_if_needed(&executor).expect("begin");
        transaction.defer_superseded_insert(users_name_superseded_candidate());
        let mut verifier = exact_users_2070980_verifier(true);

        let error = transaction
            .verify_deferred_superseded_inserts_at_xid_with_conflicts(
                &executor,
                &mut verifier,
                &mut conflicts,
                superseded_xid_context("cdc.row_conflicts"),
            )
            .expect_err("invalid predecessor must fail closed");

        assert!(error.to_string().contains(expected), "{error}");
        assert!(!executor.operations().contains(&"CHECKPOINT"));
        assert!(!executor.operations().contains(&"COMMIT"));
        assert_eq!(conflicts.records().len(), 1);
    }
}

fn run_failed_superseded_verification(
    executor: &TransactionRecordingExecutor,
    conflicts: &mut crate::conflict_repair::InMemoryConflictStore,
) -> ApplyBinlogError {
    let mut transaction = TargetTransaction::default();
    transaction.begin_if_needed(executor).expect("begin");
    transaction.defer_superseded_insert(users_name_superseded_candidate());
    let mut verifier = exact_users_2070980_verifier(false);
    transaction
        .verify_deferred_superseded_inserts_at_xid_with_conflicts(
            executor,
            &mut verifier,
            conflicts,
            superseded_xid_context("cdc.row_conflicts"),
        )
        .expect_err("verification failure must abort")
}

#[test]
fn rollback_failure_discards_connection_before_observing_conflict() {
    let executor = TransactionRecordingExecutor::with_rollback_failure();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();

    let error = run_failed_superseded_verification(&executor, &mut conflicts);

    assert!(error.to_string().contains("forced rollback failure"));
    assert_eq!(executor.operations(), ["BEGIN", "ROLLBACK", "DISCARD"]);
    assert_eq!(conflicts.records().len(), 1);
}

#[test]
fn failed_connection_discard_failure_stops_evidence_persistence() {
    let executor = TransactionRecordingExecutor::with_rollback_and_discard_failure();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();

    let error = run_failed_superseded_verification(&executor, &mut conflicts);

    assert!(error.to_string().contains("forced rollback failure"));
    assert!(error.to_string().contains("forced discard failure"));
    assert_eq!(executor.operations(), ["BEGIN", "ROLLBACK", "DISCARD"]);
    assert!(conflicts.records().is_empty());
}

#[test]
fn successful_rollback_observes_conflict_without_discarding_connection() {
    let executor = TransactionRecordingExecutor::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();

    run_failed_superseded_verification(&executor, &mut conflicts);

    assert_eq!(executor.operations(), ["BEGIN", "ROLLBACK"]);
    assert_eq!(conflicts.records().len(), 1);
}

struct ConfiguredConflictTableExecutor {
    inner: TransactionRecordingExecutor,
    transaction_sql: std::cell::RefCell<Vec<String>>,
}

impl TargetExecutor for ConfiguredConflictTableExecutor {
    fn execute(
        &self,
        statement: &crate::target::SqlStatement,
    ) -> Result<(), crate::target::TargetExecuteError> {
        self.inner.execute(statement)
    }
}

impl crate::target::TransactionalTargetExecutor for ConfiguredConflictTableExecutor {
    fn begin_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.inner.begin_transaction()
    }

    fn load_transaction_checkpoint_for_update(
        &self,
        checkpoint_table: &str,
        checkpoint_name: &str,
    ) -> Result<Option<crate::checkpoint::Checkpoint>, crate::target::TargetExecuteError> {
        self.inner
            .load_transaction_checkpoint_for_update(checkpoint_table, checkpoint_name)
    }

    fn save_transaction_checkpoint(
        &self,
        checkpoint_table: &str,
        checkpoint_name: &str,
        checkpoint: &crate::checkpoint::Checkpoint,
    ) -> Result<(), crate::target::TargetExecuteError> {
        self.inner
            .save_transaction_checkpoint(checkpoint_table, checkpoint_name, checkpoint)
    }

    fn execute_transaction_sql(&self, sql: &str) -> Result<(), crate::target::TargetExecuteError> {
        self.transaction_sql.borrow_mut().push(sql.to_string());
        Ok(())
    }

    fn commit_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.inner.commit_transaction()
    }

    fn rollback_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.inner.rollback_transaction()
    }
}

struct PostCommitCacheOnlyConflictStore {
    inner: crate::conflict_repair::InMemoryConflictStore,
}

impl crate::conflict_repair::ConflictStore for PostCommitCacheOnlyConflictStore {
    fn observe(
        &mut self,
        _observation: crate::conflict_repair::ConflictObservation,
    ) -> Result<(), String> {
        panic!("successful commit must not perform post-commit observation SQL")
    }

    fn resolve_existing(
        &mut self,
        _resolution: crate::conflict_repair::ConflictResolution,
    ) -> Result<(), String> {
        panic!("successful commit must not perform post-commit resolution SQL")
    }

    fn resolution_sql(&self, resolution: &crate::conflict_repair::ConflictResolution) -> String {
        crate::conflict_repair::ConflictStore::resolution_sql(&self.inner, resolution)
    }

    fn mark_observation_committed(
        &mut self,
        observation: crate::conflict_repair::ConflictObservation,
    ) {
        crate::conflict_repair::ConflictStore::mark_observation_committed(
            &mut self.inner,
            observation,
        );
    }

    fn mark_resolution_committed(
        &mut self,
        resolution: crate::conflict_repair::ConflictResolution,
    ) {
        crate::conflict_repair::ConflictStore::mark_resolution_committed(
            &mut self.inner,
            resolution,
        );
    }

    fn has_unresolved(
        &mut self,
        resolution: &crate::conflict_repair::ConflictResolution,
    ) -> Result<bool, String> {
        crate::conflict_repair::ConflictStore::has_unresolved(&mut self.inner, resolution)
    }

    fn resolve_if_equal(
        &mut self,
        table: &str,
        primary_key: &[String],
        rows_equal: bool,
        repair_run_id: &str,
        evidence: &str,
    ) -> Result<(), String> {
        crate::conflict_repair::ConflictStore::resolve_if_equal(
            &mut self.inner,
            table,
            primary_key,
            rows_equal,
            repair_run_id,
            evidence,
        )
    }

    fn unresolved_count(&self) -> usize {
        crate::conflict_repair::ConflictStore::unresolved_count(&self.inner)
    }
}

#[test]
fn superseded_transaction_sql_uses_configured_conflict_table() {
    let executor = ConfiguredConflictTableExecutor {
        inner: exact_users_predecessor_executor(),
        transaction_sql: std::cell::RefCell::new(Vec::new()),
    };
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    transaction.begin_if_needed(&executor).expect("begin");
    transaction.defer_superseded_insert(users_name_superseded_candidate());
    let mut verifier = exact_users_2070980_verifier(true);

    transaction
        .verify_deferred_superseded_inserts_at_xid_with_conflicts(
            &executor,
            &mut verifier,
            &mut conflicts,
            superseded_xid_context("custom.row_conflicts"),
        )
        .expect("configured conflict table");

    let sql = executor.transaction_sql.borrow();
    assert_eq!(sql.len(), 2);
    assert!(
        sql.iter()
            .all(|statement| statement.contains("custom.row_conflicts"))
    );
    assert!(
        sql.iter()
            .all(|statement| !statement.contains("cdc.row_conflicts"))
    );
}

#[test]
fn superseded_xid_executes_and_cache_finalizes_preexisting_pending_resolutions() {
    let executor = exact_users_predecessor_executor();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let observation = crate::conflict_repair::ConflictObservation {
        source_identity: "production-source".to_string(),
        source_server_id: 3,
        coordinate: crate::conflict_repair::ConflictCoordinate {
            file: "mysqld-bin.002709".to_string(),
            start_position: 404_030_000,
            end_position: 404_030_100,
        },
        schema: "globalcomix".to_string(),
        table: "users_profiles".to_string(),
        operation: crate::conflict_repair::ConflictOperation::Insert,
        source_primary_key: vec!["2070980".to_string()],
        duplicate_index: Some("PRIMARY".to_string()),
        duplicate_owner_primary_key: None,
        error_code: 1062,
        error_text: "prior equal users_profiles conflict".to_string(),
        observed_at_ms: 1,
        parent_recovery: None,
    };
    crate::conflict_repair::ConflictStore::observe(&mut conflicts, observation)
        .expect("seed pending resolution conflict");
    transaction.pending_conflict_resolutions_mut().push(
        crate::conflict_repair::ConflictResolution {
            source_identity: "production-source".to_string(),
            schema: "globalcomix".to_string(),
            table: "users_profiles".to_string(),
            source_primary_key: vec!["2070980".to_string()],
            repair_run_id: "prior-users-profiles-resolution".to_string(),
            evidence: "verified equal users_profiles row".to_string(),
        },
    );
    transaction.begin_if_needed(&executor).expect("begin");
    transaction.defer_superseded_insert(users_name_superseded_candidate());
    let mut verifier = exact_users_2070980_verifier(true);

    transaction
        .verify_deferred_superseded_inserts_at_xid_with_conflicts(
            &executor,
            &mut verifier,
            &mut conflicts,
            superseded_xid_context("cdc.row_conflicts"),
        )
        .expect("superseded XID commits every pending resolution");

    assert_eq!(
        executor.operations(),
        [
            "BEGIN",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "RESOLUTION",
            "OBSERVATION",
            "RESOLUTION",
            "COMMIT",
        ]
    );
    let profile_record = conflicts
        .records()
        .into_iter()
        .find(|record| record.key.table == "users_profiles")
        .expect("pre-existing pending resolution record");
    assert_eq!(
        profile_record.status,
        crate::conflict_repair::ConflictStatus::Resolved
    );
    assert_eq!(
        profile_record.repair_run_id.as_deref(),
        Some("prior-users-profiles-resolution")
    );
}

#[test]
fn successful_superseded_commit_uses_only_infallible_cache_updates_after_commit() {
    let executor = exact_users_predecessor_executor();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = PostCommitCacheOnlyConflictStore {
        inner: crate::conflict_repair::InMemoryConflictStore::default(),
    };
    transaction.begin_if_needed(&executor).expect("begin");
    transaction.defer_superseded_insert(users_name_superseded_candidate());
    let mut verifier = exact_users_2070980_verifier(true);

    transaction
        .verify_deferred_superseded_inserts_at_xid_with_conflicts(
            &executor,
            &mut verifier,
            &mut conflicts,
            superseded_xid_context("custom.row_conflicts"),
        )
        .expect("atomic commit followed by cache-only finalization");

    assert_eq!(conflicts.inner.records().len(), 1);
    assert_eq!(
        conflicts.inner.records()[0].status,
        crate::conflict_repair::ConflictStatus::Resolved
    );
}

#[test]
fn wraps_target_writes_and_checkpoint_in_source_xid_transaction() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&NoopCheckpointStore),
                transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
                transaction_checkpoint_name: Some("stream-binlog:test-source"),
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write rows");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("xid");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "LOCK_CHECKPOINT", "CHECKPOINT", "COMMIT"]
    );
}

#[test]
fn equal_duplicate_commits_multi_row_transaction_and_checkpoints() {
    let executor = TransactionRecordingExecutor::with_equal_duplicate_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&NoopCheckpointStore),
                transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
                transaction_checkpoint_name: Some("stream-binlog:test-source"),
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally_with_conflicts(
                &mut applier,
                &mut context,
                &header,
                &event,
                "test-source",
                &mut conflicts,
            )
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");

    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("first row");
    process_event!(event_header(31, 240), write_rows_event(18, 2, "beta"))
        .expect("ignored duplicate should not abort source transaction");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("xid");

    assert_eq!(
        applier.executor().operations().as_slice(),
        [
            "BEGIN",
            "EXEC",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "COMMIT"
        ]
    );
    assert!(conflicts.records().is_empty());
}

#[test]
fn child_replay_resolves_ledger_before_single_commit_at_crash_boundary() {
    let divergent_executor = TransactionRecordingExecutor {
        duplicate_row_change_number: Some(2),
        duplicate_mode: DuplicateMode::Divergent,
        ..TransactionRecordingExecutor::default()
    };
    let mut divergent_applier = crate::row::RowApplier::new(divergent_executor);
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let mut row_header = event_header(30, 0);
    row_header.event_length = 435;

    macro_rules! process_divergent_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&NoopCheckpointStore),
                transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
                transaction_checkpoint_name: Some("stream-binlog:test-source"),
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally_with_conflicts(
                &mut divergent_applier,
                &mut context,
                &header,
                &event,
                "test-source",
                &mut conflicts,
            )
        }};
    }

    process_divergent_event!(
        event_header(19, 215_329_700),
        BinlogEvent::TableMapEvent(guests_table_map_event(19))
    )
    .expect("guests table map");
    process_divergent_event!(
        event_header(19, 215_329_720),
        BinlogEvent::TableMapEvent(sessions_table_map_event(20))
    )
    .expect("sessions table map");
    process_divergent_event!(event_header(30, 215_329_760), guest_write_rows_event(19))
        .expect("guest row");
    state.record_event_position(215_330_725);
    process_divergent_event!(row_header, sessions_write_rows_event(20))
        .expect("divergent sessions conflict is deferred until XID");
    process_divergent_event!(
        event_header(16, 215_331_160),
        BinlogEvent::XidEvent(XidEvent { xid: 101 })
    )
    .expect_err("XID persists the divergent sessions conflict");

    let record = &conflicts.records()[0];
    assert_eq!(record.key.table, "sessions");
    assert_eq!(record.key.source_primary_key, ["109017694"]);
    assert_eq!(record.key.coordinate.start_position, 215_330_725);

    let equal_executor = TransactionRecordingExecutor::with_equal_duplicate_second_row_change();
    let mut equal_applier = crate::row::RowApplier::new(equal_executor);
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();
    let mut row_header = event_header(30, 0);
    row_header.event_length = 435;

    macro_rules! process_equal_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&NoopCheckpointStore),
                transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
                transaction_checkpoint_name: Some("stream-binlog:test-source"),
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally_with_conflicts(
                &mut equal_applier,
                &mut context,
                &header,
                &event,
                "test-source",
                &mut conflicts,
            )
        }};
    }

    process_equal_event!(
        event_header(19, 215_329_700),
        BinlogEvent::TableMapEvent(guests_table_map_event(19))
    )
    .expect("guests table map replay");
    process_equal_event!(
        event_header(19, 215_329_720),
        BinlogEvent::TableMapEvent(sessions_table_map_event(20))
    )
    .expect("sessions table map replay");
    process_equal_event!(event_header(30, 215_329_760), guest_write_rows_event(19))
        .expect("guest row replay");
    state.record_event_position(215_330_725);
    process_equal_event!(row_header, sessions_write_rows_event(20))
        .expect("equal sessions row replay");
    process_equal_event!(
        event_header(16, 215_331_160),
        BinlogEvent::XidEvent(XidEvent { xid: 102 })
    )
    .expect("XID replay");

    assert_eq!(
        equal_applier.executor().operations().as_slice(),
        [
            "BEGIN",
            "EXEC",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "RESOLUTION",
            "COMMIT"
        ]
    );
    let record = &conflicts.records()[0];
    let evidence = record
        .resolution_evidence
        .as_deref()
        .expect("equal-row resolution evidence");
    assert!(evidence.contains(
        "equal target row already existed; source coordinate mysqld-bin.002709:215330725"
    ));
    assert!(evidence.contains("source transaction end position 215331160"));
}

#[test]
fn process_stream_core_defers_and_finalizes_real_row_boundary_at_xid() {
    let config = ApplyBinlogConfig::default();
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut progress = crate::live::progress::StreamProgress::new(BinlogCoordinate {
        file: "mysqld-bin.002709".to_string(),
        position: 215_329_700,
    });
    let mut source_row_transaction_open = false;
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let executor = TransactionRecordingExecutor {
        duplicate_row_change_number: Some(3),
        duplicate_mode: DuplicateMode::Divergent,
        ..TransactionRecordingExecutor::default()
    };
    let mut applier = crate::row::RowApplier::new(executor);

    {
        let mut dispatch = |state: &mut StructuredEventState,
                            input: SourceStreamEvent<'_>|
         -> Result<StructuredEventOutcome, ApplyBinlogError> {
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&NoopCheckpointStore),
                transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
                transaction_checkpoint_name: Some("stream-binlog:test-source"),
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally_with_conflicts(
                &mut applier,
                &mut context,
                input.header,
                input.event,
                "test-source",
                &mut conflicts,
            )
        };

        process_stream_event_core(
            &config,
            &mut state,
            &mut progress,
            &mut source_row_transaction_open,
            SourceStreamEvent {
                header: &event_header(19, 215_329_700),
                event: &BinlogEvent::TableMapEvent(guests_table_map_event(19)),
                source_position: 215_329_700,
            },
            &mut dispatch,
        )
        .expect("guest table map");
        process_stream_event_core(
            &config,
            &mut state,
            &mut progress,
            &mut source_row_transaction_open,
            SourceStreamEvent {
                header: &event_header(19, 215_329_720),
                event: &BinlogEvent::TableMapEvent(sessions_table_map_event(20)),
                source_position: 215_329_720,
            },
            &mut dispatch,
        )
        .expect("sessions table map");
        process_stream_event_core(
            &config,
            &mut state,
            &mut progress,
            &mut source_row_transaction_open,
            SourceStreamEvent {
                header: &event_header(30, 215_329_760),
                event: &guest_write_rows_event(19),
                source_position: 215_329_760,
            },
            &mut dispatch,
        )
        .expect("guest row");
        let mut row_header = event_header(30, 0);
        row_header.event_length = 435;
        process_stream_event_core(
            &config,
            &mut state,
            &mut progress,
            &mut source_row_transaction_open,
            SourceStreamEvent {
                header: &row_header,
                event: &sessions_write_rows_event_with_conflict_followup(20),
                source_position: 215_330_725,
            },
            &mut dispatch,
        )
        .expect("divergent row observation is deferred until XID");
    }
    assert!(conflicts.records().is_empty());
    assert!(transaction.has_pending_conflict_observations());
    let operations_after_conflict = applier.executor().operations();
    assert_eq!(operations_after_conflict, ["BEGIN", "EXEC", "EXEC", "EXEC"]);
    {
        let mut dispatch_doomed_row = |state: &mut StructuredEventState,
                                       input: SourceStreamEvent<'_>|
         -> Result<StructuredEventOutcome, ApplyBinlogError> {
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&NoopCheckpointStore),
                transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
                transaction_checkpoint_name: Some("stream-binlog:test-source"),
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally_with_conflicts(
                &mut applier,
                &mut context,
                input.header,
                input.event,
                "test-source",
                &mut conflicts,
            )
        };
        let mut doomed_row_header = event_header(30, 0);
        doomed_row_header.event_length = 435;
        process_stream_event_core(
            &config,
            &mut state,
            &mut progress,
            &mut source_row_transaction_open,
            SourceStreamEvent {
                header: &doomed_row_header,
                event: &sessions_write_rows_event(20),
                source_position: 215_330_900,
            },
            &mut dispatch_doomed_row,
        )
        .expect("doomed transaction drains later row without target write");
    }
    assert_eq!(applier.executor().operations(), operations_after_conflict);

    let mut dispatch_xid = |state: &mut StructuredEventState,
                            input: SourceStreamEvent<'_>|
     -> Result<StructuredEventOutcome, ApplyBinlogError> {
        let mut context = StreamEventContext {
            schema_resolver: &resolver,
            state,
            target_transaction: &mut transaction,
            checkpoint_store: Some(&NoopCheckpointStore),
            transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
            transaction_checkpoint_name: Some("stream-binlog:test-source"),
            current_file: &mut current_file,
            group_config: TargetTransactionGroupConfig::default(),
        };
        apply_stream_event_transactionally_with_conflicts(
            &mut applier,
            &mut context,
            input.header,
            input.event,
            "test-source",
            &mut conflicts,
        )
    };
    let xid_header = event_header(16, 215_331_160);
    process_stream_event_core(
        &config,
        &mut state,
        &mut progress,
        &mut source_row_transaction_open,
        SourceStreamEvent {
            header: &xid_header,
            event: &BinlogEvent::XidEvent(XidEvent { xid: 102 }),
            source_position: 215_331_160,
        },
        &mut dispatch_xid,
    )
    .expect_err("XID persists the finalized conflict and stops replay");
    assert_eq!(
        conflicts.records()[0].key.coordinate.start_position,
        215_330_725
    );
    assert_eq!(
        conflicts.records()[0].key.coordinate.end_position,
        215_331_160
    );
}

#[test]
fn replaced_divergent_primary_commits_and_checkpoints_with_durable_evidence() {
    let executor = TransactionRecordingExecutor::with_replaced_duplicate_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    crate::conflict_repair::ConflictStore::observe(
        &mut conflicts,
        crate::conflict_repair::ConflictObservation {
            source_identity: "test-source".to_string(),
            source_server_id: 1,
            coordinate: crate::conflict_repair::ConflictCoordinate {
                file: "prior-binlog".to_string(),
                start_position: 1,
                end_position: 2,
            },
            schema: "fixture_cdc".to_string(),
            table: "accounts".to_string(),
            operation: crate::conflict_repair::ConflictOperation::Insert,
            source_primary_key: vec!["2".to_string()],
            duplicate_index: Some("PRIMARY".to_string()),
            duplicate_owner_primary_key: None,
            error_code: 1062,
            error_text: "prior replacement conflict".to_string(),
            observed_at_ms: 1,
            parent_recovery: None,
        },
    )
    .expect("prior conflict");
    let header = event_header(30, 240);
    let event = write_rows_event(18, 2, "beta");
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: Some(&NoopCheckpointStore),
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };

    apply_stream_event_transactionally_with_conflicts(
        &mut applier,
        &mut context,
        &header,
        &event,
        "test-source",
        &mut conflicts,
    )
    .expect("replacement should continue");

    let xid_header = event_header(16, 260);
    let xid_event = BinlogEvent::XidEvent(XidEvent { xid: 42 });
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: Some(&NoopCheckpointStore),
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };
    apply_stream_event_transactionally_with_conflicts(
        &mut applier,
        &mut context,
        &xid_header,
        &xid_event,
        "test-source",
        &mut conflicts,
    )
    .expect("replacement transaction should commit");

    assert_eq!(
        applier.executor().operations().as_slice(),
        [
            "BEGIN",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "RESOLUTION",
            "COMMIT"
        ]
    );
    let record = &conflicts.records()[0];
    assert_eq!(
        record.status,
        crate::conflict_repair::ConflictStatus::Resolved
    );
    assert!(record.repair_run_id.is_some());
    assert!(
        record
            .resolution_evidence
            .as_deref()
            .is_some_and(|evidence| evidence.contains("target row replaced with source image"))
    );
    assert_eq!(record.error_text, "prior replacement conflict");
}

struct DeferredConflictFixture<'a> {
    resolver: &'a FixtureSchemaResolver,
    state: &'a mut StructuredEventState,
    current_file: &'a mut String,
    transaction: &'a mut TargetTransaction,
    conflicts: &'a mut crate::conflict_repair::InMemoryConflictStore,
}

fn apply_deferred_conflict_at_xid(
    applier: &mut crate::row::RowApplier<TransactionRecordingExecutor>,
    fixture: DeferredConflictFixture<'_>,
    header: &EventHeader,
    event: &BinlogEvent,
) -> ApplyBinlogError {
    apply_deferred_conflict_at_xid_position(applier, fixture, header, event, 260)
}

fn apply_deferred_conflict_at_xid_position(
    applier: &mut crate::row::RowApplier<TransactionRecordingExecutor>,
    fixture: DeferredConflictFixture<'_>,
    header: &EventHeader,
    event: &BinlogEvent,
    xid_end_position: u32,
) -> ApplyBinlogError {
    let DeferredConflictFixture {
        resolver,
        state,
        current_file,
        transaction,
        conflicts,
    } = fixture;
    {
        let mut context = StreamEventContext {
            schema_resolver: resolver,
            state,
            target_transaction: transaction,
            checkpoint_store: Some(&NoopCheckpointStore),
            transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
            transaction_checkpoint_name: Some("stream-binlog:test-source"),
            current_file,
            group_config: TargetTransactionGroupConfig::default(),
        };
        apply_stream_event_transactionally_with_conflicts(
            applier,
            &mut context,
            header,
            event,
            "test-source",
            conflicts,
        )
        .expect("row conflict is deferred until XID");
    }
    assert!(conflicts.records().is_empty());
    assert!(transaction.has_pending_conflict_observations());

    let mut context = StreamEventContext {
        schema_resolver: resolver,
        state,
        target_transaction: transaction,
        checkpoint_store: Some(&NoopCheckpointStore),
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };
    apply_stream_event_transactionally_with_conflicts(
        applier,
        &mut context,
        &event_header(16, xid_end_position),
        &BinlogEvent::XidEvent(XidEvent { xid: 42 }),
        "test-source",
        conflicts,
    )
    .expect_err("XID persists the deferred conflict and aborts replay")
}

#[test]
fn divergent_duplicate_rolls_back_and_persists_conflict_evidence() {
    let executor = TransactionRecordingExecutor::with_divergent_duplicate_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let header = event_header(30, 240);
    let event = write_rows_event(18, 2, "beta");
    let error = apply_deferred_conflict_at_xid(
        &mut applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut state,
            current_file: &mut current_file,
            transaction: &mut transaction,
            conflicts: &mut conflicts,
        },
        &header,
        &event,
    );

    assert!(
        error
            .to_string()
            .contains("row conflict persisted for repair")
    );
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert_eq!(conflicts.records().len(), 1);
    assert_eq!(conflicts.records()[0].error_code, 1062);
    assert_eq!(
        conflicts.records()[0].duplicate_index.as_deref(),
        Some("PRIMARY")
    );
}

#[test]
fn update_unique_conflict_under_ignore_duplicate_rolls_back_and_records_ledger() {
    let executor = TransactionRecordingExecutor::with_update_unique_conflict();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let event = BinlogEvent::UpdateRowsEvent(MysqlCdcUpdateRowsEvent {
        table_id: 18,
        flags: 0,
        columns_number: 5,
        columns_before_update: vec![true; 5],
        columns_after_update: vec![true; 5],
        rows: vec![UpdateRowData::new(
            RowData::new(vec![
                Some(MySqlValue::Int(1)),
                Some(MySqlValue::String("alpha".to_string())),
                Some(MySqlValue::Int(100)),
                Some(MySqlValue::String("safe".to_string())),
                Some(MySqlValue::DateTime(DateTime {
                    year: 2026,
                    month: 6,
                    day: 22,
                    hour: 12,
                    minute: 3,
                    second: 4,
                    millis: 0,
                })),
            ]),
            RowData::new(vec![
                Some(MySqlValue::Int(1)),
                Some(MySqlValue::String("beta".to_string())),
                Some(MySqlValue::Int(100)),
                Some(MySqlValue::String("safe".to_string())),
                Some(MySqlValue::DateTime(DateTime {
                    year: 2026,
                    month: 6,
                    day: 22,
                    hour: 12,
                    minute: 3,
                    second: 4,
                    millis: 0,
                })),
            ]),
        )],
    });
    let header = event_header(30, 240);
    let error = apply_deferred_conflict_at_xid(
        &mut applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut state,
            current_file: &mut current_file,
            transaction: &mut transaction,
            conflicts: &mut conflicts,
        },
        &header,
        &event,
    );

    assert!(
        error
            .to_string()
            .contains("row conflict persisted for repair")
    );
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert_eq!(conflicts.records().len(), 1);
    assert_eq!(
        conflicts.records()[0].key.operation,
        crate::conflict_repair::ConflictOperation::Update
    );
    assert_eq!(conflicts.records()[0].error_code, 1062);
    assert_eq!(
        conflicts.records()[0].duplicate_index.as_deref(),
        Some("uq_accounts_name")
    );
}

#[test]
fn duplicate_insert_under_default_error_policy_rolls_back_without_ledger_entry() {
    let executor = TransactionRecordingExecutor::with_default_duplicate_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let header = event_header(30, 240);
    let event = write_rows_event(18, 2, "beta");
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: Some(&NoopCheckpointStore),
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };

    let error = apply_stream_event_transactionally_with_conflicts(
        &mut applier,
        &mut context,
        &header,
        &event,
        "test-source",
        &mut conflicts,
    )
    .expect_err("default duplicate policy must abort the source transaction");

    assert!(error.to_string().contains("duplicate"));
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert!(conflicts.records().is_empty());
}

#[test]
fn sessions_109018328_fk_conflict_carries_exact_guest_recovery_after_rollback_and_persistence() {
    let executor = TransactionRecordingExecutor::with_foreign_key_conflict_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(sessions_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("globalcomix".to_string()));
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let event = BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id: 20,
        flags: 0,
        columns_number: 3,
        columns_present: vec![true, true, true],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(109_018_328)),
            Some(MySqlValue::Int(78_011_674)),
            Some(MySqlValue::String(
                "fb42c5a9-b717-4022-9f27-6b467e0ca28d515m".to_string(),
            )),
        ])],
    });

    let error = apply_deferred_conflict_at_xid_position(
        &mut applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut state,
            current_file: &mut current_file,
            transaction: &mut transaction,
            conflicts: &mut conflicts,
        },
        &event_header_at(30, 224_141_058, 1_784_246_400),
        &event,
        224_142_261,
    );

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert_eq!(conflicts.records().len(), 1);
    assert_eq!(
        conflicts.records()[0].key.coordinate.start_position,
        224_141_039
    );
    assert_eq!(
        conflicts.records()[0].key.coordinate.end_position,
        224_142_261
    );
    assert_eq!(
        error.sessions_guest_recovery(),
        Some(&crate::live::SessionsGuestRecovery {
            source_file: "mysqld-bin.002709".to_string(),
            source_start_position: 224_141_039,
            source_end_position: 224_142_261,
            child_event_timestamp: 1_784_246_400,
            schema: "globalcomix".to_string(),
            table: "sessions".to_string(),
            constraint: "fk_sessions_guest".to_string(),
            session_id: "109018328".to_string(),
            guest_id: "78011674".to_string(),
            guest_hash: "fb42c5a9-b717-4022-9f27-6b467e0ca28d515m".to_string(),
        })
    );
}

#[test]
fn home_feed_slide_4508905_fk_conflict_carries_exact_card_recovery_boundary() {
    let executor = TransactionRecordingExecutor::with_home_feed_card_foreign_key_conflict();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(home_feed_card_slides_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("globalcomix".to_string()));
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let event = BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id: 21,
        flags: 0,
        columns_number: 2,
        columns_present: vec![true, true],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(4_508_905)),
            Some(MySqlValue::Int(2_492_683)),
        ])],
    });

    let error = apply_deferred_conflict_at_xid_position(
        &mut applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut state,
            current_file: &mut current_file,
            transaction: &mut transaction,
            conflicts: &mut conflicts,
        },
        &event_header_at(30, 308_259_874, 1_784_588_463),
        &event,
        308_261_441,
    );

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert_eq!(
        error.parent_recovery(),
        Some(&crate::live::ExactParentRecovery::HomeFeedCard(
            crate::live::HomeFeedCardRecovery {
                source_file: "mysqld-bin.002709".to_string(),
                source_start_position: 308_259_855,
                source_end_position: 308_261_441,
                child_event_timestamp: 1_784_588_463,
                schema: "globalcomix".to_string(),
                table: "home_feed_card_slides".to_string(),
                constraint: "fk_hfcs_card".to_string(),
                slide_id: "4508905".to_string(),
                card_id: "2492683".to_string(),
            }
        ))
    );
}

#[derive(Default)]
struct RecordingExactParentTarget {
    inserted: Vec<crate::snapshot::SnapshotRow>,
}

struct FixtureExactParentReader {
    rows: Vec<crate::snapshot::SnapshotRow>,
}

impl crate::table_sync::ExactParentReader for FixtureExactParentReader {
    fn read_guest_identity_rows(
        &self,
        _guest_id: &str,
        _guest_hash: &str,
    ) -> Result<Vec<crate::snapshot::SnapshotRow>, crate::table_sync::TableSyncError> {
        panic!("home feed recovery must not query guests")
    }

    fn read_home_feed_card_rows_by_id(
        &self,
        _card_id: &str,
    ) -> Result<Vec<crate::snapshot::SnapshotRow>, crate::table_sync::TableSyncError> {
        Ok(self.rows.clone())
    }

    fn read_home_feed_card_identity_rows(
        &self,
        _card_id: &str,
        _card_type_id: &str,
        _source_id: Option<&str>,
    ) -> Result<Vec<crate::snapshot::SnapshotRow>, crate::table_sync::TableSyncError> {
        Ok(self.rows.clone())
    }
}

impl crate::table_sync::SyncRepairTarget for RecordingExactParentTarget {
    fn insert_row(
        &mut self,
        row: &crate::snapshot::SnapshotRow,
    ) -> Result<(), crate::table_sync::TableSyncError> {
        self.inserted.push(row.clone());
        Ok(())
    }

    fn update_row(
        &mut self,
        _row: &crate::snapshot::SnapshotRow,
    ) -> Result<(), crate::table_sync::TableSyncError> {
        panic!("exact parent recovery must not update")
    }

    fn delete_row(
        &mut self,
        _primary_key: &[String],
    ) -> Result<(), crate::table_sync::TableSyncError> {
        panic!("exact parent recovery must not delete")
    }
}

#[derive(Default)]
struct ExactCheckpointStore {
    saved: RefCell<Vec<crate::checkpoint::Checkpoint>>,
}

impl StreamCheckpointStore for ExactCheckpointStore {
    fn load_checkpoint(&self) -> Result<Option<crate::checkpoint::Checkpoint>, ApplyBinlogError> {
        Ok(self.saved.borrow().last().cloned())
    }

    fn save_checkpoint(
        &self,
        checkpoint: &crate::checkpoint::Checkpoint,
    ) -> Result<(), ApplyBinlogError> {
        self.saved.borrow_mut().push(checkpoint.clone());
        Ok(())
    }
}

fn exact_home_feed_card_parent_row() -> crate::snapshot::SnapshotRow {
    let values = [
        ("id", Some("2492683")),
        ("card_type_id", Some("1")),
        ("status", Some("active")),
        ("reading_direction", Some("l")),
        ("comic_id", Some("10175")),
        ("release_id", Some("50715")),
        ("caption", Some("exact source caption")),
        ("hook_image_url", Some("https://example.test/hook.jpg")),
        ("source_id", Some("50151")),
        ("filter_reason", None),
        ("retired_reason", None),
        ("first_published", None),
        ("last_active_time", Some("2026-07-20 22:01:03")),
        ("view_count", Some("0")),
        ("reaction_count", Some("0")),
        ("click_count", Some("0")),
        ("curator_user_id", None),
        ("curated_score", None),
        ("facets_json", None),
        ("create_time", Some("2026-06-23 05:01:16")),
        ("__recovery_create_time_epoch", Some("1782190876")),
    ];
    crate::snapshot::SnapshotRow {
        primary_key: vec!["2492683".to_string()],
        values: values
            .into_iter()
            .map(|(column, value)| (column.to_string(), value.map(ToString::to_string)))
            .collect(),
    }
}

#[test]
fn exact_home_feed_event_recovers_parent_then_replays_child_and_xid_checkpoint() {
    let resolver = FixtureSchemaResolver;
    let event = BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id: 21,
        flags: 0,
        columns_number: 2,
        columns_present: vec![true, true],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(4_508_905)),
            Some(MySqlValue::Int(2_492_683)),
        ])],
    });
    let header = event_header_at(30, 308_259_874, 1_784_588_463);

    let mut conflicting_applier = crate::row::RowApplier::new(
        TransactionRecordingExecutor::with_home_feed_card_foreign_key_conflict(),
    );
    conflicting_applier.apply_table_map(home_feed_card_slides_row_table_map());
    let mut conflict_state = StructuredEventState::new(Some("globalcomix".to_string()));
    let mut conflict_file = "mysqld-bin.002709".to_string();
    let mut conflict_transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let error = apply_deferred_conflict_at_xid_position(
        &mut conflicting_applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut conflict_state,
            current_file: &mut conflict_file,
            transaction: &mut conflict_transaction,
            conflicts: &mut conflicts,
        },
        &header,
        &event,
        308_261_441,
    );
    let recovery = error
        .parent_recovery()
        .expect("exact event must dispatch parent recovery");

    let source_parent = exact_home_feed_card_parent_row();
    let source = FixtureExactParentReader {
        rows: vec![source_parent.clone()],
    };
    let target = FixtureExactParentReader { rows: Vec::new() };
    let mut repair_target = RecordingExactParentTarget::default();
    crate::table_sync::reconcile_exact_parent(recovery, &source, &target, &mut repair_target)
        .expect("exact parent reconciliation");
    let mut canonical_parent = source_parent;
    canonical_parent
        .values
        .remove("__recovery_create_time_epoch");
    assert_eq!(repair_target.inserted, [canonical_parent]);

    let mut replay_applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    replay_applier.apply_table_map(home_feed_card_slides_row_table_map());
    let checkpoint_store = ExactCheckpointStore::default();
    let mut replay_state = StructuredEventState::new(Some("globalcomix".to_string()));
    let mut replay_file = "mysqld-bin.002709".to_string();
    let mut replay_transaction = TargetTransaction::default();
    let group_config = TargetTransactionGroupConfig {
        size: 1,
        timeout: Duration::ZERO,
    };
    for (event_header, replay_event) in [
        (header, event),
        (
            event_header(16, 308_261_441),
            BinlogEvent::XidEvent(XidEvent { xid: 308_261_441 }),
        ),
    ] {
        let mut context = StreamEventContext {
            schema_resolver: &resolver,
            state: &mut replay_state,
            target_transaction: &mut replay_transaction,
            checkpoint_store: Some(&checkpoint_store),
            transaction_checkpoint_table: None,
            transaction_checkpoint_name: None,
            current_file: &mut replay_file,
            group_config,
        };
        apply_stream_event_transactionally(
            &mut replay_applier,
            &mut context,
            &event_header,
            &replay_event,
        )
        .expect("unchanged child replay and XID");
    }

    let checkpoints = checkpoint_store.saved.borrow();
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].source_file, "mysqld-bin.002709");
    assert_eq!(checkpoints[0].source_position, 308_261_441);
}

#[test]
fn foreign_key_conflict_rolls_back_and_preserves_constraint_evidence() {
    let executor = TransactionRecordingExecutor::with_foreign_key_conflict_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let header = event_header(30, 240);
    let event = write_rows_event(18, 2, "beta");
    let error = apply_deferred_conflict_at_xid(
        &mut applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut state,
            current_file: &mut current_file,
            transaction: &mut transaction,
            conflicts: &mut conflicts,
        },
        &header,
        &event,
    );

    assert!(
        error
            .to_string()
            .contains("row conflict persisted for repair")
    );
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert_eq!(conflicts.records().len(), 1);
    assert_eq!(conflicts.records()[0].error_code, 1452);
    assert_eq!(conflicts.records()[0].duplicate_index, None);
}

#[test]
fn check_conflict_rolls_back_and_preserves_constraint_evidence() {
    let executor = TransactionRecordingExecutor::with_check_conflict_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let header = event_header(30, 240);
    let event = write_rows_event(18, 2, "beta");
    let error = apply_deferred_conflict_at_xid(
        &mut applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut state,
            current_file: &mut current_file,
            transaction: &mut transaction,
            conflicts: &mut conflicts,
        },
        &header,
        &event,
    );

    assert!(
        error
            .to_string()
            .contains("row conflict persisted for repair")
    );
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert_eq!(conflicts.records().len(), 1);
    assert_eq!(conflicts.records()[0].error_code, 3819);
}

#[test]
fn query_dml_does_not_open_or_checkpoint_target_transaction() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".to_string(),
        sql_statement: "INSERT INTO accounts (id, name) VALUES (999, 'query-event')".to_string(),
    });
    let header = event_header(99, 180);
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: Some(&NoopCheckpointStore),
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };

    let error = apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        .expect_err("statement DML must fail before target transaction");

    assert!(error.to_string().contains("ROW/FULL contract violation"));
    assert!(applier.executor().operations().is_empty());
    assert_eq!(current_file, "mysqld-bin.000777");
}

#[test]
fn file_checkpoint_waits_until_after_target_commit() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let checkpoint_store = RecordingCheckpointStore::new(applier.executor().shared_operations());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&checkpoint_store),
                transaction_checkpoint_table: None,
                transaction_checkpoint_name: None,
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write rows");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("xid");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "COMMIT", "CHECKPOINT"]
    );
}

#[test]
fn groups_multiple_xids_in_one_mysql_target_transaction() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let group_config = TargetTransactionGroupConfig {
        size: 2,
        timeout: Duration::ZERO,
    };
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&NoopCheckpointStore),
                transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
                transaction_checkpoint_name: Some("stream-binlog:test-source"),
                current_file: &mut current_file,
                group_config,
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write rows");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("first xid");
    process_event!(event_header(30, 280), write_rows_event(18, 2, "beta")).expect("write rows");
    process_event!(
        event_header(16, 320),
        BinlogEvent::XidEvent(XidEvent { xid: 43 })
    )
    .expect("second xid");

    assert_eq!(
        applier.executor().operations().as_slice(),
        [
            "BEGIN",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "COMMIT",
            "BEGIN",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "COMMIT"
        ]
    );
}

#[test]
fn grouped_file_checkpoint_saves_last_xid_after_group_commit() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let checkpoint_store = RecordingCheckpointStore::new(applier.executor().shared_operations());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let group_config = TargetTransactionGroupConfig {
        size: 2,
        timeout: Duration::ZERO,
    };
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&checkpoint_store),
                transaction_checkpoint_table: None,
                transaction_checkpoint_name: None,
                current_file: &mut current_file,
                group_config,
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write rows");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("first xid");
    process_event!(event_header(30, 280), write_rows_event(18, 2, "beta")).expect("write rows");
    process_event!(
        event_header(16, 320),
        BinlogEvent::XidEvent(XidEvent { xid: 43 })
    )
    .expect("second xid");

    assert_eq!(
        applier.executor().operations().as_slice(),
        [
            "BEGIN",
            "EXEC",
            "COMMIT",
            "CHECKPOINT",
            "BEGIN",
            "EXEC",
            "COMMIT",
            "CHECKPOINT"
        ]
    );
}

#[test]
fn rotate_flushes_open_group_before_rotate_checkpoint() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let checkpoint_store = RecordingCheckpointStore::new(applier.executor().shared_operations());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let group_config = TargetTransactionGroupConfig {
        size: 10,
        timeout: Duration::ZERO,
    };
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&checkpoint_store),
                transaction_checkpoint_table: None,
                transaction_checkpoint_name: None,
                current_file: &mut current_file,
                group_config,
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write rows");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("xid");
    process_event!(
        event_header(20, 4),
        BinlogEvent::RotateEvent(RotateEvent {
            binlog_position: 4,
            binlog_filename: "mysqld-bin.000778".to_string(),
        })
    )
    .expect("rotate");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "COMMIT", "CHECKPOINT", "CHECKPOINT"]
    );
}

#[test]
fn applies_primary_key_change_without_checkpoint_before_source_commit() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let checkpoint_store = RecordingCheckpointStore::new(applier.executor().shared_operations());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&checkpoint_store),
                transaction_checkpoint_table: None,
                transaction_checkpoint_name: None,
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");
    let update = BinlogEvent::UpdateRowsEvent(MysqlCdcUpdateRowsEvent {
        table_id: 18,
        flags: 0,
        columns_number: 5,
        columns_before_update: vec![true; 5],
        columns_after_update: vec![true; 5],
        rows: vec![UpdateRowData::new(
            RowData::new(vec![
                Some(MySqlValue::Int(1)),
                Some(MySqlValue::String("alpha".to_string())),
                Some(MySqlValue::Int(100)),
                Some(MySqlValue::String("safe".to_string())),
                Some(MySqlValue::DateTime(DateTime {
                    year: 2026,
                    month: 6,
                    day: 22,
                    hour: 12,
                    minute: 3,
                    second: 4,
                    millis: 0,
                })),
            ]),
            RowData::new(vec![
                Some(MySqlValue::Int(2)),
                Some(MySqlValue::String("beta".to_string())),
                Some(MySqlValue::Int(100)),
                Some(MySqlValue::String("safe".to_string())),
                Some(MySqlValue::DateTime(DateTime {
                    year: 2026,
                    month: 6,
                    day: 22,
                    hour: 12,
                    minute: 3,
                    second: 4,
                    millis: 0,
                })),
            ]),
        )],
    });

    process_event!(event_header(30, 220), update).expect("apply primary-key change");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC"]
    );
    assert!(transaction.is_open());

    process_event!(
        event_header(31, 240),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("commit source transaction");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "COMMIT", "CHECKPOINT"]
    );
    assert!(!transaction.is_open());
}

#[test]
fn rolls_back_open_target_transaction_when_row_apply_fails() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::failing());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: None::<&NoopCheckpointStore>,
                transaction_checkpoint_table: None,
                transaction_checkpoint_name: None,
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");
    let result = process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha"));

    assert!(result.is_err());
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert!(!transaction.is_open());
}
