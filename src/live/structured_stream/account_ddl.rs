use mariadb_mysql_cdc::account_ddl::{
    TranslatedCreateUser, TranslatedUnlockUser, translate_create_user, translate_unlock_user,
};
use sha2::{Digest, Sha256};

use super::ddl::{
    ddl_event, ensure_automatic_ddl_checkpoint_predecessor, finalize_automatic_ddl_checkpoint,
    resolved_ddl_outcome,
};
use super::*;
use crate::live::ddl_semantics::DdlSemanticEvidence;
use crate::target::SqlStatement;

const ACCOUNT_DDL_VERSION: &str = "locked-tls-account-v1";
pub(super) const UNLOCK_APPROVAL_ENV: &str = "CDC_APPROVED_ACCOUNT_UNLOCK";

enum AccountOperation<'a> {
    Create(TranslatedCreateUser),
    Unlock(TranslatedUnlockUser<'a>),
}

pub(super) fn handle_account_ddl<E, R, C, J>(
    executor: &E,
    journal: &J,
    source_identity: &str,
    input: AutomaticDdlInput<'_, '_, R, C>,
    unlock_approval: impl FnOnce() -> Option<String>,
) -> Result<Option<StructuredEventOutcome>, ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    R: TableSchemaResolver,
    C: StreamCheckpointStore,
    J: DdlReplayJournal,
{
    let AutomaticDdlInput {
        context,
        header,
        event,
    } = input;
    let BinlogEvent::QueryEvent(query) = event else {
        return Ok(None);
    };
    if !query.database_name.is_empty() && !context.state.should_apply_schema(&query.database_name) {
        return Ok(None);
    }
    let Some(operation) = parse_account_operation(&query.sql_statement) else {
        return Ok(None);
    };
    let source_event = ddl_event(source_identity, context.current_file, header, query);
    let digest = Sha256::digest(&source_event.raw_sql);
    if matches!(operation, AccountOperation::Unlock(_)) {
        let event_key = format!(
            "{}:{}:{digest:x}",
            source_event.binlog_file, source_event.event_start_position
        );
        if unlock_approval().as_deref() != Some(event_key.as_str()) {
            return Ok(None);
        }
    }
    flush_grouped_transaction(executor, context)?;
    if context.target_transaction.is_open() {
        return Err(account_blocked(
            &source_event,
            "target row transaction is still open",
        ));
    }
    validate_account_checkpoint(context, &source_event)?;
    let fingerprint = format!("account-ddl:sha256:{digest:x}");
    let journal_event = journal
        .account_event_identity(source_event.clone(), &fingerprint)
        .map_err(|_| account_blocked(&source_event, "journal identity rejected"))?;
    let evidence = account_evidence(fingerprint, &operation);
    replay_account(executor, journal, &journal_event, &evidence, &operation)?;
    finalize_automatic_ddl_checkpoint(executor, journal, context, event, &journal_event)
        .map_err(|_| account_blocked(&journal_event, "checkpoint transaction not confirmed"))?;
    context.state.clear_query_context();
    Ok(Some(resolved_ddl_outcome(journal_event)))
}

fn parse_account_operation(sql: &str) -> Option<AccountOperation<'_>> {
    if let Ok(translated) = translate_create_user(sql) {
        return Some(AccountOperation::Create(translated));
    }
    translate_unlock_user(sql)
        .ok()
        .map(AccountOperation::Unlock)
}

fn validate_account_checkpoint<R, C: StreamCheckpointStore>(
    context: &StreamEventContext<'_, R, C>,
    event: &DdlEvent,
) -> Result<(), ApplyBinlogError> {
    if context.transaction_checkpoint_table.is_none()
        || context.transaction_checkpoint_name.is_none()
    {
        return Err(account_blocked(
            event,
            "transactional checkpoint is required",
        ));
    }
    let Some(store) = context.checkpoint_store else {
        return Err(account_blocked(event, "checkpoint store is required"));
    };
    let current = store.load_checkpoint()?;
    ensure_automatic_ddl_checkpoint_predecessor(current.as_ref(), event)
}

fn replay_account(
    executor: &impl TransactionalTargetExecutor,
    journal: &impl DdlReplayJournal,
    event: &DdlEvent,
    evidence: &DdlSemanticEvidence,
    operation: &AccountOperation<'_>,
) -> Result<(), ApplyBinlogError> {
    let status = journal
        .read_status(event)
        .map_err(|_| account_blocked(event, "cannot read journal status"))?;
    match status {
        None | Some(DdlReplayStatus::TranslationPending) => journal
            .prepare(event, evidence)
            .map_err(|_| account_blocked(event, "cannot prepare journal row"))?,
        Some(DdlReplayStatus::Prepared | DdlReplayStatus::Applied) => {
            verify_replay_evidence(journal, event, evidence)?;
            if status == Some(DdlReplayStatus::Applied) {
                return Ok(());
            }
        }
        Some(DdlReplayStatus::Checkpointed | DdlReplayStatus::Blocked) => {
            return Err(account_blocked(event, "unexpected journal state"));
        }
    }
    match operation {
        AccountOperation::Create(translated) => execute_create_user(executor, event, translated)?,
        AccountOperation::Unlock(translated) => execute_unlock(executor, event, translated)?,
    }
    journal
        .mark_applied(event)
        .map_err(|_| account_blocked(event, "account DDL acknowledgment not persisted"))
}

fn verify_replay_evidence(
    journal: &impl DdlReplayJournal,
    event: &DdlEvent,
    expected: &DdlSemanticEvidence,
) -> Result<(), ApplyBinlogError> {
    let recorded = journal
        .read_evidence(event)
        .map_err(|_| account_blocked(event, "cannot read journal evidence"))?;
    if recorded.as_ref() != Some(expected) {
        return Err(account_blocked(event, "journal evidence differs"));
    }
    Ok(())
}

fn execute_create_user(
    executor: &impl TransactionalTargetExecutor,
    event: &DdlEvent,
    translated: &TranslatedCreateUser,
) -> Result<(), ApplyBinlogError> {
    executor
        .execute(&SqlStatement {
            sql: translated.as_sql().to_owned(),
            params: Vec::new(),
        })
        .map_err(|error| {
            let outcome = match error.mysql_code() {
                Some(1396) => "target rejected strict account creation; no overwrite attempted",
                Some(1227) => "target refused account-management privilege",
                _ => "target account creation was not confirmed",
            };
            account_blocked(event, outcome)
        })
}

fn execute_unlock(
    executor: &impl TransactionalTargetExecutor,
    event: &DdlEvent,
    translated: &TranslatedUnlockUser<'_>,
) -> Result<(), ApplyBinlogError> {
    executor
        .execute(&SqlStatement {
            sql: translated.as_sql().to_owned(),
            params: Vec::new(),
        })
        .map_err(|_| account_blocked(event, "target unlock was not confirmed"))
}

fn account_evidence(fingerprint: String, operation: &AccountOperation<'_>) -> DdlSemanticEvidence {
    let (version, pre_state, expected_post_state) = match operation {
        AccountOperation::Create(_) => (
            ACCOUNT_DDL_VERSION,
            "strict-create-must-not-overwrite",
            "strict-create-acknowledged",
        ),
        AccountOperation::Unlock(_) => (
            "approved-account-unlock-v1",
            "operator-approved-unlock-event",
            "account-unlock-acknowledged",
        ),
    };
    DdlSemanticEvidence {
        transformation_version: version.to_owned(),
        generated_sql: None,
        canonical_ast: fingerprint,
        pre_state: pre_state.to_owned(),
        expected_post_state: expected_post_state.to_owned(),
    }
}

fn account_blocked(event: &DdlEvent, reason: &str) -> ApplyBinlogError {
    ApplyBinlogError::DdlBlocked(format!(
        "account DDL at {}:{}: {reason}",
        event.binlog_file, event.event_start_position
    ))
}
