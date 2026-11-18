use mariadb_mysql_cdc::account_ddl::{TranslatedCreateUser, translate_create_user};
use sha2::{Digest, Sha256};

use super::ddl::{
    ddl_event, ensure_automatic_ddl_checkpoint_predecessor, finalize_automatic_ddl_checkpoint,
    resolved_ddl_outcome,
};
use super::*;
use crate::live::ddl_semantics::DdlSemanticEvidence;
use crate::target::SqlStatement;

const ACCOUNT_DDL_VERSION: &str = "locked-tls-account-v1";

pub(super) fn handle_create_user<E, R, C, J>(
    executor: &E,
    journal: &J,
    source_identity: &str,
    input: AutomaticDdlInput<'_, '_, R, C>,
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
    let Ok(translated) = translate_create_user(&query.sql_statement) else {
        return Ok(None);
    };
    let source_event = ddl_event(source_identity, context.current_file, header, query);
    flush_grouped_transaction(executor, context)?;
    if context.target_transaction.is_open() {
        return Err(account_blocked(
            &source_event,
            "target row transaction is still open",
        ));
    }
    validate_account_checkpoint(context, &source_event)?;
    let fingerprint = format!(
        "account-ddl:sha256:{:x}",
        Sha256::digest(&source_event.raw_sql)
    );
    let journal_event = journal
        .account_event_identity(source_event.clone(), &fingerprint)
        .map_err(|_| account_blocked(&source_event, "journal identity rejected"))?;
    let evidence = account_evidence(fingerprint);
    replay_account(executor, journal, &journal_event, &evidence, &translated)?;
    finalize_automatic_ddl_checkpoint(executor, journal, context, event, &journal_event)
        .map_err(|_| account_blocked(&journal_event, "checkpoint transaction not confirmed"))?;
    context.state.clear_query_context();
    Ok(Some(resolved_ddl_outcome(journal_event)))
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
    translated: &TranslatedCreateUser,
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
    execute_create_user(executor, event, translated)?;
    journal
        .mark_applied(event)
        .map_err(|_| account_blocked(event, "account creation acknowledgment not persisted"))
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

fn account_evidence(fingerprint: String) -> DdlSemanticEvidence {
    DdlSemanticEvidence {
        transformation_version: ACCOUNT_DDL_VERSION.to_owned(),
        generated_sql: None,
        canonical_ast: fingerprint,
        pre_state: "strict-create-must-not-overwrite".to_owned(),
        expected_post_state: "strict-create-acknowledged".to_owned(),
    }
}

fn account_blocked(event: &DdlEvent, reason: &str) -> ApplyBinlogError {
    ApplyBinlogError::DdlBlocked(format!(
        "account DDL at {}:{}: {reason}",
        event.binlog_file, event.event_start_position
    ))
}
