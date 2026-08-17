use super::*;
mod completion;
pub(super) use completion::*;

pub(crate) fn stream_remote_binlog(config: &ApplyBinlogConfig) -> Result<(), ApplyBinlogError> {
    verify_source_binlog_contract(config)?;
    let checkpoint_store = crate::stream_checkpoint::MySqlStreamCheckpointStore::new(
        config.target.clone(),
        config.checkpoint_table.clone(),
        &config.source_identity,
    );
    let checkpoint_name = crate::stream_checkpoint::stream_checkpoint_name(&config.source_identity);
    checkpoint_store
        .ensure()
        .map_err(ApplyBinlogError::Checkpoint)?;
    validate_startup_contract(config)?;
    stream_with_checkpoint_store(
        config,
        Some(&checkpoint_store),
        Some(config.checkpoint_table.as_str()),
        Some(checkpoint_name.as_str()),
    )
}

pub(super) fn validate_startup_contract(
    config: &ApplyBinlogConfig,
) -> Result<(), ApplyBinlogError> {
    let executor = crate::mysql_client::PersistentTargetExecutor::new(&config.target)
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    executor
        .acquire_stream_lease(&format!("cdc-stream:{}", config.target.database))
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    let ddl_replay_journal =
        MySqlDdlReplayJournal::new(&config.target, "cdc.ddl_replay_journal".to_string());
    ddl_replay_journal
        .ensure()
        .map_err(ApplyBinlogError::Statement)
}

pub(super) fn stream_with_checkpoint_store<C>(
    config: &ApplyBinlogConfig,
    checkpoint_store: Option<&C>,
    transaction_checkpoint_table: Option<&str>,
    transaction_checkpoint_name: Option<&str>,
) -> Result<(), ApplyBinlogError>
where
    C: StreamCheckpointStore,
{
    run_stream_reconnect_loop(
        config,
        checkpoint_store,
        |attempt_config| {
            stream_once(
                attempt_config,
                checkpoint_store,
                transaction_checkpoint_table,
                transaction_checkpoint_name,
            )
        },
        thread::sleep,
    )
}

pub(super) fn stream_once(
    config: &ApplyBinlogConfig,
    checkpoint_store: Option<&impl StreamCheckpointStore>,
    transaction_checkpoint_table: Option<&str>,
    transaction_checkpoint_name: Option<&str>,
) -> Result<(), ApplyBinlogError> {
    #[cfg(feature = "integration-failpoints")]
    super::super::configure_integration_failpoint(config.integration_failpoint);

    let mut runtime = StreamRuntime::initialize(config)?;
    loop {
        let result = if runtime.durable_progress.is_some() {
            match runtime
                .event_receiver
                .recv_timeout(PARALLEL_TARGET_RESULT_POLL_INTERVAL)
            {
                Ok(result) => result,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    reap_parallel_target_transactions(&mut runtime)?;
                    continue;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        } else {
            match runtime.event_receiver.recv() {
                Ok(result) => result,
                Err(_) => break,
            }
        };
        let (header, event, source_position) = match result {
            Ok(event) => event,
            Err(error) => {
                let target_result = reap_parallel_target_transactions(&mut runtime);
                let rollback_result = rollback_stream_transaction(&mut runtime);
                target_result?;
                rollback_result?;
                wait_for_parallel_target_transactions(&mut runtime)?;
                return Err(source_error(error));
            }
        };
        reap_parallel_target_transactions(&mut runtime)?;
        let process_result = process_stream_event(
            config,
            &mut runtime,
            StreamCheckpointContext {
                store: checkpoint_store,
                table: transaction_checkpoint_table,
                name: transaction_checkpoint_name,
            },
            SourceStreamEvent {
                header: &header,
                event: &event,
                source_position,
            },
        );
        reap_parallel_target_transactions(&mut runtime)?;
        let stop_decision = process_result?;
        if stop_decision == StopPositionDecision::DispatchAndStop {
            return complete_bounded_stop(
                &mut runtime,
                checkpoint_store,
                transaction_checkpoint_table,
                transaction_checkpoint_name,
            );
        }
    }

    finish_stream(
        config,
        &mut runtime,
        checkpoint_store,
        transaction_checkpoint_table,
        transaction_checkpoint_name,
    )
}

pub(super) struct StreamRuntime {
    applier: RowApplier<crate::mysql_client::PersistentTargetExecutor>,
    ddl_replay_journal: MySqlDdlReplayJournal,
    semantic_inventory: LiveDdlSemanticInventory,
    schema_resolver: TargetInventorySchemaResolver,
    event_receiver: std::sync::mpsc::Receiver<BinlogEventResult>,
    current_file: String,
    state: StructuredEventState,
    progress: StreamProgress,
    durable_progress: Option<StreamProgress>,
    target_transaction: TargetTransaction,
    group_config: TargetTransactionGroupConfig,
    source_identity: String,
    source_row_transaction_open: bool,
}

impl StreamRuntime {
    pub(super) fn initialize(config: &ApplyBinlogConfig) -> Result<Self, ApplyBinlogError> {
        let (applier, ddl_replay_journal) = initialize_target_services(config)?;
        let semantic_inventory = initialize_semantic_inventory(config)?;
        let schema_resolver = TargetInventorySchemaResolver::new(config);
        let (event_receiver, current_file) = start_binlog_receiver(config)?;
        let start_coordinate = BinlogCoordinate {
            file: config.source.binlog_file.clone(),
            position: config.source.start_position,
        };
        let durable_progress = applier
            .executor()
            .uses_parallel_transactions()
            .then(|| StreamProgress::new(start_coordinate.clone()));
        Ok(Self {
            applier,
            ddl_replay_journal,
            semantic_inventory,
            schema_resolver,
            event_receiver,
            current_file,
            state: StructuredEventState::new(config.source.database.clone()),
            progress: StreamProgress::new(start_coordinate),
            durable_progress,
            target_transaction: TargetTransaction::default(),
            group_config: TargetTransactionGroupConfig::from_apply_config(config),
            source_identity: config.source_identity.clone(),
            source_row_transaction_open: false,
        })
    }
}

fn initialize_target_services(
    config: &ApplyBinlogConfig,
) -> Result<
    (
        RowApplier<crate::mysql_client::PersistentTargetExecutor>,
        MySqlDdlReplayJournal,
    ),
    ApplyBinlogError,
> {
    let executor = crate::mysql_client::PersistentTargetExecutor::new_for_stream(config)
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    executor
        .acquire_stream_lease(&format!("cdc-stream:{}", config.target.database))
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    let applier = RowApplier::new(executor);
    let ddl_replay_journal =
        MySqlDdlReplayJournal::new(&config.target, "cdc.ddl_replay_journal".to_string());
    validate_ddl_replay_barrier(&ddl_replay_journal, config)?;
    Ok((applier, ddl_replay_journal))
}

fn validate_ddl_replay_barrier(
    journal: &MySqlDdlReplayJournal,
    config: &ApplyBinlogConfig,
) -> Result<(), ApplyBinlogError> {
    let barrier = journal
        .earliest_barrier(&config.source_identity)
        .map_err(ApplyBinlogError::Statement)?;
    crate::live::ddl_replay_journal::enforce_no_overtake(
        barrier.as_ref(),
        &config.source.binlog_file,
        config.source.start_position,
    )
    .map_err(ApplyBinlogError::Statement)
}

fn initialize_semantic_inventory(
    config: &ApplyBinlogConfig,
) -> Result<LiveDdlSemanticInventory, ApplyBinlogError> {
    let source_schema = config.source.database.clone().ok_or_else(|| {
        ApplyBinlogError::Config(
            "automatic DDL semantic inventory requires a source database".to_string(),
        )
    })?;
    Ok(LiveDdlSemanticInventory::new(
        source_inventory_config(config),
        target_inventory_config(config),
        source_schema,
        config.target.database.clone(),
    ))
}

fn start_binlog_receiver(
    config: &ApplyBinlogConfig,
) -> Result<(std::sync::mpsc::Receiver<BinlogEventResult>, String), ApplyBinlogError> {
    let mut client = BinlogClient::new(replica_options_from_source(&config.source)?);
    let initial_position = config.source.start_position;
    let events = client.replicate().map_err(source_error)?;
    let receiver = spawn_read_ahead_reader(client, events, initial_position);
    Ok((receiver, config.source.binlog_file.clone()))
}

#[derive(Clone, Copy)]
pub(super) struct StreamCheckpointContext<'a, C> {
    store: Option<&'a C>,
    table: Option<&'a str>,
    name: Option<&'a str>,
}

#[derive(Clone, Copy)]
pub(super) struct SourceStreamEvent<'a> {
    pub(super) header: &'a EventHeader,
    pub(super) event: &'a BinlogEvent,
    pub(super) source_position: u64,
}

pub(super) fn process_stream_event<C>(
    config: &ApplyBinlogConfig,
    runtime: &mut StreamRuntime,
    checkpoint: StreamCheckpointContext<'_, C>,
    input: SourceStreamEvent<'_>,
) -> Result<StopPositionDecision, ApplyBinlogError>
where
    C: StreamCheckpointStore,
{
    let stop_decision = match stop_position_decision(
        config.source.stop_position,
        input.header,
        runtime.source_row_transaction_open,
    ) {
        Ok(decision) => decision,
        Err(error) => {
            rollback_stream_transaction(runtime)?;
            return Err(error);
        }
    };
    let mut state = std::mem::replace(
        &mut runtime.state,
        StructuredEventState::new(config.source.database.clone()),
    );
    let mut progress = runtime.progress.clone();
    let mut source_row_transaction_open = runtime.source_row_transaction_open;
    let record_progress = runtime.durable_progress.is_none();
    let result = process_stream_event_core_after_stop_decision(
        &mut state,
        &mut progress,
        &mut source_row_transaction_open,
        input,
        stop_decision,
        record_progress,
        |state, input| dispatch_stream_event(runtime, state, &checkpoint, input),
    );
    runtime.state = state;
    runtime.progress = progress;
    runtime.source_row_transaction_open = source_row_transaction_open;
    if let Ok((_, _outcome)) = &result {
        #[cfg(feature = "integration-failpoints")]
        if _outcome.policy == EventPolicy::CommitTransaction
            && !runtime.target_transaction.is_open()
        {
            super::super::wait_for_integration_barrier(
                super::super::IntegrationFailpoint::SourceConnectionLoss,
                "after-committed-event",
            );
        }
    }
    result.map(|(stop_decision, _)| stop_decision)
}

fn process_stream_event_core_after_stop_decision<D>(
    state: &mut StructuredEventState,
    progress: &mut StreamProgress,
    source_row_transaction_open: &mut bool,
    input: SourceStreamEvent<'_>,
    stop_decision: StopPositionDecision,
    record_progress: bool,
    mut dispatch: D,
) -> Result<(StopPositionDecision, StructuredEventOutcome), ApplyBinlogError>
where
    D: FnMut(
        &mut StructuredEventState,
        SourceStreamEvent<'_>,
    ) -> Result<StructuredEventOutcome, ApplyBinlogError>,
{
    state.record_event_position(input.source_position);
    let outcome = dispatch(state, input)?;
    if record_progress {
        log_stream_progress(progress, &outcome);
    }
    update_source_row_transaction_state(source_row_transaction_open, input.event);
    Ok((stop_decision, outcome))
}

fn dispatch_stream_event<C>(
    runtime: &mut StreamRuntime,
    state: &mut StructuredEventState,
    checkpoint: &StreamCheckpointContext<'_, C>,
    input: SourceStreamEvent<'_>,
) -> Result<StructuredEventOutcome, ApplyBinlogError>
where
    C: StreamCheckpointStore,
{
    let StreamRuntime {
        applier,
        ddl_replay_journal,
        semantic_inventory,
        schema_resolver,
        current_file,
        target_transaction,
        group_config,
        source_identity,
        ..
    } = runtime;
    let mut context = StreamEventContext {
        schema_resolver,
        state,
        target_transaction,
        checkpoint_store: checkpoint.store,
        transaction_checkpoint_table: checkpoint.table,
        transaction_checkpoint_name: checkpoint.name,
        current_file,
        group_config: *group_config,
    };
    match handle_ddl_event(
        applier,
        ddl_replay_journal,
        semantic_inventory,
        source_identity,
        &mut context,
        input.header,
        input.event,
    )? {
        Some(outcome) => Ok(outcome),
        None => {
            apply_stream_event_transactionally(applier, &mut context, input.header, input.event)
        }
    }
}

type BinlogEventResult = Result<(EventHeader, BinlogEvent, u64), MysqlCdcError>;

pub(super) fn spawn_read_ahead_reader(
    mut client: BinlogClient,
    mut events: mysql_cdc::binlog_events::BinlogEvents,
    initial_position: u64,
) -> std::sync::mpsc::Receiver<BinlogEventResult> {
    let (sender, receiver) = std::sync::mpsc::sync_channel(READ_AHEAD_EVENT_BUFFER);
    thread::spawn(move || {
        let mut source_position = initial_position;
        for result in &mut events {
            let stop_after_send = result.is_err();
            let result = result.map(|(header, event)| {
                let event_position = source_position;
                source_position = next_source_position(source_position, &header, &event);
                client.commit(&header, &event);
                (header, event, event_position)
            });
            if sender.send(result).is_err() || stop_after_send {
                return;
            }
        }
    });
    receiver
}

fn next_source_position(current_position: u64, header: &EventHeader, event: &BinlogEvent) -> u64 {
    if let BinlogEvent::RotateEvent(rotate) = event {
        return rotate.binlog_position;
    }
    if header.next_event_position > 0 {
        return u64::from(header.next_event_position);
    }
    current_position.saturating_add(u64::from(header.event_length))
}

pub(super) fn replica_options_from_source(
    source: &SourceBinlogConfig,
) -> Result<ReplicaOptions, ApplyBinlogError> {
    let server_id = source
        .stop_never_slave_server_id
        .unwrap_or(DEFAULT_REPLICA_SERVER_ID);

    Ok(ReplicaOptions {
        port: source.port,
        hostname: source.host.clone(),
        ssl_mode: SslMode::Disabled,
        ssl_ca_file: None,
        username: source.user.clone(),
        password: source.password.clone(),
        database: source.database.clone(),
        server_id,
        blocking: true,
        heartbeat_interval: Duration::from_secs(MYSQL_CDC_HEARTBEAT_SECONDS),
        binlog: binlog_options_from_source_position(
            source.binlog_file.clone(),
            source.start_position,
        )?,
    })
}

pub(super) fn binlog_options_from_source_position(
    filename: String,
    position: u64,
) -> Result<BinlogOptions, ApplyBinlogError> {
    let position = u32::try_from(position).map_err(|_| {
        ApplyBinlogError::Config(format!(
            "start position {position} exceeds mysql_cdc u32 position limit"
        ))
    })?;
    Ok(BinlogOptions::from_position(filename, position))
}

pub(super) fn verify_source_binlog_contract(
    config: &ApplyBinlogConfig,
) -> Result<(), ApplyBinlogError> {
    let reader = MariaDbInventoryReader::new(source_inventory_config(config));
    let settings = reader
        .read_source_binlog_settings()
        .map_err(|error| ApplyBinlogError::SourceCommand(error.to_string()))?;
    validate_source_binlog_settings(&settings)
}

pub(super) fn validate_source_binlog_settings(
    settings: &SourceBinlogSettings,
) -> Result<(), ApplyBinlogError> {
    if settings.format.eq_ignore_ascii_case("ROW")
        && settings.row_image.eq_ignore_ascii_case("FULL")
    {
        return Ok(());
    }
    Err(ApplyBinlogError::Config(format!(
        "stream-binlog requires source binlog_format=ROW and binlog_row_image=FULL; found format={} row_image={}",
        settings.format, settings.row_image
    )))
}

pub(super) fn source_inventory_config(config: &ApplyBinlogConfig) -> InventoryConfig {
    InventoryConfig {
        host: config.source.host.clone(),
        port: config.source.port,
        user: config.source.user.clone(),
        password: config.source.password.clone(),
        endpoint_role: InventoryEndpointRole::Source,
        use_tls: false,
        tls_ca_file: None,
        ..InventoryConfig::default()
    }
}

pub(super) fn target_inventory_config(config: &ApplyBinlogConfig) -> InventoryConfig {
    InventoryConfig {
        host: config.target.host.clone(),
        port: config.target.port,
        user: config.target.user.clone(),
        password: config.target.password.clone(),
        endpoint_role: InventoryEndpointRole::Target,
        use_tls: true,
        tls_ca_file: Some(config.target.tls_ca_file.clone()),
        ..InventoryConfig::default()
    }
}
