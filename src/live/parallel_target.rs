use crate::checkpoint::Checkpoint;
use crate::target::TargetExecuteError;
use std::collections::{BTreeMap, VecDeque};
use std::marker::PhantomData;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};

pub(crate) trait SubmittedQueryConnection {
    /// Returns only after the database client accepted the query for network submission.
    fn send_query(&mut self, sql: &str) -> Result<(), TargetExecuteError>;

    /// Drains every result belonging to the most recently submitted query.
    fn read_query_result(&mut self) -> Result<(), TargetExecuteError>;
}

pub(crate) trait SubmittedQueryConnectionFactory: Clone + Send + Sync + 'static {
    type Connection: SubmittedQueryConnection;

    fn open(&self) -> Result<Self::Connection, TargetExecuteError>;
}

#[derive(Clone, Debug)]
pub(super) struct ParallelTargetTransaction {
    pub(super) body_sql: String,
    pub(super) commit_sql: String,
    pub(super) checkpoint: Checkpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CommittedTargetTransaction {
    pub(super) sequence: u64,
    pub(super) checkpoint: Checkpoint,
}

pub(super) struct ParallelTransactionPool<F>
where
    F: SubmittedQueryConnectionFactory,
{
    workers: Vec<TransactionWorker>,
    events: Receiver<WorkerEvent>,
    prepared: BTreeMap<u64, PreparedTransaction>,
    committed: VecDeque<CommittedTargetTransaction>,
    next_sequence: u64,
    next_commit_sequence: u64,
    in_flight: usize,
    failure: Option<String>,
    _factory: PhantomData<F>,
}

struct TransactionWorker {
    commands: Sender<WorkerCommand>,
    state: WorkerState,
    thread: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerState {
    Initializing,
    Idle,
    Executing(u64),
    Prepared(u64),
    Committing(u64),
    Failed,
}

enum WorkerCommand {
    Execute {
        sequence: u64,
        transaction: ParallelTargetTransaction,
        submitted: Sender<Result<(), TargetExecuteError>>,
    },
    Commit {
        sequence: u64,
        sql: String,
        checkpoint: Checkpoint,
    },
    Shutdown,
}

enum WorkerEvent {
    Initialized {
        worker: usize,
        result: Result<(), TargetExecuteError>,
    },
    Prepared {
        worker: usize,
        sequence: u64,
        commit_sql: String,
        checkpoint: Checkpoint,
        result: Result<(), TargetExecuteError>,
    },
    Committed {
        worker: usize,
        sequence: u64,
        checkpoint: Checkpoint,
        result: Result<(), TargetExecuteError>,
    },
}

struct PreparedTransaction {
    worker: usize,
    commit_sql: String,
    checkpoint: Checkpoint,
}

impl<F> ParallelTransactionPool<F>
where
    F: SubmittedQueryConnectionFactory,
{
    pub(super) fn new(worker_count: usize, factory: F) -> Result<Self, TargetExecuteError> {
        if worker_count == 0 {
            return Err(TargetExecuteError::new(
                "parallel target transaction pool requires at least one connection",
            ));
        }

        let (events_tx, events) = mpsc::channel();
        let workers = (0..worker_count)
            .map(|worker| spawn_worker(worker, factory.clone(), events_tx.clone()))
            .collect();
        let mut pool = Self {
            workers,
            events,
            prepared: BTreeMap::new(),
            committed: VecDeque::new(),
            next_sequence: 0,
            next_commit_sequence: 0,
            in_flight: 0,
            failure: None,
            _factory: PhantomData,
        };
        pool.wait_for_worker_initialization()?;
        Ok(pool)
    }

    pub(super) fn submit(
        &mut self,
        transaction: ParallelTargetTransaction,
    ) -> Result<u64, TargetExecuteError> {
        self.poll()?;
        let worker = self.wait_for_idle_worker()?;
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.workers[worker].state = WorkerState::Executing(sequence);

        let (submitted, submission) = mpsc::channel();
        self.workers[worker]
            .commands
            .send(WorkerCommand::Execute {
                sequence,
                transaction,
                submitted,
            })
            .map_err(|_| {
                self.workers[worker].state = WorkerState::Failed;
                self.fail(format!(
                    "parallel target worker {worker} stopped before transaction {sequence} submission"
                ))
            })?;
        match submission.recv() {
            Ok(Ok(())) => {
                self.in_flight += 1;
                Ok(sequence)
            }
            Ok(Err(error)) => {
                self.workers[worker].state = WorkerState::Failed;
                Err(self.fail(format!(
                    "parallel target transaction {sequence} send failed before acceptance: {error}"
                )))
            }
            Err(_) => {
                self.workers[worker].state = WorkerState::Failed;
                Err(self.fail(format!(
                    "parallel target transaction {sequence} submission acknowledgment was lost"
                )))
            }
        }
    }

    pub(super) fn poll(&mut self) -> Result<(), TargetExecuteError> {
        self.ensure_healthy()?;
        loop {
            match self.events.try_recv() {
                Ok(event) => self.accept_event(event)?,
                Err(TryRecvError::Empty) => return self.ensure_healthy(),
                Err(TryRecvError::Disconnected) => {
                    return Err(
                        self.fail("parallel target worker event channel disconnected".to_string())
                    );
                }
            }
        }
    }

    pub(super) fn wait_for_all(&mut self) -> Result<(), TargetExecuteError> {
        self.ensure_healthy()?;
        while self.in_flight > 0 {
            self.wait_for_event()?;
        }
        self.ensure_healthy()
    }

    pub(super) fn take_committed(
        &mut self,
    ) -> Result<Vec<CommittedTargetTransaction>, TargetExecuteError> {
        self.poll()?;
        Ok(self.committed.drain(..).collect())
    }

    fn wait_for_worker_initialization(&mut self) -> Result<(), TargetExecuteError> {
        let mut initialized = 0;
        while initialized < self.workers.len() {
            let event = self.events.recv().map_err(|_| {
                self.fail("parallel target worker initialization channel disconnected".to_string())
            })?;
            self.accept_worker_initialization(event)?;
            initialized += 1;
        }
        Ok(())
    }

    fn accept_worker_initialization(
        &mut self,
        event: WorkerEvent,
    ) -> Result<(), TargetExecuteError> {
        let WorkerEvent::Initialized { worker, result } = event else {
            return Err(
                self.fail("parallel target worker emitted work before initialization".to_string())
            );
        };
        if let Err(error) = result {
            self.workers[worker].state = WorkerState::Failed;
            return Err(self.fail(format!(
                "parallel target worker {worker} connection failed: {error}"
            )));
        }
        self.workers[worker].state = WorkerState::Idle;
        Ok(())
    }

    fn wait_for_idle_worker(&mut self) -> Result<usize, TargetExecuteError> {
        loop {
            self.ensure_healthy()?;
            if let Some(worker) = self
                .workers
                .iter()
                .position(|worker| worker.state == WorkerState::Idle)
            {
                return Ok(worker);
            }
            self.wait_for_event()?;
        }
    }

    fn wait_for_event(&mut self) -> Result<(), TargetExecuteError> {
        let event = self.events.recv().map_err(|_| {
            self.fail("parallel target worker event channel disconnected".to_string())
        })?;
        self.accept_event(event)
    }

    fn accept_event(&mut self, event: WorkerEvent) -> Result<(), TargetExecuteError> {
        match event {
            WorkerEvent::Initialized { .. } => {
                Err(self.fail("parallel target worker initialized more than once".to_string()))
            }
            WorkerEvent::Prepared {
                worker,
                sequence,
                commit_sql,
                checkpoint,
                result,
            } => self.accept_prepared(worker, sequence, commit_sql, checkpoint, result),
            WorkerEvent::Committed {
                worker,
                sequence,
                checkpoint,
                result,
            } => self.accept_committed(worker, sequence, checkpoint, result),
        }
    }

    fn accept_prepared(
        &mut self,
        worker: usize,
        sequence: u64,
        commit_sql: String,
        checkpoint: Checkpoint,
        result: Result<(), TargetExecuteError>,
    ) -> Result<(), TargetExecuteError> {
        self.expect_worker_state(worker, WorkerState::Executing(sequence))?;
        if let Err(error) = result {
            self.workers[worker].state = WorkerState::Failed;
            return Err(self.fail(format!(
                "parallel target transaction {sequence} failed before commit: {error}"
            )));
        }
        self.workers[worker].state = WorkerState::Prepared(sequence);
        self.prepared.insert(
            sequence,
            PreparedTransaction {
                worker,
                commit_sql,
                checkpoint,
            },
        );
        self.start_next_commit()
    }

    fn accept_committed(
        &mut self,
        worker: usize,
        sequence: u64,
        checkpoint: Checkpoint,
        result: Result<(), TargetExecuteError>,
    ) -> Result<(), TargetExecuteError> {
        self.expect_worker_state(worker, WorkerState::Committing(sequence))?;
        if let Err(error) = result {
            self.workers[worker].state = WorkerState::Failed;
            return Err(self.fail(format!(
                "parallel target transaction {sequence} commit failed: {error}"
            )));
        }
        self.workers[worker].state = WorkerState::Idle;
        self.in_flight -= 1;
        self.next_commit_sequence += 1;
        self.committed.push_back(CommittedTargetTransaction {
            sequence,
            checkpoint,
        });
        self.start_next_commit()
    }

    fn start_next_commit(&mut self) -> Result<(), TargetExecuteError> {
        let sequence = self.next_commit_sequence;
        let Some(prepared) = self.prepared.remove(&sequence) else {
            return Ok(());
        };
        self.expect_worker_state(prepared.worker, WorkerState::Prepared(sequence))?;
        self.workers[prepared.worker].state = WorkerState::Committing(sequence);
        self.workers[prepared.worker]
            .commands
            .send(WorkerCommand::Commit {
                sequence,
                sql: prepared.commit_sql,
                checkpoint: prepared.checkpoint,
            })
            .map_err(|_| {
                self.workers[prepared.worker].state = WorkerState::Failed;
                self.fail(format!(
                    "parallel target worker {} stopped before transaction {sequence} commit",
                    prepared.worker
                ))
            })?;
        Ok(())
    }

    fn expect_worker_state(
        &mut self,
        worker: usize,
        expected: WorkerState,
    ) -> Result<(), TargetExecuteError> {
        let actual = self
            .workers
            .get(worker)
            .map(|worker| worker.state)
            .ok_or_else(|| self.fail(format!("unknown parallel target worker {worker}")))?;
        if actual == expected {
            return Ok(());
        }
        Err(self.fail(format!(
            "parallel target worker {worker} state mismatch: expected {expected:?}, found {actual:?}"
        )))
    }

    fn ensure_healthy(&self) -> Result<(), TargetExecuteError> {
        match &self.failure {
            Some(message) => Err(TargetExecuteError::new(message.clone())),
            None => Ok(()),
        }
    }

    fn fail(&mut self, message: String) -> TargetExecuteError {
        self.failure.get_or_insert_with(|| message.clone());
        TargetExecuteError::new(message)
    }

    #[cfg(test)]
    fn idle_worker_count(&self) -> usize {
        self.workers
            .iter()
            .filter(|worker| worker.state == WorkerState::Idle)
            .count()
    }
}

impl<F> Drop for ParallelTransactionPool<F>
where
    F: SubmittedQueryConnectionFactory,
{
    fn drop(&mut self) {
        for worker in &self.workers {
            let _ = worker.commands.send(WorkerCommand::Shutdown);
        }
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

fn spawn_worker<F>(worker: usize, factory: F, events: Sender<WorkerEvent>) -> TransactionWorker
where
    F: SubmittedQueryConnectionFactory,
{
    let (commands, received) = mpsc::channel();
    let thread = thread::spawn(move || match factory.open() {
        Ok(connection) => {
            if events
                .send(WorkerEvent::Initialized {
                    worker,
                    result: Ok(()),
                })
                .is_ok()
            {
                run_worker(worker, connection, received, events);
            }
        }
        Err(error) => {
            let _ = events.send(WorkerEvent::Initialized {
                worker,
                result: Err(error),
            });
        }
    });
    TransactionWorker {
        commands,
        state: WorkerState::Initializing,
        thread: Some(thread),
    }
}

fn run_worker<C>(
    worker: usize,
    mut connection: C,
    commands: Receiver<WorkerCommand>,
    events: Sender<WorkerEvent>,
) where
    C: SubmittedQueryConnection,
{
    while let Ok(command) = commands.recv() {
        if !execute_worker_command(worker, &mut connection, &events, command) {
            return;
        }
    }
}

fn execute_worker_command<C>(
    worker: usize,
    connection: &mut C,
    events: &Sender<WorkerEvent>,
    command: WorkerCommand,
) -> bool
where
    C: SubmittedQueryConnection,
{
    match command {
        WorkerCommand::Execute {
            sequence,
            transaction,
            submitted,
        } => submit_and_drain_transaction_body(
            worker,
            connection,
            events,
            sequence,
            transaction,
            submitted,
        ),
        WorkerCommand::Commit {
            sequence,
            sql,
            checkpoint,
        } => submit_and_drain_transaction_commit(
            worker, connection, events, sequence, sql, checkpoint,
        ),
        WorkerCommand::Shutdown => false,
    }
}

fn submit_and_drain_transaction_body<C>(
    worker: usize,
    connection: &mut C,
    events: &Sender<WorkerEvent>,
    sequence: u64,
    transaction: ParallelTargetTransaction,
    submitted: Sender<Result<(), TargetExecuteError>>,
) -> bool
where
    C: SubmittedQueryConnection,
{
    if let Err(error) = connection.send_query(&transaction.body_sql) {
        let _ = submitted.send(Err(error));
        return false;
    }
    if submitted.send(Ok(())).is_err() {
        return false;
    }
    #[cfg(feature = "integration-failpoints")]
    if sequence == 0 {
        super::wait_for_integration_barrier(
            super::IntegrationFailpoint::ParallelTargetSubmission,
            "parallel-target-first-body-submitted",
        );
    }
    let result = connection.read_query_result();
    #[cfg(feature = "integration-failpoints")]
    if sequence == 1 && result.is_ok() {
        super::wait_for_integration_barrier(
            super::IntegrationFailpoint::ParallelTargetSubmission,
            "parallel-target-second-body-drained",
        );
    }
    events
        .send(WorkerEvent::Prepared {
            worker,
            sequence,
            commit_sql: transaction.commit_sql,
            checkpoint: transaction.checkpoint,
            result,
        })
        .is_ok()
}

fn submit_and_drain_transaction_commit<C>(
    worker: usize,
    connection: &mut C,
    events: &Sender<WorkerEvent>,
    sequence: u64,
    sql: String,
    checkpoint: Checkpoint,
) -> bool
where
    C: SubmittedQueryConnection,
{
    let result = commit_transaction(connection, sequence, &sql);
    let succeeded = result.is_ok();
    events
        .send(WorkerEvent::Committed {
            worker,
            sequence,
            checkpoint,
            result,
        })
        .is_ok()
        && succeeded
}

fn commit_transaction<C>(
    connection: &mut C,
    sequence: u64,
    sql: &str,
) -> Result<(), TargetExecuteError>
where
    C: SubmittedQueryConnection,
{
    connection.send_query(sql).map_err(|error| {
        TargetExecuteError::new(format!(
            "transaction {sequence} commit send failed before acceptance: {error}"
        ))
    })?;
    connection.read_query_result().map_err(|error| {
        TargetExecuteError::new(format!(
            "transaction {sequence} commit execution state is unknown after accepted send: {error}"
        ))
    })
}

#[cfg(test)]
#[path = "parallel_target_tests.rs"]
mod tests;
