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
            match event {
                WorkerEvent::Initialized { worker, result } => {
                    result.map_err(|error| {
                        self.workers[worker].state = WorkerState::Failed;
                        self.fail(format!(
                            "parallel target worker {worker} connection failed: {error}"
                        ))
                    })?;
                    self.workers[worker].state = WorkerState::Idle;
                    initialized += 1;
                }
                _ => {
                    return Err(self.fail(
                        "parallel target worker emitted work before initialization".to_string(),
                    ));
                }
            }
        }
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
        match command {
            WorkerCommand::Execute {
                sequence,
                transaction,
                submitted,
            } => {
                let send_result = connection.send_query(&transaction.body_sql);
                match send_result {
                    Ok(()) => {
                        if submitted.send(Ok(())).is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = submitted.send(Err(error));
                        return;
                    }
                }
                let result = connection.read_query_result();
                if events
                    .send(WorkerEvent::Prepared {
                        worker,
                        sequence,
                        commit_sql: transaction.commit_sql,
                        checkpoint: transaction.checkpoint,
                        result,
                    })
                    .is_err()
                {
                    return;
                }
            }
            WorkerCommand::Commit {
                sequence,
                sql,
                checkpoint,
            } => {
                let result = commit_transaction(&mut connection, sequence, &sql);
                let failed = result.is_err();
                if events
                    .send(WorkerEvent::Committed {
                        worker,
                        sequence,
                        checkpoint,
                        result,
                    })
                    .is_err()
                    || failed
                {
                    return;
                }
            }
            WorkerCommand::Shutdown => return,
        }
    }
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
mod tests {
    use super::{
        ParallelTargetTransaction, ParallelTransactionPool, SubmittedQueryConnection,
        SubmittedQueryConnectionFactory,
    };
    use crate::checkpoint::{Checkpoint, LastEvent};
    use crate::target::TargetExecuteError;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    type ReadGate = Arc<(Mutex<BTreeMap<(usize, usize), bool>>, Condvar)>;

    #[derive(Clone)]
    struct FakeConnectionFactory {
        next_id: Arc<Mutex<usize>>,
        sent: Sender<(usize, String)>,
        reads: ReadGate,
        failed_sends: Arc<Mutex<BTreeSet<String>>>,
        failed_reads: Arc<Mutex<BTreeSet<(usize, usize)>>>,
    }

    impl SubmittedQueryConnectionFactory for FakeConnectionFactory {
        type Connection = FakeConnection;

        fn open(&self) -> Result<Self::Connection, TargetExecuteError> {
            let mut next_id = self.next_id.lock().expect("lock next connection id");
            let id = *next_id;
            *next_id += 1;
            Ok(FakeConnection {
                id,
                sent: self.sent.clone(),
                reads: Arc::clone(&self.reads),
                failed_sends: Arc::clone(&self.failed_sends),
                failed_reads: Arc::clone(&self.failed_reads),
                awaiting_result: false,
                read_index: 0,
            })
        }
    }

    struct FakeConnection {
        id: usize,
        sent: Sender<(usize, String)>,
        reads: ReadGate,
        failed_sends: Arc<Mutex<BTreeSet<String>>>,
        failed_reads: Arc<Mutex<BTreeSet<(usize, usize)>>>,
        awaiting_result: bool,
        read_index: usize,
    }

    impl SubmittedQueryConnection for FakeConnection {
        fn send_query(&mut self, sql: &str) -> Result<(), TargetExecuteError> {
            if self.awaiting_result {
                return Err(TargetExecuteError::new(
                    "cannot send another query before reading the prior result",
                ));
            }
            if self
                .failed_sends
                .lock()
                .expect("lock failed sends")
                .contains(sql)
            {
                return Err(TargetExecuteError::new("injected send failure"));
            }
            self.sent
                .send((self.id, sql.to_string()))
                .expect("record submitted query");
            self.awaiting_result = true;
            Ok(())
        }

        fn read_query_result(&mut self) -> Result<(), TargetExecuteError> {
            let key = (self.id, self.read_index);
            if self
                .failed_reads
                .lock()
                .expect("lock failed reads")
                .contains(&key)
            {
                return Err(TargetExecuteError::new("injected read failure"));
            }
            let (states, changed) = &*self.reads;
            let states = states.lock().expect("lock read state");
            drop(
                changed
                    .wait_while(states, |states| !states.get(&key).copied().unwrap_or(false))
                    .expect("wait for query result"),
            );
            self.awaiting_result = false;
            self.read_index += 1;
            Ok(())
        }
    }

    #[test]
    fn later_transaction_is_submitted_before_earlier_completion() {
        let (factory, submitted, reads) = fake_factory();
        let mut pool = ParallelTransactionPool::new(2, factory).expect("create pool");

        pool.submit(transaction(100))
            .expect("submit first transaction");
        let (first_connection, first_sql) = receive_submission(&submitted);
        assert_eq!(first_sql, "BEGIN; INSERT INTO events VALUES (100)");

        pool.submit(transaction(200))
            .expect("submit second transaction");
        let (second_connection, second_sql) = receive_submission(&submitted);
        assert_ne!(second_connection, first_connection);
        assert_eq!(second_sql, "BEGIN; INSERT INTO events VALUES (200)");

        release_read(&reads, first_connection, 0);
        release_read(&reads, first_connection, 1);
        release_read(&reads, second_connection, 0);
        release_read(&reads, second_connection, 1);
        pool.wait_for_all().expect("finish submitted transactions");
    }

    #[test]
    fn commits_in_source_order_when_later_body_finishes_first() {
        let (factory, submitted, reads) = fake_factory();
        let mut pool = ParallelTransactionPool::new(2, factory).expect("create pool");
        pool.submit(transaction(100))
            .expect("submit first transaction");
        let (first_connection, _) = receive_submission(&submitted);
        pool.submit(transaction(200))
            .expect("submit second transaction");
        let (second_connection, _) = receive_submission(&submitted);

        release_read(&reads, second_connection, 0);
        pool.wait_for_event()
            .expect("record second transaction prepared");
        assert!(submitted.recv_timeout(Duration::from_millis(50)).is_err());

        release_read(&reads, first_connection, 0);
        pool.wait_for_event()
            .expect("record first transaction prepared");
        assert_eq!(
            receive_submission(&submitted),
            (first_connection, "CHECKPOINT 100; COMMIT".to_string())
        );

        release_read(&reads, first_connection, 1);
        pool.wait_for_event()
            .expect("record first transaction committed");
        assert_eq!(
            receive_submission(&submitted),
            (second_connection, "CHECKPOINT 200; COMMIT".to_string())
        );

        release_read(&reads, second_connection, 1);
        pool.wait_for_all().expect("finish second transaction");
        let committed = pool.take_committed().expect("take committed transactions");
        assert_eq!(
            committed
                .iter()
                .map(|transaction| transaction.checkpoint.source_position)
                .collect::<Vec<_>>(),
            [100, 200]
        );
    }

    #[test]
    fn connection_stays_busy_until_commit_result_is_drained() {
        let (factory, submitted, reads) = fake_factory();
        let mut pool = ParallelTransactionPool::new(1, factory).expect("create pool");
        pool.submit(transaction(100)).expect("submit transaction");
        let (connection, _) = receive_submission(&submitted);
        assert_eq!(pool.idle_worker_count(), 0);

        release_read(&reads, connection, 0);
        pool.wait_for_event().expect("record transaction prepared");
        assert_eq!(pool.idle_worker_count(), 0);
        assert_eq!(
            receive_submission(&submitted),
            (connection, "CHECKPOINT 100; COMMIT".to_string())
        );

        release_read(&reads, connection, 1);
        pool.wait_for_all().expect("finish transaction");
        assert_eq!(pool.idle_worker_count(), 1);
    }

    #[test]
    fn send_failure_is_returned_before_transaction_is_accepted() {
        let (factory, _submitted, _reads) =
            fake_factory_with_failed_send("BEGIN; INSERT INTO events VALUES (100)");
        let mut pool = ParallelTransactionPool::new(1, factory).expect("create pool");

        let error = pool
            .submit(transaction(100))
            .expect_err("reject failed send");

        assert!(error.to_string().contains("send failed before acceptance"));
    }

    #[test]
    fn body_failure_is_reported_after_submission_without_committing() {
        let (factory, submitted, _reads) = fake_factory_with_failed_read((0, 0));
        let mut pool = ParallelTransactionPool::new(1, factory).expect("create pool");

        pool.submit(transaction(100))
            .expect("body send should be accepted before result failure");
        assert_eq!(
            receive_submission(&submitted),
            (0, "BEGIN; INSERT INTO events VALUES (100)".to_string())
        );
        let error = pool
            .wait_for_all()
            .expect_err("body result failure must stop the pool");

        assert!(error.to_string().contains("failed before commit"));
        assert!(submitted.recv_timeout(Duration::from_millis(50)).is_err());
    }

    fn fake_factory() -> (FakeConnectionFactory, Receiver<(usize, String)>, ReadGate) {
        fake_factory_with_failed_send("")
    }

    fn fake_factory_with_failed_send(
        failed_sql: &str,
    ) -> (FakeConnectionFactory, Receiver<(usize, String)>, ReadGate) {
        let failed_sends = if failed_sql.is_empty() {
            BTreeSet::new()
        } else {
            BTreeSet::from([failed_sql.to_string()])
        };
        fake_factory_with_failures(failed_sends, BTreeSet::new())
    }

    fn fake_factory_with_failed_read(
        failed_read: (usize, usize),
    ) -> (FakeConnectionFactory, Receiver<(usize, String)>, ReadGate) {
        fake_factory_with_failures(BTreeSet::new(), BTreeSet::from([failed_read]))
    }

    fn fake_factory_with_failures(
        failed_sends: BTreeSet<String>,
        failed_reads: BTreeSet<(usize, usize)>,
    ) -> (FakeConnectionFactory, Receiver<(usize, String)>, ReadGate) {
        let (sent, submitted) = mpsc::channel();
        let reads = Arc::new((Mutex::new(BTreeMap::new()), Condvar::new()));
        (
            FakeConnectionFactory {
                next_id: Arc::new(Mutex::new(0)),
                sent,
                reads: Arc::clone(&reads),
                failed_sends: Arc::new(Mutex::new(failed_sends)),
                failed_reads: Arc::new(Mutex::new(failed_reads)),
            },
            submitted,
            reads,
        )
    }

    fn receive_submission(submitted: &Receiver<(usize, String)>) -> (usize, String) {
        submitted
            .recv_timeout(Duration::from_secs(1))
            .expect("receive query submission")
    }

    fn release_read(reads: &ReadGate, connection: usize, read_index: usize) {
        let (states, changed) = &**reads;
        states
            .lock()
            .expect("lock read state")
            .insert((connection, read_index), true);
        changed.notify_all();
    }

    fn transaction(position: u64) -> ParallelTargetTransaction {
        ParallelTargetTransaction {
            body_sql: format!("BEGIN; INSERT INTO events VALUES ({position})"),
            commit_sql: format!("CHECKPOINT {position}; COMMIT"),
            checkpoint: checkpoint(position),
        }
    }

    fn checkpoint(position: u64) -> Checkpoint {
        Checkpoint {
            source_file: "mysqld-bin.000001".to_string(),
            source_position: position,
            gtid: None,
            event_timestamp: 0,
            last_event: LastEvent {
                event_type: "XidEvent".to_string(),
                description: "parallel target test checkpoint".to_string(),
            },
        }
    }
}
