use super::{
    ParallelTargetStatement, ParallelTargetTransaction, ParallelTransactionPool,
    SubmittedQueryConnection, SubmittedQueryConnectionFactory,
};
use crate::checkpoint::{Checkpoint, LastEvent};
use crate::target::{SqlStatement, TargetExecuteError, TargetRowChange, TargetRowChangeKind};
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
    failed_reads: Arc<Mutex<BTreeMap<(usize, usize), ReadFailure>>>,
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
    failed_reads: Arc<Mutex<BTreeMap<(usize, usize), ReadFailure>>>,
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
        let failure = self
            .failed_reads
            .lock()
            .expect("lock failed reads")
            .get(&key)
            .cloned();
        let (states, changed) = &*self.reads;
        let states = states.lock().expect("lock read state");
        drop(
            changed
                .wait_while(states, |states| !states.get(&key).copied().unwrap_or(false))
                .expect("wait for query result"),
        );
        self.awaiting_result = false;
        self.read_index += 1;
        match failure {
            Some(failure) => Err(failure.into_error()),
            None => Ok(()),
        }
    }
}

#[derive(Clone)]
struct ReadFailure {
    mysql_code: Option<u16>,
    message: String,
}

impl ReadFailure {
    fn generic(message: &str) -> Self {
        Self {
            mysql_code: None,
            message: message.to_string(),
        }
    }

    fn mysql(code: u16, message: &str) -> Self {
        Self {
            mysql_code: Some(code),
            message: message.to_string(),
        }
    }

    fn into_error(self) -> TargetExecuteError {
        match self.mysql_code {
            Some(code) => TargetExecuteError::from_mysql(code, self.message),
            None => TargetExecuteError::new(self.message),
        }
    }
}

#[test]
fn later_transaction_is_submitted_before_earlier_completion() {
    let (factory, submitted, reads) = fake_factory();
    let mut pool = ParallelTransactionPool::new(2, factory).expect("create pool");

    pool.submit(transaction(100))
        .expect("submit first transaction");
    let (first_connection, first_sql) = receive_submission(&submitted);
    assert_eq!(first_sql, "BEGIN");

    pool.submit(transaction(200))
        .expect("submit second transaction");
    let (second_connection, second_sql) = receive_submission(&submitted);
    assert_ne!(second_connection, first_connection);
    assert_eq!(second_sql, "BEGIN");

    release_reads(&reads, first_connection, 0..=2);
    release_reads(&reads, second_connection, 0..=2);
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
    assert_eq!(
        receive_submission(&submitted),
        (
            second_connection,
            "INSERT INTO events VALUES (200)".to_string()
        )
    );
    release_read(&reads, second_connection, 1);
    pool.wait_for_event()
        .expect("record second transaction prepared");
    assert!(submitted.recv_timeout(Duration::from_millis(50)).is_err());

    release_read(&reads, first_connection, 0);
    assert_eq!(
        receive_submission(&submitted),
        (
            first_connection,
            "INSERT INTO events VALUES (100)".to_string()
        )
    );
    release_read(&reads, first_connection, 1);
    pool.wait_for_event()
        .expect("record first transaction prepared");
    assert_eq!(
        receive_submission(&submitted),
        (first_connection, "CHECKPOINT 100; COMMIT".to_string())
    );

    release_read(&reads, first_connection, 2);
    pool.wait_for_event()
        .expect("record first transaction committed");
    assert_eq!(
        receive_submission(&submitted),
        (second_connection, "CHECKPOINT 200; COMMIT".to_string())
    );

    release_read(&reads, second_connection, 2);
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
    assert_eq!(
        receive_submission(&submitted),
        (connection, "INSERT INTO events VALUES (100)".to_string())
    );
    release_read(&reads, connection, 1);
    pool.wait_for_event().expect("record transaction prepared");
    assert_eq!(pool.idle_worker_count(), 0);
    assert_eq!(
        receive_submission(&submitted),
        (connection, "CHECKPOINT 100; COMMIT".to_string())
    );

    release_read(&reads, connection, 2);
    pool.wait_for_all().expect("finish transaction");
    assert_eq!(pool.idle_worker_count(), 1);
}

#[test]
fn send_failure_is_returned_before_transaction_is_accepted() {
    let (factory, _submitted, _reads) = fake_factory_with_failed_send("BEGIN");
    let mut pool = ParallelTransactionPool::new(1, factory).expect("create pool");

    let error = pool
        .submit(transaction(100))
        .expect_err("reject failed send");

    assert!(error.to_string().contains("send failed before acceptance"));
}

#[test]
fn body_failure_rolls_back_after_submission_without_committing() {
    let (factory, submitted, reads) = fake_factory_with_failed_read((0, 1));
    release_reads(&reads, 0, 0..=2);
    let mut pool = ParallelTransactionPool::new(1, factory).expect("create pool");

    pool.submit(transaction(100))
        .expect("body send should be accepted before result failure");
    let error = pool
        .wait_for_all()
        .expect_err("body result failure must stop the pool");

    assert!(error.to_string().contains("failed before commit"));
    assert_eq!(receive_submission(&submitted), (0, "BEGIN".to_string()));
    assert_eq!(
        receive_submission(&submitted),
        (0, "INSERT INTO events VALUES (100)".to_string())
    );
    assert_eq!(receive_submission(&submitted), (0, "ROLLBACK".to_string()));
    assert!(submitted.recv_timeout(Duration::from_millis(50)).is_err());
}

#[test]
fn earlier_body_failure_blocks_later_prepared_commit_and_checkpoint() {
    let (factory, submitted, reads) = fake_factory();
    let failed_reads = Arc::clone(&factory.failed_reads);
    let mut pool = ParallelTransactionPool::new(2, factory).expect("create pool");

    pool.submit(transaction(100))
        .expect("submit first transaction");
    let (first_connection, first_begin) = receive_submission(&submitted);
    assert_eq!(first_begin, "BEGIN");
    failed_reads.lock().expect("lock failed reads").insert(
        (first_connection, 1),
        ReadFailure::generic("injected earlier body failure"),
    );

    pool.submit(transaction(200))
        .expect("submit second transaction");
    let (second_connection, second_begin) = receive_submission(&submitted);
    assert_eq!(second_begin, "BEGIN");
    assert_ne!(second_connection, first_connection);

    release_read(&reads, second_connection, 0);
    assert_eq!(
        receive_submission(&submitted),
        (
            second_connection,
            "INSERT INTO events VALUES (200)".to_string()
        )
    );
    release_read(&reads, second_connection, 1);
    pool.wait_for_event()
        .expect("record later transaction prepared");
    assert!(submitted.recv_timeout(Duration::from_millis(50)).is_err());

    release_read(&reads, first_connection, 0);
    assert_eq!(
        receive_submission(&submitted),
        (
            first_connection,
            "INSERT INTO events VALUES (100)".to_string()
        )
    );
    release_read(&reads, first_connection, 1);
    assert_eq!(
        receive_submission(&submitted),
        (first_connection, "ROLLBACK".to_string())
    );
    release_read(&reads, first_connection, 2);

    let error = pool
        .wait_for_all()
        .expect_err("earlier failure must stop ordered commits");

    assert!(error.to_string().contains("failed before commit"));
    assert!(submitted.recv_timeout(Duration::from_millis(50)).is_err());
}

#[test]
fn insert_duplicate_result_continues_to_later_statement_and_commits() {
    let statements = [
        transaction_statement(
            TargetRowChangeKind::Insert,
            "INSERT INTO events VALUES (100)",
        ),
        transaction_statement(
            TargetRowChangeKind::Update,
            "UPDATE events SET processed = 1 WHERE id = 100",
        ),
    ];
    let duplicate_read = statement_read_index(&statements, TargetRowChangeKind::Insert);
    let (factory, submitted, reads) =
        fake_factory_with_mysql_read_failure((0, duplicate_read), 1062);
    release_reads(&reads, 0, 0..=3);
    let mut pool = ParallelTransactionPool::new(1, factory).expect("create pool");

    pool.submit(transaction_with_statements(100, &statements))
        .expect("accept transaction submission");
    pool.wait_for_all()
        .expect("INSERT duplicate must not stop later statements or commit");

    assert_eq!(receive_submission(&submitted), (0, "BEGIN".to_string()));
    assert_eq!(
        receive_submission(&submitted),
        (0, "INSERT INTO events VALUES (100)".to_string())
    );
    assert_eq!(
        receive_submission(&submitted),
        (
            0,
            "UPDATE events SET processed = 1 WHERE id = 100".to_string()
        )
    );
    assert_eq!(
        receive_submission(&submitted),
        (0, "CHECKPOINT 100; COMMIT".to_string())
    );
    assert!(submitted.recv_timeout(Duration::from_millis(50)).is_err());
    let committed = pool.take_committed().expect("take committed transaction");
    assert_eq!(committed[0].checkpoint.source_position, 100);
}

#[test]
fn non_insert_duplicate_stops_before_later_statement_and_commit() {
    let statements = [
        transaction_statement(
            TargetRowChangeKind::Update,
            "UPDATE events SET processed = 1 WHERE id = 100",
        ),
        transaction_statement(
            TargetRowChangeKind::Insert,
            "INSERT INTO events VALUES (200)",
        ),
    ];
    let duplicate_read = statement_read_index(&statements, TargetRowChangeKind::Update);
    let (factory, submitted, reads) =
        fake_factory_with_mysql_read_failure((0, duplicate_read), 1062);
    release_reads(&reads, 0, 0..=3);
    let mut pool = ParallelTransactionPool::new(1, factory).expect("create pool");

    pool.submit(transaction_with_statements(100, &statements))
        .expect("accept transaction submission");
    let error = pool
        .wait_for_all()
        .expect_err("UPDATE duplicate must stop the transaction");

    assert!(error.to_string().contains("1062"));
    assert_eq!(receive_submission(&submitted), (0, "BEGIN".to_string()));
    assert_eq!(
        receive_submission(&submitted),
        (
            0,
            "UPDATE events SET processed = 1 WHERE id = 100".to_string()
        )
    );
    assert_eq!(receive_submission(&submitted), (0, "ROLLBACK".to_string()));
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
    fake_factory_with_failures(failed_sends, BTreeMap::new())
}

fn fake_factory_with_failed_read(
    failed_read: (usize, usize),
) -> (FakeConnectionFactory, Receiver<(usize, String)>, ReadGate) {
    fake_factory_with_failures(
        BTreeSet::new(),
        BTreeMap::from([(failed_read, ReadFailure::generic("injected read failure"))]),
    )
}

fn fake_factory_with_mysql_read_failure(
    failed_read: (usize, usize),
    mysql_code: u16,
) -> (FakeConnectionFactory, Receiver<(usize, String)>, ReadGate) {
    fake_factory_with_failures(
        BTreeSet::new(),
        BTreeMap::from([(
            failed_read,
            ReadFailure::mysql(mysql_code, &format!("injected MySQL error {mysql_code}")),
        )]),
    )
}

fn fake_factory_with_failures(
    failed_sends: BTreeSet<String>,
    failed_reads: BTreeMap<(usize, usize), ReadFailure>,
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

fn transaction_statement(kind: TargetRowChangeKind, sql: &str) -> ParallelTargetStatement {
    ParallelTargetStatement::RowChange(TargetRowChange {
        statement: SqlStatement {
            sql: sql.to_string(),
            params: Vec::new(),
        },
        kind,
        schema: "globalcomix".to_string(),
        table: "events".to_string(),
        values: BTreeMap::new(),
    })
}

fn statement_read_index(
    statements: &[ParallelTargetStatement],
    kind: TargetRowChangeKind,
) -> usize {
    statements
        .iter()
        .position(|statement| {
            matches!(statement, ParallelTargetStatement::RowChange(change) if change.kind == kind)
        })
        .expect("fixture contains statement kind")
        + 1
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

fn release_reads(
    reads: &ReadGate,
    connection: usize,
    read_indexes: impl IntoIterator<Item = usize>,
) {
    for read_index in read_indexes {
        release_read(reads, connection, read_index);
    }
}

fn transaction(position: u64) -> ParallelTargetTransaction {
    let sql = format!("INSERT INTO events VALUES ({position})");
    transaction_with_statements(
        position,
        &[transaction_statement(TargetRowChangeKind::Insert, &sql)],
    )
}

fn transaction_with_statements(
    position: u64,
    statements: &[ParallelTargetStatement],
) -> ParallelTargetTransaction {
    ParallelTargetTransaction {
        statements: statements.to_vec(),
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
