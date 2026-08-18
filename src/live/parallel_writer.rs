use super::parallel_target::{
    ParallelTargetStatement, ParallelTargetStatementKind, ParallelTargetTransaction,
    ParallelTransactionPool, SubmittedQueryConnectionFactory,
};
use crate::checkpoint::Checkpoint;
use crate::target::{
    SqlStatement, TargetExecuteError, TargetRowChange, render_submitted_sql_statement,
};

pub(crate) struct ParallelTargetWriter<F>
where
    F: SubmittedQueryConnectionFactory,
{
    pool: ParallelTransactionPool<F>,
    active: Option<BufferedTargetTransaction>,
    logical_checkpoint: Checkpoint,
}

#[derive(Default)]
struct BufferedTargetTransaction {
    statements: Vec<ParallelTargetStatement>,
    checkpoint_sql: Option<String>,
    checkpoint: Option<Checkpoint>,
}

impl<F> ParallelTargetWriter<F>
where
    F: SubmittedQueryConnectionFactory,
{
    pub(crate) fn new(
        worker_count: usize,
        factory: F,
        initial_checkpoint: Checkpoint,
    ) -> Result<Self, TargetExecuteError> {
        Ok(Self {
            pool: ParallelTransactionPool::new(worker_count, factory)?,
            active: None,
            logical_checkpoint: initial_checkpoint,
        })
    }

    pub(crate) fn begin(&mut self) -> Result<(), TargetExecuteError> {
        self.pool.poll()?;
        if self.active.is_some() {
            return Err(TargetExecuteError::new(
                "parallel target transaction is already active",
            ));
        }
        self.active = Some(BufferedTargetTransaction::default());
        Ok(())
    }

    pub(crate) fn execute(&mut self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        self.buffer_statement(statement, ParallelTargetStatementKind::Other)
    }

    pub(crate) fn execute_row_change(
        &mut self,
        change: &TargetRowChange,
    ) -> Result<(), TargetExecuteError> {
        self.buffer_statement(&change.statement, change.kind.into())
    }

    pub(crate) fn logical_checkpoint(&self) -> Checkpoint {
        self.logical_checkpoint.clone()
    }

    pub(crate) fn save_checkpoint(
        &mut self,
        checkpoint_table: &str,
        checkpoint_name: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), TargetExecuteError> {
        let sql = crate::stream_checkpoint::build_checkpoint_upsert_sql_for_checkpoint(
            checkpoint_table,
            checkpoint_name,
            checkpoint,
        )
        .map_err(TargetExecuteError::new)?;
        let active = self.active_mut()?;
        active.checkpoint_sql = Some(sql);
        active.checkpoint = Some(checkpoint.clone());
        Ok(())
    }

    pub(crate) fn commit(&mut self) -> Result<(), TargetExecuteError> {
        let transaction = self
            .active
            .take()
            .ok_or_else(|| TargetExecuteError::new("parallel target transaction is not active"))?;
        let checkpoint_sql = transaction.checkpoint_sql.ok_or_else(|| {
            TargetExecuteError::new("parallel target transaction has no checkpoint write")
        })?;
        let checkpoint = transaction.checkpoint.ok_or_else(|| {
            TargetExecuteError::new("parallel target transaction has no checkpoint")
        })?;
        let submitted = ParallelTargetTransaction {
            statements: transaction.statements,
            commit_sql: transaction_commit_sql(&checkpoint_sql),
            checkpoint: checkpoint.clone(),
        };
        self.pool.submit(submitted)?;
        Ok(())
    }

    pub(crate) fn rollback(&mut self) -> Result<(), TargetExecuteError> {
        if self.active.take().is_some() {
            return Ok(());
        }
        Err(TargetExecuteError::new(
            "parallel target transaction is not active",
        ))
    }

    pub(crate) fn wait_for_all(&mut self) -> Result<(), TargetExecuteError> {
        if self.active.is_some() {
            return Err(TargetExecuteError::new(
                "cannot wait for parallel target transactions while one is active",
            ));
        }
        self.pool.wait_for_all()
    }

    pub(crate) fn take_committed_checkpoints(
        &mut self,
    ) -> Result<Vec<Checkpoint>, TargetExecuteError> {
        let checkpoints = self
            .pool
            .take_committed()?
            .into_iter()
            .map(|transaction| transaction.checkpoint)
            .collect::<Vec<_>>();
        if let Some(checkpoint) = checkpoints.last() {
            self.logical_checkpoint.clone_from(checkpoint);
        }
        Ok(checkpoints)
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.is_some()
    }

    fn buffer_statement(
        &mut self,
        statement: &SqlStatement,
        kind: ParallelTargetStatementKind,
    ) -> Result<(), TargetExecuteError> {
        let sql = render_submitted_sql_statement(statement)?;
        self.active_mut()?
            .statements
            .push(ParallelTargetStatement { sql, kind });
        Ok(())
    }

    fn active_mut(&mut self) -> Result<&mut BufferedTargetTransaction, TargetExecuteError> {
        self.active
            .as_mut()
            .ok_or_else(|| TargetExecuteError::new("parallel target transaction is not active"))
    }
}

fn transaction_commit_sql(checkpoint_sql: &str) -> String {
    format!("{checkpoint_sql};\nCOMMIT")
}

#[cfg(test)]
mod tests {
    use super::ParallelTargetWriter;
    use crate::checkpoint::{Checkpoint, LastEvent};
    use crate::live::parallel_target::{SubmittedQueryConnection, SubmittedQueryConnectionFactory};
    use crate::target::{SqlStatement, TargetExecuteError, TargetRowChange, TargetRowChangeKind};
    use mysql::Value;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::time::Duration;

    #[derive(Clone)]
    struct RecordingFactory {
        sent: Sender<String>,
    }

    impl SubmittedQueryConnectionFactory for RecordingFactory {
        type Connection = RecordingConnection;

        fn open(&self) -> Result<Self::Connection, TargetExecuteError> {
            Ok(RecordingConnection {
                sent: self.sent.clone(),
                awaiting_result: false,
            })
        }
    }

    struct RecordingConnection {
        sent: Sender<String>,
        awaiting_result: bool,
    }

    impl SubmittedQueryConnection for RecordingConnection {
        fn send_query(&mut self, sql: &str) -> Result<(), TargetExecuteError> {
            if self.awaiting_result {
                return Err(TargetExecuteError::new(
                    "query submitted before prior result drain",
                ));
            }
            self.sent
                .send(sql.to_string())
                .expect("record submitted query");
            self.awaiting_result = true;
            Ok(())
        }

        fn read_query_result(&mut self) -> Result<(), TargetExecuteError> {
            self.awaiting_result = false;
            Ok(())
        }
    }

    #[test]
    fn submits_individual_body_statements_and_one_ordered_commit_batch() {
        let (sent, submitted) = mpsc::channel();
        let initial = checkpoint(4);
        let mut writer = ParallelTargetWriter::new(1, RecordingFactory { sent }, initial)
            .expect("create writer");

        writer.begin().expect("begin transaction");
        writer
            .execute_row_change(&insert_change(1))
            .expect("buffer first insert");
        writer
            .execute_row_change(&insert_change(2))
            .expect("buffer second insert");
        writer
            .save_checkpoint(
                "cdc.stream_checkpoint",
                "stream-binlog:test",
                &checkpoint(200),
            )
            .expect("buffer checkpoint");
        writer.commit().expect("submit transaction");
        writer.wait_for_all().expect("finish transaction");

        assert_eq!(receive(&submitted), "BEGIN");
        assert_eq!(
            receive(&submitted),
            "INSERT INTO `events` (`id`) VALUES (1)"
        );
        assert_eq!(
            receive(&submitted),
            "INSERT INTO `events` (`id`) VALUES (2)"
        );
        let commit = receive(&submitted);
        assert!(commit.starts_with(
            "INSERT INTO `cdc`.`stream_checkpoint` (checkpoint_name, checkpoint_json)"
        ));
        assert!(commit.ends_with(";\nCOMMIT"));
        assert!(submitted.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn logical_checkpoint_advances_only_after_ordered_commit_completion() {
        let (sent, _submitted) = mpsc::channel();
        let initial = checkpoint(4);
        let mut writer = ParallelTargetWriter::new(1, RecordingFactory { sent }, initial)
            .expect("create writer");

        writer.begin().expect("begin transaction");
        writer.execute(&insert(1)).expect("buffer insert");
        writer
            .save_checkpoint(
                "cdc.stream_checkpoint",
                "stream-binlog:test",
                &checkpoint(200),
            )
            .expect("buffer checkpoint");
        assert_eq!(writer.logical_checkpoint().source_position, 4);
        writer.commit().expect("submit transaction");
        assert_eq!(writer.logical_checkpoint().source_position, 4);
        writer.wait_for_all().expect("finish transaction");
        assert_eq!(writer.logical_checkpoint().source_position, 4);
        assert_eq!(
            writer
                .take_committed_checkpoints()
                .expect("drain commits")
                .len(),
            1
        );
        assert_eq!(writer.logical_checkpoint().source_position, 200);
    }

    fn receive(submitted: &Receiver<String>) -> String {
        submitted
            .recv_timeout(Duration::from_secs(1))
            .expect("receive submitted query")
    }

    fn insert(id: u64) -> SqlStatement {
        SqlStatement {
            sql: "INSERT INTO `events` (`id`) VALUES (?)".to_string(),
            params: vec![Value::UInt(id)],
        }
    }

    fn insert_change(id: u64) -> TargetRowChange {
        TargetRowChange {
            statement: insert(id),
            kind: TargetRowChangeKind::Insert,
            schema: "globalcomix".to_string(),
            table: "events".to_string(),
            values: [("id".to_string(), Value::UInt(id))].into_iter().collect(),
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
                description: "parallel writer test checkpoint".to_string(),
            },
        }
    }
}
