use crate::probe::BinlogCoordinate;
use crate::target::{SqlStatement, TargetExecuteError, TargetExecutor};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatementEvent {
    pub coordinate: BinlogCoordinate,
    pub resume_position: u64,
    pub default_database: Option<String>,
    pub sql: String,
}

impl StatementEvent {
    pub fn resume_coordinate(&self) -> BinlogCoordinate {
        BinlogCoordinate {
            file: self.coordinate.file.clone(),
            position: self.resume_position,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantinedStatement {
    pub coordinate: BinlogCoordinate,
    pub default_database: Option<String>,
    pub sql: String,
    pub reason: QuarantineReason,
}

impl fmt::Display for QuarantinedStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{} database={} reason={:?} sql={}",
            self.coordinate.file,
            self.coordinate.position,
            self.default_database.as_deref().unwrap_or("<none>"),
            self.reason,
            self.sql
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QuarantineReason {
    EmptyStatement,
    MultiStatement,
    UnsupportedStatementType(String),
    UnsafePattern(String),
    MariaDbOnlySyntax(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatementOutcome {
    Replayed,
    AlreadyApplied,
    Quarantined(QuarantineReason),
}

pub trait StatementQuarantine {
    fn record(&self, statement: &QuarantinedStatement) -> Result<(), QuarantineError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantineError {
    pub message: String,
}

impl fmt::Display for QuarantineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for QuarantineError {}

#[derive(Debug)]
pub enum StatementApplyError {
    Target {
        coordinate: BinlogCoordinate,
        sql: String,
        source: TargetExecuteError,
    },
    Quarantine {
        coordinate: BinlogCoordinate,
        sql: String,
        source: QuarantineError,
    },
}

impl fmt::Display for StatementApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Target {
                coordinate,
                sql,
                source,
            } => write!(
                formatter,
                "failed to replay statement at {}:{}: {source}: {sql}",
                coordinate.file, coordinate.position
            ),
            Self::Quarantine {
                coordinate,
                sql,
                source,
            } => write!(
                formatter,
                "failed to quarantine statement at {}:{}: {source}: {sql}",
                coordinate.file, coordinate.position
            ),
        }
    }
}

impl std::error::Error for StatementApplyError {}

pub struct StatementApplier<E, Q> {
    executor: E,
    quarantine: Q,
}

impl<E, Q> StatementApplier<E, Q>
where
    E: TargetExecutor,
    Q: StatementQuarantine,
{
    pub fn new(executor: E, quarantine: Q) -> Self {
        Self {
            executor,
            quarantine,
        }
    }

    pub fn quarantine_recorder(&self) -> &Q {
        &self.quarantine
    }

    pub fn apply(&self, event: &StatementEvent) -> Result<StatementOutcome, StatementApplyError> {
        let normalized_sql = normalize_statement(&event.sql);
        let decision = classify_statement(&normalized_sql);

        match decision {
            StatementDecision::Replay => self.replay(event, normalized_sql),
            StatementDecision::Quarantine(reason) => self.quarantine(event, normalized_sql, reason),
        }
    }

    fn replay(
        &self,
        event: &StatementEvent,
        sql: String,
    ) -> Result<StatementOutcome, StatementApplyError> {
        let statement = SqlStatement {
            sql: sql.clone(),
            params: Vec::new(),
        };
        match self.executor.execute(&statement) {
            Ok(()) => Ok(StatementOutcome::Replayed),
            Err(source) if is_ddl_already_applied_error(&sql, &source.to_string()) => {
                Ok(StatementOutcome::AlreadyApplied)
            }
            Err(source) => Err(StatementApplyError::Target {
                coordinate: event.coordinate.clone(),
                sql,
                source,
            }),
        }
    }

    fn quarantine(
        &self,
        event: &StatementEvent,
        sql: String,
        reason: QuarantineReason,
    ) -> Result<StatementOutcome, StatementApplyError> {
        let quarantined = QuarantinedStatement {
            coordinate: event.coordinate.clone(),
            default_database: event.default_database.clone(),
            sql: sql.clone(),
            reason: reason.clone(),
        };

        self.quarantine
            .record(&quarantined)
            .map_err(|source| StatementApplyError::Quarantine {
                coordinate: event.coordinate.clone(),
                sql,
                source,
            })?;

        Ok(StatementOutcome::Quarantined(reason))
    }
}

enum StatementDecision {
    Replay,
    Quarantine(QuarantineReason),
}

fn classify_statement(sql: &str) -> StatementDecision {
    if sql.is_empty() {
        return StatementDecision::Quarantine(QuarantineReason::EmptyStatement);
    }

    if contains_multi_statement(sql) {
        return StatementDecision::Quarantine(QuarantineReason::MultiStatement);
    }

    if let Some(pattern) = find_mariadb_only_pattern(sql) {
        return StatementDecision::Quarantine(QuarantineReason::MariaDbOnlySyntax(pattern));
    }

    if let Some(pattern) = find_unsafe_pattern(sql) {
        return StatementDecision::Quarantine(QuarantineReason::UnsafePattern(pattern));
    }

    if let Some(pattern) = find_ddl_if_exists_pattern(sql) {
        return StatementDecision::Quarantine(QuarantineReason::MariaDbOnlySyntax(pattern));
    }

    if is_known_compatible_dml(sql) || is_known_compatible_ddl(sql) {
        return StatementDecision::Replay;
    }

    let keyword = first_keyword(sql).unwrap_or("unknown").to_string();
    StatementDecision::Quarantine(QuarantineReason::UnsupportedStatementType(keyword))
}

fn normalize_statement(sql: &str) -> String {
    sql.trim().trim_end_matches(';').trim().to_string()
}

fn contains_multi_statement(sql: &str) -> bool {
    let mut quote = None;
    let mut escaped = false;
    let mut chars = sql.char_indices().peekable();

    while let Some((index, character)) = chars.next() {
        if let Some(quote_character) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == quote_character {
                quote = None;
            }
            continue;
        }

        match character {
            '\'' | '"' => quote = Some(character),
            '-' if chars.peek().is_some_and(|(_, next)| *next == '-') => {
                chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next)| *next == '*') => {
                chars.next();
                skip_block_comment(&mut chars);
            }
            ';' if !sql[index + 1..].trim().is_empty() => return true,
            _ => {}
        }
    }

    false
}

fn skip_line_comment<I>(chars: &mut std::iter::Peekable<I>)
where
    I: Iterator<Item = (usize, char)>,
{
    for (_, character) in chars.by_ref() {
        if character == '\n' || character == '\r' {
            break;
        }
    }
}

fn skip_block_comment<I>(chars: &mut std::iter::Peekable<I>)
where
    I: Iterator<Item = (usize, char)>,
{
    let mut previous = None;
    for (_, character) in chars.by_ref() {
        if previous == Some('*') && character == '/' {
            break;
        }
        previous = Some(character);
    }
}

fn find_mariadb_only_pattern(sql: &str) -> Option<String> {
    let upper = uppercase_outside_string_literals(sql);
    let patterns = [
        "RETURNING",
        "SEQUENCE",
        "SYSTEM VERSIONING",
        "VERSIONING",
        "DELETE HISTORY",
        "INSERT DELAYED",
    ];

    find_pattern(&upper, &patterns)
}

fn find_unsafe_pattern(sql: &str) -> Option<String> {
    let upper = uppercase_outside_string_literals(sql);
    let patterns = [
        "LOAD_FILE(",
        "INTO OUTFILE",
        "INTO DUMPFILE",
        "LOAD DATA",
        "DEFINER=",
        "SQL SECURITY DEFINER",
    ];

    find_pattern(&upper, &patterns)
}

fn find_pattern(upper_sql: &str, patterns: &[&str]) -> Option<String> {
    patterns
        .iter()
        .find(|pattern| contains_bounded_pattern(upper_sql, pattern))
        .map(|pattern| pattern.trim().to_string())
}

fn uppercase_outside_string_literals(sql: &str) -> String {
    let mut upper = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut quote = QuoteState::default();
    let mut escaped = false;

    while let Some(character) = chars.next() {
        if quote.is_inside() {
            upper.push(' ');
            quote.update(character, &mut escaped, &mut chars, &mut upper);
            continue;
        }

        match character {
            '\'' | '"' => {
                quote.enter(character);
                upper.push(' ');
            }
            _ => upper.extend(character.to_uppercase()),
        }
    }

    upper
}

#[derive(Default)]
struct QuoteState {
    quote_character: Option<char>,
}

impl QuoteState {
    fn is_inside(&self) -> bool {
        self.quote_character.is_some()
    }

    fn enter(&mut self, quote_character: char) {
        self.quote_character = Some(quote_character);
    }

    fn update<I>(
        &mut self,
        character: char,
        escaped: &mut bool,
        chars: &mut std::iter::Peekable<I>,
        upper: &mut String,
    ) where
        I: Iterator<Item = char>,
    {
        if *escaped {
            *escaped = false;
            return;
        }

        if character == '\\' {
            *escaped = true;
            return;
        }

        if self.is_escaped_sql_quote(character, chars) {
            upper.push(' ');
            chars.next();
            return;
        }

        if self.quote_character == Some(character) {
            self.quote_character = None;
        }
    }

    fn is_escaped_sql_quote<I>(&self, character: char, chars: &mut std::iter::Peekable<I>) -> bool
    where
        I: Iterator<Item = char>,
    {
        self.quote_character == Some('\'') && character == '\'' && chars.peek() == Some(&'\'')
    }
}

fn contains_bounded_pattern(sql: &str, pattern: &str) -> bool {
    sql.match_indices(pattern)
        .any(|(index, _)| pattern_has_valid_bounds(sql, pattern, index))
}

fn pattern_has_valid_bounds(sql: &str, pattern: &str, index: usize) -> bool {
    let pattern_start = pattern.as_bytes()[0];
    let pattern_end = pattern.as_bytes()[pattern.len() - 1];
    let before = sql[..index].bytes().next_back();
    let after = sql[index + pattern.len()..].bytes().next();

    has_valid_boundary_before(pattern_start, before) && has_valid_boundary_after(pattern_end, after)
}

fn has_valid_boundary_before(pattern_start: u8, before: Option<u8>) -> bool {
    !is_sql_word_byte(pattern_start) || before.is_none_or(|byte| !is_sql_word_byte(byte))
}

fn has_valid_boundary_after(pattern_end: u8, after: Option<u8>) -> bool {
    !is_sql_word_byte(pattern_end) || after.is_none_or(|byte| !is_sql_word_byte(byte))
}

fn is_sql_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn is_known_compatible_dml(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    upper.starts_with("INSERT INTO ")
        || upper.starts_with("UPDATE ")
        || upper.starts_with("DELETE FROM ")
        || upper.starts_with("REPLACE INTO ")
}

fn is_known_compatible_ddl(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    upper.starts_with("ALTER TABLE ")
        || upper.starts_with("CREATE TABLE ")
        || upper.starts_with("DROP TABLE ")
        || upper.starts_with("TRUNCATE ")
        || upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE UNIQUE INDEX ")
        || upper.starts_with("DROP INDEX ")
        || upper.starts_with("RENAME TABLE ")
}

pub fn is_schema_changing_statement(sql: &str) -> bool {
    is_known_compatible_ddl(&normalize_statement(sql))
}

// MySQL 8 does not accept IF [NOT] EXISTS on ALTER TABLE clauses or index DDL;
// CREATE TABLE IF NOT EXISTS and DROP TABLE IF EXISTS stay replayable.
fn find_ddl_if_exists_pattern(sql: &str) -> Option<String> {
    let upper = uppercase_outside_string_literals(sql);
    let guarded = upper.starts_with("ALTER TABLE ")
        || upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE UNIQUE INDEX ")
        || upper.starts_with("DROP INDEX ");
    if !guarded {
        return None;
    }

    find_pattern(&upper, &["IF NOT EXISTS", "IF EXISTS"])
}

// Replaying DDL is not idempotent; after a crash between apply and checkpoint,
// the re-applied statement reports one of these already-applied server errors.
const DDL_ALREADY_APPLIED_ERROR_CODES: [&str; 5] = [
    "ERROR 1050", // table already exists
    "ERROR 1051", // unknown table (already dropped)
    "ERROR 1060", // duplicate column name (already added)
    "ERROR 1061", // duplicate key name (already added)
    "ERROR 1091", // cannot drop column or key (already dropped)
];

fn is_ddl_already_applied_error(sql: &str, error: &str) -> bool {
    is_known_compatible_ddl(sql)
        && DDL_ALREADY_APPLIED_ERROR_CODES
            .iter()
            .any(|code| error.contains(code))
}

fn first_keyword(sql: &str) -> Option<&str> {
    sql.split_whitespace().next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn replays_known_compatible_dml() {
        let executor = RecordingExecutor::default();
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let outcome = applier
            .apply(&statement("UPDATE accounts SET name = 'Ada' WHERE id = 7;"))
            .expect("apply statement");

        assert_eq!(outcome, StatementOutcome::Replayed);
        assert_eq!(
            applier.executor.statements.borrow().as_slice(),
            &[SqlStatement {
                sql: "UPDATE accounts SET name = 'Ada' WHERE id = 7".to_string(),
                params: Vec::new(),
            }]
        );
        assert!(applier.quarantine.statements.borrow().is_empty());
    }

    #[test]
    fn formats_quarantined_statement_with_context() {
        let quarantined = QuarantinedStatement {
            coordinate: BinlogCoordinate {
                file: "mysqld-bin.000001".to_string(),
                position: 123,
            },
            default_database: Some("globalcomix".to_string()),
            sql: "CREATE TABLE accounts (id BIGINT)".to_string(),
            reason: QuarantineReason::UnsupportedStatementType("CREATE".to_string()),
        };

        let formatted = quarantined.to_string();

        assert!(formatted.contains("mysqld-bin.000001:123"));
        assert!(formatted.contains("database=globalcomix"));
        assert!(formatted.contains("UnsupportedStatementType(\"CREATE\")"));
        assert!(formatted.contains("CREATE TABLE accounts"));
    }

    #[test]
    fn replays_known_compatible_ddl() {
        let executor = RecordingExecutor::default();
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let event = statement("ALTER TABLE accounts ADD COLUMN handle VARCHAR(64)");
        let outcome = applier.apply(&event).expect("apply statement");

        assert_eq!(outcome, StatementOutcome::Replayed);
        assert_eq!(
            applier.executor.statements.borrow().as_slice(),
            &[SqlStatement {
                sql: "ALTER TABLE accounts ADD COLUMN handle VARCHAR(64)".to_string(),
                params: Vec::new(),
            }]
        );
        assert!(applier.quarantine.statements.borrow().is_empty());
    }

    #[test]
    fn quarantines_unsupported_statement_with_binlog_coordinate() {
        let executor = RecordingExecutor::default();
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let event = statement("GRANT SELECT ON accounts TO 'reader'@'%'");
        let outcome = applier.apply(&event).expect("apply statement");

        assert_eq!(
            outcome,
            StatementOutcome::Quarantined(QuarantineReason::UnsupportedStatementType(
                "GRANT".to_string()
            ))
        );
        assert!(applier.executor.statements.borrow().is_empty());
        assert_eq!(
            applier.quarantine.statements.borrow().as_slice(),
            &[QuarantinedStatement {
                coordinate: event.coordinate,
                default_database: Some("app".to_string()),
                sql: "GRANT SELECT ON accounts TO 'reader'@'%'".to_string(),
                reason: QuarantineReason::UnsupportedStatementType("GRANT".to_string()),
            }]
        );
    }

    #[test]
    fn quarantines_alter_with_mariadb_only_if_exists_clause() {
        let executor = RecordingExecutor::default();
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let event = statement("ALTER TABLE accounts DROP COLUMN IF EXISTS handle");
        let outcome = applier.apply(&event).expect("apply statement");

        assert_eq!(
            outcome,
            StatementOutcome::Quarantined(QuarantineReason::MariaDbOnlySyntax(
                "IF EXISTS".to_string()
            ))
        );
        assert!(applier.executor.statements.borrow().is_empty());
    }

    #[test]
    fn replays_create_table_if_not_exists() {
        let executor = RecordingExecutor::default();
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let event = statement("CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY)");
        let outcome = applier.apply(&event).expect("apply statement");

        assert_eq!(outcome, StatementOutcome::Replayed);
        assert!(applier.quarantine.statements.borrow().is_empty());
    }

    #[test]
    fn replays_create_table_if_not_exists_with_semicolons_in_line_comments() {
        let executor = RecordingExecutor::default();
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);
        let ddl = [
            "CREATE TABLE IF NOT EXISTS `kg_credits` (",
            "  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,",
            "  `creator_name_hash` CHAR(40) NOT NULL, -- sha1 of normalized name; ALWAYS set",
            "  `source` VARCHAR(16) NOT NULL, -- ingest | cover; gap-fill source",
            "  PRIMARY KEY (`id`)",
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        ]
        .join("\n");

        let outcome = applier.apply(&statement(&ddl)).expect("apply statement");

        assert_eq!(outcome, StatementOutcome::Replayed);
        assert_eq!(applier.executor.statements.borrow()[0].sql, ddl);
        assert!(applier.quarantine.statements.borrow().is_empty());
    }

    #[test]
    fn tolerates_already_applied_ddl_replay_error() {
        let executor = RecordingExecutor {
            statements: RefCell::new(Vec::new()),
            error: Some(TargetExecuteError::new(
                "target mysql query failed: ERROR 1091 (42000): Can't DROP COLUMN `handle`; check that it exists",
            )),
        };
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let event = statement("ALTER TABLE accounts DROP COLUMN handle");
        let outcome = applier.apply(&event).expect("apply statement");

        assert_eq!(outcome, StatementOutcome::AlreadyApplied);
    }

    #[test]
    fn does_not_tolerate_already_applied_errors_for_dml() {
        let executor = RecordingExecutor {
            statements: RefCell::new(Vec::new()),
            error: Some(TargetExecuteError::new(
                "target mysql query failed: ERROR 1091 (42000): unexpected",
            )),
        };
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let event = statement("UPDATE accounts SET name = 'Ada' WHERE id = 7");
        let error = applier
            .apply(&event)
            .expect_err("dml error should propagate");

        assert!(error.to_string().contains("ERROR 1091"));
    }

    #[test]
    fn quarantines_mariadb_only_returning_clause() {
        let executor = RecordingExecutor::default();
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let outcome = applier
            .apply(&statement("DELETE FROM accounts WHERE id = 7 RETURNING id"))
            .expect("apply statement");

        assert_eq!(
            outcome,
            StatementOutcome::Quarantined(QuarantineReason::MariaDbOnlySyntax(
                "RETURNING".to_string()
            ))
        );
        assert!(applier.executor.statements.borrow().is_empty());
    }

    #[test]
    fn replays_sequence_text_inside_string_literal() {
        let executor = RecordingExecutor::default();
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let outcome = applier
            .apply(&statement(
                r#"INSERT INTO guests (original_uri) VALUES ("https://globalcomix.com/forums/16355/chastity-blood-consequences")"#,
            ))
            .expect("apply statement");

        assert_eq!(outcome, StatementOutcome::Replayed);
        assert_eq!(applier.executor.statements.borrow().len(), 1);
        assert!(applier.quarantine.statements.borrow().is_empty());
    }

    #[test]
    fn quarantines_multi_statement_text() {
        let executor = RecordingExecutor::default();
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let outcome = applier
            .apply(&statement(
                "UPDATE accounts SET name = 'Ada'; DROP TABLE accounts",
            ))
            .expect("apply statement");

        assert_eq!(
            outcome,
            StatementOutcome::Quarantined(QuarantineReason::MultiStatement)
        );
        assert!(applier.executor.statements.borrow().is_empty());
    }

    #[test]
    fn replays_semicolon_inside_string_literal() {
        let executor = RecordingExecutor::default();
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let outcome = applier
            .apply(&statement(
                r#"INSERT INTO guests (http_user_agent) VALUES ("Mozilla/5.0 (Macintosh; Intel Mac OS X)")"#,
            ))
            .expect("apply statement");

        assert_eq!(outcome, StatementOutcome::Replayed);
        assert_eq!(applier.executor.statements.borrow().len(), 1);
        assert!(applier.quarantine.statements.borrow().is_empty());
    }

    #[test]
    fn target_errors_include_coordinate_and_sql() {
        let executor = RecordingExecutor {
            error: Some(TargetExecuteError::new("duplicate key")),
            ..RecordingExecutor::default()
        };
        let quarantine = RecordingQuarantine::default();
        let applier = StatementApplier::new(executor, quarantine);

        let error = applier
            .apply(&statement("INSERT INTO accounts (id) VALUES (1)"))
            .expect_err("target should fail")
            .to_string();

        assert!(error.contains("mysql-bin.000001:42"));
        assert!(error.contains("duplicate key"));
        assert!(error.contains("INSERT INTO accounts"));
    }

    fn statement(sql: &str) -> StatementEvent {
        StatementEvent {
            coordinate: BinlogCoordinate {
                file: "mysql-bin.000001".to_string(),
                position: 42,
            },
            resume_position: 84,
            default_database: Some("app".to_string()),
            sql: sql.to_string(),
        }
    }

    #[derive(Default)]
    struct RecordingExecutor {
        statements: RefCell<Vec<SqlStatement>>,
        error: Option<TargetExecuteError>,
    }

    impl TargetExecutor for RecordingExecutor {
        fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
            self.statements.borrow_mut().push(statement.clone());

            match &self.error {
                Some(error) => Err(error.clone()),
                None => Ok(()),
            }
        }
    }

    #[derive(Default)]
    struct RecordingQuarantine {
        statements: RefCell<Vec<QuarantinedStatement>>,
    }

    impl StatementQuarantine for RecordingQuarantine {
        fn record(&self, statement: &QuarantinedStatement) -> Result<(), QuarantineError> {
            self.statements.borrow_mut().push(statement.clone());
            Ok(())
        }
    }
}
