use super::super::ddl_replay_journal::DdlFamily;
use crate::inventory::SchemaInventory;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlObjectKind {
    Table,
    Index,
    View,
    Procedure,
    Function,
    Event,
    Trigger,
}

impl DdlObjectKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Index => "index",
            Self::View => "view",
            Self::Procedure => "procedure",
            Self::Function => "function",
            Self::Event => "event",
            Self::Trigger => "trigger",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedIndexKeyPart {
    pub column: String,
    pub prefix_length: Option<u32>,
    pub order: String,
    pub collation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedIndexAst {
    pub create: bool,
    pub name: String,
    pub table: String,
    pub unique: bool,
    pub index_type: String,
    pub visible: bool,
    pub comment: Option<String>,
    pub key_parts: Vec<ParsedIndexKeyPart>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedCreateColumnAst {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
    /// MySQL-rendered default: `NULL`, an integer, a quoted string, `CURRENT_TIMESTAMP[(6)]`,
    /// or the expression default `(_utf8mb4'...')` MySQL requires for TEXT columns.
    pub default_sql: Option<String>,
    pub auto_increment: bool,
    pub on_update_current_timestamp: bool,
    pub character_set: Option<String>,
    pub collation: Option<String>,
}

/// One predicate of a bounded CHECK constraint; predicates are joined with `OR`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckPredicate {
    IsNull { column: String },
    JsonValid { column: String },
    OctetLengthAtMost { column: String, limit: u64 },
    InStrings { column: String, values: Vec<String> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedCheckConstraintAst {
    pub name: String,
    pub disjuncts: Vec<CheckPredicate>,
}

/// A same-schema CREATE foreign key with ON DELETE CASCADE and implicit RESTRICT on update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedCreateForeignKeyAst {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedCreateTableAst {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ParsedCreateColumnAst>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<ParsedIndexAst>,
    pub check_constraints: Vec<ParsedCheckConstraintAst>,
    pub foreign_keys: Vec<ParsedCreateForeignKeyAst>,
    pub engine: String,
    pub character_set: Option<String>,
    pub collation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedAddColumnAst {
    pub name: String,
    pub if_not_exists: bool,
    pub column_type: String,
    pub data_type: String,
    pub nullable: bool,
    /// Source literal default as MariaDB reports it (`0`, `{}`), or `None` for `NULL`.
    pub default_value: Option<String>,
    pub comment: String,
    pub after: Option<String>,
    pub character_set: Option<String>,
    pub collation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedDropColumnAst {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedDropIndexAst {
    pub name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParsedAlterAlgorithm {
    Inplace,
    Instant,
}

impl ParsedAlterAlgorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inplace => "inplace",
            Self::Instant => "instant",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParsedAlterLock {
    None,
}

impl ParsedAlterLock {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParsedAlterClause {
    AddColumn(ParsedAddColumnAst),
    ModifyVarchar {
        name: String,
        column_type: String,
        nullable: bool,
    },
    AddKey {
        index: ParsedIndexAst,
        if_not_exists: bool,
    },
    AddCheck(ParsedCheckConstraintAst),
    DropColumn(ParsedDropColumnAst),
    DropIndex(ParsedDropIndexAst),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedAlterTableAst {
    pub table: String,
    pub clauses: Vec<ParsedAlterClause>,
    pub algorithm: Option<ParsedAlterAlgorithm>,
    pub lock: Option<ParsedAlterLock>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlOperation {
    pub family: DdlFamily,
    pub object_kind: DdlObjectKind,
    pub primary_object: String,
    pub secondary_object: Option<String>,
    pub index_ast: Option<ParsedIndexAst>,
    pub create_table_ast: Option<ParsedCreateTableAst>,
    pub alter_table_ast: Option<ParsedAlterTableAst>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableRuntimeState {
    pub row_count: u64,
    pub auto_increment: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SemanticSchemaSnapshot {
    pub inventory: SchemaInventory,
    pub table_runtime: BTreeMap<String, TableRuntimeState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlSemanticEvidence {
    pub transformation_version: String,
    pub generated_sql: Option<String>,
    pub canonical_ast: String,
    pub pre_state: String,
    pub expected_post_state: String,
}
