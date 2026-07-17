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

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedCreateColumnAst {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedCreateTableAst {
    pub name: String,
    pub columns: Vec<ParsedCreateColumnAst>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<ParsedIndexAst>,
    pub engine: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedAddColumnAst {
    pub name: String,
    pub column_type: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub comment: String,
    pub after: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedDropColumnAst {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParsedAlterClause {
    AddColumn(ParsedAddColumnAst),
    AddKey(ParsedIndexAst),
    DropColumn(ParsedDropColumnAst),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedAlterTableAst {
    pub table: String,
    pub clauses: Vec<ParsedAlterClause>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlOperation {
    pub family: DdlFamily,
    pub object_kind: DdlObjectKind,
    pub primary_object: String,
    pub secondary_object: Option<String>,
    pub index_ast: Option<ParsedIndexAst>,
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
