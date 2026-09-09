//! Coordinated RESTRICT transitions inside a caller-owned locked transaction.
//! No transaction, checkpoint, source mutation, or MySQL implementation lives here.
use std::collections::{BTreeMap, BTreeSet};

pub type Key = Vec<String>;
pub type Row = BTreeMap<String, Option<String>>;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RowKey {
    pub table: String,
    pub pk: Key,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Relation {
    pub child_table: String,
    pub child_columns: Vec<String>,
    pub parent_columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParentReference {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Key,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Error<E> {
    Backend(E),
    Cycle(String),
    WorkLimit,
    InvalidPage,
    InvalidRelation,
    MissingColumn(String),
    MissingTarget(RowKey),
    MissingParent {
        child: RowKey,
        parent: ParentReference,
    },
}

/// All reads must share caller-controlled stable source evidence and target locks.
/// Keys use backend PK order, not Rust string order. Pages must advance strictly,
/// contain at most `limit` keys and obey `byte_limit` (one oversized key allowed).
/// Exact row reads preserve one oversized row, matching the page-budget contract.
/// Strict writes must report missing rows, duplicates and constraint failures.
pub trait Backend {
    type Error;
    fn incoming(&mut self, table: &str) -> Result<Vec<Relation>, Self::Error>;
    fn dependent_page(
        &mut self,
        relation: &Relation,
        old: &Row,
        after: Option<&Key>,
        limit: usize,
        byte_limit: usize,
    ) -> Result<Vec<Key>, Self::Error>;
    fn target_row(&mut self, key: &RowKey) -> Result<Option<Row>, Self::Error>;
    fn source_row(&mut self, key: &RowKey) -> Result<Option<Row>, Self::Error>;
    /// Check ALL outgoing FKs against target, including parents outside traversal.
    /// NULL-containing FK tuples are satisfied, per SQL semantics.
    fn missing_parents(
        &mut self,
        table: &str,
        desired: &Row,
    ) -> Result<Vec<ParentReference>, Self::Error>;
    fn delete(&mut self, key: &RowKey) -> Result<(), Self::Error>;
    fn update(&mut self, key: &RowKey, desired: &Row) -> Result<(), Self::Error>;
    fn insert(&mut self, key: &RowKey, desired: &Row) -> Result<(), Self::Error>;
}

#[derive(Clone, Copy)]
pub struct Limits {
    pub page_rows: usize,
    pub max_keys: usize,
}

pub const PAGE_BYTES: usize = 64 * 1024 * 1024;

pub fn transition<B: Backend>(
    backend: &mut B,
    root: &RowKey,
    current: &Row,
    old: &Row,
    limits: Limits,
) -> Result<(), Error<B::Error>> {
    if limits.page_rows == 0 || limits.max_keys == 0 {
        return Err(Error::WorkLimit);
    }
    let mut schema = BTreeMap::new();
    let mut order = Vec::new();
    load_schema(
        backend,
        &root.table,
        &mut schema,
        &mut BTreeSet::new(),
        &mut order,
    )?;
    let keys = discover(backend, root, current, old, limits, &schema)?;
    for table in &order {
        for key in keys.iter().filter(|key| key.table == *table) {
            backend.delete(key).map_err(Error::Backend)?;
        }
    }
    check_parents(backend, root, current)?;
    backend.update(root, current).map_err(Error::Backend)?;
    for table in order.iter().rev() {
        for key in keys.iter().filter(|key| key.table == *table) {
            if let Some(desired) = backend.source_row(key).map_err(Error::Backend)? {
                check_parents(backend, key, &desired)?;
                backend.insert(key, &desired).map_err(Error::Backend)?;
            }
        }
    }
    Ok(())
}

fn load_schema<B: Backend>(
    backend: &mut B,
    table: &str,
    schema: &mut BTreeMap<String, Vec<Relation>>,
    visiting: &mut BTreeSet<String>,
    order: &mut Vec<String>,
) -> Result<(), Error<B::Error>> {
    if visiting.contains(table) {
        return Err(Error::Cycle(table.into()));
    }
    if schema.contains_key(table) {
        return Ok(());
    }
    visiting.insert(table.into());
    let relations = backend.incoming(table).map_err(Error::Backend)?;
    for relation in &relations {
        if relation.child_columns.is_empty()
            || relation.child_columns.len() != relation.parent_columns.len()
        {
            return Err(Error::InvalidRelation);
        }
        load_schema(backend, &relation.child_table, schema, visiting, order)?;
    }
    visiting.remove(table);
    schema.insert(table.into(), relations);
    order.push(table.into());
    Ok(())
}

fn changed<E>(relation: &Relation, current: &Row, old: &Row) -> Result<bool, Error<E>> {
    let mut differs = false;
    for column in &relation.parent_columns {
        let before = old
            .get(column)
            .ok_or_else(|| Error::MissingColumn(column.clone()))?;
        let after = current
            .get(column)
            .ok_or_else(|| Error::MissingColumn(column.clone()))?;
        differs |= before != after;
    }
    Ok(differs)
}

fn discover<B: Backend>(
    backend: &mut B,
    root: &RowKey,
    current: &Row,
    old: &Row,
    limits: Limits,
    schema: &BTreeMap<String, Vec<Relation>>,
) -> Result<BTreeSet<RowKey>, Error<B::Error>> {
    let mut keys = BTreeSet::new();
    let mut pending = Vec::new();
    for relation in &schema[&root.table] {
        if changed(relation, current, old)? {
            read_keys(backend, relation, old, limits, &mut keys, &mut pending)?;
        }
    }
    while let Some(key) = pending.pop() {
        let row = backend
            .target_row(&key)
            .map_err(Error::Backend)?
            .ok_or_else(|| Error::MissingTarget(key.clone()))?;
        for relation in &schema[&key.table] {
            read_keys(backend, relation, &row, limits, &mut keys, &mut pending)?;
        }
    }
    Ok(keys)
}

fn read_keys<B: Backend>(
    backend: &mut B,
    relation: &Relation,
    old: &Row,
    limits: Limits,
    keys: &mut BTreeSet<RowKey>,
    pending: &mut Vec<RowKey>,
) -> Result<(), Error<B::Error>> {
    let mut after = None;
    let mut seen = BTreeSet::new();
    loop {
        let page = backend
            .dependent_page(relation, old, after.as_ref(), limits.page_rows, PAGE_BYTES)
            .map_err(Error::Backend)?;
        if page.is_empty() {
            return Ok(());
        }
        let bytes: usize = page.iter().flatten().map(String::len).sum();
        if page.len() > limits.page_rows || (page.len() > 1 && bytes > PAGE_BYTES) {
            return Err(Error::InvalidPage);
        }
        for pk in &page {
            if pk.is_empty() || !seen.insert(pk.clone()) {
                return Err(Error::InvalidPage);
            }
            let key = RowKey {
                table: relation.child_table.clone(),
                pk: pk.clone(),
            };
            if keys.insert(key.clone()) {
                if keys.len() > limits.max_keys {
                    return Err(Error::WorkLimit);
                }
                pending.push(key);
            }
        }
        after = page.last().cloned();
    }
}

fn check_parents<B: Backend>(
    backend: &mut B,
    child: &RowKey,
    desired: &Row,
) -> Result<(), Error<B::Error>> {
    if let Some(parent) = backend
        .missing_parents(&child.table, desired)
        .map_err(Error::Backend)?
        .into_iter()
        .next()
    {
        return Err(Error::MissingParent {
            child: child.clone(),
            parent,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Memory {
        source: BTreeMap<RowKey, Row>,
        target: BTreeMap<RowKey, Row>,
        relations: BTreeMap<String, Vec<Relation>>,
        fail: Option<&'static str>,
        pages: usize,
    }
    fn key(table: &str, id: &str) -> RowKey {
        RowKey {
            table: table.into(),
            pk: vec![id.into()],
        }
    }
    fn row(id: &str, value: &str) -> Row {
        [
            ("id".into(), Some(id.into())),
            ("value".into(), Some(value.into())),
            ("tenant".into(), Some("42".into())),
        ]
        .into()
    }
    fn relation(child: &str) -> Relation {
        Relation {
            child_table: child.into(),
            child_columns: vec!["value".into()],
            parent_columns: vec!["value".into()],
        }
    }
    impl Backend for Memory {
        type Error = &'static str;
        fn incoming(&mut self, table: &str) -> Result<Vec<Relation>, Self::Error> {
            Ok(self.relations.get(table).cloned().unwrap_or_default())
        }
        fn dependent_page(
            &mut self,
            relation: &Relation,
            old: &Row,
            after: Option<&Key>,
            limit: usize,
            _: usize,
        ) -> Result<Vec<Key>, Self::Error> {
            self.pages += 1;
            Ok(self
                .target
                .iter()
                .filter(|(key, row)| {
                    key.table == relation.child_table
                        && after.is_none_or(|after| key.pk > *after)
                        && relation
                            .child_columns
                            .iter()
                            .zip(&relation.parent_columns)
                            .all(|(child, parent)| {
                                row[child].is_some() && row[child] == old[parent]
                            })
                })
                .take(limit)
                .map(|(key, _)| key.pk.clone())
                .collect())
        }
        fn target_row(&mut self, key: &RowKey) -> Result<Option<Row>, Self::Error> {
            Ok(self.target.get(key).cloned())
        }
        fn source_row(&mut self, key: &RowKey) -> Result<Option<Row>, Self::Error> {
            Ok(self.source.get(key).cloned())
        }
        fn missing_parents(
            &mut self,
            table: &str,
            desired: &Row,
        ) -> Result<Vec<ParentReference>, Self::Error> {
            let mut missing = vec![];
            for (parent, relations) in &self.relations {
                for relation in relations
                    .iter()
                    .filter(|relation| relation.child_table == table)
                {
                    let values: Option<Key> = relation
                        .child_columns
                        .iter()
                        .map(|column| desired[column].clone())
                        .collect();
                    let Some(values) = values else { continue };
                    let exists = self.target.iter().any(|(key, row)| {
                        key.table == *parent
                            && relation
                                .parent_columns
                                .iter()
                                .zip(&values)
                                .all(|(column, value)| row[column].as_ref() == Some(value))
                    });
                    if !exists {
                        missing.push(ParentReference {
                            table: parent.clone(),
                            columns: relation.parent_columns.clone(),
                            values,
                        });
                    }
                }
            }
            Ok(missing)
        }
        fn delete(&mut self, key: &RowKey) -> Result<(), Self::Error> {
            if self.fail == Some("delete") {
                return Err("delete");
            }
            let old = self.target[key].clone();
            for relation in self.incoming(&key.table)? {
                if !self
                    .dependent_page(&relation, &old, None, 1, PAGE_BYTES)?
                    .is_empty()
                {
                    return Err("restrict");
                }
            }
            self.target.remove(key).ok_or("absent")?;
            Ok(())
        }
        fn update(&mut self, key: &RowKey, desired: &Row) -> Result<(), Self::Error> {
            if self.fail == Some("update") {
                return Err("update");
            }
            self.delete(key)?;
            self.insert(key, desired)
        }
        fn insert(&mut self, key: &RowKey, desired: &Row) -> Result<(), Self::Error> {
            if self.fail == Some("insert") {
                return Err("insert");
            }
            if self.target.contains_key(key) {
                return Err("duplicate");
            }
            if !self.missing_parents(&key.table, desired)?.is_empty() {
                return Err("parent");
            }
            self.target.insert(key.clone(), desired.clone());
            Ok(())
        }
    }
    fn fixture() -> Memory {
        let target = [
            (key("parent", "1"), row("1", "old")),
            (key("child", "2"), row("2", "old")),
            (key("child", "3"), row("3", "old")),
            (key("grandchild", "4"), row("4", "old")),
        ]
        .into();
        let source = [
            (key("parent", "1"), row("1", "new")),
            (key("child", "2"), row("2", "new")),
            (key("grandchild", "4"), row("4", "new")),
        ]
        .into();
        Memory {
            target,
            source,
            relations: [
                ("parent".into(), vec![relation("child")]),
                ("child".into(), vec![relation("grandchild")]),
            ]
            .into(),
            ..Memory::default()
        }
    }
    fn apply(db: &mut Memory) -> Result<(), Error<&'static str>> {
        transition(
            db,
            &key("parent", "1"),
            &row("1", "new"),
            &row("1", "old"),
            Limits {
                page_rows: 1,
                max_keys: 20,
            },
        )
    }
    #[test]
    fn restores_three_levels_and_leaves_source_absent_deleted() {
        let mut db = fixture();
        apply(&mut db).unwrap();
        assert_eq!(db.target, db.source);
        assert!(db.pages > 2);
    }
    #[test]
    fn reparent_to_existing_external_parent() {
        let mut db = fixture();
        db.target.insert(key("parent", "9"), row("9", "elsewhere"));
        db.source.insert(key("parent", "9"), row("9", "elsewhere"));
        db.source.insert(key("child", "2"), row("2", "elsewhere"));
        db.source
            .insert(key("grandchild", "4"), row("4", "elsewhere"));
        apply(&mut db).unwrap();
        assert_eq!(db.target, db.source);
    }
    #[test]
    fn missing_parent_is_concrete_error() {
        let mut db = fixture();
        db.source.insert(key("child", "2"), row("2", "absent"));
        assert_eq!(
            apply(&mut db),
            Err(Error::MissingParent {
                child: key("child", "2"),
                parent: ParentReference {
                    table: "parent".into(),
                    columns: vec!["value".into()],
                    values: vec!["absent".into()]
                }
            })
        );
    }
    #[test]
    fn rejects_schema_cycle_before_mutation() {
        let mut db = fixture();
        db.relations
            .insert("grandchild".into(), vec![relation("parent")]);
        let before = db.target.clone();
        assert!(matches!(apply(&mut db), Err(Error::Cycle(_))));
        assert_eq!(db.target, before);
    }
    #[test]
    fn propagates_each_write_failure_for_caller_rollback() {
        for operation in ["delete", "update", "insert"] {
            let mut db = fixture();
            db.fail = Some(operation);
            assert_eq!(apply(&mut db), Err(Error::Backend(operation)));
        }
    }
    #[test]
    fn duplicate_composite_relations_detach_each_key_once() {
        let mut db = fixture();
        let composite = Relation {
            child_table: "child".into(),
            child_columns: vec!["value".into(), "tenant".into()],
            parent_columns: vec!["value".into(), "tenant".into()],
        };
        db.relations
            .get_mut("parent")
            .unwrap()
            .extend([composite.clone(), composite]);
        apply(&mut db).unwrap();
        assert_eq!(db.target, db.source);
    }
    #[test]
    fn work_limit_fails_before_mutation() {
        let mut db = fixture();
        let before = db.target.clone();
        let result = transition(
            &mut db,
            &key("parent", "1"),
            &row("1", "new"),
            &row("1", "old"),
            Limits {
                page_rows: 1,
                max_keys: 1,
            },
        );
        assert_eq!(result, Err(Error::WorkLimit));
        assert_eq!(db.target, before);
    }
}
