use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParentRepairRow {
    pub(crate) table: String,
    pub(crate) values: BTreeMap<String, Option<String>>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ForeignKeyEdge {
    pub(crate) child_table: String,
    pub(crate) parent_table: String,
    pub(crate) columns: Vec<ForeignKeyColumn>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ForeignKeyColumn {
    pub(crate) child: String,
    pub(crate) parent: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ParentIdentity {
    pub(crate) table: String,
    pub(crate) values: Vec<(String, String)>,
}

pub(crate) trait ParentRepairStore {
    fn read_source_parent(
        &mut self,
        identity: &ParentIdentity,
    ) -> Result<Option<ParentRepairRow>, String>;

    fn read_target_parent(
        &mut self,
        identity: &ParentIdentity,
    ) -> Result<Option<ParentRepairRow>, String>;

    fn repair_parent(&mut self, row: &ParentRepairRow) -> Result<(), String>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ParentRepairError {
    InvalidChildRow {
        table: String,
        column: String,
    },
    SourceRead {
        identity: ParentIdentity,
        message: String,
    },
    TargetRead {
        identity: ParentIdentity,
        message: String,
    },
    MissingSourceParent {
        identity: ParentIdentity,
    },
    Cycle {
        path: Vec<ParentIdentity>,
    },
    Repair {
        identity: ParentIdentity,
        message: String,
    },
}

impl fmt::Display for ParentRepairError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

pub(crate) fn repair_fk_parents_and_retry(
    _child_table: &str,
    child_rows: &[ParentRepairRow],
    ordered_edges: &[ForeignKeyEdge],
    store: &mut impl ParentRepairStore,
) -> Result<(), ParentRepairError> {
    let mut repaired = BTreeSet::new();
    let mut path = Vec::new();

    for child_row in child_rows {
        repair_row_parents(child_row, ordered_edges, store, &mut repaired, &mut path)?;
    }

    Ok(())
}

fn repair_row_parents(
    child_row: &ParentRepairRow,
    ordered_edges: &[ForeignKeyEdge],
    store: &mut impl ParentRepairStore,
    repaired: &mut BTreeSet<ParentIdentity>,
    path: &mut Vec<ParentIdentity>,
) -> Result<(), ParentRepairError> {
    for edge in ordered_edges
        .iter()
        .filter(|edge| edge.child_table == child_row.table)
    {
        let Some(identity) = parent_identity(child_row, edge)? else {
            continue;
        };
        repair_parent(identity, ordered_edges, store, repaired, path)?;
    }
    Ok(())
}

fn parent_identity(
    child_row: &ParentRepairRow,
    edge: &ForeignKeyEdge,
) -> Result<Option<ParentIdentity>, ParentRepairError> {
    let mut values = Vec::with_capacity(edge.columns.len());
    for column in &edge.columns {
        let value = child_row.values.get(&column.child).ok_or_else(|| {
            ParentRepairError::InvalidChildRow {
                table: child_row.table.clone(),
                column: column.child.clone(),
            }
        })?;
        let Some(value) = value else {
            return Ok(None);
        };
        values.push((column.parent.clone(), value.clone()));
    }
    Ok(Some(ParentIdentity {
        table: edge.parent_table.clone(),
        values,
    }))
}

fn repair_parent(
    identity: ParentIdentity,
    ordered_edges: &[ForeignKeyEdge],
    store: &mut impl ParentRepairStore,
    repaired: &mut BTreeSet<ParentIdentity>,
    path: &mut Vec<ParentIdentity>,
) -> Result<(), ParentRepairError> {
    if repaired.contains(&identity) {
        return Ok(());
    }
    if let Some(cycle_start) = path.iter().position(|entry| entry == &identity) {
        let mut cycle = path[cycle_start..].to_vec();
        cycle.push(identity);
        return Err(ParentRepairError::Cycle { path: cycle });
    }

    let source = store
        .read_source_parent(&identity)
        .map_err(|message| ParentRepairError::SourceRead {
            identity: identity.clone(),
            message,
        })?
        .ok_or_else(|| ParentRepairError::MissingSourceParent {
            identity: identity.clone(),
        })?;
    let target =
        store
            .read_target_parent(&identity)
            .map_err(|message| ParentRepairError::TargetRead {
                identity: identity.clone(),
                message,
            })?;

    if target.as_ref() == Some(&source) {
        repaired.insert(identity);
        return Ok(());
    }

    path.push(identity.clone());
    let parent_result = repair_row_parents(&source, ordered_edges, store, repaired, path);
    path.pop();
    parent_result?;

    if let Err(message) = store.repair_parent(&source) {
        let concurrent_parent = store.read_target_parent(&identity).map_err(|read_error| {
            ParentRepairError::TargetRead {
                identity: identity.clone(),
                message: read_error,
            }
        })?;
        if concurrent_parent.as_ref() == Some(&source) {
            repaired.insert(identity);
            return Ok(());
        }
        return Err(ParentRepairError::Repair { identity, message });
    }
    let repaired_parent =
        store
            .read_target_parent(&identity)
            .map_err(|message| ParentRepairError::TargetRead {
                identity: identity.clone(),
                message,
            })?;
    if repaired_parent.as_ref() != Some(&source) {
        return Err(ParentRepairError::Repair {
            identity,
            message: "target parent does not match source after repair".to_string(),
        });
    }
    repaired.insert(identity);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingStore {
        source: BTreeMap<ParentIdentity, ParentRepairRow>,
        target: BTreeMap<ParentIdentity, ParentRepairRow>,
        source_errors: BTreeMap<ParentIdentity, String>,
        drop_parent_repair: bool,
        error_after_parent_repair: Option<String>,
        repaired: Vec<ParentIdentity>,
    }

    impl ParentRepairStore for RecordingStore {
        fn read_source_parent(
            &mut self,
            identity: &ParentIdentity,
        ) -> Result<Option<ParentRepairRow>, String> {
            if let Some(error) = self.source_errors.get(identity) {
                return Err(error.clone());
            }
            Ok(self.source.get(identity).cloned())
        }

        fn read_target_parent(
            &mut self,
            identity: &ParentIdentity,
        ) -> Result<Option<ParentRepairRow>, String> {
            Ok(self.target.get(identity).cloned())
        }

        fn repair_parent(&mut self, row: &ParentRepairRow) -> Result<(), String> {
            let id = row.values["id"].as_deref().expect("parent id");
            let identity = identity(&row.table, "id", id);
            self.repaired.push(identity.clone());
            if !self.drop_parent_repair {
                self.target.insert(identity, row.clone());
            }
            match &self.error_after_parent_repair {
                Some(error) => Err(error.clone()),
                None => Ok(()),
            }
        }
    }

    fn row(table: &str, values: &[(&str, Option<&str>)]) -> ParentRepairRow {
        ParentRepairRow {
            table: table.to_string(),
            values: values
                .iter()
                .map(|(column, value)| {
                    (
                        column.to_string(),
                        value.map(std::string::ToString::to_string),
                    )
                })
                .collect(),
        }
    }

    fn identity(table: &str, column: &str, value: &str) -> ParentIdentity {
        ParentIdentity {
            table: table.to_string(),
            values: vec![(column.to_string(), value.to_string())],
        }
    }

    fn edge(child: &str, child_column: &str, parent: &str, parent_column: &str) -> ForeignKeyEdge {
        ForeignKeyEdge {
            child_table: child.to_string(),
            parent_table: parent.to_string(),
            columns: vec![ForeignKeyColumn {
                child: child_column.to_string(),
                parent: parent_column.to_string(),
            }],
        }
    }

    #[test]
    fn repairs_transitive_parents_before_retrying_child_batch() {
        let edges = vec![
            edge("guests", "utm_id", "utms", "id"),
            edge("utms", "campaign_id", "campaigns", "id"),
        ];
        let child = row("guests", &[("guest_id", Some("7")), ("utm_id", Some("41"))]);
        let utm = row("utms", &[("id", Some("41")), ("campaign_id", Some("9"))]);
        let campaign = row("campaigns", &[("id", Some("9"))]);
        let mut store = RecordingStore::default();
        store.source.insert(identity("utms", "id", "41"), utm);
        store
            .source
            .insert(identity("campaigns", "id", "9"), campaign);

        repair_fk_parents_and_retry("guests", &[child], &edges, &mut store).unwrap();

        assert_eq!(
            store.repaired,
            [
                identity("campaigns", "id", "9"),
                identity("utms", "id", "41")
            ]
        );
    }

    #[test]
    fn returns_after_parent_repair_without_retrying_child_batch() {
        let edges = vec![edge("guests", "utm_id", "utms", "id")];
        let child = row("guests", &[("guest_id", Some("7")), ("utm_id", Some("41"))]);
        let utm = row("utms", &[("id", Some("41")), ("utm_hash", Some("source"))]);
        let mut store = RecordingStore::default();
        store.source.insert(identity("utms", "id", "41"), utm);

        repair_fk_parents_and_retry("guests", &[child], &edges, &mut store).unwrap();

        assert_eq!(store.repaired, [identity("utms", "id", "41")]);
    }

    #[test]
    fn accepts_equal_target_parent_without_repair() {
        let edges = vec![edge("guests", "utm_id", "utms", "id")];
        let child = row("guests", &[("guest_id", Some("7")), ("utm_id", Some("41"))]);
        let utm = row("utms", &[("id", Some("41")), ("utm_hash", Some("same"))]);
        let mut store = RecordingStore::default();
        store
            .source
            .insert(identity("utms", "id", "41"), utm.clone());
        store.target.insert(identity("utms", "id", "41"), utm);

        repair_fk_parents_and_retry("guests", &[child], &edges, &mut store).unwrap();

        assert!(store.repaired.is_empty());
    }

    #[test]
    fn skips_nullable_foreign_key_without_reading_parent() {
        let edges = vec![edge("guests", "utm_id", "utms", "id")];
        let child = row("guests", &[("guest_id", Some("7")), ("utm_id", None)]);
        let mut store = RecordingStore::default();

        repair_fk_parents_and_retry("guests", &[child], &edges, &mut store).unwrap();

        assert!(store.repaired.is_empty());
    }

    #[test]
    fn accepts_concurrent_equal_parent_after_duplicate_write_error() {
        let edges = vec![edge("guests", "utm_id", "utms", "id")];
        let child = row("guests", &[("guest_id", Some("7")), ("utm_id", Some("41"))]);
        let utm = row("utms", &[("id", Some("41")), ("utm_hash", Some("source"))]);
        let mut store = RecordingStore {
            error_after_parent_repair: Some("duplicate key".to_string()),
            ..RecordingStore::default()
        };
        store.source.insert(identity("utms", "id", "41"), utm);

        repair_fk_parents_and_retry("guests", &[child], &edges, &mut store)
            .expect("concurrent equal parent");
    }

    #[test]
    fn rejects_parent_repair_that_does_not_converge() {
        let edges = vec![edge("guests", "utm_id", "utms", "id")];
        let child = row("guests", &[("guest_id", Some("7")), ("utm_id", Some("41"))]);
        let utm = row("utms", &[("id", Some("41")), ("utm_hash", Some("source"))]);
        let mut store = RecordingStore {
            drop_parent_repair: true,
            ..RecordingStore::default()
        };
        store.source.insert(identity("utms", "id", "41"), utm);

        let error = repair_fk_parents_and_retry("guests", &[child], &edges, &mut store)
            .expect_err("unverified parent repair");

        assert_eq!(
            error,
            ParentRepairError::Repair {
                identity: identity("utms", "id", "41"),
                message: "target parent does not match source after repair".to_string()
            }
        );
    }

    #[test]
    fn returns_structured_error_for_absent_source_parent() {
        let edges = vec![edge("guests", "utm_id", "utms", "id")];
        let child = row("guests", &[("guest_id", Some("7")), ("utm_id", Some("41"))]);
        let mut store = RecordingStore::default();

        let error = repair_fk_parents_and_retry("guests", &[child], &edges, &mut store)
            .expect_err("missing source parent");

        assert_eq!(
            error,
            ParentRepairError::MissingSourceParent {
                identity: identity("utms", "id", "41")
            }
        );
    }

    #[test]
    fn returns_structured_error_for_ambiguous_source_parent() {
        let edges = vec![edge("guests", "utm_id", "utms", "id")];
        let child = row("guests", &[("guest_id", Some("7")), ("utm_id", Some("41"))]);
        let parent = identity("utms", "id", "41");
        let mut store = RecordingStore::default();
        store.source_errors.insert(
            parent.clone(),
            "exact source parent is ambiguous: 2 rows".to_string(),
        );

        let error = repair_fk_parents_and_retry("guests", &[child], &edges, &mut store)
            .expect_err("ambiguous source parent");

        assert_eq!(
            error,
            ParentRepairError::SourceRead {
                identity: parent,
                message: "exact source parent is ambiguous: 2 rows".to_string()
            }
        );
    }

    #[test]
    fn detects_cycle_by_table_and_identity() {
        let edges = vec![edge("a", "b_id", "b", "id"), edge("b", "a_id", "a", "id")];
        let child = row("a", &[("id", Some("1")), ("b_id", Some("2"))]);
        let parent_b = row("b", &[("id", Some("2")), ("a_id", Some("1"))]);
        let parent_a = row("a", &[("id", Some("1")), ("b_id", Some("2"))]);
        let mut store = RecordingStore::default();
        store.source.insert(identity("b", "id", "2"), parent_b);
        store.source.insert(identity("a", "id", "1"), parent_a);

        let error = repair_fk_parents_and_retry("a", &[child], &edges, &mut store).unwrap_err();

        assert_eq!(
            error,
            ParentRepairError::Cycle {
                path: vec![
                    identity("b", "id", "2"),
                    identity("a", "id", "1"),
                    identity("b", "id", "2")
                ]
            }
        );
    }
}
