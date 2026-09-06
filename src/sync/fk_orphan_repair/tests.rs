use super::*;

#[derive(Default)]
struct FakeBackend {
    source_children: BTreeMap<Vec<String>, DatabaseRow>,
    source_parents: BTreeMap<Vec<String>, DatabaseRow>,
    target_children: BTreeMap<Vec<String>, DatabaseRow>,
    target_parents: BTreeMap<Vec<String>, DatabaseRow>,
    events: Vec<String>,
    fail_begin: bool,
}

impl FkOrphanRepairBackend for FakeBackend {
    fn validate_case(&mut self, spec: &RepairCaseSpec) -> Result<RepairMetadata, String> {
        Ok(metadata(spec))
    }

    fn orphan_keys(
        &mut self,
        spec: &RepairCaseSpec,
        _metadata: &RepairMetadata,
        limit: usize,
    ) -> Result<Vec<Vec<String>>, String> {
        Ok(self
            .target_children
            .iter()
            .filter(|(_, child)| !parent_matches(spec, child, &self.target_parents))
            .map(|(primary_key, _)| primary_key.clone())
            .take(limit)
            .collect())
    }

    fn begin_batch(
        &mut self,
        _spec: &RepairCaseSpec,
        _metadata: &RepairMetadata,
    ) -> Result<(), String> {
        self.events.push("begin".to_string());
        if self.fail_begin {
            return Err("begin failed".to_string());
        }
        Ok(())
    }

    fn is_target_orphan(
        &mut self,
        spec: &RepairCaseSpec,
        _metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<bool, String> {
        Ok(self
            .target_children
            .get(primary_key)
            .is_some_and(|child| !parent_matches(spec, child, &self.target_parents)))
    }

    fn read_source_child(
        &mut self,
        _metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<Option<DatabaseRow>, String> {
        Ok(self.source_children.get(primary_key).cloned())
    }

    fn read_source_parent(
        &mut self,
        spec: &RepairCaseSpec,
        _metadata: &RepairMetadata,
        child: &DatabaseRow,
    ) -> Result<Option<DatabaseRow>, String> {
        Ok(self
            .source_parents
            .get(&parent_primary_key(spec, child)?)
            .cloned())
    }

    fn read_target_child(
        &mut self,
        _metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<Option<DatabaseRow>, String> {
        Ok(self.target_children.get(primary_key).cloned())
    }

    fn read_target_parent(
        &mut self,
        spec: &RepairCaseSpec,
        _metadata: &RepairMetadata,
        child: &DatabaseRow,
    ) -> Result<Option<DatabaseRow>, String> {
        Ok(self
            .target_parents
            .get(&parent_primary_key(spec, child)?)
            .cloned())
    }

    fn update_target_child(
        &mut self,
        _metadata: &RepairMetadata,
        child: &DatabaseRow,
    ) -> Result<(), String> {
        self.events.push("update".to_string());
        self.target_children
            .insert(child.primary_key.clone(), child.clone());
        Ok(())
    }

    fn delete_target_child(
        &mut self,
        _metadata: &RepairMetadata,
        primary_key: &[String],
    ) -> Result<(), String> {
        self.events.push("delete".to_string());
        self.target_children.remove(primary_key);
        Ok(())
    }

    fn commit_batch(&mut self) -> Result<(), String> {
        self.events.push("commit".to_string());
        Ok(())
    }

    fn rollback_batch(&mut self) -> Result<(), String> {
        self.events.push("rollback".to_string());
        Ok(())
    }

    fn unlock_batch(&mut self) -> Result<(), String> {
        self.events.push("unlock".to_string());
        Ok(())
    }
}

#[test]
fn allowlisted_cases_preserve_exact_production_identities() {
    assert_eq!(
        FkOrphanRepairCase::PhrasesSuggestions.spec(),
        RepairCaseSpec {
            name: "phrases-suggestions",
            constraint_name: "phrases_suggestions_ibfk_1",
            child_table: "phrases_suggestions",
            child_primary_key: &["id", "lang", "author_id"],
            child_foreign_key: &["author_id", "author_username"],
            parent_table: "users",
            parent_primary_key: &["id"],
            parent_key: &["id", "name"],
            update_rule: "CASCADE",
            delete_rule: "CASCADE",
        }
    );
    assert!(FkOrphanRepairCase::parse("other").is_err());
}

#[test]
fn repairs_stale_target_child_from_exact_source_row() {
    let spec = FkOrphanRepairCase::ArtistsFavorites.spec();
    let primary_key = vec!["1".to_string()];
    let source_parent = parent("7", "Current");
    let target_parent = source_parent.clone();
    let source_child = child(&primary_key, "7", "Current");
    let target_child = child(&primary_key, "7", "Stale");
    let mut backend = FakeBackend::default();
    backend
        .source_children
        .insert(primary_key.clone(), source_child.clone());
    backend
        .target_children
        .insert(primary_key.clone(), target_child);
    backend
        .source_parents
        .insert(vec!["7".to_string()], source_parent);
    backend
        .target_parents
        .insert(vec!["7".to_string()], target_parent);

    let report = repair_with_backend(&config(spec, 1), &mut backend).expect("repair");

    assert_eq!(report.updated, 1);
    assert_eq!(report.remaining, 0);
    assert_eq!(backend.target_children[&primary_key], source_child);
    assert_eq!(backend.events, vec!["begin", "update", "commit", "unlock"]);
}

#[test]
fn deletes_target_orphan_only_when_source_child_is_absent() {
    let spec = FkOrphanRepairCase::ArtistsFavorites.spec();
    let primary_key = vec!["1".to_string()];
    let mut backend = FakeBackend::default();
    backend
        .target_children
        .insert(primary_key.clone(), child(&primary_key, "7", "Stale"));

    let report = repair_with_backend(&config(spec, 1), &mut backend).expect("repair");

    assert_eq!(report.deleted, 1);
    assert!(!backend.target_children.contains_key(&primary_key));
}

#[test]
fn invalid_source_relationship_rolls_back_without_target_write() {
    let spec = FkOrphanRepairCase::PhrasesSuggestions.spec();
    let primary_key = vec!["expand".to_string(), "fr".to_string(), "46603".to_string()];
    let source_child = row(
        &primary_key,
        [("author_id", "46603"), ("author_username", "Osso")],
    );
    let source_parent = parent("46603", "MasteringGreyDragon971");
    let mut backend = FakeBackend::default();
    backend
        .source_children
        .insert(primary_key.clone(), source_child.clone());
    backend
        .target_children
        .insert(primary_key.clone(), source_child);
    backend
        .source_parents
        .insert(vec!["46603".to_string()], source_parent.clone());
    backend
        .target_parents
        .insert(vec!["46603".to_string()], source_parent);

    let error = repair_with_backend(&config(spec, 1), &mut backend).expect_err("invalid source");

    assert!(error.contains("source FK relationship is invalid"));
    assert!(!backend.events.contains(&"update".to_string()));
    assert_eq!(backend.events, vec!["begin", "rollback", "unlock"]);
}

#[test]
fn failed_batch_start_attempts_rollback_and_unlock() {
    let spec = FkOrphanRepairCase::ArtistsFavorites.spec();
    let primary_key = vec!["1".to_string()];
    let mut backend = FakeBackend {
        fail_begin: true,
        ..FakeBackend::default()
    };
    backend
        .target_children
        .insert(primary_key.clone(), child(&primary_key, "7", "Stale"));

    let error = repair_with_backend(&config(spec, 1), &mut backend).expect_err("begin fails");

    assert_eq!(error, "begin failed");
    assert_eq!(backend.events, vec!["begin", "rollback", "unlock"]);
}

#[test]
fn exact_zero_orphan_expectation_is_idempotent() {
    let spec = FkOrphanRepairCase::ForumsReplies.spec();
    let mut backend = FakeBackend::default();

    let report = repair_with_backend(&config(spec, 0), &mut backend).expect("empty repair");

    assert_eq!(report, FkOrphanRepairReport::default());
    assert!(backend.events.is_empty());
}

fn config(spec: RepairCaseSpec, expected_orphans: usize) -> FkOrphanRepairConfig {
    FkOrphanRepairConfig {
        source: MySqlConnectionConfig {
            host: "source".to_string(),
            user: "user".to_string(),
            password: "password".to_string(),
            database: "db".to_string(),
            ..MySqlConnectionConfig::default()
        },
        target: TargetMySqlConfig {
            host: "target".to_string(),
            user: "user".to_string(),
            password: "password".to_string(),
            database: "db".to_string(),
            tls_ca_file: "/ca.pem".to_string(),
            ..TargetMySqlConfig::default()
        },
        case: FkOrphanRepairCase::parse(spec.name).expect("case"),
        expected_orphans,
        batch_size: 50,
        limit: 1000,
    }
}

fn metadata(spec: &RepairCaseSpec) -> RepairMetadata {
    RepairMetadata {
        source_child: sync_table(spec.child_table, spec.child_primary_key),
        target_child: sync_table(spec.child_table, spec.child_primary_key),
        source_parent: sync_table(spec.parent_table, spec.parent_primary_key),
        target_parent: sync_table(spec.parent_table, spec.parent_primary_key),
    }
}

fn sync_table(name: &str, primary_key: &[&str]) -> SyncTable {
    let mut columns = primary_key
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    for column in [
        "user_id",
        "user_username",
        "author_id",
        "author_username",
        "id",
        "name",
    ] {
        if !columns.iter().any(|existing| existing == column) {
            columns.push(column.to_string());
        }
    }
    SyncTable {
        name: name.to_string(),
        primary_key: primary_key.iter().map(|value| value.to_string()).collect(),
        primary_key_ordering: primary_key
            .iter()
            .map(|_| super::super::model::SyncPrimaryKeyOrdering::Native)
            .collect(),
        columns,
        bit_columns: Vec::new(),
    }
}

fn parent(id: &str, name: &str) -> DatabaseRow {
    row(&[id.to_string()], [("id", id), ("name", name)])
}

fn child(primary_key: &[String], parent_id: &str, parent_name: &str) -> DatabaseRow {
    row(
        primary_key,
        [("user_id", parent_id), ("user_username", parent_name)],
    )
}

fn row<const N: usize>(primary_key: &[String], values: [(&str, &str); N]) -> DatabaseRow {
    DatabaseRow {
        primary_key: primary_key.to_vec(),
        values: values
            .into_iter()
            .map(|(column, value)| (column.to_string(), Some(value.to_string())))
            .collect(),
    }
}

fn parent_primary_key(spec: &RepairCaseSpec, child: &DatabaseRow) -> Result<Vec<String>, String> {
    spec.parent_primary_key
        .iter()
        .map(|parent_column| {
            let position = spec
                .parent_key
                .iter()
                .position(|candidate| candidate == parent_column)
                .ok_or_else(|| format!("parent PK column `{parent_column}` is not referenced"))?;
            required_row_value(child, spec.child_foreign_key[position], "child")
                .map(ToString::to_string)
        })
        .collect()
}

fn parent_matches(
    spec: &RepairCaseSpec,
    child: &DatabaseRow,
    parents: &BTreeMap<Vec<String>, DatabaseRow>,
) -> bool {
    parent_primary_key(spec, child)
        .ok()
        .and_then(|primary_key| parents.get(&primary_key))
        .is_some_and(|parent| {
            validate_child_parent_relationship(spec, child, parent, "fake").is_ok()
        })
}
