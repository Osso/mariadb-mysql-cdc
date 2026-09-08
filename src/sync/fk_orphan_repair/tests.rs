use super::*;

type FixtureRows = BTreeMap<Vec<String>, DatabaseRow>;

#[derive(Default)]
struct FakeBackend {
    source_children: BTreeMap<Vec<String>, DatabaseRow>,
    source_parents: BTreeMap<Vec<String>, DatabaseRow>,
    target_children: BTreeMap<Vec<String>, DatabaseRow>,
    target_parents: BTreeMap<Vec<String>, DatabaseRow>,
    source_artists: FixtureRows,
    target_artists: FixtureRows,
    artist_snapshot: FixtureRows,
    mutate_source_artist: bool,
    corrupt_inserted_artist: bool,
    events: Vec<String>,
    fail_begin: bool,
    fail_child: bool,
    mutate_source_parent: bool,
    cascade_parent: bool,
    snapshot: Option<(FixtureRows, FixtureRows)>,
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
        self.snapshot = Some((self.target_children.clone(), self.target_parents.clone()));
        self.artist_snapshot = self.target_artists.clone();
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

    fn read_source_artist(
        &mut self,
        _metadata: &ArtistMetadata,
        primary_key: &[String],
    ) -> Result<Option<DatabaseRow>, String> {
        Ok(self.source_artists.get(primary_key).cloned())
    }

    fn read_target_artist(
        &mut self,
        _metadata: &ArtistMetadata,
        primary_key: &[String],
    ) -> Result<Option<DatabaseRow>, String> {
        Ok(self.target_artists.get(primary_key).cloned())
    }

    fn insert_target_artist(
        &mut self,
        _metadata: &ArtistMetadata,
        artist: &DatabaseRow,
    ) -> Result<(), String> {
        self.target_artists
            .insert(artist.primary_key.clone(), artist.clone());
        Ok(())
    }

    fn restore_target_parent(
        &mut self,
        _metadata: &RepairMetadata,
        parent: &DatabaseRow,
        _exists: bool,
    ) -> Result<(), String> {
        validate_child_parent_relationship(
            &FkOrphanRepairCase::Comics.spec(),
            parent,
            self.target_artists
                .get(&vec![parent.values["artist_id"].clone().unwrap()])
                .ok_or("missing required target artist")?,
            "target",
        )?;
        self.events.push("parent".into());
        self.target_parents
            .insert(parent.primary_key.clone(), parent.clone());
        if self.cascade_parent {
            for child in self.target_children.values_mut() {
                if child.values.get("comic_id") == parent.values.get("id") {
                    child
                        .values
                        .insert("comic_name".into(), parent.values["name"].clone());
                }
            }
        }
        if self.mutate_source_parent {
            self.source_parents
                .get_mut(&parent.primary_key)
                .unwrap()
                .values
                .insert("payload".into(), Some("changed".into()));
        }
        Ok(())
    }

    fn update_target_child(
        &mut self,
        _metadata: &RepairMetadata,
        child: &DatabaseRow,
    ) -> Result<(), String> {
        if self.fail_child {
            return Err("child constraint failure".into());
        }
        if self.mutate_source_artist {
            self.source_artists
                .values_mut()
                .next()
                .unwrap()
                .values
                .insert("payload".into(), Some("changed".into()));
        }
        if self.corrupt_inserted_artist {
            self.target_artists
                .values_mut()
                .next()
                .unwrap()
                .values
                .insert("payload".into(), Some("corrupt".into()));
        }
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
        if let Some((children, parents)) = self.snapshot.take() {
            self.target_children = children;
            self.target_parents = parents;
        }
        self.target_artists = self.artist_snapshot.clone();
        self.events.push("rollback".to_string());
        Ok(())
    }

    fn unlock_batch(&mut self) -> Result<(), String> {
        self.events.push("unlock".to_string());
        Ok(())
    }
}

fn comics_fixture(case: FkOrphanRepairCase, target_parent: Option<DatabaseRow>) -> FakeBackend {
    let spec = case.spec();
    let mut source_child = row(
        &["91".into()],
        [("id", "91"), ("comic_id", "7"), ("payload", "source")],
    );
    let mut source_parent = row(
        &["7".into()],
        [
            ("id", "7"),
            ("payload", "complete parent"),
            ("artist_id", "34734"),
            ("artist_name", "publisher"),
        ],
    );
    let artist = row(
        &["34734".into()],
        [
            ("id", "34734"),
            ("name", "publisher"),
            ("payload", "complete artist"),
        ],
    );
    for (child_column, parent_column) in spec.child_foreign_key.iter().zip(spec.parent_key) {
        let value = if *parent_column == "id" {
            "7"
        } else {
            "current"
        };
        source_child
            .values
            .insert((*child_column).into(), Some(value.into()));
        source_parent
            .values
            .insert((*parent_column).into(), Some(value.into()));
    }
    let mut target_child = source_child.clone();
    target_child.values.insert(
        spec.child_foreign_key.last().unwrap().to_string(),
        Some("stale".into()),
    );
    FakeBackend {
        source_artists: [(artist.primary_key.clone(), artist.clone())].into(),
        target_artists: [(artist.primary_key.clone(), artist)].into(),
        source_children: [(source_child.primary_key.clone(), source_child)].into(),
        source_parents: [(source_parent.primary_key.clone(), source_parent)].into(),
        target_children: [(target_child.primary_key.clone(), target_child)].into(),
        target_parents: target_parent
            .into_iter()
            .map(|row| (row.primary_key.clone(), row))
            .collect(),
        ..Default::default()
    }
}

#[test]
fn inserts_missing_artist_before_comic_and_child() {
    let case = FkOrphanRepairCase::ComicsLangsCategory;
    let mut backend = comics_fixture(case, None);
    backend.target_artists.clear();
    repair_with_backend(&config(case.spec(), 1), &mut backend).unwrap();
    assert_eq!(backend.target_artists, backend.source_artists);
    assert_eq!(backend.target_parents, backend.source_parents);
    assert_eq!(backend.target_children, backend.source_children);
}

#[test]
fn existing_artist_payload_is_not_overwritten() {
    let case = FkOrphanRepairCase::ReleasesName;
    let mut backend = comics_fixture(case, None);
    backend
        .target_artists
        .values_mut()
        .next()
        .unwrap()
        .values
        .insert("payload".into(), Some("unrelated target value".into()));
    let before = backend.target_artists.clone();
    repair_with_backend(&config(case.spec(), 1), &mut backend).unwrap();
    assert_eq!(backend.target_artists, before);
}

#[test]
fn artist_failures_roll_back_entire_repair_batch() {
    for (failure, expected) in [
        ("missing source", "source artist is missing"),
        (
            "source relation",
            "source comic artist FK relationship is invalid",
        ),
        ("identity", "target parent differs from source"),
        ("unstable", "source artist changed"),
        ("corrupt", "inserted target artist verification failed"),
        ("child", "child constraint failure"),
    ] {
        let case = FkOrphanRepairCase::ReleasesName;
        let mut backend = comics_fixture(case, None);
        backend.target_artists.clear();
        match failure {
            "missing source" => backend.source_artists.clear(),
            "source relation" => {
                backend
                    .source_artists
                    .values_mut()
                    .next()
                    .unwrap()
                    .values
                    .insert("name".into(), Some("wrong source publisher".into()));
            }
            "identity" => {
                backend.target_artists = backend.source_artists.clone();
                backend
                    .target_artists
                    .values_mut()
                    .next()
                    .unwrap()
                    .values
                    .insert("name".into(), Some("wrong publisher".into()));
            }
            "unstable" => backend.mutate_source_artist = true,
            "corrupt" => backend.corrupt_inserted_artist = true,
            "child" => backend.fail_child = true,
            _ => unreachable!(),
        }
        let before_artists = backend.target_artists.clone();
        let before_children = backend.target_children.clone();
        let error = repair_with_backend(&config(case.spec(), 1), &mut backend).unwrap_err();
        assert!(error.contains(expected), "{failure}: {error}");
        assert_eq!(backend.target_artists, before_artists, "{failure}");
        assert_eq!(backend.target_children, before_children, "{failure}");
        assert!(backend.target_parents.is_empty(), "{failure}");
    }
}

#[test]
fn restores_missing_and_stale_comics_parents_before_children() {
    for case in [
        FkOrphanRepairCase::ComicsLangsCategory,
        FkOrphanRepairCase::ReleasesName,
        FkOrphanRepairCase::ReleasesSlug,
    ] {
        for stale in [false, true] {
            let mut backend = comics_fixture(case, None);
            if stale {
                let mut parent = backend.source_parents[&vec!["7".into()]].clone();
                parent
                    .values
                    .insert("payload".into(), Some("stale parent".into()));
                backend
                    .target_parents
                    .insert(parent.primary_key.clone(), parent);
            }
            repair_with_backend(&config(case.spec(), 1), &mut backend)
                .expect("restore parent and child");
            assert_eq!(backend.target_children, backend.source_children);
            assert_eq!(backend.target_parents, backend.source_parents);
        }
    }
}

#[test]
fn parent_restore_rolls_back_on_child_failure_or_source_instability() {
    for unstable in [false, true] {
        let case = FkOrphanRepairCase::ReleasesName;
        let mut backend = comics_fixture(case, None);
        let children_before = backend.target_children.clone();
        backend.fail_child = !unstable;
        backend.mutate_source_parent = unstable;
        let error = repair_with_backend(&config(case.spec(), 1), &mut backend).unwrap_err();
        assert!(
            error.contains(if unstable {
                "source parent changed"
            } else {
                "child constraint failure"
            }),
            "{error}"
        );
        assert_eq!(backend.target_children, children_before);
        assert!(backend.target_parents.is_empty());
        assert!(!backend.events.contains(&"commit".into()));
    }
}

#[test]
fn restored_parent_cascade_can_make_child_update_unnecessary() {
    let case = FkOrphanRepairCase::ReleasesName;
    let mut backend = comics_fixture(case, None);
    backend.cascade_parent = true;
    backend.fail_child = true;
    let report = repair_with_backend(&config(case.spec(), 1), &mut backend).unwrap();
    assert_eq!(report.unchanged, 1);
    assert_eq!(backend.target_children, backend.source_children);
    assert_eq!(backend.target_parents, backend.source_parents);
}

#[test]
fn later_failure_rolls_back_prior_parent_and_cascade_changes_in_batch() {
    let case = FkOrphanRepairCase::ReleasesName;
    let mut backend = comics_fixture(case, None);
    backend.target_artists.clear();
    backend.cascade_parent = true;
    let key = vec!["92".into()];
    let second = row(
        &key,
        [("id", "92"), ("comic_id", "8"), ("comic_name", "missing")],
    );
    backend.source_children.insert(key.clone(), second.clone());
    backend.target_children.insert(key, second);
    let before = backend.target_children.clone();
    let error = repair_with_backend(&config(case.spec(), 2), &mut backend).unwrap_err();
    assert!(error.contains("source parent is missing"), "{error}");
    assert_eq!(backend.target_children, before);
    assert!(backend.target_parents.is_empty());
    assert!(backend.target_artists.is_empty());
}

#[test]
fn comics_source_absent_child_is_deleted_without_restoring_parent() {
    let case = FkOrphanRepairCase::ReleasesName;
    let mut backend = comics_fixture(case, None);
    backend.source_children.clear();
    let report = repair_with_backend(&config(case.spec(), 1), &mut backend).unwrap();
    assert_eq!(report.deleted, 1);
    assert!(backend.target_children.is_empty());
    assert!(backend.target_parents.is_empty());
}

#[test]
fn legacy_cases_refuse_missing_or_stale_target_parent() {
    for stale in [false, true] {
        let spec = FkOrphanRepairCase::ArtistsFavorites.spec();
        let key = vec!["1".into()];
        let mut backend = FakeBackend::default();
        backend
            .source_children
            .insert(key.clone(), child(&key, "7", "Current"));
        backend
            .target_children
            .insert(key.clone(), child(&key, "7", "Orphan"));
        backend
            .source_parents
            .insert(vec!["7".into()], parent("7", "Current"));
        if stale {
            backend
                .target_parents
                .insert(vec!["7".into()], parent("7", "Old"));
        }
        let before = backend.target_parents.clone();
        assert!(repair_with_backend(&config(spec, 1), &mut backend).is_err());
        assert_eq!(backend.target_parents, before);
        assert!(!backend.events.contains(&"parent".into()));
    }
}

#[test]
fn correct_parent_is_not_written_and_completed_repair_is_noop() {
    let case = FkOrphanRepairCase::ComicsLangsCategory;
    let mut backend = comics_fixture(case, None);
    backend.target_parents = backend.source_parents.clone();
    repair_with_backend(&config(case.spec(), 1), &mut backend).unwrap();
    assert!(!backend.events.contains(&"parent".into()));
    backend.events.clear();
    repair_with_backend(&config(case.spec(), 0), &mut backend).unwrap();
    assert!(backend.events.is_empty());
}

#[test]
fn slug_parent_must_match_explicit_comic_id_and_source_slug() {
    let case = FkOrphanRepairCase::ReleasesSlug;
    let mut backend = comics_fixture(case, None);
    backend
        .source_parents
        .values_mut()
        .next()
        .unwrap()
        .values
        .insert("slug".into(), Some("wrong".into()));
    assert!(
        repair_with_backend(&config(case.spec(), 1), &mut backend)
            .unwrap_err()
            .contains("source FK relationship is invalid")
    );
    assert!(backend.target_parents.is_empty());
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
        artists: spec.restores_comics_parent().then(|| ArtistMetadata {
            source: sync_table("artists", &["id"]),
            target: sync_table("artists", &["id"]),
        }),
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
        enum_columns: std::collections::BTreeMap::new(),
        mediumblob_columns: Vec::new(),
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
    if spec.restores_comics_parent() {
        return Ok(vec![
            required_row_value(child, "comic_id", "child")?.to_string(),
        ]);
    }
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

#[test]
fn comics_relationship_cases_validate_each_denormalized_value() {
    let cases = [
        (
            "comics-langs-category",
            "ibfk_accl_category",
            "comics_langs",
            "comic_category_id",
            "section_id",
        ),
        (
            "comics-langs-type",
            "ibfk_accl_type",
            "comics_langs",
            "comic_type_id",
            "comic_type_id",
        ),
        (
            "releases-name",
            "releases_ibfk_1",
            "releases",
            "comic_name",
            "name",
        ),
        (
            "releases-type",
            "releases_ibfk_10",
            "releases",
            "comic_type_id",
            "comic_type_id",
        ),
        (
            "releases-category",
            "releases_ibfk_2",
            "releases",
            "comic_category_id",
            "section_id",
        ),
        (
            "releases-visibility",
            "releases_ibfk_3",
            "releases",
            "comic_is_visible",
            "is_visible",
        ),
        (
            "releases-id",
            "releases_ibfk_6",
            "releases",
            "comic_id",
            "id",
        ),
        (
            "releases-slug",
            "releases_ibfk_7",
            "releases",
            "comic_slug",
            "slug",
        ),
        (
            "releases-show-in-list",
            "releases_ibfk_9",
            "releases",
            "comic_show_in_list",
            "show_in_list",
        ),
        (
            "releases-format",
            "releases_ibfk_format",
            "releases",
            "comic_format_id",
            "comic_format_id",
        ),
    ];
    for (name, constraint, table, child_column, parent_column) in cases {
        let spec = FkOrphanRepairCase::parse(name).expect(name).spec();
        assert_eq!(spec.constraint_name, constraint);
        assert_eq!(spec.child_table, table);
        assert_eq!(spec.child_primary_key, &["id"]);
        assert_eq!(spec.parent_table, "comics");
        assert_eq!(spec.parent_primary_key, &["id"]);
        assert_eq!(
            (spec.update_rule, spec.delete_rule),
            ("CASCADE", "RESTRICT")
        );
        let mut child = row(
            &["91".to_string()],
            [("comic_id", "7"), (child_column, "7")],
        );
        let parent = row(&["7".to_string()], [("id", "7"), (parent_column, "7")]);
        assert!(validate_child_parent_relationship(&spec, &child, &parent, "source").is_ok());
        child
            .values
            .insert(child_column.to_string(), Some("8".to_string()));
        assert!(
            validate_child_parent_relationship(&spec, &child, &parent, "source").is_err(),
            "{name}"
        );
        child.values.remove(child_column);
        assert!(
            validate_child_parent_relationship(&spec, &child, &parent, "source").is_err(),
            "{name}"
        );
    }
    for name in [
        "releases",
        "comics-langs",
        "releases-artist",
        "releases-any",
    ] {
        assert!(FkOrphanRepairCase::parse(name).is_err());
    }
}
