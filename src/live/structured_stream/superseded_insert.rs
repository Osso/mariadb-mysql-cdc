#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct BinlogCoordinate {
    pub(crate) file: String,
    pub(crate) position: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SupersededInsertVerificationInput {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) operation: crate::conflict_repair::ConflictOperation,
    pub(crate) duplicate_index: String,
    pub(crate) candidate_xid: BinlogCoordinate,
    pub(crate) source_snapshot: BinlogCoordinate,
    pub(crate) historical_primary_key: String,
    pub(crate) historical_name: String,
    pub(crate) historical_image_hash: String,
    pub(crate) source_primary_row_count: usize,
    pub(crate) source_primary_name: String,
    pub(crate) source_primary_hash: String,
    pub(crate) source_owner_row_count: usize,
    pub(crate) source_owner_primary_key: String,
    pub(crate) source_owner_hash: String,
    pub(crate) target_rows_read_for_update: bool,
    pub(crate) target_primary_row_count: usize,
    pub(crate) target_primary_hash: String,
    pub(crate) target_owner_row_count: usize,
    pub(crate) target_owner_hash: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SupersededInsertProof {
    pub(crate) source_snapshot: BinlogCoordinate,
    pub(crate) historical_image_hash: String,
    pub(crate) source_primary_hash: String,
    pub(crate) source_owner_hash: String,
    pub(crate) target_primary_hash: String,
    pub(crate) target_owner_hash: String,
    pub(crate) current_row_install: Option<crate::target::SqlStatement>,
}

impl SupersededInsertProof {
    pub(crate) fn resolution_evidence(&self) -> String {
        let install = if self.current_row_install.is_some() {
            "; exact current source row installed"
        } else {
            "; exact current source row already present"
        };
        format!(
            "verified superseded historical insert at source snapshot {}:{}; historical image hash {}; source primary row hash {}; source unique owner row hash {}; target primary row hash {}; target unique owner row hash {}{}",
            self.source_snapshot.file,
            self.source_snapshot.position,
            self.historical_image_hash,
            self.source_primary_hash,
            self.source_owner_hash,
            self.target_primary_hash,
            self.target_owner_hash,
            install,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SupersededReleaseVerificationInput {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) operation: crate::conflict_repair::ConflictOperation,
    pub(crate) error_code: u16,
    pub(crate) constraint: String,
    pub(crate) candidate_xid: BinlogCoordinate,
    pub(crate) source_snapshot: BinlogCoordinate,
    pub(crate) historical_release_id: String,
    pub(crate) historical_comic_id: String,
    pub(crate) historical_category_id: String,
    pub(crate) current_release_row_count: usize,
    pub(crate) current_release_id: String,
    pub(crate) current_release_comic_id: String,
    pub(crate) current_release_category_id: String,
    pub(crate) current_release_hash: String,
    pub(crate) source_parent_row_count: usize,
    pub(crate) source_parent_comic_id: String,
    pub(crate) source_parent_category_id: String,
    pub(crate) source_parent_hash: String,
    pub(crate) target_release_rows_read_for_update: bool,
    pub(crate) target_release_row_count: usize,
    pub(crate) target_release_hash: String,
    pub(crate) target_parent_read_for_update: bool,
    pub(crate) target_parent_row_count: usize,
    pub(crate) target_parent_comic_id: String,
    pub(crate) target_parent_category_id: String,
    pub(crate) target_parent_hash: String,
    pub(crate) historical_image_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SupersededReleaseProof {
    pub(crate) source_snapshot: BinlogCoordinate,
    pub(crate) historical_image_hash: String,
    pub(crate) current_release_hash: String,
    pub(crate) source_parent_hash: String,
    pub(crate) target_parent_hash: String,
    pub(crate) install_current_release: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SupersededReleaseRejection {
    WrongScope,
    WrongOperation,
    WrongErrorCode,
    WrongConstraint,
    SourceSnapshotNotBeyondCandidateXid,
    CurrentReleaseRowNotUnique,
    MissingLaterSourceHistory,
    CurrentReleaseIdMismatch,
    CurrentReleaseComicMismatch,
    MissingCurrentReleaseHash,
    SourceParentRowNotUnique,
    SourceParentMismatch,
    MissingSourceParentHash,
    TargetReleaseNotReadForUpdate,
    TargetReleaseRowAmbiguous,
    TargetReleaseHashMismatch,
    TargetParentNotReadForUpdate,
    TargetParentRowNotUnique,
    TargetParentMismatch,
    MissingHistoricalImageHash,
}

pub(crate) fn verify_superseded_release_insert(
    candidate: &SupersededReleaseVerificationInput,
) -> Result<SupersededReleaseProof, SupersededReleaseRejection> {
    if candidate.schema != "globalcomix" || candidate.table != "releases" {
        return Err(SupersededReleaseRejection::WrongScope);
    }
    if candidate.operation != crate::conflict_repair::ConflictOperation::Insert {
        return Err(SupersededReleaseRejection::WrongOperation);
    }
    if candidate.error_code != 1452 {
        return Err(SupersededReleaseRejection::WrongErrorCode);
    }
    if candidate.constraint != "releases_ibfk_2" {
        return Err(SupersededReleaseRejection::WrongConstraint);
    }
    if candidate.source_snapshot <= candidate.candidate_xid {
        return Err(SupersededReleaseRejection::SourceSnapshotNotBeyondCandidateXid);
    }
    if candidate.current_release_row_count != 1 {
        return Err(SupersededReleaseRejection::CurrentReleaseRowNotUnique);
    }
    if candidate.current_release_category_id == candidate.historical_category_id {
        return Err(SupersededReleaseRejection::MissingLaterSourceHistory);
    }
    if candidate.current_release_id != candidate.historical_release_id {
        return Err(SupersededReleaseRejection::CurrentReleaseIdMismatch);
    }
    if candidate.current_release_comic_id != candidate.historical_comic_id {
        return Err(SupersededReleaseRejection::CurrentReleaseComicMismatch);
    }
    if candidate.current_release_hash.is_empty() {
        return Err(SupersededReleaseRejection::MissingCurrentReleaseHash);
    }
    if candidate.source_parent_row_count != 1 {
        return Err(SupersededReleaseRejection::SourceParentRowNotUnique);
    }
    if candidate.source_parent_comic_id != candidate.current_release_comic_id
        || candidate.source_parent_category_id != candidate.current_release_category_id
    {
        return Err(SupersededReleaseRejection::SourceParentMismatch);
    }
    if candidate.source_parent_hash.is_empty() {
        return Err(SupersededReleaseRejection::MissingSourceParentHash);
    }
    if !candidate.target_release_rows_read_for_update {
        return Err(SupersededReleaseRejection::TargetReleaseNotReadForUpdate);
    }
    if candidate.target_release_row_count > 1 {
        return Err(SupersededReleaseRejection::TargetReleaseRowAmbiguous);
    }
    if candidate.target_release_row_count == 1
        && candidate.target_release_hash != candidate.current_release_hash
    {
        return Err(SupersededReleaseRejection::TargetReleaseHashMismatch);
    }
    if !candidate.target_parent_read_for_update {
        return Err(SupersededReleaseRejection::TargetParentNotReadForUpdate);
    }
    if candidate.target_parent_row_count != 1 {
        return Err(SupersededReleaseRejection::TargetParentRowNotUnique);
    }
    if candidate.target_parent_comic_id != candidate.current_release_comic_id
        || candidate.target_parent_category_id != candidate.current_release_category_id
    {
        return Err(SupersededReleaseRejection::TargetParentMismatch);
    }
    if candidate.historical_image_hash.is_empty() {
        return Err(SupersededReleaseRejection::MissingHistoricalImageHash);
    }

    Ok(SupersededReleaseProof {
        source_snapshot: candidate.source_snapshot.clone(),
        historical_image_hash: candidate.historical_image_hash.clone(),
        current_release_hash: candidate.current_release_hash.clone(),
        source_parent_hash: candidate.source_parent_hash.clone(),
        target_parent_hash: candidate.target_parent_hash.clone(),
        install_current_release: candidate.target_release_row_count == 0,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SupersededInsertRejection {
    WrongScope,
    WrongOperation,
    WrongDuplicateIndex,
    SourceSnapshotNotBeyondCandidateXid,
    SourcePrimaryRowNotUnique,
    MissingSourcePrimaryHash,
    SourcePrimaryStillOwnsHistoricalName,
    SourceOwnerRowNotUnique,
    SourceOwnerMatchesHistoricalPrimaryKey,
    MissingSourceOwnerHash,
    TargetRowsNotReadForUpdate,
    TargetPrimaryRowNotUnique,
    TargetPrimaryHashMismatch,
    TargetOwnerRowNotUnique,
    TargetOwnerHashMismatch,
    MissingHistoricalImageHash,
}

pub(crate) fn verify_superseded_insert(
    candidate: &SupersededInsertVerificationInput,
) -> Result<SupersededInsertProof, SupersededInsertRejection> {
    let supported_scope = candidate.schema == "globalcomix"
        && ((candidate.table == "users" && candidate.duplicate_index == "users.name")
            || (candidate.table == "comics" && candidate.duplicate_index == "comics.slug"));
    if !supported_scope {
        return Err(SupersededInsertRejection::WrongScope);
    }
    if candidate.operation != crate::conflict_repair::ConflictOperation::Insert {
        return Err(SupersededInsertRejection::WrongOperation);
    }
    let expected_index = if candidate.table == "comics" {
        "comics.slug"
    } else {
        "users.name"
    };
    if candidate.duplicate_index != expected_index {
        return Err(SupersededInsertRejection::WrongDuplicateIndex);
    }
    if candidate.source_snapshot <= candidate.candidate_xid {
        return Err(SupersededInsertRejection::SourceSnapshotNotBeyondCandidateXid);
    }
    if candidate.source_primary_row_count != 1 {
        return Err(SupersededInsertRejection::SourcePrimaryRowNotUnique);
    }
    if candidate.source_primary_hash.is_empty() {
        return Err(SupersededInsertRejection::MissingSourcePrimaryHash);
    }
    if candidate.source_primary_name == candidate.historical_name {
        return Err(SupersededInsertRejection::SourcePrimaryStillOwnsHistoricalName);
    }
    if candidate.source_owner_row_count != 1 {
        return Err(SupersededInsertRejection::SourceOwnerRowNotUnique);
    }
    if candidate.source_owner_primary_key == candidate.historical_primary_key {
        return Err(SupersededInsertRejection::SourceOwnerMatchesHistoricalPrimaryKey);
    }
    if candidate.source_owner_hash.is_empty() {
        return Err(SupersededInsertRejection::MissingSourceOwnerHash);
    }
    if !candidate.target_rows_read_for_update {
        return Err(SupersededInsertRejection::TargetRowsNotReadForUpdate);
    }
    if candidate.target_primary_row_count != 1 {
        return Err(SupersededInsertRejection::TargetPrimaryRowNotUnique);
    }
    if candidate.target_primary_hash != candidate.source_primary_hash {
        return Err(SupersededInsertRejection::TargetPrimaryHashMismatch);
    }
    if candidate.target_owner_row_count != 1 {
        return Err(SupersededInsertRejection::TargetOwnerRowNotUnique);
    }
    if candidate.target_owner_hash != candidate.source_owner_hash {
        return Err(SupersededInsertRejection::TargetOwnerHashMismatch);
    }
    if candidate.historical_image_hash.is_empty() {
        return Err(SupersededInsertRejection::MissingHistoricalImageHash);
    }

    Ok(SupersededInsertProof {
        source_snapshot: candidate.source_snapshot.clone(),
        historical_image_hash: candidate.historical_image_hash.clone(),
        source_primary_hash: candidate.source_primary_hash.clone(),
        source_owner_hash: candidate.source_owner_hash.clone(),
        target_primary_hash: candidate.target_primary_hash.clone(),
        target_owner_hash: candidate.target_owner_hash.clone(),
        current_row_install: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_candidate() -> SupersededInsertVerificationInput {
        SupersededInsertVerificationInput {
            schema: "globalcomix".to_string(),
            table: "users".to_string(),
            operation: crate::conflict_repair::ConflictOperation::Insert,
            duplicate_index: "users.name".to_string(),
            candidate_xid: BinlogCoordinate {
                file: "mysqld-bin.002709".to_string(),
                position: 404_038_011,
            },
            source_snapshot: BinlogCoordinate {
                file: "mysqld-bin.002740".to_string(),
                position: 1_004_163_590,
            },
            historical_primary_key: "2070980".to_string(),
            historical_name: "-3572".to_string(),
            historical_image_hash: "historical-hash".to_string(),
            source_primary_row_count: 1,
            source_primary_name: "vngt".to_string(),
            source_primary_hash: "primary-hash".to_string(),
            source_owner_row_count: 1,
            source_owner_primary_key: "2071305".to_string(),
            source_owner_hash: "owner-hash".to_string(),
            target_rows_read_for_update: true,
            target_primary_row_count: 1,
            target_primary_hash: "primary-hash".to_string(),
            target_owner_row_count: 1,
            target_owner_hash: "owner-hash".to_string(),
        }
    }

    #[test]
    fn returns_proof_when_every_superseded_insert_predicate_passes() {
        let candidate = valid_candidate();

        let proof = verify_superseded_insert(&candidate).expect("valid superseded insert proof");

        assert_eq!(proof.source_snapshot, candidate.source_snapshot);
        assert_eq!(proof.historical_image_hash, "historical-hash");
        assert_eq!(proof.source_primary_hash, "primary-hash");
        assert_eq!(proof.source_owner_hash, "owner-hash");
    }

    fn valid_comics_slug_candidate() -> SupersededInsertVerificationInput {
        let mut candidate = valid_candidate();
        candidate.table = "comics".to_string();
        candidate.duplicate_index = "comics.slug".to_string();
        candidate.candidate_xid.position = 531_241_781;
        candidate.historical_primary_key = "48054".to_string();
        candidate.historical_name = "misc".to_string();
        candidate.source_primary_name =
            "DELETED_misccf7e8b9d-5851-4616-910e-5bfb755bd55e9HrF".to_string();
        candidate.source_owner_primary_key = "48058".to_string();
        candidate
    }

    #[test]
    fn comics_slug_supersession_accepts_renamed_primary_and_current_owner() {
        verify_superseded_insert(&valid_comics_slug_candidate())
            .expect("verified comics.slug supersession");
    }

    #[test]
    fn comics_slug_supersession_rejects_mismatched_locked_owner() {
        let mut candidate = valid_comics_slug_candidate();
        candidate.target_owner_hash = "different-owner".to_string();

        assert_eq!(
            verify_superseded_insert(&candidate),
            Err(SupersededInsertRejection::TargetOwnerHashMismatch)
        );
    }

    fn valid_release_candidate() -> SupersededReleaseVerificationInput {
        SupersededReleaseVerificationInput {
            schema: "globalcomix".to_string(),
            table: "releases".to_string(),
            operation: crate::conflict_repair::ConflictOperation::Insert,
            error_code: 1452,
            constraint: "releases_ibfk_2".to_string(),
            candidate_xid: BinlogCoordinate {
                file: "mysqld-bin.002709".to_string(),
                position: 404_038_011,
            },
            source_snapshot: BinlogCoordinate {
                file: "mysqld-bin.002740".to_string(),
                position: 1_004_163_590,
            },
            historical_release_id: "77".to_string(),
            historical_comic_id: "12".to_string(),
            historical_category_id: "4".to_string(),
            current_release_row_count: 1,
            current_release_id: "77".to_string(),
            current_release_comic_id: "12".to_string(),
            current_release_category_id: "9".to_string(),
            current_release_hash: "release-hash".to_string(),
            source_parent_row_count: 1,
            source_parent_comic_id: "12".to_string(),
            source_parent_category_id: "9".to_string(),
            source_parent_hash: "source-parent-hash".to_string(),
            target_release_rows_read_for_update: true,
            target_release_row_count: 0,
            target_release_hash: String::new(),
            target_parent_read_for_update: true,
            target_parent_row_count: 1,
            target_parent_comic_id: "12".to_string(),
            target_parent_category_id: "9".to_string(),
            target_parent_hash: "source-parent-hash".to_string(),
            historical_image_hash: "historical-release-hash".to_string(),
        }
    }

    #[test]
    fn release_fk_recovery_returns_exact_current_install_when_all_predicates_pass() {
        let candidate = valid_release_candidate();

        let proof =
            verify_superseded_release_insert(&candidate).expect("valid superseded release proof");

        assert!(proof.install_current_release);
        assert_eq!(proof.current_release_hash, "release-hash");
        assert_eq!(proof.source_parent_hash, proof.target_parent_hash);
    }

    #[test]
    fn release_fk_recovery_accepts_lagged_mutable_parent_fields() {
        let mut candidate = valid_release_candidate();
        candidate.target_parent_hash = "lagged-mutable-fields".to_string();

        let proof = verify_superseded_release_insert(&candidate)
            .expect("exact target FK identity permits lagged mutable fields");

        assert!(proof.install_current_release);
        assert_ne!(proof.source_parent_hash, proof.target_parent_hash);
    }

    #[test]
    fn identical_ordered_rows_use_one_canonical_hash() {
        let values = vec![
            mysql::Value::UInt(12),
            mysql::Value::Bytes(b"same".to_vec()),
        ];
        assert_eq!(
            crate::target::hash_ordered_mysql_row(&values),
            crate::target::hash_ordered_mysql_row(&values)
        );
    }

    #[test]
    fn release_fk_recovery_accepts_exact_current_target_row_without_reinstalling() {
        let mut candidate = valid_release_candidate();
        candidate.target_release_row_count = 1;
        candidate.target_release_hash = candidate.current_release_hash.clone();

        let proof =
            verify_superseded_release_insert(&candidate).expect("exact current release is safe");

        assert!(!proof.install_current_release);
    }

    #[test]
    fn release_fk_recovery_rejects_scope_history_parent_and_target_ambiguity() {
        struct Case {
            name: &'static str,
            alter: fn(&mut SupersededReleaseVerificationInput),
            expected: SupersededReleaseRejection,
        }

        let cases = [
            Case {
                name: "wrong constraint",
                alter: |value| value.constraint = "releases_ibfk_1".to_string(),
                expected: SupersededReleaseRejection::WrongConstraint,
            },
            Case {
                name: "snapshot not later",
                alter: |value| value.source_snapshot = value.candidate_xid.clone(),
                expected: SupersededReleaseRejection::SourceSnapshotNotBeyondCandidateXid,
            },
            Case {
                name: "no later category",
                alter: |value| {
                    value.current_release_category_id = value.historical_category_id.clone()
                },
                expected: SupersededReleaseRejection::MissingLaterSourceHistory,
            },
            Case {
                name: "release id changed",
                alter: |value| value.current_release_id = "78".to_string(),
                expected: SupersededReleaseRejection::CurrentReleaseIdMismatch,
            },
            Case {
                name: "release comic changed",
                alter: |value| value.current_release_comic_id = "13".to_string(),
                expected: SupersededReleaseRejection::CurrentReleaseComicMismatch,
            },
            Case {
                name: "source parent mismatch",
                alter: |value| value.source_parent_category_id = "10".to_string(),
                expected: SupersededReleaseRejection::SourceParentMismatch,
            },
            Case {
                name: "target parent unlocked",
                alter: |value| value.target_parent_read_for_update = false,
                expected: SupersededReleaseRejection::TargetParentNotReadForUpdate,
            },
            Case {
                name: "target parent mismatch",
                alter: |value| value.target_parent_category_id = "8".to_string(),
                expected: SupersededReleaseRejection::TargetParentMismatch,
            },
            Case {
                name: "target release divergent",
                alter: |value| {
                    value.target_release_row_count = 1;
                    value.target_release_hash = "historical".to_string();
                },
                expected: SupersededReleaseRejection::TargetReleaseHashMismatch,
            },
            Case {
                name: "target release ambiguous",
                alter: |value| value.target_release_row_count = 2,
                expected: SupersededReleaseRejection::TargetReleaseRowAmbiguous,
            },
        ];

        for case in cases {
            let mut candidate = valid_release_candidate();
            (case.alter)(&mut candidate);
            assert_eq!(
                verify_superseded_release_insert(&candidate),
                Err(case.expected),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn rejects_each_failed_predicate_precisely() {
        struct Case {
            name: &'static str,
            alter: fn(&mut SupersededInsertVerificationInput),
            expected: SupersededInsertRejection,
        }

        let cases = [
            Case {
                name: "wrong schema scope",
                alter: |candidate| candidate.schema = "other".to_string(),
                expected: SupersededInsertRejection::WrongScope,
            },
            Case {
                name: "wrong table scope",
                alter: |candidate| candidate.table = "guests".to_string(),
                expected: SupersededInsertRejection::WrongScope,
            },
            Case {
                name: "non-insert operation",
                alter: |candidate| {
                    candidate.operation = crate::conflict_repair::ConflictOperation::Update
                },
                expected: SupersededInsertRejection::WrongOperation,
            },
            Case {
                name: "wrong duplicate index",
                alter: |candidate| candidate.duplicate_index = "users.email".to_string(),
                expected: SupersededInsertRejection::WrongDuplicateIndex,
            },
            Case {
                name: "snapshot coordinate equals candidate xid",
                alter: |candidate| candidate.source_snapshot = candidate.candidate_xid.clone(),
                expected: SupersededInsertRejection::SourceSnapshotNotBeyondCandidateXid,
            },
            Case {
                name: "source primary row missing",
                alter: |candidate| candidate.source_primary_row_count = 0,
                expected: SupersededInsertRejection::SourcePrimaryRowNotUnique,
            },
            Case {
                name: "source primary row ambiguous",
                alter: |candidate| candidate.source_primary_row_count = 2,
                expected: SupersededInsertRejection::SourcePrimaryRowNotUnique,
            },
            Case {
                name: "source primary hash empty",
                alter: |candidate| candidate.source_primary_hash.clear(),
                expected: SupersededInsertRejection::MissingSourcePrimaryHash,
            },
            Case {
                name: "source primary still owns historical name",
                alter: |candidate| {
                    candidate.source_primary_name = candidate.historical_name.clone()
                },
                expected: SupersededInsertRejection::SourcePrimaryStillOwnsHistoricalName,
            },
            Case {
                name: "source owner missing",
                alter: |candidate| candidate.source_owner_row_count = 0,
                expected: SupersededInsertRejection::SourceOwnerRowNotUnique,
            },
            Case {
                name: "source owner ambiguous",
                alter: |candidate| candidate.source_owner_row_count = 2,
                expected: SupersededInsertRejection::SourceOwnerRowNotUnique,
            },
            Case {
                name: "source owner has historical primary key",
                alter: |candidate| {
                    candidate.source_owner_primary_key = candidate.historical_primary_key.clone()
                },
                expected: SupersededInsertRejection::SourceOwnerMatchesHistoricalPrimaryKey,
            },
            Case {
                name: "source owner hash empty",
                alter: |candidate| candidate.source_owner_hash.clear(),
                expected: SupersededInsertRejection::MissingSourceOwnerHash,
            },
            Case {
                name: "target rows not read for update",
                alter: |candidate| candidate.target_rows_read_for_update = false,
                expected: SupersededInsertRejection::TargetRowsNotReadForUpdate,
            },
            Case {
                name: "target primary row missing",
                alter: |candidate| candidate.target_primary_row_count = 0,
                expected: SupersededInsertRejection::TargetPrimaryRowNotUnique,
            },
            Case {
                name: "target primary row ambiguous",
                alter: |candidate| candidate.target_primary_row_count = 2,
                expected: SupersededInsertRejection::TargetPrimaryRowNotUnique,
            },
            Case {
                name: "target primary hash differs",
                alter: |candidate| candidate.target_primary_hash = "different".to_string(),
                expected: SupersededInsertRejection::TargetPrimaryHashMismatch,
            },
            Case {
                name: "target owner missing",
                alter: |candidate| candidate.target_owner_row_count = 0,
                expected: SupersededInsertRejection::TargetOwnerRowNotUnique,
            },
            Case {
                name: "target owner ambiguous",
                alter: |candidate| candidate.target_owner_row_count = 2,
                expected: SupersededInsertRejection::TargetOwnerRowNotUnique,
            },
            Case {
                name: "target owner hash differs",
                alter: |candidate| candidate.target_owner_hash = "different".to_string(),
                expected: SupersededInsertRejection::TargetOwnerHashMismatch,
            },
            Case {
                name: "historical image hash empty",
                alter: |candidate| candidate.historical_image_hash.clear(),
                expected: SupersededInsertRejection::MissingHistoricalImageHash,
            },
        ];

        for case in cases {
            let mut candidate = valid_candidate();
            (case.alter)(&mut candidate);
            assert_eq!(
                verify_superseded_insert(&candidate),
                Err(case.expected),
                "{}",
                case.name
            );
        }
    }
}
