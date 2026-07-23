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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SupersededInsertProof {
    pub(crate) source_snapshot: BinlogCoordinate,
    pub(crate) historical_image_hash: String,
    pub(crate) source_primary_hash: String,
    pub(crate) source_owner_hash: String,
    pub(crate) target_primary_hash: String,
    pub(crate) target_owner_hash: String,
}

impl SupersededInsertProof {
    pub(crate) fn resolution_evidence(&self) -> String {
        format!(
            "verified superseded historical insert at source snapshot {}:{}; historical image hash {}; source primary row hash {}; source unique owner row hash {}; target primary row hash {}; target unique owner row hash {}",
            self.source_snapshot.file,
            self.source_snapshot.position,
            self.historical_image_hash,
            self.source_primary_hash,
            self.source_owner_hash,
            self.target_primary_hash,
            self.target_owner_hash,
        )
    }
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
    if candidate.schema != "globalcomix" || candidate.table != "users" {
        return Err(SupersededInsertRejection::WrongScope);
    }
    if candidate.operation != crate::conflict_repair::ConflictOperation::Insert {
        return Err(SupersededInsertRejection::WrongOperation);
    }
    if candidate.duplicate_index != "users.name" {
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
