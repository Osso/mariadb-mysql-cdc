use super::*;

fn table() -> SyncTable {
    SyncTable {
        name: "child`rows".into(),
        primary_key: vec!["state".into(), "id".into()],
        primary_key_ordering: vec![
            SyncPrimaryKeyOrdering::Enum(vec!["z".into(), "a".into()]),
            SyncPrimaryKeyOrdering::Native,
        ],
        columns: ["state", "id", "flags", "payload", "parent", "optional"]
            .map(String::from)
            .to_vec(),
        bit_columns: vec!["flags".into()],
        enum_columns: [("state".into(), vec!["z".into(), "a".into()])].into(),
        mediumblob_columns: vec!["payload".into()],
    }
}

fn request() -> SyncChunkReadRequest {
    SyncChunkReadRequest {
        start_after: None,
        end_at: None,
        limit: 17,
    }
}

#[test]
fn relation_sql_preserves_full_typed_relation_and_projection() {
    let columns = ["parent", "flags", "state", "payload", "optional"].map(String::from);
    let values = [
        Some("x' OR 1=1 --".into()),
        Some("18446744073709551615".into()),
        Some("2".into()),
        Some("00fF80".into()),
        None,
    ];
    let statement =
        build_related_rows_select_statement(&table(), &columns, &values, &request()).unwrap();
    assert_eq!(
        statement.sql,
        "SELECT CAST(`state` AS UNSIGNED) AS `state`, `id`, CAST(`flags` AS UNSIGNED) AS `flags`, HEX(`payload`) AS `payload`, `parent`, `optional` FROM `child``rows` WHERE `parent` <=> ? AND `flags` <=> ? AND `state` <=> ? AND `payload` <=> ? AND `optional` <=> ? ORDER BY FIELD(`state`, 'z', 'a'), `id` LIMIT 17"
    );
    assert_eq!(
        statement.params,
        vec![
            Value::Bytes(b"x' OR 1=1 --".to_vec()),
            Value::UInt(u64::MAX),
            Value::UInt(2),
            Value::Bytes(vec![0, 255, 128]),
            Value::NULL
        ]
    );
}

#[test]
fn relation_sql_keeps_composite_bounds_grouped() {
    let request = SyncChunkReadRequest {
        start_after: Some(vec!["z".into(), "10".into()]),
        end_at: Some(vec!["a".into(), "20".into()]),
        limit: 3,
    };
    let statement = build_related_rows_select_statement(
        &table(),
        &["parent".into()],
        &[Some("42".into())],
        &request,
    )
    .unwrap();
    assert_eq!(
        statement.sql,
        "SELECT CAST(`state` AS UNSIGNED) AS `state`, `id`, CAST(`flags` AS UNSIGNED) AS `flags`, HEX(`payload`) AS `payload`, `parent`, `optional` FROM `child``rows` WHERE `parent` <=> ? AND ((FIELD(`state`, 'z', 'a') > FIELD('z', 'z', 'a')) OR (`state` = 'z' AND `id` > '10')) AND NOT ((FIELD(`state`, 'z', 'a') > FIELD('a', 'z', 'a')) OR (`state` = 'a' AND `id` > '20')) ORDER BY FIELD(`state`, 'z', 'a'), `id` LIMIT 3"
    );
    assert_eq!(statement.params, vec![Value::Bytes(b"42".to_vec())]);
}

#[test]
fn relation_sql_rejects_invalid_relation_shape() {
    for (columns, values) in [
        (vec![], vec![]),
        (vec!["parent".into()], vec![]),
        (vec!["missing".into()], vec![None]),
        (vec!["parent".into(), "parent".into()], vec![None, None]),
    ] {
        assert!(
            build_related_rows_select_statement(&table(), &columns, &values, &request()).is_err()
        );
    }
}

#[test]
fn relation_sql_rejects_zero_limit_and_wrong_cursor_widths() {
    let mut requests = vec![SyncChunkReadRequest {
        limit: 0,
        ..request()
    }];
    for cursor in [
        vec![],
        vec!["z".into()],
        vec!["z".into(), "1".into(), "2".into()],
    ] {
        requests.push(SyncChunkReadRequest {
            start_after: Some(cursor.clone()),
            ..request()
        });
        requests.push(SyncChunkReadRequest {
            end_at: Some(cursor),
            ..request()
        });
    }
    for request in requests {
        assert!(
            build_related_rows_select_statement(&table(), &["parent".into()], &[None], &request)
                .is_err()
        );
    }
}

#[test]
fn relation_sql_rejects_invalid_typed_values() {
    for (column, value) in [
        ("flags", "-1"),
        ("state", "3"),
        ("state", "a"),
        ("payload", "abc"),
        ("payload", "zz"),
    ] {
        assert!(
            build_related_rows_select_statement(
                &table(),
                &[column.into()],
                &[Some(value.into())],
                &request()
            )
            .is_err()
        );
    }
}
