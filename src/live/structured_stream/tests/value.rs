use super::*;

const SMALL_INT_BIT_PATTERN: u16 = 64_872;

#[test]
fn formats_mysql_cdc_values_like_snapshot_text_rows() {
    assert_eq!(format_timestamp(1_782_075_535_000), "2026-06-21 20:58:55");
    assert_eq!(
        convert_mysql_value(&Some(MySqlValue::Blob(b"hello".to_vec())), false),
        Value::Bytes(b"hello".to_vec())
    );
    assert_eq!(
        convert_mysql_value(&Some(MySqlValue::Bit(vec![true])), false),
        Value::Bytes(vec![1])
    );
    assert_eq!(
        convert_mysql_value(
            &Some(MySqlValue::Bit(vec![
                true, false, true, false, true, false, true, false, true
            ])),
            false,
        ),
        Value::Bytes(vec![1, 85])
    );
    assert_eq!(
        convert_mysql_value(
            &Some(MySqlValue::Time(Time {
                hour: 26,
                minute: 3,
                second: 4,
                millis: 0,
            })),
            false,
        ),
        Value::Bytes(b"26:03:04".to_vec())
    );
    assert_eq!(
        convert_mysql_value(&Some(MySqlValue::SmallInt(SMALL_INT_BIT_PATTERN)), true),
        Value::Int(-664)
    );
    assert_eq!(
        convert_mysql_value(&Some(MySqlValue::SmallInt(840)), true),
        Value::Int(840)
    );
    assert_eq!(
        convert_mysql_value(&Some(MySqlValue::SmallInt(SMALL_INT_BIT_PATTERN)), false),
        Value::UInt(64872)
    );
}

#[test]
fn converts_every_mysql_value_variant_without_enum_metadata() {
    let cases = vec![
        (None, false, Value::NULL),
        (Some(MySqlValue::TinyInt(0xfb)), true, Value::Int(-5)),
        (Some(MySqlValue::TinyInt(0xfb)), false, Value::UInt(251)),
        (
            Some(MySqlValue::SmallInt(SMALL_INT_BIT_PATTERN)),
            true,
            Value::Int(-664),
        ),
        (
            Some(MySqlValue::SmallInt(SMALL_INT_BIT_PATTERN)),
            false,
            Value::UInt(64872),
        ),
        (
            Some(MySqlValue::MediumInt(0x80_0000)),
            true,
            Value::Int(-8_388_608),
        ),
        (
            Some(MySqlValue::MediumInt(0x80_0000)),
            false,
            Value::UInt(8_388_608),
        ),
        (Some(MySqlValue::Int(u32::MAX)), true, Value::Int(-1)),
        (
            Some(MySqlValue::Int(u32::MAX)),
            false,
            Value::UInt(u64::from(u32::MAX)),
        ),
        (Some(MySqlValue::BigInt(u64::MAX)), true, Value::Int(-1)),
        (
            Some(MySqlValue::BigInt(u64::MAX)),
            false,
            Value::UInt(u64::MAX),
        ),
        (Some(MySqlValue::Float(1.25)), false, Value::Float(1.25)),
        (Some(MySqlValue::Double(-2.5)), false, Value::Double(-2.5)),
        (
            Some(MySqlValue::Decimal("12.3400".to_string())),
            false,
            Value::Bytes(b"12.3400".to_vec()),
        ),
        (
            Some(MySqlValue::String("hello".to_string())),
            false,
            Value::Bytes(b"hello".to_vec()),
        ),
        (
            Some(MySqlValue::Bit(vec![true, false, true])),
            false,
            Value::Bytes(vec![5]),
        ),
        (Some(MySqlValue::Enum(2)), false, Value::UInt(2)),
        (Some(MySqlValue::Set(5)), false, Value::UInt(5)),
        (
            Some(MySqlValue::Blob(vec![0, 255])),
            false,
            Value::Bytes(vec![0, 255]),
        ),
        (Some(MySqlValue::Year(2026)), false, Value::UInt(2026)),
        (
            Some(MySqlValue::Date(Date {
                year: 2026,
                month: 7,
                day: 16,
            })),
            false,
            Value::Bytes(b"2026-07-16".to_vec()),
        ),
        (
            Some(MySqlValue::Time(Time {
                hour: 3,
                minute: 4,
                second: 5,
                millis: 600,
            })),
            false,
            Value::Bytes(b"03:04:05.600".to_vec()),
        ),
        (
            Some(MySqlValue::DateTime(DateTime {
                year: 2026,
                month: 7,
                day: 16,
                hour: 3,
                minute: 4,
                second: 5,
                millis: 600,
            })),
            false,
            Value::Bytes(b"2026-07-16 03:04:05.600".to_vec()),
        ),
        (
            Some(MySqlValue::Timestamp(1_782_075_535_000)),
            false,
            Value::Bytes(b"2026-06-21 20:58:55".to_vec()),
        ),
    ];

    for (value, signed, expected) in cases {
        assert_eq!(
            mysql_value_to_target_value(&value, signed, None).expect("convert mysql value"),
            expected
        );
    }
}

#[test]
fn duplicate_insert_compares_binlog_set_bitmask_to_target_text_semantically() {
    let source_value = mysql_value_to_target_value(&Some(MySqlValue::Set(0b101)), false, None)
        .expect("decode source SET value");
    let source_values = vec![source_value];
    let target_values = vec![Value::Bytes(b"red,blue".to_vec())];
    let set_columns = vec![Some(vec![
        "red".to_string(),
        "green".to_string(),
        "blue".to_string(),
    ])];
    let conflict = crate::target::DuplicateConflict {
        error_code: 1062,
        error_text: "duplicate".to_string(),
        duplicate_index: Some("PRIMARY".to_string()),
    };

    assert_eq!(
        crate::target::duplicate_insert_outcome(
            conflict.clone(),
            Some(&target_values),
            &source_values,
            &set_columns,
        ),
        crate::target::TargetExecutionOutcome::DuplicateIgnored(conflict.clone())
    );

    let divergent_target_values = vec![Value::Bytes(b"red,green".to_vec())];
    assert_eq!(
        crate::target::duplicate_insert_outcome(
            conflict,
            Some(&divergent_target_values),
            &source_values,
            &set_columns,
        ),
        crate::target::TargetExecutionOutcome::ConstraintConflict(
            crate::target::DuplicateConflict {
                error_code: 1062,
                error_text: "duplicate".to_string(),
                duplicate_index: Some("PRIMARY".to_string()),
            },
        )
    );
}

#[test]
fn converts_enum_ordinals_to_metadata_strings() {
    let enum_values = vec!["1".to_string(), "2".to_string(), "14".to_string()];

    assert_eq!(
        mysql_value_to_target_value(&Some(MySqlValue::Enum(3)), false, Some(&enum_values))
            .expect("enum value"),
        Value::Bytes(b"14".to_vec())
    );
}

#[test]
fn converts_enum_zero_ordinal_to_mysql_empty_value() {
    let enum_values = vec!["1".to_string()];

    assert_eq!(
        mysql_value_to_target_value(&Some(MySqlValue::Enum(0)), false, Some(&enum_values))
            .expect("enum zero value"),
        Value::Bytes(Vec::new())
    );
}

#[test]
fn rejects_enum_ordinals_outside_metadata() {
    let enum_values = vec!["1".to_string()];
    let error = mysql_value_to_target_value(&Some(MySqlValue::Enum(2)), false, Some(&enum_values))
        .expect_err("enum ordinal error")
        .to_string();

    assert!(error.contains("enum ordinal 2 exceeds 1 metadata values"));
}
