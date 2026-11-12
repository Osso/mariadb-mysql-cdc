use super::*;
use std::collections::HashMap;

const SMALL_INT_BIT_PATTERN: u16 = 64_872;

#[test]
fn formats_mysql_cdc_values_like_snapshot_text_rows() {
    assert_eq!(
        format_timestamp(1_782_075_535_000_000),
        "2026-06-21 20:58:55"
    );
    assert_eq!(
        format_timestamp(1_782_075_535_000_001),
        "2026-06-21 20:58:55.000001"
    );
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
                micros: 0,
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
                micros: 600_000,
            })),
            false,
            Value::Bytes(b"03:04:05.600000".to_vec()),
        ),
        (
            Some(MySqlValue::DateTime(DateTime {
                year: 2026,
                month: 7,
                day: 16,
                hour: 3,
                minute: 4,
                second: 5,
                micros: 654_321,
            })),
            false,
            Value::Bytes(b"2026-07-16 03:04:05.654321".to_vec()),
        ),
        (
            Some(MySqlValue::Timestamp(1_782_075_535_000_000)),
            false,
            Value::Bytes(b"2026-06-21 20:58:55".to_vec()),
        ),
        (
            Some(MySqlValue::Timestamp(1_782_075_535_123_456)),
            false,
            Value::Bytes(b"2026-06-21 20:58:55.123456".to_vec()),
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
fn converts_enum_ordinals_to_metadata_strings() {
    let enum_values = vec!["1".to_string(), "2".to_string(), "14".to_string()];

    assert_eq!(
        mysql_value_to_target_value(&Some(MySqlValue::Enum(3)), false, Some(&enum_values))
            .expect("enum value"),
        Value::Bytes(b"14".to_vec())
    );
}

#[test]
fn experiments_create_enum_ordinals_preserve_declared_status_values() {
    let labels = ["Draft", "running", "ended", "archived"]
        .map(str::to_string)
        .to_vec();
    for (ordinal, expected) in [(1, "Draft"), (2, "running"), (3, "ended"), (4, "archived")] {
        assert_eq!(
            mysql_value_to_target_value(&Some(MySqlValue::Enum(ordinal)), false, Some(&labels))
                .expect("declared experiment status"),
            Value::Bytes(expected.as_bytes().to_vec())
        );
    }
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

#[test]
fn string_family_row_payloads_reach_target_without_byte_replacement() {
    let payloads = [
        Vec::new(),
        vec![0xff],
        vec![0xfe],
        vec![0, 0xff, 0],
        b"ascii\0padding\0".to_vec(),
        "café 日本語".as_bytes().to_vec(),
    ];
    // STRING metadata encodes the actual type as well as the maximum length.
    for (column_type, metadata, wide_length) in [
        (254, 0xfeff, false),
        (254, 0xee00, true),
        (15, 255, false),
        (15, 256, true),
        (253, 255, false),
        (253, 256, true),
    ] {
        let mut table = accounts_table_map_event(2);
        table.column_types = vec![column_type, 1];
        table.column_metadata = vec![metadata, 0];
        let tables = HashMap::from([(table.table_id, table)]);
        for payload in &payloads {
            let mut encoded = vec![0]; // Both columns non-null.
            if wide_length {
                encoded.extend_from_slice(&(payload.len() as u16).to_le_bytes());
            } else {
                encoded.push(payload.len() as u8);
            }
            encoded.extend_from_slice(payload);
            encoded.push(42); // A following column catches length/cursor errors.
            let row = parse_binary_test_row(&tables, &encoded);
            assert_eq!(
                convert_mysql_value(&row.cells[0], false),
                Value::Bytes(payload.clone()),
                "wire type {column_type}, metadata {metadata}"
            );
            if std::str::from_utf8(payload).is_ok() {
                assert!(matches!(&row.cells[0], Some(MySqlValue::String(_))));
            } else {
                assert!(matches!(&row.cells[0], Some(MySqlValue::Blob(_))));
            }
            assert_eq!(convert_mysql_value(&row.cells[1], false), Value::UInt(42));
        }

        // NULL consumes no string length or payload; an empty value above is not NULL.
        let row = parse_binary_test_row(&tables, &[1, 42]);
        assert_eq!(convert_mysql_value(&row.cells[0], false), Value::NULL);
        assert_eq!(convert_mysql_value(&row.cells[1], false), Value::UInt(42));
    }
}

#[test]
fn long_varbinary_payload_reaches_target_unchanged() {
    let payload: Vec<u8> = (0..=255).collect();
    let mut table = accounts_table_map_event(1);
    table.column_types = vec![15];
    table.column_metadata = vec![256];
    let mut encoded = vec![0, 0, 1]; // Non-null, 256-byte little-endian length.
    encoded.extend_from_slice(&payload);
    let tables = HashMap::from([(table.table_id, table)]);
    let row = parse_binary_test_row(&tables, &encoded);
    assert_eq!(
        convert_mysql_value(&row.cells[0], false),
        Value::Bytes(payload)
    );
}

// Exercise the public row-event parser without exposing vendored internals.
fn parse_binary_test_row(
    tables: &HashMap<u64, MysqlCdcTableMapEvent>,
    row_bytes: &[u8],
) -> mysql_cdc::events::row_events::row_data::RowData {
    use mysql_cdc::events::row_events::write_rows_event::WriteRowsEvent;
    use std::io::Cursor;

    let table = tables.values().next().unwrap();
    let columns = table.column_types.len();
    let mut encoded = table.table_id.to_le_bytes()[..6].to_vec();
    encoded.extend_from_slice(&[0, 0]); // V1 flags.
    encoded.push(columns as u8);
    encoded.push((1_u8 << columns) - 1); // All columns present.
    encoded.extend_from_slice(row_bytes);
    let mut cursor = Cursor::new(encoded.as_slice());
    let mut event = WriteRowsEvent::parse(&mut cursor, tables, 1).unwrap();
    assert_eq!(cursor.position(), encoded.len() as u64);
    assert_eq!(event.rows.len(), 1);
    event.rows.remove(0)
}
