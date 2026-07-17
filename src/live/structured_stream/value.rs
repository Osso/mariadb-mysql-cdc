use super::*;

pub(super) fn mysql_value_to_target_value(
    value: &Option<MySqlValue>,
    signed: bool,
    enum_values: Option<&Vec<String>>,
) -> Result<Value, ApplyBinlogError> {
    let Some(value) = value else {
        return Ok(Value::NULL);
    };

    if let Some(target_value) = convert_mysql_integer_value(value, signed) {
        return Ok(target_value);
    }
    if let Some(target_value) = convert_mysql_scalar_value(value, enum_values)? {
        return Ok(target_value);
    }
    if let Some(target_value) = convert_mysql_temporal_value(value) {
        return Ok(target_value);
    }

    unreachable!("all MySqlValue variants are covered by conversion helpers")
}

pub(super) fn convert_mysql_integer_value(value: &MySqlValue, signed: bool) -> Option<Value> {
    let target_value = match value {
        MySqlValue::TinyInt(value) if signed => Value::Int(i64::from(*value as i8)),
        MySqlValue::TinyInt(value) => Value::UInt(u64::from(*value)),
        MySqlValue::SmallInt(value) if signed => Value::Int(i64::from(*value as i16)),
        MySqlValue::SmallInt(value) => Value::UInt(u64::from(*value)),
        MySqlValue::MediumInt(value) if signed => Value::Int(sign_extend_u24(*value)),
        MySqlValue::MediumInt(value) => Value::UInt(u64::from(*value)),
        MySqlValue::Int(value) if signed => Value::Int(i64::from(*value as i32)),
        MySqlValue::Int(value) => Value::UInt(u64::from(*value)),
        MySqlValue::BigInt(value) if signed => Value::Int(*value as i64),
        MySqlValue::BigInt(value) => Value::UInt(*value),
        _ => return None,
    };
    Some(target_value)
}

pub(super) fn convert_mysql_scalar_value(
    value: &MySqlValue,
    enum_values: Option<&Vec<String>>,
) -> Result<Option<Value>, ApplyBinlogError> {
    let target_value = match value {
        MySqlValue::Float(value) => Value::Float(*value),
        MySqlValue::Double(value) => Value::Double(*value),
        MySqlValue::Decimal(value) => bytes_value(value.as_str()),
        MySqlValue::String(value) => bytes_value(value.as_str()),
        MySqlValue::Bit(value) => Value::Bytes(pack_bit_value(value)),
        MySqlValue::Enum(value) => {
            return enum_value_to_target_value(*value, enum_values).map(Some);
        }
        MySqlValue::Set(value) => Value::UInt(*value),
        MySqlValue::Blob(value) => Value::Bytes(value.clone()),
        MySqlValue::Year(value) => Value::UInt(u64::from(*value)),
        _ => return Ok(None),
    };
    Ok(Some(target_value))
}

pub(super) fn convert_mysql_temporal_value(value: &MySqlValue) -> Option<Value> {
    let target_value = match value {
        MySqlValue::Date(value) => bytes_value(format_date(value)),
        MySqlValue::Time(value) => bytes_value(format_time(value)),
        MySqlValue::DateTime(value) => bytes_value(format_datetime(value)),
        MySqlValue::Timestamp(value) => bytes_value(format_timestamp(*value)),
        _ => return None,
    };
    Some(target_value)
}

pub(super) fn enum_value_to_target_value(
    ordinal: u32,
    enum_values: Option<&Vec<String>>,
) -> Result<Value, ApplyBinlogError> {
    let Some(enum_values) = enum_values else {
        return Ok(Value::UInt(u64::from(ordinal)));
    };
    if ordinal == 0 {
        return Ok(bytes_value(""));
    }
    let value_index = usize::try_from(ordinal)
        .map_err(|_| mapping_error(format!("enum ordinal {ordinal} cannot fit usize")))?
        - 1;
    let value = enum_values.get(value_index).ok_or_else(|| {
        mapping_error(format!(
            "enum ordinal {ordinal} exceeds {} metadata values",
            enum_values.len()
        ))
    })?;
    Ok(bytes_value(value.as_str()))
}

pub(super) fn bytes_value(value: impl Into<Vec<u8>>) -> Value {
    Value::Bytes(value.into())
}

pub(super) fn sign_extend_u24(value: u32) -> i64 {
    if value & 0x80_0000 == 0 {
        i64::from(value)
    } else {
        i64::from(value) - 0x1_000000
    }
}

pub(super) fn pack_bit_value(bits: &[bool]) -> Vec<u8> {
    let numeric_value = bits
        .iter()
        .fold(0_u64, |value, bit| (value << 1) | u64::from(*bit));
    let byte_count = bits.len().max(1).div_ceil(8);

    (0..byte_count)
        .map(|index| {
            let shift = (byte_count - index - 1) * 8;
            ((numeric_value >> shift) & 0xff) as u8
        })
        .collect()
}

pub(super) fn format_date(value: &Date) -> String {
    format!("{:04}-{:02}-{:02}", value.year, value.month, value.day)
}

pub(super) fn format_time(value: &Time) -> String {
    let base = format!("{:02}:{:02}:{:02}", value.hour, value.minute, value.second);
    append_millis(base, value.millis)
}

pub(super) fn format_datetime(value: &DateTime) -> String {
    let base = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        value.year, value.month, value.day, value.hour, value.minute, value.second
    );
    append_millis(base, value.millis)
}

pub(super) fn format_timestamp(millis: u64) -> String {
    let seconds = (millis / MILLIS_PER_SECOND) as i64;
    let (date, time) = split_unix_seconds(seconds);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        date.year, date.month, date.day, time.hour, time.minute, time.second
    )
}

pub(super) fn append_millis(base: String, millis: u32) -> String {
    if millis == 0 {
        base
    } else {
        format!("{base}.{millis:03}")
    }
}

pub(super) fn split_unix_seconds(seconds: i64) -> (DateParts, TimeParts) {
    let days = seconds.div_euclid(SECONDS_PER_DAY);
    let seconds_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    (civil_from_days(days), time_from_seconds(seconds_of_day))
}

pub(super) fn civil_from_days(days_since_epoch: i64) -> DateParts {
    let days = days_since_epoch + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_phase = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_phase + 2) / 5 + 1;
    let month = month_phase + if month_phase < 10 { 3 } else { -9 };
    year += if month <= 2 { 1 } else { 0 };

    DateParts {
        year,
        month: month as u8,
        day: day as u8,
    }
}

pub(super) fn time_from_seconds(seconds_of_day: i64) -> TimeParts {
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    TimeParts {
        hour: hour as u8,
        minute: minute as u8,
        second: second as u8,
    }
}
