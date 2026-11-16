use crate::errors::Error;
use crate::events::row_events::mysql_value::{Date, DateTime, Time};
use crate::extensions::read_bitmap_big_endian;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read};

/// Parses UTF-8 text without silently replacing invalid bytes.
/// Row events must use `parse_string_bytes`: these wire types also encode binary data.
pub fn parse_string(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<String, Error> {
    String::from_utf8(parse_string_bytes(cursor, metadata)?)
        .map_err(|error| Error::String(format!("Invalid UTF-8 string: {}", error)))
}

/// Reads the byte-length-prefixed payload shared by CHAR/BINARY and VARCHAR/VARBINARY.
pub fn parse_string_bytes(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<Vec<u8>, Error> {
    let length = if metadata < 256 {
        cursor.read_u8()? as usize
    } else {
        cursor.read_u16::<LittleEndian>()? as usize
    };
    let mut bytes = vec![0; length];
    cursor.read_exact(&mut bytes)?;
    Ok(bytes)
}

pub fn parse_bit(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<Vec<bool>, Error> {
    let length = (metadata >> 8) * 8 + (metadata & 0xFF);
    let mut bitmap = read_bitmap_big_endian(cursor, length as usize)?;
    bitmap.reverse();
    Ok(bitmap)
}

pub fn parse_blob(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<Vec<u8>, Error> {
    let length = cursor.read_uint::<LittleEndian>(metadata as usize)? as usize;
    let mut vec = vec![0; length];
    cursor.read_exact(&mut vec)?;
    Ok(vec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_string_rejects_invalid_utf8_without_replacement() {
        let mut cursor = Cursor::new([1, 0xff].as_slice());
        assert!(parse_string(&mut cursor, 255).is_err());
    }

    #[test]
    fn parse_string_keeps_utf8_text() {
        let mut cursor = Cursor::new([3, b'a', 0xc3, 0xa9].as_slice());
        assert_eq!(parse_string(&mut cursor, 255).unwrap(), "aé");
    }

    #[test]
    fn parse_string_bytes_preserves_payload_and_cursor_position() {
        for maximum_length in [255, 256] {
            for payload in [vec![], vec![0xff], vec![0xfe], vec![0, 0xff, 0]] {
                let mut encoded = if maximum_length < 256 {
                    vec![payload.len() as u8]
                } else {
                    (payload.len() as u16).to_le_bytes().to_vec()
                };
                encoded.extend_from_slice(&payload);
                let end = encoded.len() as u64;
                encoded.push(0x42);
                let mut cursor = Cursor::new(encoded.as_slice());
                assert_eq!(
                    parse_string_bytes(&mut cursor, maximum_length).unwrap(),
                    payload
                );
                assert_eq!(cursor.position(), end);
                assert_eq!(cursor.read_u8().unwrap(), 0x42);
            }
        }
    }

    #[test]
    fn parse_string_bytes_rejects_truncated_prefix_and_payload() {
        for (encoded, maximum_length) in [
            (vec![], 255),
            (vec![1], 256),
            (vec![2, 0xff], 255),
            (vec![2, 0, 0xff], 256),
        ] {
            let mut cursor = Cursor::new(encoded.as_slice());
            assert!(parse_string_bytes(&mut cursor, maximum_length).is_err());
        }
    }
}

pub fn parse_year(cursor: &mut Cursor<&[u8]>, _metadata: u16) -> Result<u16, Error> {
    Ok(1900 + cursor.read_u8()? as u16)
}

pub fn parse_date(cursor: &mut Cursor<&[u8]>, _metadata: u16) -> Result<Date, Error> {
    let value = cursor.read_u24::<LittleEndian>()?;

    // Bits 1-5 store the day. Bits 6-9 store the month. The remaining bits store the year.
    let day = value % (1 << 5);
    let month = (value >> 5) % (1 << 4);
    let year = value >> 9;

    Ok(Date {
        year: year as u16,
        month: month as u8,
        day: day as u8,
    })
}

pub fn parse_time(cursor: &mut Cursor<&[u8]>, _metadata: u16) -> Result<Time, Error> {
    let mut value = (cursor.read_i24::<LittleEndian>()? << 8) >> 8;

    if value < 0 {
        return Err(Error::String(
            "Parsing negative TIME values is not supported in this version".to_string(),
        ));
    }

    let second = value % 100;
    value = value / 100;
    let minute = value % 100;
    value = value / 100;
    let hour = value;
    Ok(Time {
        hour: hour as i16,
        minute: minute as u8,
        second: second as u8,
        micros: 0,
    })
}

pub fn parse_time2(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<Time, Error> {
    let value = cursor.read_u24::<BigEndian>()?;
    let micros = parse_fractional_part(cursor, metadata)?;

    let negative = ((value >> 23) & 1) == 0;
    if negative {
        // It looks like other similar clients don't parse TIME2 values properly
        // In negative time values both TIME and FSP are stored in reverse order
        // See https://github.com/mysql/mysql-server/blob/ea7d2e2d16ac03afdd9cb72a972a95981107bf51/sql/log_event.cc#L2022
        // See https://github.com/mysql/mysql-server/blob/ea7d2e2d16ac03afdd9cb72a972a95981107bf51/mysys/my_time.cc#L1784
        return Err(Error::String(
            "Parsing negative TIME values is not supported in this version".to_string(),
        ));
    }

    // 1 bit sign. 1 bit unused. 10 bits hour. 6 bits minute. 6 bits second.
    let hour = (value >> 12) % (1 << 10);
    let minute = (value >> 6) % (1 << 6);
    let second = value % (1 << 6);

    Ok(Time {
        hour: hour as i16,
        minute: minute as u8,
        second: second as u8,
        micros: micros as u32,
    })
}

pub fn parse_date_time(cursor: &mut Cursor<&[u8]>, _metadata: u16) -> Result<DateTime, Error> {
    let mut value = cursor.read_u64::<LittleEndian>()?;
    let second = value % 100;
    value = value / 100;
    let minute = value % 100;
    value = value / 100;
    let hour = value % 100;
    value = value / 100;
    let day = value % 100;
    value = value / 100;
    let month = value % 100;
    value = value / 100;
    let year = value;

    Ok(DateTime {
        year: year as u16,
        month: month as u8,
        day: day as u8,
        hour: hour as u8,
        minute: minute as u8,
        second: second as u8,
        micros: 0,
    })
}

pub fn parse_date_time2(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<DateTime, Error> {
    let value = cursor.read_uint::<BigEndian>(5)?;
    let micros = parse_fractional_part(cursor, metadata)?;

    // 1 bit sign(always true). 17 bits year*13+month. 5 bits day. 5 bits hour. 6 bits minute. 6 bits second.
    let year_month = (value >> 22) % (1 << 17);
    let year = year_month / 13;
    let month = year_month % 13;
    let day = (value >> 17) % (1 << 5);
    let hour = (value >> 12) % (1 << 5);
    let minute = (value >> 6) % (1 << 6);
    let second = value % (1 << 6);

    Ok(DateTime {
        year: year as u16,
        month: month as u8,
        day: day as u8,
        hour: hour as u8,
        minute: minute as u8,
        second: second as u8,
        micros: micros as u32,
    })
}

pub fn parse_timestamp(cursor: &mut Cursor<&[u8]>, _metadata: u16) -> Result<u64, Error> {
    let seconds = cursor.read_u32::<LittleEndian>()? as u64;
    Ok(seconds * MICROS_PER_SECOND)
}

pub fn parse_timestamp2(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<u64, Error> {
    let seconds = cursor.read_u32::<BigEndian>()? as u64;
    let micros = parse_fractional_part(cursor, metadata)?;
    Ok(seconds * MICROS_PER_SECOND + micros)
}

const MICROS_PER_SECOND: u64 = 1_000_000;

/// The fractional second of a `*2` temporal value in microseconds, as MySQL stores it:
/// `fsp` digits packed big-endian into `(fsp + 1) / 2` bytes.
fn parse_fractional_part(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<u64, Error> {
    let length = (metadata + 1) / 2;
    if length == 0 {
        return Ok(0);
    }

    let fraction = cursor.read_uint::<BigEndian>(length as usize)?;
    Ok(fraction * u64::pow(100, 3 - length as u32))
}

#[cfg(test)]
mod fractional_tests {
    use super::*;

    #[test]
    fn datetime2_and_timestamp2_keep_microseconds() {
        let mut cursor = Cursor::new(&[153, 186, 226, 200, 184, 9, 251, 241][..]);
        let value = parse_date_time2(&mut cursor, 6).expect("DATETIME(6)");
        assert_eq!(
            (
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second
            ),
            (2026, 9, 17, 12, 34, 56)
        );
        assert_eq!(value.micros, 654_321);

        let mut cursor = Cursor::new(&[106, 161, 249, 64, 9, 251, 241][..]);
        assert_eq!(
            parse_timestamp2(&mut cursor, 6).expect("TIMESTAMP(6)"),
            1_789_000_000_654_321
        );

        let mut cursor = Cursor::new(&[153, 186, 226, 200, 184, 25, 143][..]);
        let value = parse_date_time2(&mut cursor, 3).expect("DATETIME(3)");
        assert_eq!(value.micros, 654_300);
    }
}
