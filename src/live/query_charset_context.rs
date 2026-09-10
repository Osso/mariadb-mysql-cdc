//! MariaDB 11.8 QueryEvent status-variable decoding, without name resolution.
//!
//! Wire sources: MariaDB/server branch 11.8, sql/log_event.h (Q_* constants),
//! sql/log_event.cc (Query_log_event status switch), and
//! sql/charset_collations.h (Charset_collations_map_st::to_binary/from_binary).
//! https://github.com/MariaDB/server/blob/11.8/sql/log_event.cc
//! https://github.com/MariaDB/server/blob/11.8/sql/charset_collations.h

/// Numeric IDs only; the caller must resolve names using the source catalog.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryCharsetContext {
    pub charset: Option<QueryCharsetIds>,
    pub database_collation: Option<u16>,
    /// None means the status variable was absent, not an empty/default mapping.
    /// Each pair is (source charset's primary collation ID, selected collation ID).
    pub character_set_collations: Option<Vec<(u16, u16)>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryCharsetIds {
    pub client: u16,
    pub connection: u16,
    pub server: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeError {
    UnknownTag { tag: u8, offset: usize },
    Truncated { offset: usize, needed: usize },
    DuplicateTag { tag: u8, offset: usize },
    DuplicateCharset { id: u16 },
    InvalidCatalogTerminator { offset: usize },
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "invalid MariaDB QueryEvent charset context: {self:?}"
        )
    }
}

impl std::error::Error for DecodeError {}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], DecodeError> {
        let remaining = &self.bytes[self.offset..];
        let value = remaining.get(..length).ok_or(DecodeError::Truncated {
            offset: self.offset,
            needed: length,
        })?;
        self.offset += length;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn skip_string(&mut self) -> Result<(), DecodeError> {
        let length = usize::from(self.byte()?);
        self.take(length)?;
        Ok(())
    }

    fn read_mapping(&mut self) -> Result<Vec<(u16, u16)>, DecodeError> {
        let count = usize::from(self.byte()?);
        // Validate the whole payload before interpreting any entry.
        let mut entries = Cursor {
            bytes: self.take(count * 4)?,
            offset: 0,
        };
        let mut mapping = std::collections::BTreeMap::new();
        for _ in 0..count {
            let from = entries.u16()?;
            let to = entries.u16()?;
            if mapping.insert(from, to).is_some() {
                return Err(DecodeError::DuplicateCharset { id: from });
            }
        }
        Ok(mapping.into_iter().collect())
    }
}

/// Decode MariaDB 11.8 status variables, rejecting unknown or ambiguous input.
/// Q_DUMMY (255) terminates the section, as in the server's status switch.
/// No collation defaults are inferred when context is absent.
pub fn decode_query_charset_context(bytes: &[u8]) -> Result<QueryCharsetContext, DecodeError> {
    let mut cursor = Cursor { bytes, offset: 0 };
    let mut context = QueryCharsetContext::default();
    let mut seen = [false; 256];
    while cursor.offset < bytes.len() {
        let offset = cursor.offset;
        let tag = cursor.byte()?;
        if seen[usize::from(tag)] {
            return Err(DecodeError::DuplicateTag { tag, offset });
        }
        seen[usize::from(tag)] = true;
        match tag {
            4 => {
                context.charset = Some(QueryCharsetIds {
                    client: cursor.u16()?,
                    connection: cursor.u16()?,
                    server: cursor.u16()?,
                })
            }
            8 => context.database_collation = Some(cursor.u16()?),
            131 => context.character_set_collations = Some(cursor.read_mapping()?),
            255 => break,
            _ => skip_known_variable(&mut cursor, tag, offset)?,
        }
    }
    Ok(context)
}

fn skip_known_variable(cursor: &mut Cursor<'_>, tag: u8, offset: usize) -> Result<(), DecodeError> {
    match tag {
        0 | 3 | 10 => {
            cursor.take(4)?;
        }
        1 | 9 | 129 => {
            cursor.take(8)?;
        }
        7 => {
            cursor.take(2)?;
        }
        128 => {
            cursor.take(3)?;
        }
        5 | 6 => cursor.skip_string()?,
        2 => {
            cursor.skip_string()?;
            let offset = cursor.offset;
            if cursor.byte()? != 0 {
                return Err(DecodeError::InvalidCatalogTerminator { offset });
            }
        }
        11 => {
            cursor.skip_string()?;
            cursor.skip_string()?;
        }
        130 => {
            // Gtid_log_event::FL_COMMIT_ALTER_E1=4, FL_ROLLBACK_ALTER_E1=8.
            let flags = cursor.byte()?;
            if flags & (4 | 8) != 0 {
                cursor.take(8)?;
            }
        }
        _ => return Err(DecodeError::UnknownTag { tag, offset }),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_server_serialization_and_reordered_fields() {
        // to_binary: u8 count, then int2store(from), int2store(to) per entry.
        let mapping = [131, 2, 45, 0, 0x2b, 0x09, 8, 0, 8, 0];
        let charset = [4, 45, 0, 46, 0, 224, 0];
        let database = [8, 224, 0];
        let expected = QueryCharsetContext {
            charset: Some(QueryCharsetIds {
                client: 45,
                connection: 46,
                server: 224,
            }),
            database_collation: Some(224),
            character_set_collations: Some(vec![(8, 8), (45, 2347)]),
        };
        for bytes in [
            [mapping.as_slice(), &charset, &database].concat(),
            [database.as_slice(), &charset, &mapping].concat(),
        ] {
            assert_eq!(decode_query_charset_context(&bytes), Ok(expected.clone()));
        }
    }

    #[test]
    fn distinguishes_absent_and_empty_mapping() {
        assert_eq!(
            decode_query_charset_context(&[]),
            Ok(QueryCharsetContext::default())
        );
        assert_eq!(
            decode_query_charset_context(&[131, 0])
                .unwrap()
                .character_set_collations,
            Some(vec![])
        );
    }

    #[test]
    fn rejects_truncated_fields_and_unknown_tags() {
        let fields: &[&[u8]] = &[
            &[0, 0, 0, 0, 0],
            &[1, 0, 0, 0, 0, 0, 0, 0, 0],
            &[2, 1, b'x', 0],
            &[3, 1, 0, 1, 0],
            &[4, 45, 0, 46, 0, 224, 0],
            &[5, 1, b'Z'],
            &[6, 1, b'x'],
            &[7, 0, 0],
            &[8, 224, 0],
            &[9, 0, 0, 0, 0, 0, 0, 0, 0],
            &[10, 0, 0, 0, 0],
            &[11, 1, b'u', 1, b'h'],
            &[128, 1, 2, 3],
            &[129, 0, 0, 0, 0, 0, 0, 0, 0],
            &[130, 4, 0, 0, 0, 0, 0, 0, 0, 0],
            &[131, 1, 45, 0, 0x2b, 0x09],
        ];
        for field in fields {
            assert!(decode_query_charset_context(field).is_ok(), "{field:?}");
            for end in 1..field.len() {
                assert!(
                    decode_query_charset_context(&field[..end]).is_err(),
                    "{field:?} truncated at {end}"
                );
            }
        }
        for tag in [12, 18, 127, 132, 254] {
            assert_eq!(
                decode_query_charset_context(&[tag]),
                Err(DecodeError::UnknownTag { tag, offset: 0 })
            );
        }
    }

    #[test]
    fn skips_known_fields_without_losing_charset_context() {
        let bytes = [
            0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, b'x', 0, 3, 1, 0, 1, 0, 5, 1, b'Z', 6,
            0, 7, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 11, 1, b'u', 1, b'h', 128, 1, 2,
            3, 129, 0, 0, 0, 0, 0, 0, 0, 0, 130, 0, 8, 224, 0,
        ];
        assert_eq!(
            decode_query_charset_context(&bytes)
                .unwrap()
                .database_collation,
            Some(224)
        );
        // Both COMMIT_ALTER and ROLLBACK_ALTER flags include an eight-byte sequence.
        for flags in [4, 8, 12] {
            let mut bytes = vec![130, flags];
            bytes.extend([0; 8]);
            bytes.extend([8, 224, 0]);
            assert_eq!(
                decode_query_charset_context(&bytes)
                    .unwrap()
                    .database_collation,
                Some(224)
            );
        }
    }

    #[test]
    fn rejects_ambiguous_context_and_malformed_catalog() {
        for bytes in [
            vec![8, 1, 0, 8, 2, 0],
            vec![131, 0, 131, 0],
            vec![131, 2, 45, 0, 1, 0, 45, 0, 2, 0],
            vec![2, 1, b'x', 1],
        ] {
            assert!(decode_query_charset_context(&bytes).is_err());
        }
        // Q_DUMMY is explicitly terminal padding in MariaDB, not another tag.
        assert_eq!(
            decode_query_charset_context(&[8, 224, 0, 255, 18, 99])
                .unwrap()
                .database_collation,
            Some(224)
        );
    }
}
