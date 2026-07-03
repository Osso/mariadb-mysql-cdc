use std::io::{Cursor, Read, Seek, SeekFrom};

use crate::errors::Error;

/// Represents query that caused row events.
/// See <a href="https://dev.mysql.com/doc/internals/en/rows-query-event.html">MySQL docs</a>
/// See <a href="https://mariadb.com/kb/en/annotate_rows_event/">MariaDB docs</a>
#[derive(Debug)]
pub struct RowsQueryEvent {
    /// Gets SQL statement
    pub query: String,
}

impl RowsQueryEvent {
    /// Supports MySQL 5.6+.
    pub fn parse_mysql(cursor: &mut Cursor<&[u8]>) -> Result<Self, Error> {
        cursor.seek(SeekFrom::Current(1))?;

        Ok(Self {
            query: read_remaining_lossy(cursor)?,
        })
    }

    /// Supports MariaDB 5.3+.
    pub fn parse_mariadb(cursor: &mut Cursor<&[u8]>) -> Result<Self, Error> {
        Ok(Self {
            query: read_remaining_lossy(cursor)?,
        })
    }
}

fn read_remaining_lossy(cursor: &mut Cursor<&[u8]>) -> Result<String, Error> {
    let mut bytes = Vec::new();
    cursor.read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_mariadb_annotation_preserves_non_utf8_bytes_lossily() {
        let bytes = [162, 115, 146, 171, 116, 13, 97, 107, 102, 172, 93, 36, 187, 4, 11, 70, 81, 244, 255, 170, 85, 181, 120, 171, 186, 118, 3, 196, 183, 63, 234, 164];
        let mut cursor = Cursor::new(bytes.as_slice());

        let event = RowsQueryEvent::parse_mariadb(&mut cursor).expect("lossy annotation parse");

        assert!(event.query.contains('�'));
        assert!(event.query.contains("akf"));
    }
}
