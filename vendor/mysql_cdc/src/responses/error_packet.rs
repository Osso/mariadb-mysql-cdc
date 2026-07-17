use byteorder::{LittleEndian, ReadBytesExt};
use std::fmt;
use std::io::{self, Cursor, Read};

/// ERR_Packet indicates that an error occured.
/// <a href="https://mariadb.com/kb/en/library/err_packet/">See more</a>
#[derive(Debug)]
pub struct ErrorPacket {
    pub error_code: u16,
    pub error_message: String,
    pub sql_state: Option<String>,
}

impl fmt::Display for ErrorPacket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MySQL error {}", self.error_code)?;
        if let Some(sql_state) = &self.sql_state {
            write!(formatter, " (SQLSTATE {sql_state})")?;
        }
        write!(formatter, ": {}", self.error_message)
    }
}

impl ErrorPacket {
    pub fn parse(packet: &[u8]) -> Result<Self, io::Error> {
        let mut cursor = Cursor::new(packet);

        let error_code = cursor.read_u16::<LittleEndian>()?;

        let mut error_message = String::new();
        cursor.read_to_string(&mut error_message)?;

        let mut sql_state = None;
        if error_message.starts_with('#') {
            sql_state = Some(error_message.chars().skip(1).take(5).collect());
            error_message = error_message.chars().skip(6).collect();
        }

        Ok(Self {
            error_code,
            error_message,
            sql_state,
        })
    }
}
