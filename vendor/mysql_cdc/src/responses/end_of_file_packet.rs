use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{self, Cursor};

/// EOF packet marks the end of a resultset.
/// <a href="https://mariadb.com/kb/en/library/eof_packet/">See more</a>
#[derive(Debug)]
pub struct EndOfFilePacket;

impl EndOfFilePacket {
    pub fn parse(packet: &[u8]) -> Result<Self, io::Error> {
        let mut cursor = Cursor::new(packet);
        let _warning_count = cursor.read_u16::<LittleEndian>()?;
        let _server_status = cursor.read_u16::<LittleEndian>()?;
        Ok(Self)
    }
}
