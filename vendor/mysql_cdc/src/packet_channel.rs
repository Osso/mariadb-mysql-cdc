use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use openssl::ssl::{SslConnector, SslMethod, SslStream, SslVerifyMode};
use std::io::{self, Read, Write};
use std::net::TcpStream;

use crate::constants::{PACKET_HEADER_SIZE, TIMEOUT_LATENCY_DELTA};
use crate::errors::Error;
use crate::replica_options::ReplicaOptions;
use crate::ssl_mode::SslMode;

enum PacketStream {
    Plain(TcpStream),
    Tls(SslStream<TcpStream>),
}

impl Read for PacketStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buffer),
            Self::Tls(stream) => stream.read(buffer),
        }
    }
}

impl Write for PacketStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buffer),
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

pub struct PacketChannel {
    stream: Option<PacketStream>,
    hostname: String,
    ssl_mode: SslMode,
    ssl_ca_file: Option<String>,
}

impl PacketChannel {
    pub fn new(options: &ReplicaOptions) -> Result<Self, io::Error> {
        let address = format!("{}:{}", options.hostname, options.port);
        let stream = TcpStream::connect(address)?;
        let read_timeout = options.heartbeat_interval + TIMEOUT_LATENCY_DELTA;
        stream.set_read_timeout(Some(read_timeout))?;
        Ok(Self {
            stream: Some(PacketStream::Plain(stream)),
            hostname: options.hostname.clone(),
            ssl_mode: options.ssl_mode,
            ssl_ca_file: options.ssl_ca_file.clone(),
        })
    }

    pub fn read_packet(&mut self) -> Result<(Vec<u8>, u8), io::Error> {
        let mut header_buffer = [0; PACKET_HEADER_SIZE];
        let stream = self.stream_mut()?;

        stream.read_exact(&mut header_buffer)?;
        let packet_size = (&header_buffer[0..3]).read_u24::<LittleEndian>()?;
        let seq_num = header_buffer[3];

        let mut packet = vec![0; packet_size as usize];
        stream.read_exact(&mut packet)?;

        Ok((packet, seq_num))
    }

    pub fn write_packet(&mut self, packet: &[u8], seq_num: u8) -> Result<(), io::Error> {
        let stream = self.stream_mut()?;
        let packet_len = packet.len() as u32;
        stream.write_u24::<LittleEndian>(packet_len)?;
        stream.write_u8(seq_num)?;
        stream.write_all(packet)?;
        stream.flush()?;
        Ok(())
    }

    pub fn upgrade_to_ssl(&mut self) -> Result<(), Error> {
        let stream = self
            .stream
            .take()
            .ok_or_else(|| Error::String("MySQL packet stream is unavailable".to_string()))?;
        let PacketStream::Plain(stream) = stream else {
            return Err(Error::String(
                "MySQL packet stream is already using TLS".to_string(),
            ));
        };
        let connector = build_ssl_connector(self.ssl_mode, self.ssl_ca_file.as_deref())?;
        let mut configuration = connector.configure()?;
        configuration.set_verify_hostname(self.ssl_mode == SslMode::RequireVerifyFull);
        let tls_stream = configuration
            .connect(&self.hostname, stream)
            .map_err(|error| {
                Error::String(format!(
                    "TLS handshake with MySQL source `{}` failed in {:?} mode: {error}",
                    self.hostname, self.ssl_mode
                ))
            })?;
        self.stream = Some(PacketStream::Tls(tls_stream));
        Ok(())
    }

    fn stream_mut(&mut self) -> io::Result<&mut PacketStream> {
        self.stream.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "MySQL packet stream is unavailable",
            )
        })
    }
}

fn build_ssl_connector(mode: SslMode, ca_file: Option<&str>) -> Result<SslConnector, Error> {
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    let verify_mode = ssl_verify_mode(mode);
    builder.set_verify(verify_mode);
    if verify_mode == SslVerifyMode::PEER {
        if let Some(ca_file) = ca_file {
            builder.set_ca_file(ca_file)?;
        } else {
            builder.set_default_verify_paths()?;
        }
    }
    Ok(builder.build())
}

fn ssl_verify_mode(mode: SslMode) -> SslVerifyMode {
    match mode {
        SslMode::RequireVerifyCa | SslMode::RequireVerifyFull => SslVerifyMode::PEER,
        SslMode::Disabled | SslMode::IfAvailable | SslMode::Require => SslVerifyMode::NONE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verified_ssl_modes_require_peer_certificate_verification() {
        assert_eq!(
            ssl_verify_mode(SslMode::RequireVerifyCa),
            SslVerifyMode::PEER
        );
        assert_eq!(
            ssl_verify_mode(SslMode::RequireVerifyFull),
            SslVerifyMode::PEER
        );
    }

    #[test]
    fn encryption_only_ssl_modes_do_not_require_peer_verification() {
        assert_eq!(ssl_verify_mode(SslMode::IfAvailable), SslVerifyMode::NONE);
        assert_eq!(ssl_verify_mode(SslMode::Require), SslVerifyMode::NONE);
    }
}
