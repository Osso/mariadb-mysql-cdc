use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use openssl::ssl::{SslConnector, SslMethod, SslStream, SslVerifyMode};
use openssl::x509::X509;
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
            add_ca_bundle(&mut builder, ca_file)?;
        } else {
            builder.set_default_verify_paths()?;
        }
    }
    Ok(builder.build())
}

fn add_ca_bundle(builder: &mut openssl::ssl::SslConnectorBuilder, path: &str) -> Result<(), Error> {
    let bytes = std::fs::read(path).map_err(|error| {
        Error::String(format!("failed to read TLS CA bundle `{path}`: {error}"))
    })?;
    if bytes.is_empty() {
        return Err(Error::String(format!(
            "failed to parse TLS CA bundle `{path}`: bundle is empty"
        )));
    }
    let certificates = X509::stack_from_pem(&bytes).map_err(|error| {
        Error::String(format!("failed to parse TLS CA bundle `{path}`: {error}"))
    })?;
    if certificates.is_empty() {
        return Err(Error::String(format!(
            "failed to parse TLS CA bundle `{path}`: no certificates found"
        )));
    }
    for (index, certificate) in certificates.into_iter().enumerate() {
        builder
            .cert_store_mut()
            .add_cert(certificate)
            .map_err(|error| {
                Error::String(format!(
                    "failed to add certificate {index} from TLS CA bundle `{path}`: {error}"
                ))
            })?;
    }
    Ok(())
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
    use openssl::asn1::Asn1Time;
    use openssl::hash::MessageDigest;
    use openssl::pkey::{PKey, Private};
    use openssl::rsa::Rsa;
    use openssl::ssl::SslAcceptor;
    use openssl::x509::extension::{BasicConstraints, ExtendedKeyUsage, KeyUsage};
    use openssl::x509::{X509NameBuilder, X509};
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::{Path, PathBuf};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestCertificateAuthority {
        key: PKey<Private>,
        certificate: X509,
    }

    struct TestServer {
        key: PKey<Private>,
        certificate: X509,
    }

    impl TestCertificateAuthority {
        fn new(common_name: &str) -> Self {
            let key = generate_key();
            let certificate = build_certificate(&key, common_name, common_name, &key, true);
            Self { key, certificate }
        }
    }

    impl TestServer {
        fn signed_by(ca: &TestCertificateAuthority, common_name: &str) -> Self {
            let key = generate_key();
            let certificate = build_certificate(
                &key,
                common_name,
                &format_subject(&ca.certificate),
                &ca.key,
                false,
            );
            Self { key, certificate }
        }
    }

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

    #[test]
    fn configured_ca_verifies_server_certificate_chain() {
        let ca = TestCertificateAuthority::new("configured-ca");
        let server = TestServer::signed_by(&ca, "server");
        let ca_path = write_bundle(&ca.certificate.to_pem().unwrap());

        let result = connect_to_test_server(&server, &ca_path);

        remove_bundle(&ca_path);
        assert!(
            result.is_ok(),
            "configured CA should verify server: {result:?}"
        );
    }

    #[test]
    fn unrelated_ca_rejects_server_certificate_chain() {
        let configured_ca = TestCertificateAuthority::new("configured-ca");
        let unrelated_ca = TestCertificateAuthority::new("unrelated-ca");
        let server = TestServer::signed_by(&configured_ca, "server");
        let ca_path = write_bundle(&unrelated_ca.certificate.to_pem().unwrap());

        let result = connect_to_test_server(&server, &ca_path);

        remove_bundle(&ca_path);
        assert!(
            result.is_err(),
            "unrelated CA must reject server certificate"
        );
    }

    #[test]
    fn multi_certificate_bundle_verifies_with_root_after_unrelated_root() {
        let configured_ca = TestCertificateAuthority::new("configured-ca");
        let unrelated_ca = TestCertificateAuthority::new("unrelated-ca");
        let server = TestServer::signed_by(&configured_ca, "server");
        let mut bundle = unrelated_ca.certificate.to_pem().unwrap();
        bundle.extend_from_slice(&configured_ca.certificate.to_pem().unwrap());
        let ca_path = write_bundle(&bundle);

        let result = connect_to_test_server(&server, &ca_path);

        remove_bundle(&ca_path);
        assert!(
            result.is_ok(),
            "all certificates in the CA bundle must be trusted: {result:?}"
        );
    }

    #[test]
    fn missing_ca_bundle_fails_with_path_context() {
        let path = unique_bundle_path();
        let error = build_ssl_connector(SslMode::RequireVerifyCa, path.to_str()).unwrap_err();
        let message = format!("{error:?}");

        assert!(message.contains(path.to_str().unwrap()));
        assert!(message.contains("read TLS CA bundle"));
    }

    #[test]
    fn empty_ca_bundle_fails_with_parse_context() {
        let path = write_bundle(&[]);
        let error = build_ssl_connector(SslMode::RequireVerifyCa, path.to_str()).unwrap_err();
        let message = format!("{error:?}");

        remove_bundle(&path);
        assert!(message.contains(path.to_str().unwrap()));
        assert!(message.contains("parse TLS CA bundle"));
    }

    #[test]
    fn malformed_ca_bundle_fails_with_parse_context() {
        let path = write_bundle(b"not a certificate");
        let error = build_ssl_connector(SslMode::RequireVerifyCa, path.to_str()).unwrap_err();
        let message = format!("{error:?}");

        remove_bundle(&path);
        assert!(message.contains(path.to_str().unwrap()));
        assert!(message.contains("parse TLS CA bundle"));
    }

    fn connect_to_test_server(server: &TestServer, ca_path: &Path) -> Result<(), String> {
        let acceptor = build_test_server_acceptor(server)?;
        let listener = TcpListener::bind("127.0.0.1:0")
            .map_err(|error| format!("bind test server: {error}"))?;
        let address = listener
            .local_addr()
            .map_err(|error| format!("read test server address: {error}"))?;
        let server_thread = spawn_test_server(listener, acceptor);
        let client_result = connect_test_client(address, ca_path);
        let server_result = server_thread
            .join()
            .map_err(|_| "test server thread panicked".to_string())?;
        client_result.and(server_result)
    }

    fn build_test_server_acceptor(server: &TestServer) -> Result<SslAcceptor, String> {
        let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls())
            .map_err(|error| format!("build test server acceptor: {error}"))?;
        acceptor
            .set_private_key(&server.key)
            .map_err(|error| format!("configure test server key: {error}"))?;
        acceptor
            .set_certificate(&server.certificate)
            .map_err(|error| format!("configure test server certificate: {error}"))?;
        Ok(acceptor.build())
    }

    fn spawn_test_server(
        listener: TcpListener,
        acceptor: SslAcceptor,
    ) -> thread::JoinHandle<Result<(), String>> {
        thread::spawn(move || {
            let (stream, _) = listener.accept().map_err(|error| error.to_string())?;
            let mut stream = acceptor.accept(stream).map_err(|error| error.to_string())?;
            let mut byte = [0; 1];
            stream
                .read_exact(&mut byte)
                .map_err(|error| error.to_string())
        })
    }

    fn connect_test_client(address: std::net::SocketAddr, ca_path: &Path) -> Result<(), String> {
        let connector = build_ssl_connector(SslMode::RequireVerifyCa, ca_path.to_str())
            .map_err(|error| format!("build test client: {error:?}"))?;
        let stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
        let configuration = connector
            .configure()
            .map_err(|error| format!("configure test client: {error}"))?;
        let mut stream = configuration
            .connect("server", stream)
            .map_err(|error| format!("TLS test client handshake failed: {error}"))?;
        stream
            .write_all(&[1])
            .map_err(|error| format!("write test byte: {error}"))
    }

    fn generate_key() -> PKey<Private> {
        PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap()
    }

    fn build_certificate(
        key: &PKey<Private>,
        subject: &str,
        issuer: &str,
        issuer_key: &PKey<Private>,
        is_ca: bool,
    ) -> X509 {
        let subject_name = build_name(subject);
        let issuer_name = build_name(issuer);
        let mut builder = X509::builder().unwrap();
        builder.set_version(2).unwrap();
        builder.set_subject_name(&subject_name).unwrap();
        builder.set_issuer_name(&issuer_name).unwrap();
        builder.set_pubkey(key).unwrap();
        builder
            .set_not_before(Asn1Time::days_from_now(0).unwrap().as_ref())
            .unwrap();
        builder
            .set_not_after(Asn1Time::days_from_now(1).unwrap().as_ref())
            .unwrap();
        append_certificate_extensions(&mut builder, is_ca);
        builder.sign(issuer_key, MessageDigest::sha256()).unwrap();
        builder.build()
    }

    fn build_name(common_name: &str) -> openssl::x509::X509Name {
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", common_name).unwrap();
        name.build()
    }

    fn append_certificate_extensions(builder: &mut openssl::x509::X509Builder, is_ca: bool) {
        if is_ca {
            builder
                .append_extension(BasicConstraints::new().critical().ca().build().unwrap())
                .unwrap();
            builder
                .append_extension(
                    KeyUsage::new()
                        .critical()
                        .key_cert_sign()
                        .crl_sign()
                        .build()
                        .unwrap(),
                )
                .unwrap();
            return;
        }
        builder
            .append_extension(BasicConstraints::new().critical().build().unwrap())
            .unwrap();
        builder
            .append_extension(ExtendedKeyUsage::new().server_auth().build().unwrap())
            .unwrap();
    }

    fn format_subject(certificate: &X509) -> String {
        certificate
            .subject_name()
            .entries()
            .next()
            .unwrap()
            .data()
            .to_string()
            .unwrap()
    }

    fn unique_bundle_path() -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "mysql-cdc-ca-{}-{timestamp}.pem",
            std::process::id()
        ))
    }

    fn write_bundle(contents: &[u8]) -> PathBuf {
        let path = unique_bundle_path();
        fs::write(&path, contents).unwrap();
        path
    }

    fn remove_bundle(path: &Path) {
        fs::remove_file(path).unwrap();
    }
}
