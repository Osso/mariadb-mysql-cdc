use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::net::TcpStream;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SOURCE_IMAGE: &str = "mariadb:11.8.8";
const TARGET_IMAGE: &str = "mysql:8.4.11";
const ROOT_PASSWORD: &str = "zero-ddl-native-root-only";
const SOURCE_PASSWORD: &str = "zero-ddl-native-source-only";
const TARGET_PASSWORD: &str = "cdc-stream-password";
const SOURCE_IDENTITY: &str = "zero-ddl-position-native";
const CHECKPOINT_NAME: &str = "stream-binlog:zero-ddl-position-native";

struct Container {
    id: String,
    client: &'static str,
}

impl Container {
    fn start(
        image: &str,
        name: &str,
        env_args: &[&str],
        mount_args: &[String],
        command_args: &[&str],
    ) -> Self {
        let mut args = vec![
            "run".to_owned(),
            "--detach".to_owned(),
            "--pull=never".to_owned(),
            "--name".to_owned(),
            name.to_owned(),
            "--publish".to_owned(),
            "127.0.0.1::3306".to_owned(),
            "--network=bridge".to_owned(),
            "--tmpfs=/var/lib/mysql:rw,size=768m".to_owned(),
        ];
        args.extend(env_args.iter().map(|arg| (*arg).to_owned()));
        args.extend(mount_args.iter().cloned());
        args.push(image.to_owned());
        args.extend(command_args.iter().map(|arg| (*arg).to_owned()));
        let output = Command::new("docker")
            .args(&args)
            .output()
            .expect("start disposable container");
        assert!(
            output.status.success(),
            "start disposable {image}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let id = String::from_utf8(output.stdout).expect("container id");
        let client = if image.starts_with("mariadb:") {
            "mariadb"
        } else {
            "mysql"
        };
        let container = Self {
            id: id.trim().to_owned(),
            client,
        };
        container.wait_until_ready();
        container
    }

    fn port(&self) -> u16 {
        let output = docker(&["port", &self.id, "3306/tcp"]);
        assert!(output.status.success(), "read mapped port");
        let mapping = String::from_utf8(output.stdout).expect("port mapping");
        mapping
            .trim()
            .rsplit(':')
            .next()
            .expect("mapped port")
            .parse()
            .expect("numeric mapped port")
    }

    fn wait_until_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(90);
        let mut last_error = String::new();
        while Instant::now() < deadline {
            let output = docker(&[
                "exec",
                "-e",
                &format!("MYSQL_PWD={ROOT_PASSWORD}"),
                &self.id,
                self.client,
                "--protocol=socket",
                "--user=root",
                "--batch",
                "--skip-column-names",
                "-e",
                "SELECT 1",
            ]);
            if output.status.success() {
                return;
            }
            last_error = String::from_utf8_lossy(&output.stderr).into_owned();
            thread::sleep(Duration::from_millis(500));
        }
        let logs = docker(&["logs", &self.id]);
        panic!(
            "disposable database {} did not become ready; last client error: {}; container logs: {}",
            self.id,
            last_error.trim(),
            String::from_utf8_lossy(&logs.stdout)
        );
    }

    fn sql(&self, user: &str, password: &str, sql: &str) -> String {
        let output = docker_with_input(
            &[
                "exec",
                "-i",
                "-e",
                &format!("MYSQL_PWD={password}"),
                &self.id,
                self.client,
                "--protocol=socket",
                "--user",
                user,
                "--batch",
                "--skip-column-names",
                "--default-character-set=utf8mb4",
            ],
            sql,
        );
        assert!(
            output.status.success(),
            "SQL failed in disposable container {}:\n{}\nSQL:\n{}",
            self.id,
            String::from_utf8_lossy(&output.stderr),
            sql
        );
        String::from_utf8(output.stdout).expect("SQL output")
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let output = docker(&["rm", "--force", &self.id]);
        if !output.status.success() {
            eprintln!("could not remove disposable container {}", self.id);
        }
    }
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("zero-ddl-position-{}-{nonce}", std::process::id()));
        fs::create_dir(&path).expect("create native CDC temporary directory");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            eprintln!(
                "could not remove temporary directory {}: {error}",
                self.0.display()
            );
        }
    }
}

fn generate_tls_material(directory: &Path) -> PathBuf {
    let ca_key = directory.join("ca-key.pem");
    let ca_file = directory.join("ca.pem");
    let server_key = directory.join("server-key.pem");
    let csr = directory.join("server.csr");
    let cert = directory.join("server-cert.pem");
    let extensions = directory.join("server-ext.cnf");
    fs::write(
        &extensions,
        "subjectAltName=IP:127.0.0.1,DNS:localhost\\nextendedKeyUsage=serverAuth\\n",
    )
    .expect("write TLS certificate extensions");
    run_command(&[
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "2",
        "-subj",
        "/CN=zero-ddl-test-ca",
        "-keyout",
        ca_key.to_str().unwrap(),
        "-out",
        ca_file.to_str().unwrap(),
    ]);
    run_command(&[
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-subj",
        "/CN=127.0.0.1",
        "-keyout",
        server_key.to_str().unwrap(),
        "-out",
        csr.to_str().unwrap(),
    ]);
    run_command(&[
        "x509",
        "-req",
        "-in",
        csr.to_str().unwrap(),
        "-CA",
        ca_file.to_str().unwrap(),
        "-CAkey",
        ca_key.to_str().unwrap(),
        "-CAcreateserial",
        "-days",
        "2",
        "-extfile",
        extensions.to_str().unwrap(),
        "-out",
        cert.to_str().unwrap(),
    ]);
    for path in [&ca_file, &server_key, &cert] {
        fs::set_permissions(path, fs::Permissions::from_mode(0o644))
            .expect("make TLS material readable inside the fixture containers");
    }
    ca_file
}

fn run_command(args: &[&str]) {
    let output = Command::new("openssl")
        .args(args)
        .output()
        .expect("run openssl for disposable TLS material");
    assert!(
        output.status.success(),
        "openssl {} failed: {}",
        args.first().unwrap_or(&""),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn docker(args: &[&str]) -> Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("run docker")
}

fn docker_with_input(args: &[&str], input: &str) -> Output {
    let mut child = Command::new("docker")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run docker SQL client");
    child
        .stdin
        .take()
        .expect("docker stdin")
        .write_all(input.as_bytes())
        .expect("write SQL to docker client");
    child
        .wait_with_output()
        .expect("wait for docker SQL client")
}

fn run_stream(
    source_port: u16,
    target_port: u16,
    ca_file: &Path,
    start_file: &str,
    start_position: u64,
    approval: Option<&str>,
) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mariadb-mysql-cdc"));
    command
        .args([
            "stream-binlog",
            "--source-host",
            "127.0.0.1",
            "--source-port",
            &source_port.to_string(),
            "--source-user",
            "cdc_reader",
            "--source-password-env",
            "ZERO_DDL_SOURCE_PASSWORD",
            "--source-database",
            "zero_ddl_fixture",
            "--source-identity",
            SOURCE_IDENTITY,
            "--binlog-file",
            start_file,
            "--start-position",
            &start_position.to_string(),
            "--target-host",
            "127.0.0.1",
            "--target-port",
            &target_port.to_string(),
            "--target-user",
            "cdc_stream",
            "--target-password-env",
            "ZERO_DDL_TARGET_PASSWORD",
            "--target-database",
            "zero_ddl_fixture",
            "--target-tls-ca-file",
            ca_file.to_str().expect("CA file path"),
            "--max-reconnects",
            "0",
        ])
        .env("ZERO_DDL_SOURCE_PASSWORD", SOURCE_PASSWORD)
        .env("ZERO_DDL_TARGET_PASSWORD", TARGET_PASSWORD)
        .env_remove("CDC_APPROVED_DDL_POSITION_REPAIR");
    if let Some(approval) = approval {
        command.env("CDC_APPROVED_DDL_POSITION_REPAIR", approval);
    }
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start actual stream-binlog process")
}

fn stop_running_stream(mut child: Child, duration: Duration, context: &str) -> Output {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll stream-binlog") {
            let output = child
                .wait_with_output()
                .expect("collect stream-binlog output");
            panic!(
                "{context} exited before the barrier observation (status {status}); stdout: {}; stderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
    child
        .kill()
        .expect("stop stream-binlog after barrier observation");
    child
        .wait_with_output()
        .expect("collect stopped stream-binlog output")
}

fn wait_for_pending_and_stop(
    child: Child,
    target: &Container,
    journal_query: &str,
    context: &str,
) -> (String, Output) {
    let mut child = child;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let journal = target.sql("root", ROOT_PASSWORD, journal_query);
        if !journal.trim().is_empty() {
            let output = stop_running_stream(child, Duration::from_millis(500), context);
            return (journal, output);
        }
        if let Some(status) = child.try_wait().expect("poll stream-binlog") {
            let output = child
                .wait_with_output()
                .expect("collect exited stream-binlog output");
            panic!(
                "{context} exited before writing the journal (status {status}); stdout: {}; stderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        if Instant::now() >= deadline {
            child
                .kill()
                .expect("stop stream-binlog after journal timeout");
            let output = child
                .wait_with_output()
                .expect("collect stream-binlog after journal timeout");
            panic!(
                "{context} did not write a pending journal row; last query result: {journal:?}; stdout: {}; stderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_process_exit(mut child: Child, duration: Duration, context: &str) -> Output {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if child.try_wait().expect("poll stream-binlog").is_some() {
            return child
                .wait_with_output()
                .expect("collect exited stream-binlog output");
        }
        thread::sleep(Duration::from_millis(50));
    }
    child.kill().expect("stop stream-binlog after exit timeout");
    let output = child
        .wait_with_output()
        .expect("collect stream-binlog after exit timeout");
    panic!(
        "{context} did not exit promptly; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn approval_for_position(
    scoped_identity: &str,
    binlog_file: &str,
    checkpoint_position: u64,
    event_start: u64,
    event_end: u64,
    statement_hash: &str,
) -> String {
    let identity_hash = format!("{:x}", Sha256::digest(scoped_identity.as_bytes()));
    format!(
        "{identity_hash}:{binlog_file}:{checkpoint_position}:{event_start}:{event_end}:{statement_hash}"
    )
}

fn journal_rows(target: &Container) -> String {
    target.sql(
        "root",
        ROOT_PASSWORD,
        "SELECT source_identity,source_server_id,binlog_file,event_start_position,event_end_position,schema_name,raw_sql,transformation_version,COALESCE(generated_sql,'<null>'),canonical_ast,pre_state,expected_post_state,status FROM cdc.ddl_replay_journal ORDER BY event_start_position;",
    )
}

fn install_legacy_zero_position(target: &Container, start: u64) -> String {
    target.sql(
        "root",
        ROOT_PASSWORD,
        &format!(
            "INSERT INTO cdc.ddl_replay_journal (source_identity,source_server_id,binlog_file,event_start_position,event_end_position,schema_name,raw_sql,transformation_version,generated_sql,canonical_ast,pre_state,expected_post_state,status) SELECT source_identity,source_server_id,binlog_file,0,0,schema_name,raw_sql,transformation_version,generated_sql,canonical_ast,pre_state,expected_post_state,status FROM cdc.ddl_replay_journal WHERE event_start_position={start}; DELETE FROM cdc.ddl_replay_journal WHERE event_start_position={start};"
        ),
    );
    journal_rows(target)
}

fn assert_fixture_has_no_side_effects(target: &Container, checkpoint: &str) {
    assert_eq!(
        target.sql(
            "root",
            ROOT_PASSWORD,
            "SHOW TABLES FROM zero_ddl_fixture LIKE 'created_from_select';"
        ),
        ""
    );
    assert_eq!(
        target
            .sql(
                "root",
                ROOT_PASSWORD,
                "SELECT COUNT(*) FROM zero_ddl_fixture.later_rows;"
            )
            .trim(),
        "0"
    );
    let observed_checkpoint = target.sql(
        "root",
        ROOT_PASSWORD,
        &format!(
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(checkpoint_json,'$.source_file')), JSON_EXTRACT(checkpoint_json,'$.source_position') FROM cdc.stream_checkpoint WHERE checkpoint_name='{CHECKPOINT_NAME}';"
        ),
    );
    assert_eq!(observed_checkpoint, checkpoint);
}

fn wait_for_tcp(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("mapped local database port did not accept connections");
}

#[test]
#[ignore = "starts locally available MariaDB 11.8.8 and MySQL 8.4.11 containers; images are never pulled"]
fn zero_end_create_as_select_persists_exact_barrier_and_survives_restart() {
    let temp = TempDir::new();
    let ca_file = generate_tls_material(&temp.0);
    let tls_mount = format!("--volume={}:/etc/cdc-tls:ro", temp.0.display());
    let mounts = [tls_mount];
    let suffix = format!("{}", std::process::id());
    let source = Container::start(
        SOURCE_IMAGE,
        &format!("zero-ddl-source-{suffix}"),
        &["--env=MYSQL_ROOT_PASSWORD=zero-ddl-native-root-only"],
        &mounts,
        &[
            "--server-id=881188",
            "--log-bin=source-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--binlog-legacy-event-pos=OFF",
            "--gtid-domain-id=881188",
            "--ssl-ca=/etc/cdc-tls/ca.pem",
            "--ssl-cert=/etc/cdc-tls/server-cert.pem",
            "--ssl-key=/etc/cdc-tls/server-key.pem",
        ],
    );
    let target = Container::start(
        TARGET_IMAGE,
        &format!("zero-ddl-target-{suffix}"),
        &["--env=MYSQL_ROOT_PASSWORD=zero-ddl-native-root-only"],
        &mounts,
        &[
            "--ssl-ca=/etc/cdc-tls/ca.pem",
            "--ssl-cert=/etc/cdc-tls/server-cert.pem",
            "--ssl-key=/etc/cdc-tls/server-key.pem",
        ],
    );
    let source_port = source.port();
    let target_port = target.port();
    wait_for_tcp(source_port);
    wait_for_tcp(target_port);

    source.sql(
        "root",
        ROOT_PASSWORD,
        "CREATE DATABASE zero_ddl_fixture; \
         CREATE TABLE zero_ddl_fixture.source_seed (id INT PRIMARY KEY, value_text VARCHAR(32)); \
         INSERT INTO zero_ddl_fixture.source_seed VALUES (1, 'copied'); \
         CREATE TABLE zero_ddl_fixture.later_rows (id INT PRIMARY KEY, value_text VARCHAR(32)); \
         CREATE USER 'cdc_reader'@'%' IDENTIFIED BY 'zero-ddl-native-source-only'; \
         GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'cdc_reader'@'%';",
    );
    target.sql(
        "root",
        ROOT_PASSWORD,
        "CREATE DATABASE zero_ddl_fixture; CREATE DATABASE cdc CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;",
    );
    let target_bootstrap = fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/cdc-harness-target-bootstrap.sql"),
    )
    .expect("read native target bootstrap fixture");
    target.sql("root", ROOT_PASSWORD, &target_bootstrap);
    let recovery_bootstrap = fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("docs/stream-recovery-records-bootstrap.sql"),
    )
    .expect("read native recovery bootstrap fixture");
    target.sql("root", ROOT_PASSWORD, &recovery_bootstrap);
    target.sql(
        "root",
        ROOT_PASSWORD,
        "CREATE TABLE zero_ddl_fixture.source_seed (id INT PRIMARY KEY, value_text VARCHAR(32)); \
         CREATE TABLE zero_ddl_fixture.later_rows (id INT PRIMARY KEY, value_text VARCHAR(32)); \
         GRANT ALL PRIVILEGES ON zero_ddl_fixture.* TO 'cdc_stream'@'%';",
    );
    let initial = source.sql("root", ROOT_PASSWORD, "SHOW MASTER STATUS;");
    let initial_fields: Vec<&str> = initial.trim().split('\t').collect();
    assert!(initial_fields.len() >= 2, "SHOW MASTER STATUS: {initial}");
    let binlog_file = initial_fields[0];
    let initial_position: u64 = initial_fields[1].parse().expect("initial position");
    let checkpoint = format!(
        "INSERT INTO cdc.stream_checkpoint(checkpoint_name, checkpoint_json) VALUES ('{CHECKPOINT_NAME}', '{{\"source_file\":\"{binlog_file}\",\"source_position\":{initial_position},\"gtid\":null,\"event_timestamp\":0,\"last_event\":{{\"event_type\":\"bootstrap\",\"description\":\"native fixture\"}}}}');"
    );
    target.sql("root", ROOT_PASSWORD, &checkpoint);

    source.sql(
        "root",
        ROOT_PASSWORD,
        "USE zero_ddl_fixture; \
         CREATE TABLE IF NOT EXISTS created_from_select AS SELECT id, value_text FROM source_seed; \
         INSERT INTO later_rows VALUES (99, 'must remain unapplied');",
    );

    let events = source.sql(
        "root",
        ROOT_PASSWORD,
        &format!("SHOW BINLOG EVENTS IN '{binlog_file}' FROM {initial_position};"),
    );
    let event_rows: Vec<Vec<&str>> = events
        .lines()
        .map(|line| line.split('\t').collect())
        .collect();
    let create_index = event_rows
        .iter()
        .position(|row| {
            row.len() > 5 && row[2] == "Query" && row[5].contains("CREATE TABLE IF NOT EXISTS")
        })
        .expect("physical Query event for CREATE TABLE IF NOT EXISTS");
    let create_row = &event_rows[create_index];
    let create_start: u64 = create_row[1].parse().expect("CREATE event start position");
    let next_event_start: u64 = event_rows
        .get(create_index + 1)
        .expect("event after CREATE Query")
        .get(1)
        .expect("next event start")
        .parse()
        .expect("next event start position");
    let xid = event_rows
        .iter()
        .skip(create_index + 1)
        .find(|row| row.len() > 4 && row[2] == "Xid")
        .expect("transaction XID after CREATE Query");
    let xid_start: u64 = xid[1].parse().expect("XID start position");

    let journal_query = format!(
        "SELECT source_identity,binlog_file,event_start_position,event_end_position,status,raw_sql FROM cdc.ddl_replay_journal;"
    );
    let first = run_stream(
        source_port,
        target_port,
        &ca_file,
        binlog_file,
        initial_position,
        None,
    );
    let (journal, first_output) =
        wait_for_pending_and_stop(first, &target, &journal_query, "first stream-binlog run");
    let first_log = format!(
        "{}{}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );
    assert!(
        first_log.contains("DDL_blocked"),
        "stream did not report its barrier: {first_log}"
    );
    let fields: Vec<&str> = journal.trim().split('\t').collect();
    assert_eq!(
        fields.len(),
        6,
        "expected one complete journal row: {journal}"
    );
    assert_eq!(fields[0], format!("{SOURCE_IDENTITY}#server-id=881188"));
    assert_eq!(fields[1], binlog_file);
    assert_eq!(
        fields[2].parse::<u64>().expect("journal start"),
        create_start
    );
    assert_eq!(
        fields[3].parse::<u64>().expect("journal end"),
        next_event_start
    );
    assert_eq!(fields[4], "translation_pending");
    assert!(
        !fields[2..4].contains(&"0"),
        "journal has zero coordinate: {journal}"
    );
    assert!(fields[5].contains("CREATE TABLE IF NOT EXISTS"));

    assert_eq!(
        target.sql(
            "root",
            ROOT_PASSWORD,
            "SHOW TABLES FROM zero_ddl_fixture LIKE 'created_from_select';"
        ),
        ""
    );
    assert_eq!(
        target
            .sql(
                "root",
                ROOT_PASSWORD,
                "SELECT COUNT(*) FROM zero_ddl_fixture.later_rows;"
            )
            .trim(),
        "0"
    );
    let checkpoint_query = format!(
        "SELECT JSON_UNQUOTE(JSON_EXTRACT(checkpoint_json,'$.source_file')), JSON_EXTRACT(checkpoint_json,'$.source_position') FROM cdc.stream_checkpoint WHERE checkpoint_name='{CHECKPOINT_NAME}';"
    );
    let checkpoint_state = target.sql("root", ROOT_PASSWORD, &checkpoint_query);
    let checkpoint_fields: Vec<&str> = checkpoint_state.trim().split('\t').collect();
    assert_eq!(checkpoint_fields[0], binlog_file);
    assert_eq!(
        checkpoint_fields[1]
            .parse::<u64>()
            .expect("checkpoint position"),
        initial_position
    );
    assert!(
        checkpoint_fields[1]
            .parse::<u64>()
            .expect("checkpoint position")
            < xid_start,
        "checkpoint passed the blocked transaction XID: {checkpoint_state}"
    );

    let restarted = run_stream(
        source_port,
        target_port,
        &ca_file,
        binlog_file,
        initial_position,
        None,
    );
    let restart_output = stop_running_stream(
        restarted,
        Duration::from_secs(3),
        "restarted stream-binlog run",
    );
    let restart_log = format!(
        "{}{}",
        String::from_utf8_lossy(&restart_output.stdout),
        String::from_utf8_lossy(&restart_output.stderr)
    );
    assert!(
        restart_log.contains("DDL_blocked"),
        "restart did not reach the same barrier: {restart_log}"
    );
    assert_eq!(target.sql("root", ROOT_PASSWORD, &journal_query), journal);
    assert_eq!(
        target.sql("root", ROOT_PASSWORD, &checkpoint_query),
        checkpoint_state
    );
    assert_eq!(
        target.sql(
            "root",
            ROOT_PASSWORD,
            "SHOW TABLES FROM zero_ddl_fixture LIKE 'created_from_select';"
        ),
        ""
    );
    assert_eq!(
        target
            .sql(
                "root",
                ROOT_PASSWORD,
                "SELECT COUNT(*) FROM zero_ddl_fixture.later_rows;"
            )
            .trim(),
        "0"
    );

    let corrected_journal = journal_rows(&target);
    let legacy_journal = install_legacy_zero_position(&target, create_start);
    let legacy_fields: Vec<&str> = legacy_journal.trim().split('\t').collect();
    assert_eq!(legacy_fields[3..5], ["0", "0"]);
    assert_eq!(legacy_fields[12], "translation_pending");
    assert_eq!(legacy_fields[6], fields[5]);
    assert_fixture_has_no_side_effects(&target, &checkpoint_state);

    let unapproved = run_stream(
        source_port,
        target_port,
        &ca_file,
        binlog_file,
        initial_position,
        None,
    );
    let unapproved_output = wait_for_process_exit(
        unapproved,
        Duration::from_secs(8),
        "stream without position approval",
    );
    assert!(!unapproved_output.status.success());
    assert_eq!(journal_rows(&target), legacy_journal);
    assert_fixture_has_no_side_effects(&target, &checkpoint_state);
    assert!(
        String::from_utf8_lossy(&unapproved_output.stderr).contains("automatic DDL barrier"),
        "unapproved legacy barrier was not held: {}",
        String::from_utf8_lossy(&unapproved_output.stderr)
    );

    let invalid_approval = "wrong-position-approval";
    let rejected = run_stream(
        source_port,
        target_port,
        &ca_file,
        binlog_file,
        initial_position,
        Some(invalid_approval),
    );
    let rejected_output = wait_for_process_exit(
        rejected,
        Duration::from_secs(8),
        "stream with incorrect position approval",
    );
    assert!(!rejected_output.status.success());
    assert!(
        String::from_utf8_lossy(&rejected_output.stderr)
            .contains("automatic DDL position repair rejected"),
        "incorrect approval did not fail with a safe reason: {}",
        String::from_utf8_lossy(&rejected_output.stderr)
    );
    assert_eq!(journal_rows(&target), legacy_journal);
    assert_fixture_has_no_side_effects(&target, &checkpoint_state);

    let corrected_fields: Vec<&str> = corrected_journal.trim().split('\t').collect();
    let scoped_identity = corrected_fields[0];
    assert_eq!(
        scoped_identity,
        format!("{SOURCE_IDENTITY}#server-id={}", corrected_fields[1])
    );
    let statement_hash = target.sql(
        "root",
        ROOT_PASSWORD,
        &format!(
            "SELECT SHA2(raw_sql,256) FROM cdc.ddl_replay_journal WHERE event_start_position=0 AND event_end_position=0;"
        ),
    );
    let statement_hash = statement_hash.trim();
    assert_eq!(
        statement_hash.len(),
        64,
        "unexpected raw SQL hash: {statement_hash:?}"
    );
    let approval = approval_for_position(
        scoped_identity,
        binlog_file,
        initial_position,
        create_start,
        next_event_start,
        statement_hash.trim(),
    );
    let approved = run_stream(
        source_port,
        target_port,
        &ca_file,
        binlog_file,
        initial_position,
        Some(&approval),
    );
    let approved_output = wait_for_process_exit(
        approved,
        Duration::from_secs(8),
        "stream with exact position approval",
    );
    assert!(!approved_output.status.success());
    assert!(
        String::from_utf8_lossy(&approved_output.stderr)
            .contains("automatic DDL position verified"),
        "approved repair did not report the corrected barrier: {}",
        String::from_utf8_lossy(&approved_output.stderr)
    );
    let repaired_journal = journal_rows(&target);
    let repaired_rows: Vec<&str> = repaired_journal.lines().collect();
    assert_eq!(
        repaired_rows.len(),
        2,
        "expected preserved legacy and corrected rows: {repaired_journal}"
    );
    assert!(repaired_rows.contains(&corrected_journal.trim()));
    let repaired_legacy: Vec<&str> = repaired_rows[0].split('\t').collect();
    assert_eq!(repaired_legacy[3..5], ["0", "0"]);
    assert_eq!(repaired_legacy[12], "translation_pending");
    assert_fixture_has_no_side_effects(&target, &checkpoint_state);

    let repeated = run_stream(
        source_port,
        target_port,
        &ca_file,
        binlog_file,
        initial_position,
        Some(&approval),
    );
    let repeated_output = wait_for_process_exit(
        repeated,
        Duration::from_secs(8),
        "repeat stream with exact position approval",
    );
    assert!(!repeated_output.status.success());
    assert!(
        String::from_utf8_lossy(&repeated_output.stderr)
            .contains("automatic DDL position verified"),
        "repeated approval did not reach the same corrected barrier: {}",
        String::from_utf8_lossy(&repeated_output.stderr)
    );
    assert_eq!(journal_rows(&target), repaired_journal);
    assert_fixture_has_no_side_effects(&target, &checkpoint_state);
}
