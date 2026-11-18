use std::io::Write;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use mariadb_mysql_cdc::account_ddl::{translate_create_user, translate_unlock_user};

const MYSQL_IMAGE: &str =
    "mysql:8.4.11@sha256:0f51e63e537a1f176423fa03ef559aabe6fa4372a53160a44bc7e75da7d15364";
const ROOT_PASSWORD: &str = "fixture-root-only";
const FIXTURE_PASSWORD: &str = "cdc-native-fixture-only-2026";
const WORKER_PASSWORD: &str = "cdc-native-worker-only-2026";
const SYNTHETIC_CREATE: &str = "CREATE USER 'cdc_fixture'@'127.0.0.%' IDENTIFIED VIA mysql_native_password USING '*AB0347385E7793A014B4D9C33308736D4362FB3C' REQUIRE SSL ACCOUNT LOCK";

struct DisposableMysql(String);

impl DisposableMysql {
    fn start() -> Self {
        let output = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--pull=never",
                "--network=none",
                "--tmpfs=/var/lib/mysql:rw,size=768m",
                "-e",
                &format!("MYSQL_ROOT_PASSWORD={ROOT_PASSWORD}"),
                MYSQL_IMAGE,
                "--mysql-native-password=ON",
                "--skip-name-resolve",
            ])
            .output()
            .expect("start disposable MySQL");
        assert!(output.status.success(), "disposable MySQL did not start");
        let id = String::from_utf8(output.stdout).expect("container ID");
        let container = Self(id.trim().to_owned());
        let deadline = Instant::now() + Duration::from_secs(75);
        while Instant::now() < deadline {
            if container
                .query("root", ROOT_PASSWORD, "REQUIRED", "SELECT 1;")
                .status
                .success()
            {
                return container;
            }
            thread::sleep(Duration::from_millis(500));
        }
        panic!("disposable MySQL did not accept TLS connections before the deadline");
    }

    fn query(&self, user: &str, password: &str, ssl_mode: &str, sql: &str) -> Output {
        let mut child = Command::new("docker")
            .args([
                "exec",
                "-i",
                "-e",
                &format!("MYSQL_PWD={password}"),
                &self.0,
                "mysql",
                "--protocol=TCP",
                "--host=127.0.0.1",
                "--batch",
                "--skip-column-names",
                &format!("--ssl-mode={ssl_mode}"),
                &format!("--user={user}"),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start disposable MySQL client");
        let mut input = child.stdin.take().expect("client input");
        let write_result = input.write_all(sql.as_bytes());
        drop(input);
        let output = child.wait_with_output().expect("wait for MySQL client");
        if output.status.success() {
            write_result.expect("write client input");
        }
        output
    }

    fn root_query(&self, sql: &str) -> String {
        let result = self.query("root", ROOT_PASSWORD, "REQUIRED", sql);
        assert!(
            result.status.success(),
            "fixture administrator query failed"
        );
        String::from_utf8(result.stdout).expect("fixture query text")
    }
}

impl Drop for DisposableMysql {
    fn drop(&mut self) {
        let result = Command::new("docker").args(["rm", "-f", &self.0]).output();
        if !result.is_ok_and(|result| result.status.success()) {
            eprintln!("Could not remove disposable MySQL container {}", self.0);
        }
    }
}

#[test]
#[ignore = "starts a disposable MySQL 8.4.11 Docker container"]
fn translated_create_user_preserves_authentication_tls_and_lock() {
    let mysql = DisposableMysql::start();
    let translated = translate_create_user(SYNTHETIC_CREATE)
        .ok()
        .expect("translate fixture account");
    mysql.root_query(translated.as_sql());

    let state = mysql.root_query(
        "SELECT Host, plugin, ssl_type, account_locked FROM mysql.user WHERE User='cdc_fixture';",
    );
    assert_eq!(state.trim(), "127.0.0.%\tmysql_native_password\tANY\tY");
    let grants = mysql.root_query("SHOW GRANTS FOR 'cdc_fixture'@'127.0.0.%';");
    assert_eq!(
        grants.trim(),
        "GRANT USAGE ON *.* TO `cdc_fixture`@`127.0.0.%`"
    );

    let locked = mysql.query("cdc_fixture", FIXTURE_PASSWORD, "REQUIRED", "SELECT 1;");
    assert!(!locked.status.success());
    assert!(String::from_utf8_lossy(&locked.stderr).contains("ERROR 3118"));

    mysql.root_query("ALTER USER 'cdc_fixture'@'127.0.0.%' ACCOUNT UNLOCK;");
    let insecure = mysql.query("cdc_fixture", FIXTURE_PASSWORD, "DISABLED", "SELECT 1;");
    assert!(!insecure.status.success());
    assert!(String::from_utf8_lossy(&insecure.stderr).contains("ERROR 1045"));

    let secure = mysql.query("cdc_fixture", FIXTURE_PASSWORD, "REQUIRED", "SELECT 1;");
    assert!(
        secure.status.success(),
        "fixture credential failed over TLS"
    );
    assert_eq!(secure.stdout, b"1\n");
}

#[test]
#[ignore = "starts a disposable MySQL 8.4.11 Docker container"]
fn translated_unlock_runs_as_create_user_worker_without_changing_account_grants() {
    let mysql = DisposableMysql::start();
    let create = translate_create_user(SYNTHETIC_CREATE)
        .ok()
        .expect("translate fixture account");
    mysql.root_query(create.as_sql());
    mysql.root_query(
        "CREATE DATABASE cdc_fixture_db; \
         CREATE TABLE cdc_fixture_db.fixture_rows (id INT PRIMARY KEY, label VARCHAR(32)); \
         INSERT INTO cdc_fixture_db.fixture_rows VALUES (42, 'fixture'); \
         GRANT SELECT ON cdc_fixture_db.fixture_rows TO 'cdc_fixture'@'127.0.0.%'; \
         CREATE USER 'cdc_worker'@'127.0.0.%' IDENTIFIED BY 'cdc-native-worker-only-2026'; \
         GRANT CREATE USER ON *.* TO 'cdc_worker'@'127.0.0.%';",
    );

    let before_state = mysql.root_query(
        "SELECT plugin, ssl_type, account_locked FROM mysql.user WHERE User='cdc_fixture' AND Host='127.0.0.%';",
    );
    assert_eq!(before_state.trim(), "mysql_native_password\tANY\tY");
    let before_grants = mysql.root_query("SHOW GRANTS FOR 'cdc_fixture'@'127.0.0.%';");
    assert!(before_grants.contains("GRANT SELECT ON `cdc_fixture_db`.`fixture_rows`"));

    let worker_grants = mysql.root_query("SHOW GRANTS FOR 'cdc_worker'@'127.0.0.%';");
    assert!(worker_grants.contains("GRANT CREATE USER ON *.*"));
    assert!(!worker_grants.contains("WITH GRANT OPTION"));
    assert!(!worker_grants.contains("SUPER"));

    let locked = mysql.query("cdc_fixture", FIXTURE_PASSWORD, "REQUIRED", "SELECT 1;");
    assert!(!locked.status.success());
    assert!(String::from_utf8_lossy(&locked.stderr).contains("ERROR 3118"));

    let unlock = translate_unlock_user("ALTER USER 'cdc_fixture'@'127.0.0.%' ACCOUNT UNLOCK;")
        .ok()
        .expect("translate fixture unlock");
    let applied = mysql.query("cdc_worker", WORKER_PASSWORD, "REQUIRED", unlock.as_sql());
    assert!(
        applied.status.success(),
        "CREATE USER worker could not apply translated unlock: {}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let repeated = mysql.query("cdc_worker", WORKER_PASSWORD, "REQUIRED", unlock.as_sql());
    assert!(
        repeated.status.success(),
        "repeated unlock failed: {}",
        String::from_utf8_lossy(&repeated.stderr)
    );

    let after_state = mysql.root_query(
        "SELECT plugin, ssl_type, account_locked FROM mysql.user WHERE User='cdc_fixture' AND Host='127.0.0.%';",
    );
    assert_eq!(after_state.trim(), "mysql_native_password\tANY\tN");
    let after_grants = mysql.root_query("SHOW GRANTS FOR 'cdc_fixture'@'127.0.0.%';");
    assert_eq!(after_grants, before_grants);

    let secure_select = mysql.query(
        "cdc_fixture",
        FIXTURE_PASSWORD,
        "REQUIRED",
        "SELECT id, label FROM cdc_fixture_db.fixture_rows WHERE id=42;",
    );
    assert!(
        secure_select.status.success(),
        "fixture credential could not SELECT over TLS: {}",
        String::from_utf8_lossy(&secure_select.stderr)
    );
    assert_eq!(secure_select.stdout, b"42\tfixture\n");

    let denied_insert = mysql.query(
        "cdc_fixture",
        FIXTURE_PASSWORD,
        "REQUIRED",
        "INSERT INTO cdc_fixture_db.fixture_rows VALUES (43, 'denied');",
    );
    assert!(!denied_insert.status.success());
    assert!(String::from_utf8_lossy(&denied_insert.stderr).contains("ERROR 1142"));

    let no_tls = mysql.query("cdc_fixture", FIXTURE_PASSWORD, "DISABLED", "SELECT 1;");
    assert!(!no_tls.status.success());
    assert!(String::from_utf8_lossy(&no_tls.stderr).contains("ERROR 1045"));

    let missing_unlock =
        translate_unlock_user("ALTER USER 'cdc_missing'@'127.0.0.%' ACCOUNT UNLOCK;")
            .ok()
            .expect("translate missing-account unlock");
    let missing = mysql.query(
        "cdc_worker",
        WORKER_PASSWORD,
        "REQUIRED",
        missing_unlock.as_sql(),
    );
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("ERROR 1396"));
}
