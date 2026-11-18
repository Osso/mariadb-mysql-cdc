/// A redacted failure for unsupported or unsafe account DDL.
pub struct TranslationError;

/// Translated account DDL. The SQL includes a credential verifier.
pub struct TranslatedCreateUser(String);

impl TranslatedCreateUser {
    /// Borrow the translated SQL without adding formatting traits to its wrapper.
    pub fn as_sql(&self) -> &str {
        &self.0
    }
}

/// A translated account-unlock statement with borrowed account identity.
pub struct TranslatedUnlockUser<'a> {
    user: &'a str,
    host: &'a str,
    sql: String,
}

impl TranslatedUnlockUser<'_> {
    /// Return the account user name.
    pub fn user(&self) -> &str {
        self.user
    }

    /// Return the account host pattern.
    pub fn host(&self) -> &str {
        self.host
    }

    /// Borrow the normalized SQL statement.
    pub fn as_sql(&self) -> &str {
        &self.sql
    }
}

/// Translate the supported single-account MariaDB form to MySQL 8.4.
pub fn translate_create_user(sql: &str) -> Result<TranslatedCreateUser, TranslationError> {
    let mut input = AccountStatement(sql);
    input.keywords(&["CREATE", "USER"])?;
    let user = input.quoted(is_user_name)?;
    input.consume('@')?;
    let host = input.quoted(is_host_pattern)?;
    input.keywords(&["IDENTIFIED", "VIA", "MYSQL_NATIVE_PASSWORD", "USING"])?;
    let verifier = input.quoted(is_native_verifier)?;
    input.keywords(&["REQUIRE", "SSL", "ACCOUNT", "LOCK"])?;
    let terminator = input.finish()?;

    Ok(TranslatedCreateUser(format!(
        "CREATE USER '{user}'@'{host}' IDENTIFIED WITH mysql_native_password AS '{verifier}' REQUIRE SSL ACCOUNT LOCK{terminator}"
    )))
}

/// Translate an exact single-account `ALTER USER ... ACCOUNT UNLOCK` statement.
pub fn translate_unlock_user(sql: &str) -> Result<TranslatedUnlockUser<'_>, TranslationError> {
    let mut input = AccountStatement(sql);
    input.keywords(&["ALTER", "USER"])?;
    let user = input.quoted(is_user_name)?;
    input.consume('@')?;
    let host = input.quoted(is_host_pattern)?;
    input.keywords(&["ACCOUNT", "UNLOCK"])?;
    let terminator = input.finish()?;

    Ok(TranslatedUnlockUser {
        user,
        host,
        sql: format!("ALTER USER '{user}'@'{host}' ACCOUNT UNLOCK{terminator}"),
    })
}

fn is_user_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn is_host_pattern(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b':' | b'%' | b'_' | b'-')
        })
}

fn is_native_verifier(value: &str) -> bool {
    value
        .strip_prefix('*')
        .is_some_and(|hex| hex.len() == 40 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

struct AccountStatement<'a>(&'a str);

impl<'a> AccountStatement<'a> {
    fn keywords(&mut self, expected: &[&str]) -> Result<(), TranslationError> {
        for keyword in expected {
            let input = self.remaining();
            let end = input.find(|c: char| !c.is_ascii_alphanumeric() && c != '_');
            let (word, tail) = input.split_at(end.unwrap_or(input.len()));
            if !word.eq_ignore_ascii_case(keyword) {
                return Err(TranslationError);
            }
            self.0 = tail;
        }
        Ok(())
    }

    fn quoted(&mut self, accepts: fn(&str) -> bool) -> Result<&'a str, TranslationError> {
        self.consume('\'')?;
        let (value, tail) = self.0.split_once('\'').ok_or(TranslationError)?;
        if !accepts(value) {
            return Err(TranslationError);
        }
        self.0 = tail;
        Ok(value)
    }

    fn consume(&mut self, character: char) -> Result<(), TranslationError> {
        self.0 = self
            .remaining()
            .strip_prefix(character)
            .ok_or(TranslationError)?;
        Ok(())
    }

    fn remaining(&self) -> &'a str {
        self.0.trim_start_matches(|c: char| c.is_ascii_whitespace())
    }

    fn finish(mut self) -> Result<&'static str, TranslationError> {
        if self.remaining().is_empty() {
            return Ok("");
        }
        self.consume(';')?;
        if self.remaining().is_empty() {
            Ok(";")
        } else {
            Err(TranslationError)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TranslationError, translate_create_user, translate_unlock_user};

    const VERIFIER: &str = "*0123456789ABCDEF0123456789ABCDEF01234567";

    #[test]
    fn translates_supported_form_preserving_account_verifier_and_terminator() {
        let source = format!(
            "CREATE USER 'User_Name'@'%.Example' IDENTIFIED VIA mysql_native_password USING '{VERIFIER}' REQUIRE SSL ACCOUNT LOCK;"
        );
        let translated = translate_create_user(&source)
            .unwrap_or_else(|_| panic!("supported statement rejected"));
        assert_eq!(
            translated.as_sql(),
            format!(
                "CREATE USER 'User_Name'@'%.Example' IDENTIFIED WITH mysql_native_password AS '{VERIFIER}' REQUIRE SSL ACCOUNT LOCK;"
            )
        );
    }

    #[test]
    fn accepts_case_variation_whitespace_and_no_terminator() {
        let source = format!(
            " cReAtE\tUsEr 'User' @ 'HoSt.%' iDeNtIfIeD vIa mysql_native_password\nUsInG '{VERIFIER}' ReQuIrE sSl AcCoUnT lOcK "
        );
        let translated = translate_create_user(&source)
            .unwrap_or_else(|_| panic!("supported statement rejected"));
        assert_eq!(
            translated.as_sql(),
            format!(
                "CREATE USER 'User'@'HoSt.%' IDENTIFIED WITH mysql_native_password AS '{VERIFIER}' REQUIRE SSL ACCOUNT LOCK"
            )
        );
    }

    #[test]
    fn translates_account_unlock_preserving_identity_and_terminator() {
        let translated =
            translate_unlock_user("ALTER USER 'User_Name'@'%.Example' ACCOUNT UNLOCK;")
                .unwrap_or_else(|_| panic!("supported statement rejected"));

        assert_eq!(translated.user(), "User_Name");
        assert_eq!(translated.host(), "%.Example");
        assert_eq!(
            translated.as_sql(),
            "ALTER USER 'User_Name'@'%.Example' ACCOUNT UNLOCK;"
        );
    }

    #[test]
    fn accepts_case_variation_whitespace_and_no_terminator_for_unlock() {
        let translated = translate_unlock_user(" alTeR\tUsEr 'User' @ 'Host.%'\nAcCoUnT UnLoCk ")
            .unwrap_or_else(|_| panic!("supported statement rejected"));

        assert_eq!(translated.user(), "User");
        assert_eq!(translated.host(), "Host.%");
        assert_eq!(
            translated.as_sql(),
            "ALTER USER 'User'@'Host.%' ACCOUNT UNLOCK"
        );
    }

    #[test]
    fn rejects_other_alter_user_forms_accounts_and_statements() {
        let negatives = [
            "ALTER USER 'user'@'host' ACCOUNT LOCK",
            "ALTER USER 'user'@'host' IDENTIFIED BY 'password' ACCOUNT UNLOCK",
            "ALTER USER 'user'@'host' ACCOUNT UNLOCK, 'other'@'host' ACCOUNT UNLOCK",
            "ALTER USER 'user'@'host', 'other'@'host' ACCOUNT UNLOCK",
            "ALTER USER 'user' ACCOUNT UNLOCK",
            "ALTER USER 'user.name'@'host' ACCOUNT UNLOCK",
            "ALTER USER 'user'@'host/name' ACCOUNT UNLOCK",
            "ALTER USER 'user'@'host' ACCOUNT UNLOCK; DROP USER 'user'@'host'",
            "ALTER USER 'user'@'host' ACCOUNT UNLOCK;;",
        ];

        for sql in negatives {
            assert!(
                translate_unlock_user(sql).is_err(),
                "unexpectedly accepted input"
            );
        }
    }

    #[test]
    fn rejects_unsupported_shapes_without_exposing_input() {
        let negatives = [
            "CREATE USER 'user'@'host' IDENTIFIED BY 'plaintext' REQUIRE SSL ACCOUNT LOCK",
            "CREATE USER 'user'@'host' IDENTIFIED VIA ed25519 USING 'secret' REQUIRE SSL ACCOUNT LOCK",
            "CREATE USER 'user'@'host' IDENTIFIED VIA mysql_native_password USING '*0123456789ABCDEF0123456789ABCDEF01234567' REQUIRE SSL ACCOUNT UNLOCK",
            "CREATE USER 'user' IDENTIFIED VIA mysql_native_password USING '*0123456789ABCDEF0123456789ABCDEF01234567' REQUIRE SSL ACCOUNT LOCK",
            "CREATE USER 'user'@'host' IDENTIFIED VIA mysql_native_password USING '*0123456789ABCDEF0123456789ABCDEF01234567' REQUIRE NONE ACCOUNT LOCK",
            "CREATE USER 'user'@'host' IDENTIFIED VIA mysql_native_password USING '*0123456789ABCDEF0123456789ABCDEF01234567' REQUIRE SSL ACCOUNT LOCK, 'other'@'host'",
            "CREATE USER 'user'@'host' IDENTIFIED VIA mysql_native_password USING '*0123456789ABCDEF0123456789ABCDEF01234567' REQUIRE SSL ACCOUNT LOCK; DROP USER 'user'@'host'",
            "CREATE USER 'user'@'host' /* active comment */ IDENTIFIED VIA mysql_native_password USING '*0123456789ABCDEF0123456789ABCDEF01234567' REQUIRE SSL ACCOUNT LOCK",
            "CREATE USER 'user'@'host' IDENTIFIED VIA mysql_native_password USING '*not-a-verifier' REQUIRE SSL ACCOUNT LOCK",
            "CREATE USER 'user.name'@'host' IDENTIFIED VIA mysql_native_password USING '*0123456789ABCDEF0123456789ABCDEF01234567' REQUIRE SSL ACCOUNT LOCK",
            "CREATE USER 'user'@'host/name' IDENTIFIED VIA mysql_native_password USING '*0123456789ABCDEF0123456789ABCDEF01234567' REQUIRE SSL ACCOUNT LOCK",
            "CREATE USER 'user'@'host' IDENTIFIED VIA mysql_native_password USING '*0123456789ABCDEF0123456789ABCDEF01234567' REQUIRE SSL ACCOUNT LOCK PASSWORD EXPIRE",
        ];

        for sql in negatives {
            assert!(
                translate_create_user(sql).is_err(),
                "unexpectedly accepted input"
            );
        }

        assert_eq!(std::mem::size_of::<TranslationError>(), 0);
    }
}
