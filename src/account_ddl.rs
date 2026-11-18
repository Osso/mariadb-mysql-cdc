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

/// Translate the one verified MariaDB account statement form to MySQL 8.4 syntax.
///
/// This function does not retain or format the input, which contains a credential verifier.
pub fn translate_create_user(sql: &str) -> Result<TranslatedCreateUser, TranslationError> {
    let tokens = tokenize(sql)?;
    let mut parser = Parser {
        tokens: &tokens,
        index: 0,
    };

    parser.keyword("CREATE")?;
    parser.keyword("USER")?;
    let user = parser.account_part()?;
    parser.symbol(TokenKind::At)?;
    let host = parser.account_part()?;
    if !user
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || !host.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b':' | b'%' | b'_' | b'-')
        })
    {
        return Err(TranslationError);
    }
    parser.keyword("IDENTIFIED")?;
    parser.keyword("VIA")?;
    parser.keyword("MYSQL_NATIVE_PASSWORD")?;
    parser.keyword("USING")?;
    let verifier = parser.verifier()?;
    parser.keyword("REQUIRE")?;
    parser.keyword("SSL")?;
    parser.keyword("ACCOUNT")?;
    parser.keyword("LOCK")?;
    let semicolon = parser.optional_semicolon()?;
    parser.finish()?;

    Ok(TranslatedCreateUser(format!(
        "CREATE USER '{user}'@'{host}' IDENTIFIED WITH mysql_native_password AS '{verifier}' REQUIRE SSL ACCOUNT LOCK{}",
        if semicolon { ";" } else { "" }
    )))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    Word,
    Quoted,
    At,
    Semicolon,
}

struct Token<'a> {
    kind: TokenKind,
    value: &'a str,
}

fn tokenize(sql: &str) -> Result<Vec<Token<'_>>, TranslationError> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut cursor = 0;

    while cursor < bytes.len() {
        if bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
            continue;
        }

        match bytes[cursor] {
            b'\'' => {
                let start = cursor + 1;
                cursor = start;
                while cursor < bytes.len() && bytes[cursor] != b'\'' {
                    if !bytes[cursor].is_ascii_graphic() || bytes[cursor] == b'\\' {
                        return Err(TranslationError);
                    }
                    cursor += 1;
                }
                if cursor == bytes.len() {
                    return Err(TranslationError);
                }
                tokens.push(Token {
                    kind: TokenKind::Quoted,
                    value: &sql[start..cursor],
                });
                cursor += 1;
            }
            b'@' => {
                tokens.push(Token {
                    kind: TokenKind::At,
                    value: "@",
                });
                cursor += 1;
            }
            b';' => {
                tokens.push(Token {
                    kind: TokenKind::Semicolon,
                    value: ";",
                });
                cursor += 1;
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = cursor;
                cursor += 1;
                while cursor < bytes.len()
                    && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
                {
                    cursor += 1;
                }
                tokens.push(Token {
                    kind: TokenKind::Word,
                    value: &sql[start..cursor],
                });
            }
            _ => return Err(TranslationError),
        }
    }

    Ok(tokens)
}

struct Parser<'a> {
    tokens: &'a [Token<'a>],
    index: usize,
}

impl Parser<'_> {
    fn keyword(&mut self, expected: &str) -> Result<(), TranslationError> {
        let token = self.next()?;
        if token.kind == TokenKind::Word && token.value.eq_ignore_ascii_case(expected) {
            Ok(())
        } else {
            Err(TranslationError)
        }
    }

    fn account_part(&mut self) -> Result<String, TranslationError> {
        let token = self.next()?;
        if token.kind == TokenKind::Quoted && !token.value.is_empty() {
            Ok(token.value.to_string())
        } else {
            Err(TranslationError)
        }
    }

    fn verifier(&mut self) -> Result<String, TranslationError> {
        let value = self.account_part()?;
        let bytes = value.as_bytes();
        if bytes.len() == 41
            && bytes[0] == b'*'
            && bytes[1..].iter().all(|byte| byte.is_ascii_hexdigit())
        {
            Ok(value)
        } else {
            Err(TranslationError)
        }
    }

    fn symbol(&mut self, expected: TokenKind) -> Result<(), TranslationError> {
        if self.next()?.kind == expected {
            Ok(())
        } else {
            Err(TranslationError)
        }
    }

    fn optional_semicolon(&mut self) -> Result<bool, TranslationError> {
        if self
            .tokens
            .get(self.index)
            .is_some_and(|token| token.kind == TokenKind::Semicolon)
        {
            self.index += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn finish(&self) -> Result<(), TranslationError> {
        if self.index == self.tokens.len() {
            Ok(())
        } else {
            Err(TranslationError)
        }
    }

    fn next(&mut self) -> Result<&Token<'_>, TranslationError> {
        let token = self.tokens.get(self.index).ok_or(TranslationError)?;
        self.index += 1;
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::{translate_create_user, TranslationError};

    const VERIFIER: &str = "*0123456789ABCDEF0123456789ABCDEF01234567";

    #[test]
    fn translates_supported_form_preserving_account_verifier_and_terminator() {
        let source = format!(
            "CREATE USER 'User.Name'@'%.Example' IDENTIFIED VIA mysql_native_password USING '{VERIFIER}' REQUIRE SSL ACCOUNT LOCK;"
        );
        let translated = translate_create_user(&source)
            .unwrap_or_else(|_| panic!("supported statement rejected"));
        assert_eq!(
            translated.as_sql(),
            format!(
                "CREATE USER 'User.Name'@'%.Example' IDENTIFIED WITH mysql_native_password AS '{VERIFIER}' REQUIRE SSL ACCOUNT LOCK;"
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
