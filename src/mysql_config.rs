#[derive(Clone, Debug)]
pub struct MySqlConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl Default for MySqlConnectionConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 3306,
            user: String::new(),
            password: String::new(),
            database: String::new(),
        }
    }
}
