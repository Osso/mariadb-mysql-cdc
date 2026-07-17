/// Server and client capability flags
/// <a href="https://mariadb.com/kb/en/library/connection/#capabilities">See more</a>

pub const LONG_FLAG: u64 = 1 << 2;
pub const CONNECT_WITH_DB: u64 = 1 << 3;
pub const PROTOCOL_41: u64 = 1 << 9;
pub const SSL: u64 = 1 << 11;
pub const SECURE_CONNECTION: u64 = 1 << 15;
pub const PLUGIN_AUTH: u64 = 1 << 19;
