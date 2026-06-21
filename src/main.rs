use std::env;

const USAGE: &str = "\
mariadb-mysql-cdc

Usage:
  mariadb-mysql-cdc plan

Commands:
  plan    Print the current migration tool design.
";

fn main() {
    let command = env::args().nth(1);

    match command.as_deref() {
        Some("plan") => print_plan(),
        Some("-h" | "--help") | None => print!("{USAGE}"),
        Some(other) => {
            eprintln!("unknown command: {other}\n\n{USAGE}");
            std::process::exit(2);
        }
    }
}

fn print_plan() {
    println!(
        "\
Goal: migrate MariaDB to MySQL-compatible targets with minimal downtime.

Constraints:
- Keep production MariaDB binlog_format=MIXED.
- Do not require DigitalOcean Managed MySQL to serve traffic before rehearsals pass.
- Treat incompatible SQL as migration bugs to capture and fix before cutover.

Initial phases:
1. Snapshot source tables into target in primary-key chunks.
2. Stream MariaDB binlog from a recorded start position.
3. Apply supported row and statement events to the target.
4. Quarantine unsupported events with exact binlog coordinates.
5. Validate counts/checksums before cutover.
"
    );
}
