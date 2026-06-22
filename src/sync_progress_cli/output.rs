use std::io::{self, ErrorKind, Write};

pub(super) fn write_report_or_exit(report: &str) {
    if let Err(error) = io::stdout().write_all(report.as_bytes()) {
        if error.kind() == ErrorKind::BrokenPipe {
            std::process::exit(0);
        }
        eprintln!("failed to write sync-progress output: {error}");
        std::process::exit(1);
    }
}
