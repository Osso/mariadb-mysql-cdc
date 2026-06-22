use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Checkpoint {
    pub source_file: String,
    pub source_position: u64,
    pub gtid: Option<String>,
    pub event_timestamp: u64,
    pub last_event: LastEvent,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LastEvent {
    pub event_type: String,
    pub description: String,
}

#[derive(Clone, Debug)]
pub struct FileCheckpointStore {
    path: PathBuf,
}

impl FileCheckpointStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> Result<Option<Checkpoint>, CheckpointError> {
        match fs::read_to_string(&self.path) {
            Ok(contents) => decode_checkpoint(&contents, Some(self.path.clone())).map(Some),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(CheckpointError::Read {
                path: self.path.clone(),
                source: error,
            }),
        }
    }

    pub fn save(&self, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        let encoded = encode_checkpoint(checkpoint)?;
        let temp_path = temp_checkpoint_path(&self.path);

        fs::write(&temp_path, encoded).map_err(|error| CheckpointError::Write {
            path: temp_path.clone(),
            source: error,
        })?;

        fs::rename(&temp_path, &self.path).map_err(|error| CheckpointError::Rename {
            from: temp_path,
            to: self.path.clone(),
            source: error,
        })
    }
}

#[derive(Debug)]
pub enum CheckpointError {
    Decode {
        path: Option<PathBuf>,
        source: serde_json::Error,
    },
    Encode(serde_json::Error),
    Read {
        path: PathBuf,
        source: io::Error,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
        source: io::Error,
    },
    Write {
        path: PathBuf,
        source: io::Error,
    },
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode { path, source } => match path {
                Some(path) => write!(formatter, "failed to decode {}: {source}", path.display()),
                None => write!(formatter, "failed to decode checkpoint: {source}"),
            },
            Self::Encode(source) => write!(formatter, "failed to encode checkpoint: {source}"),
            Self::Read { path, source } => {
                write!(formatter, "failed to read {}: {source}", path.display())
            }
            Self::Rename { from, to, source } => write!(
                formatter,
                "failed to move {} to {}: {source}",
                from.display(),
                to.display()
            ),
            Self::Write { path, source } => {
                write!(formatter, "failed to write {}: {source}", path.display())
            }
        }
    }
}

impl std::error::Error for CheckpointError {}

pub fn encode_checkpoint(checkpoint: &Checkpoint) -> Result<String, CheckpointError> {
    serde_json::to_string_pretty(checkpoint)
        .map(|json| format!("{json}\n"))
        .map_err(CheckpointError::Encode)
}

fn decode_checkpoint(contents: &str, path: Option<PathBuf>) -> Result<Checkpoint, CheckpointError> {
    serde_json::from_str(contents).map_err(|source| CheckpointError::Decode { path, source })
}

fn temp_checkpoint_path(path: &Path) -> PathBuf {
    let mut temp_path = path.to_path_buf();
    let extension = match path.extension().and_then(|value| value.to_str()) {
        Some(extension) => format!("{extension}.tmp"),
        None => "tmp".to_string(),
    };
    temp_path.set_extension(extension);
    temp_path
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn saves_and_loads_checkpoint() {
        let path = unique_path("checkpoint-roundtrip.json");
        let checkpoint = sample_checkpoint();
        let store = FileCheckpointStore::new(path.clone());

        store.save(&checkpoint).expect("save checkpoint");
        let loaded = store.load().expect("load checkpoint").expect("checkpoint");

        assert_eq!(loaded, checkpoint);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn missing_checkpoint_loads_as_none() {
        let path = unique_path("checkpoint-missing.json");
        let store = FileCheckpointStore::new(path);

        assert_eq!(store.load().expect("load missing checkpoint"), None);
    }

    #[test]
    fn checkpoint_json_contains_required_fields() {
        let checkpoint = sample_checkpoint();

        let encoded = encode_checkpoint(&checkpoint).expect("encode checkpoint");

        assert!(encoded.contains("\"source_file\""));
        assert!(encoded.contains("\"source_position\""));
        assert!(encoded.contains("\"gtid\""));
        assert!(encoded.contains("\"event_timestamp\""));
        assert!(encoded.contains("\"last_event\""));
    }

    #[test]
    fn corrupt_checkpoint_reports_decode_error_with_path() {
        let path = unique_path("checkpoint-corrupt.json");
        fs::write(&path, "{not json").expect("write corrupt checkpoint");
        let store = FileCheckpointStore::new(path.clone());

        let error = store.load().expect_err("decode error").to_string();

        assert!(error.contains("failed to decode"));
        assert!(error.contains(path.to_string_lossy().as_ref()));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn temp_checkpoint_path_preserves_existing_extension() {
        let path = std::path::Path::new("/tmp/checkpoint.json");

        assert_eq!(
            temp_checkpoint_path(path),
            std::path::PathBuf::from("/tmp/checkpoint.json.tmp")
        );
    }

    #[test]
    fn temp_checkpoint_path_adds_extension_when_missing() {
        let path = std::path::Path::new("/tmp/checkpoint");

        assert_eq!(
            temp_checkpoint_path(path),
            std::path::PathBuf::from("/tmp/checkpoint.tmp")
        );
    }

    fn sample_checkpoint() -> Checkpoint {
        Checkpoint {
            source_file: "mysql-bin.000001".to_string(),
            source_position: 1234,
            gtid: Some("0-17-10".to_string()),
            event_timestamp: 1_782_075_535,
            last_event: LastEvent {
                event_type: "WriteRowsEvent".to_string(),
                description: "fixture_cdc.accounts insert".to_string(),
            },
        }
    }

    fn unique_path(file_name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        path.push(format!("mariadb-mysql-cdc-{nanos}-{file_name}"));
        path
    }
}
