use std::{fs::File, io::Read, path::Path};

use sha2::{Digest, Sha256};

use crate::{error::DtooError, path_utils::is_cloud_path_buf};

/// Compute `sha256:<hex>` for a local path.
pub fn fingerprint_file(path: &Path) -> Result<String, DtooError> {
    if is_cloud_path_buf(path) {
        return Err(DtooError::Config {
            message: format!(
                "cloud storage ({}) is not supported in this build yet",
                path.to_string_lossy()
            ),
        });
    }
    let display_path = path.to_string_lossy().to_string();
    let mut file = File::open(path).map_err(|source| map_io_error(&display_path, source))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];
    loop {
        let bytes = file
            .read(&mut buffer)
            .map_err(|source| map_io_error(&display_path, source))?;
        if bytes == 0 {
            break;
        }
        hasher.update(&buffer[..bytes]);
    }
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

/// Return the display file name used in `dtoo fingerprint` output.
pub fn display_name(path: &Path) -> String {
    if is_cloud_path_buf(path) {
        let value = path.to_string_lossy();
        if let Some(last) = value.rsplit('/').find(|segment| !segment.is_empty()) {
            return last.to_string();
        }
        return value.to_string();
    }

    path.file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string_lossy().to_string())
}

fn map_io_error(path: &str, source: std::io::Error) -> DtooError {
    match source.kind() {
        std::io::ErrorKind::NotFound => DtooError::FileNotFound {
            path: path.to_string(),
        },
        std::io::ErrorKind::PermissionDenied => DtooError::PermissionDenied {
            path: path.to_string(),
        },
        _ => DtooError::FileRead {
            path: path.to_string(),
            source: Box::new(source),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprints_local_file() {
        let path = std::env::temp_dir().join(format!(
            "dtoo-fp-{}.txt",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        std::fs::write(&path, "hello\n").expect("write file");
        let hash = fingerprint_file(&path).expect("fingerprint");
        assert!(hash.starts_with("sha256:"));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn returns_file_not_found_error_for_missing_file() {
        let err = fingerprint_file(Path::new("/tmp/dtoo-missing-fingerprint-input.bin"))
            .expect_err("missing file should fail");
        assert!(matches!(err, DtooError::FileNotFound { .. }));
    }

    #[test]
    fn display_name_uses_basename_for_local_path() {
        let name = display_name(Path::new("/tmp/data/sales.parquet"));
        assert_eq!(name, "sales.parquet");
    }

    #[test]
    fn display_name_uses_last_segment_for_cloud_path() {
        let name = display_name(Path::new("s3://bucket/path/to/sales.parquet"));
        assert_eq!(name, "sales.parquet");
    }

    #[test]
    fn cloud_fingerprint_is_rejected_with_clear_error() {
        let err = fingerprint_file(Path::new("s3://bucket/data.parquet"))
            .expect_err("cloud should error");
        match err {
            DtooError::Config { message } => {
                assert!(message.contains("cloud storage"));
                assert!(message.contains("not supported"));
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }
}
