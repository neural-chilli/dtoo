use std::path::{Path, PathBuf};

use polars::prelude::DataFrame;

use crate::{
    error::DtooError,
    polars_engine::PolarsEngine,
    types::{CompressionCodec, ExportFormat},
};

/// Output writer configuration for exporting the result DataFrame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputWriterConfig {
    pub output: Option<PathBuf>,
    pub format: ExportFormat,
    pub header: bool,
    pub delimiter: char,
    pub compression: Option<CompressionCodec>,
}

/// Writes final output to file or stdout.
pub struct OutputWriter {
    config: OutputWriterConfig,
}

impl OutputWriter {
    /// Build a new output writer.
    pub fn new(config: OutputWriterConfig) -> Self {
        Self { config }
    }

    /// Export a Polars DataFrame to configured destination via `PolarsEngine`.
    pub fn write(&self, engine: &PolarsEngine, df: DataFrame) -> Result<(), DtooError> {
        let destination = self.effective_destination_path();
        let compression = self.effective_compression(&destination);
        if let Some(path) = &destination
            && let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
            && !parent.exists()
        {
            return Err(DtooError::Output {
                message: format!("output directory does not exist: {}", parent.display()),
            });
        }

        engine.write(
            df,
            destination.as_deref(),
            self.config.format,
            self.config.header,
            self.config.delimiter,
            compression,
        )?;

        if let (Some(original), Some(resolved)) = (&self.config.output, &destination)
            && original != resolved
        {
            eprintln!("Info: output path adjusted to {}", resolved.display());
        }

        Ok(())
    }

    /// Write a Polars DataFrame and return effective destination path (`None` when stdout).
    pub fn write_and_get_destination(
        &self,
        engine: &PolarsEngine,
        df: DataFrame,
    ) -> Result<Option<PathBuf>, DtooError> {
        let destination = self.effective_destination_path();
        let compression = self.effective_compression(&destination);
        if let Some(path) = &destination
            && let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
            && !parent.exists()
        {
            return Err(DtooError::Output {
                message: format!("output directory does not exist: {}", parent.display()),
            });
        }
        engine.write(
            df,
            destination.as_deref(),
            self.config.format,
            self.config.header,
            self.config.delimiter,
            compression,
        )?;
        if let (Some(original), Some(resolved)) = (&self.config.output, &destination)
            && original != resolved
        {
            eprintln!("Info: output path adjusted to {}", resolved.display());
        }
        Ok(destination)
    }

    fn effective_destination_path(&self) -> Option<PathBuf> {
        let Some(output) = &self.config.output else {
            return None;
        };
        Some(adjust_compressed_extension(
            output,
            self.config.format,
            self.config.compression,
        ))
    }

    fn effective_compression(&self, destination: &Option<PathBuf>) -> Option<CompressionCodec> {
        if destination.is_none()
            && self.config.compression.is_some()
            && !matches!(self.config.format, ExportFormat::Parquet)
        {
            eprintln!(
                "Warning: --compress with stdout output is not supported. Pipe through gzip/zstd instead:\n  dtoo query ... | gzip > output.csv.gz"
            );
            return None;
        }
        self.config.compression
    }
}

fn adjust_compressed_extension(
    path: &Path,
    format: ExportFormat,
    compression: Option<CompressionCodec>,
) -> PathBuf {
    if matches!(format, ExportFormat::Parquet) || compression.is_none() {
        return path.to_path_buf();
    }

    let suffix = match compression {
        Some(CompressionCodec::Gzip) => ".gz",
        Some(CompressionCodec::Zstd) => ".zst",
        None => return path.to_path_buf(),
    };

    let display = path.to_string_lossy();
    if display.ends_with(suffix) {
        return path.to_path_buf();
    }

    PathBuf::from(format!("{display}{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;
    use std::fs;

    #[test]
    fn appends_gzip_extension_for_csv_when_missing() {
        let path = adjust_compressed_extension(
            Path::new("/tmp/out.csv"),
            ExportFormat::Csv,
            Some(CompressionCodec::Gzip),
        );
        assert_eq!(path, PathBuf::from("/tmp/out.csv.gz"));
    }

    #[test]
    fn appends_zstd_extension_for_ndjson_when_missing() {
        let path = adjust_compressed_extension(
            Path::new("/tmp/out.ndjson"),
            ExportFormat::Ndjson,
            Some(CompressionCodec::Zstd),
        );
        assert_eq!(path, PathBuf::from("/tmp/out.ndjson.zst"));
    }

    #[test]
    fn keeps_existing_compression_extension() {
        let path = adjust_compressed_extension(
            Path::new("/tmp/out.csv.gz"),
            ExportFormat::Csv,
            Some(CompressionCodec::Gzip),
        );
        assert_eq!(path, PathBuf::from("/tmp/out.csv.gz"));
    }

    #[test]
    fn parquet_never_appends_suffix() {
        let path = adjust_compressed_extension(
            Path::new("/tmp/out.parquet"),
            ExportFormat::Parquet,
            Some(CompressionCodec::Gzip),
        );
        assert_eq!(path, PathBuf::from("/tmp/out.parquet"));
    }

    #[test]
    fn write_uses_adjusted_output_path() {
        let engine = PolarsEngine::new();
        let df = df!["id" => [1i64]].expect("df macro should work");

        let base = std::env::temp_dir().join(format!(
            "dtoo-output-writer-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));

        let writer = OutputWriter::new(OutputWriterConfig {
            output: Some(base.clone()),
            format: ExportFormat::Csv,
            header: true,
            delimiter: ',',
            compression: Some(CompressionCodec::Gzip),
        });
        writer.write(&engine, df).expect("write should succeed");

        let adjusted = PathBuf::from(format!("{}.gz", base.to_string_lossy()));
        assert!(adjusted.exists());

        fs::remove_file(adjusted).ok();
    }

    #[test]
    fn errors_when_output_directory_missing() {
        let engine = PolarsEngine::new();
        let df = df!["id" => [1i64]].expect("df macro should work");

        let writer = OutputWriter::new(OutputWriterConfig {
            output: Some(PathBuf::from("/tmp/dtoo-nonexistent-subdir-12345/out.csv")),
            format: ExportFormat::Csv,
            header: true,
            delimiter: ',',
            compression: None,
        });
        let err = writer
            .write(&engine, df)
            .expect_err("should fail for missing dir");
        assert!(matches!(err, DtooError::Output { .. }));
    }

    #[test]
    fn disables_compression_for_stdout_csv() {
        let writer = OutputWriter::new(OutputWriterConfig {
            output: None,
            format: ExportFormat::Csv,
            header: true,
            delimiter: ',',
            compression: Some(CompressionCodec::Gzip),
        });
        let compression = writer.effective_compression(&None);
        assert_eq!(compression, None);
    }

    #[test]
    fn returns_effective_destination_path() {
        let engine = PolarsEngine::new();
        let df = df!["id" => [1i64]].expect("df macro should work");

        let base = std::env::temp_dir().join(format!(
            "dtoo-output-writer-path-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        let writer = OutputWriter::new(OutputWriterConfig {
            output: Some(base.clone()),
            format: ExportFormat::Csv,
            header: true,
            delimiter: ',',
            compression: Some(CompressionCodec::Gzip),
        });
        let destination = writer
            .write_and_get_destination(&engine, df)
            .expect("write should succeed");
        assert_eq!(
            destination,
            Some(PathBuf::from(format!("{}.gz", base.to_string_lossy())))
        );
        std::fs::remove_file(format!("{}.gz", base.to_string_lossy())).ok();
    }
}
