//! Engine-agnostic data types shared across the dtoo pipeline.

/// Input file format for scanning source files.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputFormat {
    Parquet,
    Csv { delimiter: char },
    Ndjson,
    Excel { sheet: Option<String> },
}

/// Export format for writing the result DataFrame.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ExportFormat {
    Csv,
    Parquet,
    Ndjson,
}

/// Compression codec for export operations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CompressionCodec {
    Gzip,
    Zstd,
}

/// One explicit schema column from a user-provided schema file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: String,
}
