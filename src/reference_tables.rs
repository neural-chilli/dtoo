use std::{collections::HashSet, path::Path};

use polars::prelude::LazyFrame;

use crate::{
    error::DtooError,
    path_utils::{is_cloud_path, split_excel_sheet_from_path},
    polars_engine::PolarsEngine,
    types::InputFormat,
};

/// One parsed `--ref NAME=PATH` entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceTable {
    pub name: String,
    pub path: String,
    pub format: InputFormat,
}

/// Parse and validate CLI `--ref NAME=PATH` entries.
pub fn parse_reference_tables(
    refs: &[String],
    delimiter: char,
    default_sheet: Option<&str>,
) -> Result<Vec<ReferenceTable>, DtooError> {
    let mut seen = HashSet::new();
    let mut parsed = Vec::with_capacity(refs.len());

    for raw in refs {
        let mut parts = raw.splitn(2, '=');
        let name = parts.next().map(str::trim).unwrap_or_default();
        let path = parts.next().map(str::trim).unwrap_or_default();
        if name.is_empty() || path.is_empty() {
            return Err(DtooError::Config {
                message: format!("invalid --ref `{raw}`; expected NAME=PATH"),
            });
        }
        if name == "_" || name == "temp_results" {
            return Err(DtooError::Config {
                message: format!("reference table name `{name}` is reserved"),
            });
        }
        if !is_valid_identifier(name) {
            return Err(DtooError::Config {
                message: format!(
                    "reference table name `{name}` must be a valid identifier (letters, numbers, underscore)"
                ),
            });
        }
        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(DtooError::Config {
                message: format!("duplicate reference table name `{name}`"),
            });
        }

        let (base_path, sheet_from_path) = split_excel_sheet_from_path(path);
        if !is_cloud_path(&base_path) && !Path::new(&base_path).exists() {
            return Err(DtooError::FileNotFound {
                path: base_path.clone(),
            });
        }

        let format = detect_input_format(&base_path, delimiter, sheet_from_path, default_sheet);
        parsed.push(ReferenceTable {
            name: name.to_string(),
            path: base_path,
            format,
        });
    }

    Ok(parsed)
}

/// Load each reference table as a `(name, LazyFrame)` pair via the Polars engine.
pub fn load_reference_lazyframes(
    engine: &PolarsEngine,
    refs: &[ReferenceTable],
) -> Result<Vec<(String, LazyFrame)>, DtooError> {
    let mut loaded = Vec::with_capacity(refs.len());
    for spec in refs {
        let lf = engine.scan(&spec.path, &spec.format)?;
        loaded.push((spec.name.clone(), lf));
    }
    Ok(loaded)
}

fn is_valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn detect_input_format(
    base_path: &str,
    delimiter: char,
    sheet_from_path: Option<String>,
    default_sheet: Option<&str>,
) -> InputFormat {
    let lower = base_path.to_ascii_lowercase();
    if lower.ends_with(".parquet") {
        return InputFormat::Parquet;
    }
    if lower.ends_with(".ndjson") || lower.ends_with(".jsonl") {
        return InputFormat::Ndjson;
    }
    if lower.ends_with(".xlsx") || lower.ends_with(".xls") {
        let sheet = sheet_from_path.or_else(|| default_sheet.map(ToString::to_string));
        return InputFormat::Excel { sheet };
    }
    let delim = if lower.ends_with(".tsv") {
        '\t'
    } else {
        delimiter
    };
    InputFormat::Csv { delimiter: delim }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn parses_excel_colon_sheet_and_detects_format() {
        let excel = temp_path("refs", "xlsx");
        fs::write(&excel, "stub").expect("write excel");

        let refs = vec![format!("products={}:Pricing", excel.to_string_lossy())];
        let parsed = parse_reference_tables(&refs, ',', Some("Global")).expect("parse refs");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].name, "products");
        match &parsed[0].format {
            InputFormat::Excel { sheet } => assert_eq!(sheet.as_deref(), Some("Pricing")),
            other => panic!("expected excel format, got {other:?}"),
        }

        let _ = fs::remove_file(excel);
    }

    #[test]
    fn rejects_missing_local_file() {
        let refs = vec!["regions=/tmp/does-not-exist-ref-123.csv".to_string()];
        let err = parse_reference_tables(&refs, ',', None).expect_err("missing file must fail");
        assert!(matches!(err, DtooError::FileNotFound { .. }));
    }

    #[test]
    fn allows_cloud_reference_path() {
        let refs = vec!["regions=s3://bucket/regions.parquet".to_string()];
        let parsed = parse_reference_tables(&refs, ',', None).expect("cloud refs should parse");
        assert_eq!(parsed[0].format, InputFormat::Parquet);
    }

    #[test]
    fn rejects_reserved_names() {
        let err = parse_reference_tables(&["_=a.csv".to_string()], ',', None)
            .expect_err("reserved name must fail");
        assert!(matches!(err, DtooError::Config { .. }));
    }

    #[test]
    fn rejects_invalid_identifier_and_duplicate_names() {
        let invalid = parse_reference_tables(&["bad-name=a.csv".to_string()], ',', None)
            .expect_err("invalid name must fail");
        assert!(matches!(invalid, DtooError::Config { .. }));

        let dup = parse_reference_tables(
            &[
                "regions=s3://bucket/a.csv".to_string(),
                "regions=s3://bucket/b.csv".to_string(),
            ],
            ',',
            None,
        )
        .expect_err("duplicate names must fail");
        assert!(matches!(dup, DtooError::Config { .. }));
    }

    fn temp_path(prefix: &str, ext: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let counter = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dtoo-{prefix}-{nanos}-{counter}.{ext}"))
    }

    #[test]
    fn load_reference_lazyframes_loads_csv_with_row_count() {
        use crate::polars_engine::PolarsEngine;
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "dtoo-ref-{}.csv",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "id,region_name\n10,EMEA\n20,APAC\n").unwrap();

        let refs = vec![ReferenceTable {
            name: "regions".to_string(),
            path: path.to_string_lossy().to_string(),
            format: crate::types::InputFormat::Csv { delimiter: ',' },
        }];
        let engine = PolarsEngine::new();
        let loaded = load_reference_lazyframes(&engine, &refs).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, "regions");
        // second tuple element is a LazyFrame; collecting it yields 2 rows
        assert_eq!(engine.collect(loaded[0].1.clone()).unwrap().height(), 2);
        let _ = std::fs::remove_file(path);
    }
}
