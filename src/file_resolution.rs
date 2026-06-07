use std::{
    fs,
    io::{self, BufRead, Read},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use glob::Pattern;

use crate::{
    cli::{OnErrorMode, PipeMode, StdinFormat},
    error::DtooError,
    path_utils::{is_cloud_path, split_excel_sheet_from_path},
};

/// A resolved input file with normalized metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedFile {
    pub path: String,
    pub format: FileFormat,
    pub sheet: Option<String>,
    pub is_temp: bool,
}

/// Summary of resolver output including exclusion metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolutionReport {
    pub files: Vec<ResolvedFile>,
    pub excluded_count: usize,
}

/// Supported file formats for query ingestion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileFormat {
    Parquet,
    Csv { delimiter: char },
    Ndjson,
    Excel { sheet: Option<String> },
}

/// Input options used for resolving files.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileResolverConfig {
    pub glob: Option<String>,
    pub paths: Vec<String>,
    pub pipe_mode: Option<PipeMode>,
    pub stdin_format: Option<StdinFormat>,
    pub sheet: Option<String>,
    pub exclude: Vec<String>,
    pub delimiter: char,
    pub on_error: OnErrorMode,
    pub s3_region: Option<String>,
    pub s3_profile: Option<String>,
    pub gcs_project: Option<String>,
    pub azure_account: Option<String>,
}

impl Default for FileResolverConfig {
    fn default() -> Self {
        Self {
            glob: None,
            paths: Vec::new(),
            pipe_mode: None,
            stdin_format: None,
            sheet: None,
            exclude: Vec::new(),
            delimiter: ',',
            on_error: OnErrorMode::Fail,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
        }
    }
}

/// Resolves files from glob/path/pipe sources.
pub struct FileResolver {
    config: FileResolverConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CandidatePath {
    path: String,
    is_temp: bool,
}

impl FileResolver {
    /// Build a new file resolver from input configuration.
    pub fn new(config: FileResolverConfig) -> Self {
        Self { config }
    }

    /// Resolve files using process stdin when pipe mode is enabled.
    pub fn resolve(&self) -> Result<Vec<ResolvedFile>, DtooError> {
        Ok(self.resolve_report()?.files)
    }

    /// Resolve files and include exclusion counts.
    pub fn resolve_report(&self) -> Result<ResolutionReport, DtooError> {
        let stdin = io::stdin();
        let mut handle = stdin.lock();
        self.resolve_with_reader_report(&mut handle)
    }

    /// Resolve files from a custom reader (used for tests and pipe input).
    pub fn resolve_with_reader<R: Read>(
        &self,
        input: &mut R,
    ) -> Result<Vec<ResolvedFile>, DtooError> {
        Ok(self.resolve_with_reader_report(input)?.files)
    }

    /// Resolve files with exclusion metadata from a custom reader.
    pub fn resolve_with_reader_report<R: Read>(
        &self,
        input: &mut R,
    ) -> Result<ResolutionReport, DtooError> {
        let raw_paths = self.resolve_raw_paths(input)?;
        let (filtered_paths, excluded_count) = self.apply_excludes(raw_paths)?;
        Ok(ResolutionReport {
            files: self.to_resolved_files(filtered_paths)?,
            excluded_count,
        })
    }

    fn resolve_raw_paths<R: Read>(&self, input: &mut R) -> Result<Vec<CandidatePath>, DtooError> {
        if let Some(pattern) = &self.config.glob {
            return self.resolve_from_glob(pattern);
        }

        if let Some(mode) = self.config.pipe_mode {
            return match mode {
                PipeMode::File => self.resolve_from_pipe_file(input),
                PipeMode::Data => self.resolve_from_pipe_data(input),
            };
        }

        self.resolve_from_paths(self.config.paths.clone())
    }

    fn resolve_from_glob(&self, pattern: &str) -> Result<Vec<CandidatePath>, DtooError> {
        let files = if is_cloud_path(pattern) {
            self.resolve_cloud_glob(pattern)?
        } else {
            glob::glob(pattern)
                .map_err(|err| DtooError::Config {
                    message: format!("invalid --glob pattern `{pattern}`: {err}"),
                })?
                .filter_map(Result::ok)
                .filter(|path| path.is_file())
                .filter(|path| is_supported_data_path(path.to_string_lossy().as_ref()))
                .map(|path| CandidatePath {
                    path: path.to_string_lossy().into_owned(),
                    is_temp: false,
                })
                .collect::<Vec<_>>()
        };

        if files.is_empty() {
            if self.config.on_error == OnErrorMode::Skip {
                return Ok(Vec::new());
            }
            return Err(DtooError::NoFilesMatched {
                pattern: pattern.to_string(),
            });
        }

        Ok(files)
    }

    fn resolve_cloud_glob(&self, pattern: &str) -> Result<Vec<CandidatePath>, DtooError> {
        Err(DtooError::Config {
            message: format!("cloud storage glob ({pattern}) is not supported in this build yet"),
        })
    }

    fn resolve_from_pipe_file<R: Read>(
        &self,
        input: &mut R,
    ) -> Result<Vec<CandidatePath>, DtooError> {
        let reader = io::BufReader::new(input);
        let mut files = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|source| DtooError::FileRead {
                path: "stdin".to_string(),
                source: Box::new(source),
            })?;
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            files.push(trimmed.to_string());
        }

        if files.is_empty() {
            return Err(DtooError::Config {
                message: "--pipe file received no file paths from stdin".to_string(),
            });
        }

        self.resolve_from_paths(files)
    }

    fn resolve_from_pipe_data<R: Read>(
        &self,
        input: &mut R,
    ) -> Result<Vec<CandidatePath>, DtooError> {
        let Some(stdin_format) = self.config.stdin_format else {
            return Err(DtooError::Config {
                message: "--pipe data requires --stdin-format".to_string(),
            });
        };

        let mut bytes = Vec::new();
        input
            .read_to_end(&mut bytes)
            .map_err(|source| DtooError::FileRead {
                path: "stdin".to_string(),
                source: Box::new(source),
            })?;

        if bytes.is_empty() {
            return Err(DtooError::Config {
                message: "--pipe data received empty stdin".to_string(),
            });
        }

        let ext = match stdin_format {
            StdinFormat::Csv => "csv",
            StdinFormat::Parquet => "parquet",
            StdinFormat::Ndjson => "ndjson",
        };
        let path = std::env::temp_dir().join(format!("dtoo-pipe-{}.{}", unix_nanos(), ext));

        fs::write(&path, bytes).map_err(|source| DtooError::FileRead {
            path: path.to_string_lossy().into_owned(),
            source: Box::new(source),
        })?;

        Ok(vec![CandidatePath {
            path: path.to_string_lossy().into_owned(),
            is_temp: true,
        }])
    }

    fn resolve_from_paths(&self, paths: Vec<String>) -> Result<Vec<CandidatePath>, DtooError> {
        let mut out = Vec::new();
        for path in paths {
            if is_cloud_path(&path) {
                out.push(CandidatePath {
                    path,
                    is_temp: false,
                });
                continue;
            }

            let (base_path, _) = split_excel_sheet_from_path(&path);
            if Path::new(&base_path).exists() {
                out.push(CandidatePath {
                    path,
                    is_temp: false,
                });
            } else if self.config.on_error == OnErrorMode::Fail {
                return Err(DtooError::FileNotFound { path });
            }
        }

        Ok(out)
    }

    fn apply_excludes(
        &self,
        paths: Vec<CandidatePath>,
    ) -> Result<(Vec<CandidatePath>, usize), DtooError> {
        if paths.is_empty()
            || self.config.exclude.is_empty()
            || self.config.pipe_mode == Some(PipeMode::Data)
        {
            return Ok((paths, 0));
        }

        let patterns = self
            .config
            .exclude
            .iter()
            .map(|pattern| {
                Pattern::new(pattern).map_err(|err| DtooError::Config {
                    message: format!("invalid --exclude pattern `{pattern}`: {err}"),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let total = paths.len();
        let filtered = paths
            .into_iter()
            .filter(|candidate| {
                !patterns
                    .iter()
                    .any(|pattern| pattern.matches(&candidate.path))
            })
            .collect::<Vec<_>>();
        let excluded_count = total.saturating_sub(filtered.len());
        Ok((filtered, excluded_count))
    }

    fn to_resolved_files(&self, paths: Vec<CandidatePath>) -> Result<Vec<ResolvedFile>, DtooError> {
        if paths.is_empty()
            && self.config.glob.is_some()
            && self.config.on_error == OnErrorMode::Fail
        {
            return Err(DtooError::NoFilesMatched {
                pattern: self.config.glob.clone().unwrap_or_default(),
            });
        }

        Ok(paths
            .into_iter()
            .map(|candidate| {
                let (path, colon_sheet) = split_excel_sheet_from_path(&candidate.path);
                let (format, default_sheet) =
                    detect_format(&path, self.config.delimiter, self.config.sheet.clone());
                let sheet = colon_sheet.or(default_sheet);

                ResolvedFile {
                    path,
                    format,
                    sheet,
                    is_temp: candidate.is_temp,
                }
            })
            .collect())
    }
}

fn is_supported_data_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".parquet")
        || lower.ends_with(".csv")
        || lower.ends_with(".tsv")
        || lower.ends_with(".txt")
        || lower.ends_with(".ndjson")
        || lower.ends_with(".jsonl")
        || lower.ends_with(".xlsx")
        || lower.ends_with(".xls")
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos()
}

fn detect_format(
    path: &str,
    delimiter: char,
    sheet_flag: Option<String>,
) -> (FileFormat, Option<String>) {
    let lower = path.to_ascii_lowercase();

    if lower.ends_with(".parquet") {
        return (FileFormat::Parquet, None);
    }
    if lower.ends_with(".ndjson") || lower.ends_with(".jsonl") {
        return (FileFormat::Ndjson, None);
    }
    if lower.ends_with(".xlsx") || lower.ends_with(".xls") {
        return (
            FileFormat::Excel {
                sheet: sheet_flag.clone(),
            },
            sheet_flag,
        );
    }
    if lower.ends_with(".tsv") {
        return (FileFormat::Csv { delimiter: '\t' }, None);
    }
    if lower.ends_with(".csv") || lower.ends_with(".txt") {
        return (FileFormat::Csv { delimiter }, None);
    }

    (FileFormat::Csv { delimiter }, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_formats_and_excel_sheet_precedence() {
        let root = temp_dir("format-detection");
        let parquet = root.join("a.parquet");
        let csv = root.join("b.csv");
        let tsv = root.join("c.tsv");
        let ndjson = root.join("d.jsonl");
        let excel_colon = root.join("book.xlsx");
        let excel_default = root.join("book2.xlsx");
        fs::write(&parquet, "stub").expect("write parquet stub");
        fs::write(&csv, "id,name\n1,alice\n").expect("write csv");
        fs::write(&tsv, "id\tname\n1\talice\n").expect("write tsv");
        fs::write(&ndjson, "{\"id\":1}\n").expect("write ndjson");
        fs::write(&excel_colon, "stub").expect("write excel stub");
        fs::write(&excel_default, "stub").expect("write excel default stub");

        let config = FileResolverConfig {
            paths: vec![
                parquet.to_string_lossy().to_string(),
                csv.to_string_lossy().to_string(),
                tsv.to_string_lossy().to_string(),
                ndjson.to_string_lossy().to_string(),
                format!("{}:Sales", excel_colon.to_string_lossy()),
                excel_default.to_string_lossy().to_string(),
            ],
            sheet: Some("Global".to_string()),
            on_error: OnErrorMode::Fail,
            ..FileResolverConfig::default()
        };

        let resolver = FileResolver::new(config);
        let resolved = resolver
            .resolve_with_reader(&mut io::empty())
            .expect("resolution should succeed");

        assert_eq!(resolved[0].format, FileFormat::Parquet);
        assert_eq!(resolved[1].format, FileFormat::Csv { delimiter: ',' });
        assert_eq!(resolved[2].format, FileFormat::Csv { delimiter: '\t' });
        assert_eq!(resolved[3].format, FileFormat::Ndjson);
        assert_eq!(resolved[4].path, excel_colon.to_string_lossy().to_string());
        assert_eq!(resolved[4].sheet.as_deref(), Some("Sales"));
        assert_eq!(resolved[5].sheet.as_deref(), Some("Global"));

        let _ = fs::remove_file(parquet);
        let _ = fs::remove_file(csv);
        let _ = fs::remove_file(tsv);
        let _ = fs::remove_file(ndjson);
        let _ = fs::remove_file(excel_colon);
        let _ = fs::remove_file(excel_default);
        let _ = fs::remove_dir(root);
    }

    #[test]
    fn resolves_glob_and_applies_excludes() {
        let root = temp_dir("glob-resolution");
        let keep = root.join("keep.csv");
        let skip = root.join("skip.tmp.csv");
        fs::write(&keep, "id\n1\n").expect("write keep");
        fs::write(&skip, "id\n2\n").expect("write skip");

        let config = FileResolverConfig {
            glob: Some(format!("{}/*.csv", root.to_string_lossy())),
            exclude: vec!["**/skip.tmp.csv".to_string()],
            ..FileResolverConfig::default()
        };

        let resolver = FileResolver::new(config);
        let files = resolver
            .resolve_with_reader(&mut io::empty())
            .expect("glob should resolve");

        assert_eq!(files.len(), 1);
        assert!(files[0].path.ends_with("keep.csv"));

        let _ = fs::remove_file(keep);
        let _ = fs::remove_file(skip);
        let _ = fs::remove_dir(root);
    }

    #[test]
    fn recursive_glob_ignores_directories_and_unsupported_files() {
        let root = temp_dir("glob-recursive-filter");
        let nested = root.join("nested");
        fs::create_dir_all(&nested).expect("create nested directory");
        let keep_csv = nested.join("keep.csv");
        let keep_xlsx = root.join("book.xlsx");
        let ignored = root.join(".DS_Store");
        fs::write(&keep_csv, "id\n1\n").expect("write keep csv");
        fs::write(&keep_xlsx, "stub").expect("write keep xlsx");
        fs::write(&ignored, "not-data").expect("write ignored file");

        let config = FileResolverConfig {
            glob: Some(format!("{}/**/*", root.to_string_lossy())),
            ..FileResolverConfig::default()
        };

        let resolver = FileResolver::new(config);
        let files = resolver
            .resolve_with_reader(&mut io::empty())
            .expect("recursive glob should resolve supported files");

        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|f| f.path.ends_with("keep.csv")));
        assert!(files.iter().any(|f| f.path.ends_with("book.xlsx")));
        assert!(!files.iter().any(|f| f.path.ends_with(".DS_Store")));

        let _ = fs::remove_file(keep_csv);
        let _ = fs::remove_file(keep_xlsx);
        let _ = fs::remove_file(ignored);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn parses_pipe_file_and_ignores_comments() {
        let root = temp_dir("pipe-file");
        let a = root.join("a.csv");
        let b = root.join("b.csv");
        fs::write(&a, "id\n1\n").expect("write a");
        fs::write(&b, "id\n2\n").expect("write b");

        let input = format!(
            "# ignore me\n\n{}\n{}\n",
            a.to_string_lossy(),
            b.to_string_lossy()
        );

        let config = FileResolverConfig {
            pipe_mode: Some(PipeMode::File),
            ..FileResolverConfig::default()
        };

        let resolver = FileResolver::new(config);
        let files = resolver
            .resolve_with_reader(&mut input.as_bytes())
            .expect("pipe file should resolve");
        assert_eq!(files.len(), 2);

        let _ = fs::remove_file(a);
        let _ = fs::remove_file(b);
        let _ = fs::remove_dir(root);
    }

    #[test]
    fn rejects_empty_pipe_file_input() {
        let config = FileResolverConfig {
            pipe_mode: Some(PipeMode::File),
            ..FileResolverConfig::default()
        };

        let resolver = FileResolver::new(config);
        let err = resolver
            .resolve_with_reader(&mut "\n# comment\n".as_bytes())
            .expect_err("empty pipe should fail");

        assert!(matches!(err, DtooError::Config { .. }));
    }

    #[test]
    fn writes_pipe_data_to_temp_file() {
        let config = FileResolverConfig {
            pipe_mode: Some(PipeMode::Data),
            stdin_format: Some(StdinFormat::Ndjson),
            ..FileResolverConfig::default()
        };

        let resolver = FileResolver::new(config);
        let files = resolver
            .resolve_with_reader(&mut "{\"id\":1}\n".as_bytes())
            .expect("pipe data should resolve");

        assert_eq!(files.len(), 1);
        assert!(files[0].path.ends_with(".ndjson"));
        assert_eq!(files[0].format, FileFormat::Ndjson);
        assert!(files[0].is_temp);

        let _ = fs::remove_file(&files[0].path);
    }

    #[test]
    fn skip_mode_ignores_missing_explicit_paths() {
        let config = FileResolverConfig {
            paths: vec!["/tmp/does-not-exist-xyz.csv".to_string()],
            on_error: OnErrorMode::Skip,
            ..FileResolverConfig::default()
        };

        let resolver = FileResolver::new(config);
        let files = resolver
            .resolve_with_reader(&mut io::empty())
            .expect("skip should not fail");

        assert!(files.is_empty());
    }

    #[test]
    fn fail_mode_errors_on_missing_explicit_paths() {
        let config = FileResolverConfig {
            paths: vec!["/tmp/does-not-exist-xyz.csv".to_string()],
            on_error: OnErrorMode::Fail,
            ..FileResolverConfig::default()
        };

        let resolver = FileResolver::new(config);
        let err = resolver
            .resolve_with_reader(&mut io::empty())
            .expect_err("missing path should fail");

        assert!(matches!(err, DtooError::FileNotFound { .. }));
    }

    #[test]
    fn cloud_glob_is_rejected_with_clear_error() {
        let config = FileResolverConfig {
            glob: Some("s3://bucket/*.parquet".to_string()),
            ..FileResolverConfig::default()
        };
        let resolver = FileResolver::new(config);
        let err = resolver
            .resolve_with_reader(&mut io::empty())
            .expect_err("cloud glob should error");
        match err {
            DtooError::Config { message } => {
                assert!(message.contains("cloud storage"));
                assert!(message.contains("not supported"));
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    fn temp_dir(prefix: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("{prefix}-{}", unix_nanos()));
        fs::create_dir_all(&dir).expect("temp dir should be created");
        dir
    }
}
