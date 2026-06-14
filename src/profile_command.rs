use std::path::PathBuf;

use crate::{
    cli::{OnErrorMode, PipeMode, ProfileArgs, StdinFormat},
    crypto,
    error::DtooError,
    file_resolution::{FileFormat, FileResolver, FileResolverConfig},
    polars_engine::PolarsEngine,
    profiler::{ProfileOptions, Profiler},
    types::InputFormat,
};

pub fn run(args: &ProfileArgs) -> Result<(), DtooError> {
    let input = args.path.to_string_lossy().to_string();
    let resolver = FileResolver::new(FileResolverConfig {
        glob: None,
        paths: vec![input],
        pipe_mode: None::<PipeMode>,
        stdin_format: None::<StdinFormat>,
        sheet: None,
        exclude: Vec::new(),
        delimiter: args.delimiter,
        on_error: OnErrorMode::Fail,
        s3_region: args.s3_region.clone(),
        s3_profile: args.s3_profile.clone(),
        gcs_project: args.gcs_project.clone(),
        azure_account: args.azure_account.clone(),
    });
    let resolved = resolver.resolve()?;
    let file = resolved.first().ok_or_else(|| DtooError::Config {
        message: "profile requires exactly one input file".to_string(),
    })?;

    let engine = PolarsEngine::new();
    let format = to_input_format(&file.format, file.sheet.as_deref());
    let lf = engine.scan(&file.path, &format)?;
    let mut df = engine.collect(lf)?;

    if let Some(profile_name) = args.crypto_profile.as_deref() {
        let profile =
            crypto::resolve_profile(profile_name, args.crypto_profiles_file.as_deref(), None)?;
        let (new_df, _) = crypto::decrypt_dataframe(df, &profile)?;
        df = new_df;
    }

    let options = ProfileOptions {
        path: args.output.clone().unwrap_or_else(|| PathBuf::from("-")),
        format: args.format,
        sample_percentage: args.sample,
        detail: args.detail,
        top_k: args.top_k,
    };
    Profiler::generate(&df, &options)
}

fn to_input_format(format: &FileFormat, sheet: Option<&str>) -> InputFormat {
    match format {
        FileFormat::Parquet => InputFormat::Parquet,
        FileFormat::Csv { delimiter } => InputFormat::Csv {
            delimiter: *delimiter,
        },
        FileFormat::Ndjson => InputFormat::Ndjson,
        FileFormat::Excel { .. } => InputFormat::Excel {
            sheet: sheet.map(ToString::to_string),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::cli::ProfileFormat;

    use super::*;

    #[test]
    fn profiles_csv_input_to_json_file() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let input = std::env::temp_dir().join(format!("dtoo-profile-input-{stamp}.csv"));
        let output = std::env::temp_dir().join(format!("dtoo-profile-output-{stamp}.json"));
        fs::write(&input, "id,name\n1,Ada\n2,Turing\n").expect("write input file");

        let args = ProfileArgs {
            path: PathBuf::from(&input),
            format: ProfileFormat::Json,
            output: Some(output.clone()),
            sample: 100,
            detail: crate::cli::ProfileDetail::Standard,
            top_k: 1000,
            delimiter: ',',
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            crypto_profile: None,
            crypto_profiles_file: None,
        };

        run(&args).expect("profile command should succeed");
        let report = fs::read_to_string(&output).expect("read profile output");
        assert!(report.contains("\"row_count\": 2"));

        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);
    }

    #[test]
    fn profiles_empty_csv_with_zero_rows() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let input = std::env::temp_dir().join(format!("dtoo-profile-empty-{stamp}.csv"));
        let output = std::env::temp_dir().join(format!("dtoo-profile-empty-{stamp}.json"));
        fs::write(&input, "id,name\n").expect("write input file");

        let args = ProfileArgs {
            path: PathBuf::from(&input),
            format: ProfileFormat::Json,
            output: Some(output.clone()),
            sample: 100,
            detail: crate::cli::ProfileDetail::Standard,
            top_k: 1000,
            delimiter: ',',
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            crypto_profile: None,
            crypto_profiles_file: None,
        };

        run(&args).expect("profile command should succeed");
        let report = fs::read_to_string(&output).expect("read profile output");
        assert!(report.contains("\"row_count\": 0"));

        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);
    }

    #[test]
    fn returns_file_not_found_for_missing_input() {
        let args = ProfileArgs {
            path: PathBuf::from("/tmp/definitely-missing-dtoo-file.csv"),
            format: ProfileFormat::Json,
            output: None,
            sample: 100,
            detail: crate::cli::ProfileDetail::Standard,
            top_k: 1000,
            delimiter: ',',
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            crypto_profile: None,
            crypto_profiles_file: None,
        };

        let err = run(&args).expect_err("missing input should fail");
        assert!(matches!(err, DtooError::FileNotFound { .. }));
    }

    #[test]
    fn returns_output_error_for_unwritable_destination() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let input = std::env::temp_dir().join(format!("dtoo-profile-input-{stamp}.csv"));
        fs::write(&input, "id,name\n1,Ada\n").expect("write input file");
        let output = PathBuf::from(format!("/tmp/dtoo-missing-dir-{stamp}/report.json"));

        let args = ProfileArgs {
            path: PathBuf::from(&input),
            format: ProfileFormat::Json,
            output: Some(output),
            sample: 100,
            detail: crate::cli::ProfileDetail::Standard,
            top_k: 1000,
            delimiter: ',',
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
            crypto_profile: None,
            crypto_profiles_file: None,
        };

        let err = run(&args).expect_err("missing output directory should fail");
        assert!(matches!(err, DtooError::Output { .. }));

        let _ = fs::remove_file(input);
    }

    #[test]
    fn resolver_supports_excel_colon_sheet_syntax() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let excel = std::env::temp_dir().join(format!("dtoo-profile-{stamp}.xlsx"));
        fs::write(&excel, "stub").expect("write excel stub");

        let resolver = FileResolver::new(FileResolverConfig {
            glob: None,
            paths: vec![format!("{}:Sheet2", excel.display())],
            pipe_mode: None::<PipeMode>,
            stdin_format: None::<StdinFormat>,
            sheet: None,
            exclude: Vec::new(),
            delimiter: ',',
            on_error: OnErrorMode::Fail,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
        });

        let files = resolver.resolve().expect("resolve should succeed");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].sheet.as_deref(), Some("Sheet2"));

        let _ = fs::remove_file(excel);
    }
}
