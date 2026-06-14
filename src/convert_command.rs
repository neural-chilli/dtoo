use std::path::PathBuf;

use crate::{
    cli::{CompressMethod, ConvertArgs, OnErrorMode, PipeMode, QueryArgs, StdinFormat},
    query_pipeline::QueryPipeline,
};

pub fn run(args: &ConvertArgs) -> Result<(), crate::error::DtooError> {
    let query = QueryArgs {
        paths: vec![args.input.clone()],
        glob: None,
        exclude: Vec::new(),
        pipe: None::<PipeMode>,
        stdin_format: None::<StdinFormat>,
        sheet: args.sheet.clone(),
        where_clause: None,
        filter_sql: None,
        post_sql: None,
        refs: Vec::new(),
        schema: None,
        output: args.output.clone(),
        output_format: args.output_format,
        delimiter: args.delimiter,
        lineage: None,
        mask: None,
        mask_salt: String::new(),
        profile: None,
        profile_format: crate::cli::ProfileFormat::Json,
        profile_sample: 100,
        limit: None,
        no_header: false,
        compress: None::<CompressMethod>,
        count: false,
        expect_at_least: None,
        fingerprint: false,
        dry_run: false,
        verbose: false,
        on_error: OnErrorMode::Fail,
        manifest: None,
        s3_region: args.s3_region.clone(),
        s3_profile: args.s3_profile.clone(),
        gcs_project: args.gcs_project.clone(),
        azure_account: args.azure_account.clone(),
        config: None::<PathBuf>,
        crypto_profile: args.crypto_profile.clone(),
        crypto_profiles_file: args.crypto_profiles_file.clone(),
        allow_plaintext_pii: args.allow_plaintext_pii,
        encrypt_output_profile: args.encrypt_profile.clone(),
        encrypt_columns: args.encrypt_columns.clone(),
        profile_detail: crate::cli::ProfileDetail::Standard,
        top_k: 1000,
    };

    if args.encrypt_profile.is_some() && args.encrypt_columns.is_none() {
        return Err(crate::error::DtooError::Config {
            message: "--encrypt-profile requires --encrypt-columns".to_string(),
        });
    }

    QueryPipeline::run(&query)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_profile_requires_encrypt_columns() {
        let args = ConvertArgs {
            input: "/tmp/in.csv".to_string(),
            output: None,
            output_format: crate::cli::OutputFormat::Csv,
            delimiter: ',',
            sheet: None,
            crypto_profile: None,
            crypto_profiles_file: None,
            allow_plaintext_pii: false,
            encrypt_profile: Some("pii_modern".to_string()),
            encrypt_columns: None,
            s3_region: None,
            s3_profile: None,
            gcs_project: None,
            azure_account: None,
        };

        let err = run(&args).expect_err("should fail");
        assert!(
            err.to_string()
                .contains("--encrypt-profile requires --encrypt-columns")
        );
    }
}
