use std::{collections::BTreeMap, path::Path};

use serde::Deserialize;

use crate::error::DtooError;

#[derive(Debug, Deserialize, Default)]
pub struct Config {
    pub glob: Option<String>,
    pub exclude: Option<Vec<String>>,
    pub sheet: Option<String>,
    #[serde(rename = "where")]
    pub where_clause: Option<String>,
    pub filter_sql: Option<String>,
    pub post_sql: Option<String>,
    #[serde(rename = "ref")]
    pub ref_tables: Option<BTreeMap<String, String>>,
    pub schema: Option<String>,
    pub output: Option<String>,
    pub output_format: Option<String>,
    pub delimiter: Option<String>,
    pub compress: Option<String>,
    pub limit: Option<usize>,
    pub no_header: Option<bool>,
    pub lineage: Option<String>,
    pub mask: Option<MaskConfig>,
    pub profile: Option<ProfileConfig>,
    pub fingerprint: Option<bool>,
    pub manifest: Option<String>,
    pub expect_at_least: Option<usize>,
    pub count: Option<bool>,
    pub on_error: Option<String>,
    pub verbose: Option<bool>,
    pub dry_run: Option<bool>,
    pub cloud: Option<CloudConfig>,
    #[allow(dead_code)]
    pub crypto_profiles: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct MaskConfig {
    pub columns: Vec<String>,
    pub salt: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ProfileConfig {
    pub path: String,
    pub format: Option<String>,
    pub sample: Option<u8>,
}

#[derive(Debug, Deserialize)]
pub struct CloudConfig {
    pub s3_region: Option<String>,
    pub s3_profile: Option<String>,
    pub gcs_project: Option<String>,
    pub azure_account: Option<String>,
}

pub fn load_query_config(path: &Path) -> Result<Config, DtooError> {
    if !path.exists() {
        return Err(DtooError::Config {
            message: format!("--config file not found: {}", path.display()),
        });
    }

    let contents = std::fs::read_to_string(path).map_err(|source| DtooError::Config {
        message: format!("--config could not be read: {source}"),
    })?;

    let value: serde_yaml::Value =
        serde_yaml::from_str(&contents).map_err(|source| DtooError::Config {
            message: format!("--config file must be valid YAML: {source}"),
        })?;

    warn_unknown_keys(&value);
    validate_top_level_types(&value)?;

    serde_yaml::from_value(value).map_err(|source| DtooError::Config {
        message: format!("--config has invalid field type or value: {source}"),
    })
}

fn warn_unknown_keys(value: &serde_yaml::Value) {
    let Some(map) = value.as_mapping() else {
        return;
    };

    const KNOWN_KEYS: &[&str] = &[
        "glob",
        "exclude",
        "sheet",
        "where",
        "filter_sql",
        "post_sql",
        "ref",
        "schema",
        "output",
        "output_format",
        "delimiter",
        "compress",
        "limit",
        "no_header",
        "lineage",
        "mask",
        "profile",
        "fingerprint",
        "manifest",
        "expect_at_least",
        "count",
        "on_error",
        "verbose",
        "dry_run",
        "cloud",
        "crypto_profiles",
    ];

    for key in map.keys() {
        let Some(key_str) = key.as_str() else {
            continue;
        };
        if !KNOWN_KEYS.contains(&key_str) {
            eprintln!("Warning: unknown config key `{key_str}` ignored");
        }
    }
}

fn validate_top_level_types(value: &serde_yaml::Value) -> Result<(), DtooError> {
    let Some(map) = value.as_mapping() else {
        return Ok(());
    };

    for (key, val) in map {
        let Some(key) = key.as_str() else {
            continue;
        };

        match key {
            "glob" | "sheet" | "where" | "filter_sql" | "post_sql" | "schema" | "output"
            | "output_format" | "delimiter" | "compress" | "lineage" | "manifest" | "on_error" => {
                ensure_string_or_null(key, val)?
            }
            "limit" | "expect_at_least" => ensure_integer_or_null(key, val)?,
            "no_header" | "fingerprint" | "count" | "verbose" | "dry_run" => {
                ensure_bool_or_null(key, val)?
            }
            "exclude" => ensure_string_list_or_null(key, val)?,
            "ref" => ensure_string_map_or_null(key, val)?,
            "mask" => ensure_mapping_or_null(key, val)?,
            "profile" => ensure_mapping_or_null(key, val)?,
            "cloud" => ensure_mapping_or_null(key, val)?,
            "crypto_profiles"
                if !(val.is_null()
                    || val.as_sequence().is_some()
                    || val.as_mapping().is_some()) =>
            {
                return Err(config_type_error(key, "list or mapping"));
            }
            "crypto_profiles" => {}
            _ => {}
        }
    }

    Ok(())
}

fn ensure_string_or_null(field: &str, value: &serde_yaml::Value) -> Result<(), DtooError> {
    if value.is_null() || value.as_str().is_some() {
        return Ok(());
    }
    Err(config_type_error(field, "string"))
}

fn ensure_integer_or_null(field: &str, value: &serde_yaml::Value) -> Result<(), DtooError> {
    if value.is_null() || value.as_i64().is_some() || value.as_u64().is_some() {
        return Ok(());
    }
    Err(config_type_error(field, "integer"))
}

fn ensure_bool_or_null(field: &str, value: &serde_yaml::Value) -> Result<(), DtooError> {
    if value.is_null() || value.as_bool().is_some() {
        return Ok(());
    }
    Err(config_type_error(field, "boolean"))
}

fn ensure_string_list_or_null(field: &str, value: &serde_yaml::Value) -> Result<(), DtooError> {
    if value.is_null() {
        return Ok(());
    }
    let Some(items) = value.as_sequence() else {
        return Err(config_type_error(field, "list of strings"));
    };
    if items.iter().all(|item| item.as_str().is_some()) {
        return Ok(());
    }
    Err(config_type_error(field, "list of strings"))
}

fn ensure_string_map_or_null(field: &str, value: &serde_yaml::Value) -> Result<(), DtooError> {
    if value.is_null() {
        return Ok(());
    }
    let Some(map) = value.as_mapping() else {
        return Err(config_type_error(field, "map of strings"));
    };
    if map
        .iter()
        .all(|(k, v)| k.as_str().is_some() && v.as_str().is_some())
    {
        return Ok(());
    }
    Err(config_type_error(field, "map of strings"))
}

fn ensure_mapping_or_null(field: &str, value: &serde_yaml::Value) -> Result<(), DtooError> {
    if value.is_null() || value.as_mapping().is_some() {
        return Ok(());
    }
    Err(config_type_error(field, "mapping"))
}

fn config_type_error(field: &str, expected: &str) -> DtooError {
    DtooError::Config {
        message: format!("--config field `{field}` must be a {expected}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn parses_yaml_with_aliases() {
        let path = temp_path("config-parse", "yaml");
        std::fs::write(
            &path,
            "where: \"id > 1\"\nref:\n  regions: ref/regions.csv\n",
        )
        .expect("write config");

        let config = load_query_config(&path).expect("config should parse");
        assert_eq!(config.where_clause.as_deref(), Some("id > 1"));
        assert_eq!(
            config
                .ref_tables
                .as_ref()
                .and_then(|refs| refs.get("regions"))
                .map(String::as_str),
            Some("ref/regions.csv")
        );

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn reports_yaml_type_errors() {
        let path = temp_path("config-invalid", "yaml");
        std::fs::write(&path, "limit: nope\n").expect("write config");

        let err = load_query_config(&path).expect_err("expected invalid config type error");
        assert!(err.to_string().contains("limit"));

        std::fs::remove_file(path).ok();
    }

    fn temp_path(prefix: &str, ext: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let counter = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dtoo-{prefix}-{nanos}-{counter}.{ext}"))
    }
}
