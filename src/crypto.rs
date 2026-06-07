use std::{collections::HashSet, path::Path};

use aes::Aes128;
use aes_gcm::{
    Aes256Gcm,
    aead::{Aead, KeyInit, rand_core::RngCore},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ecb::cipher::{BlockDecryptMut, BlockEncryptMut, block_padding::Pkcs7};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use crate::error::DtooError;

#[derive(Debug, Clone, Deserialize)]
pub struct CryptoProfile {
    pub name: String,
    #[serde(default)]
    pub detection: DetectionConfig,
    pub scheme: SchemeConfig,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub failure_mode: FailureMode,
    #[serde(default)]
    pub output: OutputSafety,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DetectionConfig {
    #[serde(default)]
    pub mode: DetectionMode,
    #[serde(default = "default_start_marker")]
    pub start_marker: String,
    #[serde(default = "default_end_marker")]
    pub end_marker: String,
    #[serde(default = "default_min_inner_length")]
    pub min_inner_length: usize,
    #[serde(default = "default_trim_whitespace")]
    pub trim_whitespace: bool,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DetectionMode {
    #[default]
    Auto,
    Columns,
    AutoOrColumns,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchemeConfig {
    #[serde(rename = "type")]
    pub scheme_type: SchemeType,
    pub key_env: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SchemeType {
    LegacyAes128EcbSha1,
    Aes256GcmBase64,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FailureMode {
    #[default]
    BestEffort,
    Strict,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct OutputSafety {
    #[serde(default)]
    pub allow_plaintext: bool,
}

#[derive(Debug, Clone)]
pub struct CryptoProcessResult {
    pub decrypted_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CryptoDiscoverRow {
    pub column: String,
    pub total: usize,
    pub encrypted: usize,
}

#[derive(Debug, Deserialize)]
struct ProfilesContainer {
    crypto_profiles: Option<Vec<CryptoProfile>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ModernPayload {
    v: u8,
    alg: String,
    nonce: String,
    ct: String,
    tag: String,
}

trait CryptoScheme {
    fn detect_inner(&self, inner: &str) -> bool;
    fn decrypt_inner(&self, inner: &str, key_material: &str) -> Result<String, DtooError>;
    fn encrypt_inner(&self, plaintext: &str, key_material: &str) -> Result<String, DtooError>;
}

#[derive(Default)]
struct LegacyAes128EcbSha1Scheme;

#[derive(Default)]
struct Aes256GcmBase64Scheme;

impl Default for DetectionConfig {
    fn default() -> Self {
        Self {
            mode: DetectionMode::Auto,
            start_marker: default_start_marker(),
            end_marker: default_end_marker(),
            min_inner_length: default_min_inner_length(),
            trim_whitespace: default_trim_whitespace(),
        }
    }
}

fn default_start_marker() -> String {
    "en{".to_string()
}

fn default_end_marker() -> String {
    "}cr".to_string()
}

fn default_min_inner_length() -> usize {
    16
}

fn default_trim_whitespace() -> bool {
    true
}

pub fn resolve_profile(
    profile_name: &str,
    profiles_file: Option<&Path>,
    config_file: Option<&Path>,
) -> Result<CryptoProfile, DtooError> {
    let mut profiles = Vec::new();

    if let Some(path) = profiles_file {
        profiles.extend(load_profiles_from_yaml_file(path)?);
    }

    if profiles.is_empty()
        && let Some(path) = config_file
    {
        profiles.extend(load_profiles_from_config_file(path)?);
    }

    if profiles.is_empty() {
        return Err(DtooError::Config {
            message: "no crypto profiles found; provide --crypto-profiles-file or --config with crypto_profiles".to_string(),
        });
    }

    profiles
        .into_iter()
        .find(|profile| profile.name == profile_name)
        .ok_or_else(|| DtooError::Config {
            message: format!("crypto profile `{profile_name}` not found"),
        })
}

pub fn enforce_output_safety(
    decrypted_columns: &[String],
    allow_plaintext_pii: bool,
    has_output_encryption: bool,
) -> Result<(), DtooError> {
    if decrypted_columns.is_empty() || allow_plaintext_pii || has_output_encryption {
        return Ok(());
    }

    Err(DtooError::Config {
        message: format!(
            "Decrypted columns [{}] cannot be written as plaintext. Use --allow-plaintext-pii or --encrypt-output-profile.",
            decrypted_columns.join(", ")
        ),
    })
}

fn load_profiles_from_config_file(path: &Path) -> Result<Vec<CryptoProfile>, DtooError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(path).map_err(|source| DtooError::Config {
        message: format!("failed to read config file {}: {source}", path.display()),
    })?;
    let container: ProfilesContainer =
        serde_yaml::from_str(&text).map_err(|source| DtooError::Config {
            message: format!("invalid YAML in config file {}: {source}", path.display()),
        })?;
    Ok(container.crypto_profiles.unwrap_or_default())
}

fn load_profiles_from_yaml_file(path: &Path) -> Result<Vec<CryptoProfile>, DtooError> {
    let text = std::fs::read_to_string(path).map_err(|source| DtooError::Config {
        message: format!(
            "failed to read crypto profile file {}: {source}",
            path.display()
        ),
    })?;

    if let Ok(container) = serde_yaml::from_str::<ProfilesContainer>(&text)
        && let Some(profiles) = container.crypto_profiles
    {
        return Ok(profiles);
    }

    if let Ok(profile) = serde_yaml::from_str::<CryptoProfile>(&text) {
        return Ok(vec![profile]);
    }

    if let Ok(profiles) = serde_yaml::from_str::<Vec<CryptoProfile>>(&text) {
        return Ok(profiles);
    }

    Err(DtooError::Config {
        message: format!(
            "invalid crypto profile YAML in {}; expected a profile object or crypto_profiles list",
            path.display()
        ),
    })
}

fn validate_detection_config(detection: &DetectionConfig) -> Result<(), DtooError> {
    if detection.start_marker.is_empty() || detection.end_marker.is_empty() {
        return Err(DtooError::Config {
            message: "crypto detection markers cannot be empty".to_string(),
        });
    }
    Ok(())
}

fn load_key_material(profile: &CryptoProfile) -> Result<String, DtooError> {
    std::env::var(&profile.scheme.key_env).map_err(|_| DtooError::Config {
        message: format!(
            "Environment variable {} not set (required by crypto profile '{}')",
            profile.scheme.key_env, profile.name
        ),
    })
}

fn scheme_impl(scheme_type: SchemeType) -> Box<dyn CryptoScheme> {
    match scheme_type {
        SchemeType::LegacyAes128EcbSha1 => Box::<LegacyAes128EcbSha1Scheme>::default(),
        SchemeType::Aes256GcmBase64 => Box::<Aes256GcmBase64Scheme>::default(),
    }
}

fn detect_wrapped_value(value: &str, detection: &DetectionConfig) -> bool {
    let candidate = if detection.trim_whitespace {
        value.trim()
    } else {
        value
    };

    let Some(inner) =
        strip_wrapper_inner(candidate, &detection.start_marker, &detection.end_marker)
    else {
        return false;
    };

    inner.len() >= detection.min_inner_length
}

fn strip_wrapper<'a>(value: &'a str, detection: &DetectionConfig) -> Option<&'a str> {
    let candidate = if detection.trim_whitespace {
        value.trim()
    } else {
        value
    };

    let inner = strip_wrapper_inner(candidate, &detection.start_marker, &detection.end_marker)?;
    if inner.len() < detection.min_inner_length {
        return None;
    }
    Some(inner)
}

fn strip_wrapper_inner<'a>(value: &'a str, start: &str, end: &str) -> Option<&'a str> {
    if !value.starts_with(start) || !value.ends_with(end) {
        return None;
    }

    let inner = &value[start.len()..value.len().saturating_sub(end.len())];
    Some(inner)
}

fn wrap_inner(inner: &str, detection: &DetectionConfig) -> String {
    format!(
        "{}{}{}",
        detection.start_marker, inner, detection.end_marker
    )
}

impl CryptoScheme for LegacyAes128EcbSha1Scheme {
    fn detect_inner(&self, inner: &str) -> bool {
        STANDARD.decode(inner).is_ok()
    }

    fn decrypt_inner(&self, inner: &str, key_material: &str) -> Result<String, DtooError> {
        let encrypted = STANDARD.decode(inner).map_err(|source| DtooError::Config {
            message: format!("legacy base64 decode failed: {source}"),
        })?;

        let key = derive_legacy_key(key_material);
        let mut buf = encrypted.clone();
        let plaintext = ecb::Decryptor::<Aes128>::new_from_slice(&key)
            .map_err(|source| DtooError::Config {
                message: format!("legacy decrypt key init failed: {source}"),
            })?
            .decrypt_padded_mut::<Pkcs7>(&mut buf)
            .map_err(|source| DtooError::Config {
                message: format!("legacy decrypt failed: {source}"),
            })?;

        String::from_utf8(plaintext.to_vec()).map_err(|source| DtooError::Config {
            message: format!("legacy plaintext is not UTF-8: {source}"),
        })
    }

    fn encrypt_inner(&self, plaintext: &str, key_material: &str) -> Result<String, DtooError> {
        let key = derive_legacy_key(key_material);
        let encryptor =
            ecb::Encryptor::<Aes128>::new_from_slice(&key).map_err(|source| DtooError::Config {
                message: format!("legacy encrypt key init failed: {source}"),
            })?;
        let message = plaintext.as_bytes();
        let block_size = 16usize;
        let padded_len = ((message.len() / block_size) + 1) * block_size;
        let mut buffer = vec![0u8; padded_len];
        buffer[..message.len()].copy_from_slice(message);
        let encrypted = encryptor
            .encrypt_padded_mut::<Pkcs7>(&mut buffer, message.len())
            .map_err(|source| DtooError::Config {
                message: format!("legacy encrypt failed: {source}"),
            })?;
        Ok(STANDARD.encode(encrypted))
    }
}

impl CryptoScheme for Aes256GcmBase64Scheme {
    fn detect_inner(&self, inner: &str) -> bool {
        inner.starts_with("v1:gcm:")
    }

    fn decrypt_inner(&self, inner: &str, key_material: &str) -> Result<String, DtooError> {
        let payload_b64 = inner
            .strip_prefix("v1:gcm:")
            .ok_or_else(|| DtooError::Config {
                message: "invalid modern wrapper prefix; expected v1:gcm:".to_string(),
            })?;

        let payload_bytes = STANDARD
            .decode(payload_b64)
            .map_err(|source| DtooError::Config {
                message: format!("modern payload base64 decode failed: {source}"),
            })?;

        let payload: ModernPayload =
            serde_json::from_slice(&payload_bytes).map_err(|source| DtooError::Config {
                message: format!("modern payload JSON decode failed: {source}"),
            })?;

        if payload.alg != "aes256_gcm" {
            return Err(DtooError::Config {
                message: format!("unsupported modern payload algorithm `{}`", payload.alg),
            });
        }

        let key = decode_32b_key_base64(key_material)?;
        let cipher = Aes256Gcm::new_from_slice(&key).map_err(|source| DtooError::Config {
            message: format!("aes256-gcm key init failed: {source}"),
        })?;

        let nonce = STANDARD
            .decode(payload.nonce)
            .map_err(|source| DtooError::Config {
                message: format!("modern nonce decode failed: {source}"),
            })?;
        if nonce.len() != 12 {
            return Err(DtooError::Config {
                message: format!("invalid nonce length {}; expected 12", nonce.len()),
            });
        }

        let mut combined = STANDARD
            .decode(payload.ct)
            .map_err(|source| DtooError::Config {
                message: format!("modern ciphertext decode failed: {source}"),
            })?;
        let tag = STANDARD
            .decode(payload.tag)
            .map_err(|source| DtooError::Config {
                message: format!("modern tag decode failed: {source}"),
            })?;
        combined.extend_from_slice(&tag);

        let plaintext = cipher
            .decrypt(aes_gcm::Nonce::from_slice(&nonce), combined.as_ref())
            .map_err(|_| DtooError::Config {
                message: "aes256-gcm authentication/decryption failed".to_string(),
            })?;

        String::from_utf8(plaintext).map_err(|source| DtooError::Config {
            message: format!("modern plaintext is not UTF-8: {source}"),
        })
    }

    fn encrypt_inner(&self, plaintext: &str, key_material: &str) -> Result<String, DtooError> {
        let key = decode_32b_key_base64(key_material)?;
        let cipher = Aes256Gcm::new_from_slice(&key).map_err(|source| DtooError::Config {
            message: format!("aes256-gcm key init failed: {source}"),
        })?;

        let mut nonce = [0u8; 12];
        aes_gcm::aead::OsRng.fill_bytes(&mut nonce);

        let combined = cipher
            .encrypt(aes_gcm::Nonce::from_slice(&nonce), plaintext.as_bytes())
            .map_err(|_| DtooError::Config {
                message: "aes256-gcm encryption failed".to_string(),
            })?;

        if combined.len() < 16 {
            return Err(DtooError::Config {
                message: "aes256-gcm produced invalid ciphertext".to_string(),
            });
        }
        let split = combined.len() - 16;
        let ct = &combined[..split];
        let tag = &combined[split..];

        let payload = ModernPayload {
            v: 1,
            alg: "aes256_gcm".to_string(),
            nonce: STANDARD.encode(nonce),
            ct: STANDARD.encode(ct),
            tag: STANDARD.encode(tag),
        };
        let payload_json = serde_json::to_vec(&payload).map_err(|source| DtooError::Config {
            message: format!("failed to encode modern payload JSON: {source}"),
        })?;

        Ok(format!("v1:gcm:{}", STANDARD.encode(payload_json)))
    }
}

fn derive_legacy_key(key_material: &str) -> [u8; 16] {
    let mut hasher = Sha1::new();
    hasher.update(key_material.as_bytes());
    let digest = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

fn decode_32b_key_base64(key_material: &str) -> Result<[u8; 32], DtooError> {
    let decoded = STANDARD
        .decode(key_material)
        .map_err(|source| DtooError::Config {
            message: format!("key must be base64-encoded: {source}"),
        })?;
    if decoded.len() != 32 {
        return Err(DtooError::Config {
            message: format!("key must decode to 32 bytes (got {})", decoded.len()),
        });
    }

    let mut out = [0u8; 32];
    out.copy_from_slice(&decoded);
    Ok(out)
}

// ---------------------------------------------------------------------------
// DataFrame-based API (Polars)
// ---------------------------------------------------------------------------

/// Returns the names of all `DataType::String` columns in `df`.
/// Only String columns are eligible for crypto operations.
fn string_columns_df(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .into_iter()
        .filter(|name| matches!(df.column(name).map(|s| s.dtype()), Ok(DataType::String)))
        .map(|name| name.to_string())
        .collect()
}

/// Decides which columns to operate on for a DataFrame based on the detection
/// mode and any scoped columns.
fn detection_columns_df(
    df: &DataFrame,
    mode: DetectionMode,
    scoped_columns: &[String],
) -> Vec<String> {
    let all = string_columns_df(df);
    let all_set: HashSet<_> = all.iter().cloned().collect();

    let from_scoped: Vec<String> = scoped_columns
        .iter()
        .filter(|c| all_set.contains(*c))
        .cloned()
        .collect();

    match mode {
        DetectionMode::Auto => all,
        DetectionMode::Columns => from_scoped,
        DetectionMode::AutoOrColumns => {
            if from_scoped.is_empty() {
                all
            } else {
                from_scoped
            }
        }
    }
}

/// Collect all non-null string values for a column as owned `String`s.
fn column_non_null_values_df(df: &DataFrame, column: &str) -> Result<Vec<String>, DtooError> {
    let series = df.column(column).map_err(|e| DtooError::Config {
        message: format!("column `{column}` not found in DataFrame: {e}"),
    })?;
    let ca = series
        .cast(&DataType::String)
        .map_err(|e| DtooError::Config {
            message: format!("failed to cast column `{column}` to String: {e}"),
        })?;
    let str_ca = ca.str().map_err(|e| DtooError::Config {
        message: format!("failed to access string column `{column}`: {e}"),
    })?;
    let values: Vec<String> = str_ca.iter().flatten().map(|s| s.to_string()).collect();
    Ok(values)
}

/// Replace every occurrence of `from_value` with `to_value` in the named column,
/// mutating `df` in place.
fn replace_column_value_df(
    df: &mut DataFrame,
    column: &str,
    from_value: &str,
    to_value: &str,
) -> Result<(), DtooError> {
    let series = df.column(column).map_err(|e| DtooError::Config {
        message: format!("column `{column}` not found: {e}"),
    })?;
    let ca = series.str().map_err(|e| DtooError::Config {
        message: format!("column `{column}` is not a String column: {e}"),
    })?;
    let new_ca: StringChunked = ca
        .iter()
        .map(|opt| match opt {
            Some(v) if v == from_value => Some(to_value),
            other => other,
        })
        .collect();
    df.replace(column, new_ca.with_name(column.into()).into_column())
        .map(|_| ())
        .map_err(|e| DtooError::Config {
            message: format!("failed to replace column `{column}` in DataFrame: {e}"),
        })
}

/// Scans the String-typed columns of `df` (filtered by `detection.mode` and
/// `scoped_columns`) and returns per-column discovery counts.
pub fn discover_wrapped_values_df(
    df: &DataFrame,
    detection: &DetectionConfig,
    scoped_columns: &[String],
) -> Result<Vec<CryptoDiscoverRow>, DtooError> {
    let columns = detection_columns_df(df, detection.mode, scoped_columns);
    let mut rows = Vec::with_capacity(columns.len());

    for column in columns {
        let values = column_non_null_values_df(df, &column)?;
        let total = values.len();
        let encrypted = values
            .iter()
            .filter(|value| detect_wrapped_value(value, detection))
            .count();
        rows.push(CryptoDiscoverRow {
            column,
            total,
            encrypted,
        });
    }

    Ok(rows)
}

/// Decrypts every detected-wrapped value in the eligible String columns of `df`
/// and returns the updated DataFrame together with a `CryptoProcessResult`
/// listing which columns were modified.
pub fn decrypt_dataframe(
    mut df: DataFrame,
    profile: &CryptoProfile,
) -> Result<(DataFrame, CryptoProcessResult), DtooError> {
    validate_detection_config(&profile.detection)?;

    if profile.scheme.scheme_type == SchemeType::LegacyAes128EcbSha1 {
        eprintln!(
            "Warning: legacy_aes128_ecb_sha1 uses AES-ECB with no integrity protection. Use aes256_gcm_base64 for new data."
        );
    }

    let columns = detection_columns_df(&df, profile.detection.mode, &profile.columns);
    let scheme = scheme_impl(profile.scheme.scheme_type);
    let key_material = load_key_material(profile)?;

    let mut decrypted_columns = Vec::new();

    for column in columns {
        let values = column_non_null_values_df(&df, &column)?;
        let mut changed = false;

        for original in values {
            if !detect_wrapped_value(&original, &profile.detection) {
                continue;
            }
            let Some(inner) = strip_wrapper(&original, &profile.detection) else {
                continue;
            };
            if !scheme.detect_inner(inner) {
                continue;
            }

            match scheme.decrypt_inner(inner, &key_material) {
                Ok(plaintext) => {
                    if plaintext != original {
                        replace_column_value_df(&mut df, &column, &original, &plaintext)?;
                        changed = true;
                    }
                }
                Err(err) => {
                    if profile.failure_mode == FailureMode::Strict {
                        return Err(DtooError::Config {
                            message: format!(
                                "decryption failed for column `{column}` using profile `{}`: {err}",
                                profile.name
                            ),
                        });
                    }
                    eprintln!(
                        "Warning: decryption failed in column `{column}` using profile `{}`: {err}",
                        profile.name
                    );
                }
            }
        }

        if changed {
            decrypted_columns.push(column);
        }
    }

    Ok((df, CryptoProcessResult { decrypted_columns }))
}

/// Encrypts every non-wrapped plaintext value in the specified `columns` of
/// `df` and returns the updated DataFrame.
pub fn encrypt_dataframe(
    mut df: DataFrame,
    profile: &CryptoProfile,
    columns: &[String],
) -> Result<DataFrame, DtooError> {
    if columns.is_empty() {
        return Ok(df);
    }

    validate_detection_config(&profile.detection)?;
    let available = string_columns_df(&df);
    let available_set: HashSet<_> = available.iter().cloned().collect();
    for column in columns {
        if !available_set.contains(column) {
            return Err(DtooError::Config {
                message: format!("encrypt column `{column}` not found or not string-like"),
            });
        }
    }

    let scheme = scheme_impl(profile.scheme.scheme_type);
    let key_material = load_key_material(profile)?;

    for column in columns {
        let values = column_non_null_values_df(&df, column)?;
        for plaintext in values {
            if detect_wrapped_value(&plaintext, &profile.detection) {
                continue;
            }
            let inner = scheme.encrypt_inner(&plaintext, &key_material)?;
            let wrapped = wrap_inner(&inner, &profile.detection);
            replace_column_value_df(&mut df, column, &plaintext, &wrapped)?;
        }
    }

    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_roundtrip() {
        let scheme = LegacyAes128EcbSha1Scheme;
        let key = "legacy-key-code";
        let inner = scheme.encrypt_inner("hello", key).expect("encrypt");
        let plain = scheme.decrypt_inner(&inner, key).expect("decrypt");
        assert_eq!(plain, "hello");
    }

    #[test]
    fn modern_roundtrip() {
        let scheme = Aes256GcmBase64Scheme;
        let key_b64 = STANDARD.encode([7u8; 32]);
        let inner = scheme
            .encrypt_inner("hello-modern", &key_b64)
            .expect("encrypt");
        assert!(inner.starts_with("v1:gcm:"));
        let plain = scheme.decrypt_inner(&inner, &key_b64).expect("decrypt");
        assert_eq!(plain, "hello-modern");
    }

    #[test]
    fn wrapper_detection() {
        let detection = DetectionConfig::default();
        assert!(detect_wrapped_value("en{abcdef0123456789}cr", &detection));
        assert!(!detect_wrapped_value("plain", &detection));
    }

    #[test]
    fn output_safety_blocks_plaintext_by_default() {
        let err = enforce_output_safety(&["email".to_string()], false, false)
            .expect_err("should block plaintext");
        assert!(err.to_string().contains("Decrypted columns"));
    }

    #[test]
    fn output_safety_allows_reencrypt_or_explicit_opt_in() {
        enforce_output_safety(&["email".to_string()], true, false).expect("explicit allow");
        enforce_output_safety(&["email".to_string()], false, true).expect("reencrypt path");
    }

    // -----------------------------------------------------------------------
    // DataFrame-based tests
    // -----------------------------------------------------------------------

    /// Build a `CryptoProfile` for the legacy scheme using a test key set via
    /// environment variable. The env var name is unique to these tests so it
    /// does not collide with other tests.
    fn legacy_test_profile() -> CryptoProfile {
        let key = "test-legacy-key";
        // SAFETY: single-threaded test environment; no concurrent env reads.
        unsafe { std::env::set_var("DTOO_TEST_LEGACY_KEY", key) };
        CryptoProfile {
            name: "test_legacy".to_string(),
            detection: DetectionConfig::default(),
            scheme: SchemeConfig {
                scheme_type: SchemeType::LegacyAes128EcbSha1,
                key_env: "DTOO_TEST_LEGACY_KEY".to_string(),
            },
            columns: vec![],
            failure_mode: FailureMode::BestEffort,
            output: OutputSafety {
                allow_plaintext: true,
            },
        }
    }

    /// Build a `CryptoProfile` for the modern AES-256-GCM scheme.
    fn modern_test_profile() -> CryptoProfile {
        let key_b64 = STANDARD.encode([0xABu8; 32]);
        // SAFETY: single-threaded test environment; no concurrent env reads.
        unsafe { std::env::set_var("DTOO_TEST_MODERN_KEY", &key_b64) };
        CryptoProfile {
            name: "test_modern".to_string(),
            detection: DetectionConfig::default(),
            scheme: SchemeConfig {
                scheme_type: SchemeType::Aes256GcmBase64,
                key_env: "DTOO_TEST_MODERN_KEY".to_string(),
            },
            columns: vec![],
            failure_mode: FailureMode::BestEffort,
            output: OutputSafety {
                allow_plaintext: true,
            },
        }
    }

    #[test]
    fn df_string_columns_only_string_dtype() {
        let df = df![
            "name" => ["alice", "bob"],
            "age"  => [30i32, 25],
        ]
        .unwrap();
        let cols = string_columns_df(&df);
        assert_eq!(cols, vec!["name".to_string()]);
    }

    #[test]
    fn df_column_non_null_values_filters_nulls() {
        let df = df![
            "col" => [Some("a"), None, Some("b")]
        ]
        .unwrap();
        let vals = column_non_null_values_df(&df, "col").unwrap();
        assert_eq!(vals, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn df_replace_column_value_replaces_matching() {
        let mut df = df!["col" => ["hello", "world", "hello"]].unwrap();
        replace_column_value_df(&mut df, "col", "hello", "HELLO").unwrap();
        let vals: Vec<Option<&str>> = df.column("col").unwrap().str().unwrap().iter().collect();
        assert_eq!(vals, vec![Some("HELLO"), Some("world"), Some("HELLO")]);
    }

    #[test]
    fn df_discover_wrapped_values_counts_correctly() {
        let profile = legacy_test_profile();
        let scheme = LegacyAes128EcbSha1Scheme;
        let key = "test-legacy-key";
        let inner = scheme.encrypt_inner("secret", key).unwrap();
        let wrapped = wrap_inner(&inner, &profile.detection);

        let df = df![
            "email" => [wrapped.as_str(), "plain@example.com", "other-plain"],
        ]
        .unwrap();

        let rows = discover_wrapped_values_df(&df, &profile.detection, &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].column, "email");
        assert_eq!(rows[0].total, 3);
        assert_eq!(rows[0].encrypted, 1);
    }

    #[test]
    fn df_discover_scoped_columns_mode() {
        let profile = modern_test_profile();
        let scheme = Aes256GcmBase64Scheme;
        let key_b64 = STANDARD.encode([0xABu8; 32]);
        let inner = scheme.encrypt_inner("data", &key_b64).unwrap();
        let wrapped = wrap_inner(&inner, &profile.detection);

        let df = df![
            "col_a" => [wrapped.as_str(), "plain"],
            "col_b" => ["also_plain", "also_plain2"],
        ]
        .unwrap();

        // Mode::Columns with scoped_columns=["col_a"] — only col_a examined
        let scoped = vec!["col_a".to_string()];
        let detection = DetectionConfig {
            mode: DetectionMode::Columns,
            ..DetectionConfig::default()
        };
        let rows = discover_wrapped_values_df(&df, &detection, &scoped).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].column, "col_a");
        assert_eq!(rows[0].encrypted, 1);
    }

    #[test]
    fn df_decrypt_dataframe_legacy_roundtrip() {
        let profile = legacy_test_profile();
        let scheme = LegacyAes128EcbSha1Scheme;
        let key = "test-legacy-key";
        let inner = scheme.encrypt_inner("alice@example.com", key).unwrap();
        let wrapped = wrap_inner(&inner, &profile.detection);

        let df = df![
            "email" => [wrapped.as_str(), "plain@example.com"],
        ]
        .unwrap();

        let (df_out, result) = decrypt_dataframe(df, &profile).unwrap();
        assert_eq!(result.decrypted_columns, vec!["email".to_string()]);

        let vals: Vec<Option<&str>> = df_out
            .column("email")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(
            vals,
            vec![Some("alice@example.com"), Some("plain@example.com")]
        );
    }

    #[test]
    fn df_decrypt_dataframe_modern_roundtrip() {
        let profile = modern_test_profile();
        let scheme = Aes256GcmBase64Scheme;
        let key_b64 = STANDARD.encode([0xABu8; 32]);
        let inner = scheme.encrypt_inner("secret-data", &key_b64).unwrap();
        let wrapped = wrap_inner(&inner, &profile.detection);

        let df = df![
            "payload" => [wrapped.as_str(), "unchanged"],
        ]
        .unwrap();

        let (df_out, result) = decrypt_dataframe(df, &profile).unwrap();
        assert_eq!(result.decrypted_columns, vec!["payload".to_string()]);

        let vals: Vec<Option<&str>> = df_out
            .column("payload")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(vals, vec![Some("secret-data"), Some("unchanged")]);
    }

    #[test]
    fn df_decrypt_no_change_when_no_wrapped_values() {
        let profile = legacy_test_profile();
        let df = df!["col" => ["plain", "also_plain"]].unwrap();
        let (_, result) = decrypt_dataframe(df, &profile).unwrap();
        assert!(
            result.decrypted_columns.is_empty(),
            "no columns should be listed as decrypted"
        );
    }

    #[test]
    fn df_decrypt_strict_mode_errors_on_bad_ciphertext() {
        // Build a profile with strict failure mode
        let key = "strict-key";
        // SAFETY: single-threaded test environment; no concurrent env reads.
        unsafe { std::env::set_var("DTOO_TEST_STRICT_KEY", key) };
        let profile = CryptoProfile {
            name: "strict".to_string(),
            detection: DetectionConfig::default(),
            scheme: SchemeConfig {
                scheme_type: SchemeType::LegacyAes128EcbSha1,
                key_env: "DTOO_TEST_STRICT_KEY".to_string(),
            },
            columns: vec![],
            failure_mode: FailureMode::Strict,
            output: OutputSafety {
                allow_plaintext: true,
            },
        };

        // Manufacture a value that looks wrapped but has garbage ciphertext
        // (valid base64 but cannot be AES-decrypted cleanly under any key).
        // 16 bytes of zeros will decrypt to something but PKCS7 unpadding
        // will almost certainly fail because the decrypted bytes are unlikely
        // to have a valid pad byte (0x10 = 16 repeated 16 times).
        // Use all-0xFF bytes: when decrypted under "strict-key", the result
        // will not have valid PKCS7 padding (pad byte 0xFF means 255 padding
        // bytes, which is impossible in a 16-byte block).
        let garbage_inner = STANDARD.encode([0xFFu8; 32]);
        let wrapped = format!(
            "{}{}{}",
            profile.detection.start_marker, garbage_inner, profile.detection.end_marker
        );

        let df = df!["col" => [wrapped.as_str()]].unwrap();
        let err = decrypt_dataframe(df, &profile).unwrap_err();
        assert!(err.to_string().contains("decryption failed"));
    }

    #[test]
    fn df_encrypt_dataframe_legacy_roundtrip() {
        let profile = legacy_test_profile();
        let df = df!["email" => ["alice@example.com", "bob@example.com"]].unwrap();
        let columns = vec!["email".to_string()];

        let df_enc = encrypt_dataframe(df, &profile, &columns).unwrap();

        // Every value should now be wrapped
        let detection = &profile.detection;
        let vals: Vec<Option<&str>> = df_enc
            .column("email")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .collect();
        for val in &vals {
            let v = val.unwrap();
            assert!(
                detect_wrapped_value(v, detection),
                "expected wrapped value, got: {v}"
            );
        }

        // Decrypt them back and confirm round-trip
        let (df_dec, result) = decrypt_dataframe(df_enc, &profile).unwrap();
        assert_eq!(result.decrypted_columns, vec!["email".to_string()]);
        let decrypted: Vec<Option<&str>> = df_dec
            .column("email")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(
            decrypted,
            vec![Some("alice@example.com"), Some("bob@example.com")]
        );
    }

    #[test]
    fn df_encrypt_dataframe_modern_roundtrip() {
        let profile = modern_test_profile();
        let df = df!["secret" => ["value1", "value2"]].unwrap();
        let columns = vec!["secret".to_string()];

        let df_enc = encrypt_dataframe(df, &profile, &columns).unwrap();
        let (df_dec, result) = decrypt_dataframe(df_enc, &profile).unwrap();
        assert_eq!(result.decrypted_columns, vec!["secret".to_string()]);
        let vals: Vec<Option<&str>> = df_dec
            .column("secret")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(vals, vec![Some("value1"), Some("value2")]);
    }

    #[test]
    fn df_encrypt_skips_already_wrapped_values() {
        let profile = legacy_test_profile();
        let scheme = LegacyAes128EcbSha1Scheme;
        let key = "test-legacy-key";
        let inner = scheme.encrypt_inner("already", key).unwrap();
        let wrapped = wrap_inner(&inner, &profile.detection);
        let wrapped_clone = wrapped.clone();

        let df = df!["col" => [wrapped.as_str(), "plaintext"]].unwrap();
        let columns = vec!["col".to_string()];
        let df_enc = encrypt_dataframe(df, &profile, &columns).unwrap();

        let vals: Vec<Option<&str>> = df_enc
            .column("col")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .collect();
        // The already-wrapped value should be unchanged (same bytes)
        assert_eq!(vals[0], Some(wrapped_clone.as_str()));
        // The plaintext should now be wrapped
        assert!(detect_wrapped_value(vals[1].unwrap(), &profile.detection));
    }

    #[test]
    fn df_encrypt_unknown_column_errors() {
        let profile = legacy_test_profile();
        let df = df!["col" => ["value"]].unwrap();
        let err = encrypt_dataframe(df, &profile, &["nonexistent".to_string()]).unwrap_err();
        assert!(err.to_string().contains("not found or not string-like"));
    }

    #[test]
    fn df_encrypt_empty_columns_is_noop() {
        let profile = legacy_test_profile();
        let df = df!["col" => ["value"]].unwrap();
        let df_out = encrypt_dataframe(df.clone(), &profile, &[]).unwrap();
        // shape unchanged
        assert_eq!(df_out.shape(), df.shape());
    }
}
