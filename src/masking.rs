use std::collections::HashSet;

use polars::prelude::*;
use sha2::{Digest, Sha256};

use crate::error::DtooError;

/// Replace each selected column's non-null values with `hex(sha256("{salt}:{col}:" + value))`.
///
/// NULLs are preserved. Unknown column names produce a [`DtooError::Config`].
pub fn mask_dataframe(
    mut df: DataFrame,
    columns: &[String],
    salt: &str,
) -> Result<DataFrame, DtooError> {
    if columns.is_empty() {
        return Ok(df);
    }
    let available: HashSet<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    for column in columns {
        if !available.contains(column) {
            let mut sorted: Vec<String> = available.iter().cloned().collect();
            sorted.sort();
            return Err(DtooError::Config {
                message: format!(
                    "mask column `{column}` not found. available columns: {}",
                    sorted.join(", ")
                ),
            });
        }
        let prefix = format!("{salt}:{column}:");
        let as_str = df
            .column(column)
            .map_err(|e| DtooError::Config {
                message: e.to_string(),
            })?
            .cast(&DataType::String)
            .map_err(|e| DtooError::Config {
                message: e.to_string(),
            })?;
        let chunked = as_str.str().map_err(|e| DtooError::Config {
            message: e.to_string(),
        })?;
        let masked: StringChunked = chunked
            .iter()
            .map(|opt| {
                opt.map(|v| {
                    let mut h = Sha256::new();
                    h.update(prefix.as_bytes());
                    h.update(v.as_bytes());
                    hex::encode(h.finalize())
                })
            })
            .collect();
        df.replace(column, masked.with_name(column.into()).into_column())
            .map_err(|e| DtooError::Config {
                message: e.to_string(),
            })?;
    }
    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_columns_is_deterministic_and_preserves_null() {
        use polars::prelude::*;
        let df = df![
            "email" => [Some("a@example.com"), Some("a@example.com"), None]
        ]
        .unwrap();
        let out = mask_dataframe(df, &["email".to_string()], "project-x").unwrap();
        let binding = out.column("email").unwrap();
        let col = binding.str().unwrap();
        assert_eq!(col.get(0), col.get(1)); // deterministic
        assert!(col.get(2).is_none()); // null preserved
        assert_ne!(col.get(0), Some("a@example.com")); // actually hashed
    }

    #[test]
    fn mask_dataframe_unknown_column_errors() {
        use polars::prelude::*;
        let df = df!["email" => ["x"]].unwrap();
        let err = mask_dataframe(df, &["missing".to_string()], "").unwrap_err();
        assert!(matches!(err, DtooError::Config { .. }));
    }
}
