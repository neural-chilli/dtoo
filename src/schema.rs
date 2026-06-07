use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

use polars::prelude::{DataType, Expr, LazyFrame, LiteralValue, PlSmallStr, TimeUnit, col, lit};
use serde::Deserialize;

use crate::{error::DtooError, types::SchemaColumn};

/// Handles auto-detected or explicit schema setup for the result set.
#[derive(Debug, Clone)]
pub struct SchemaManager {
    mode: SchemaMode,
}

/// Schema operation mode.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SchemaMode {
    Auto,
    Explicit(ExplicitSchema),
}

/// Explicit YAML schema definition.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExplicitSchema {
    pub columns: Vec<SchemaColumn>,
}

#[derive(Debug, Deserialize)]
struct SchemaFile {
    columns: Vec<SchemaFileColumn>,
}

#[derive(Debug, Deserialize)]
struct SchemaFileColumn {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
}

impl SchemaManager {
    /// Build manager from optional schema file path.
    pub fn from_schema_path(path: Option<&Path>) -> Result<Self, DtooError> {
        match path {
            None => Ok(Self {
                mode: SchemaMode::Auto,
            }),
            Some(path) => Ok(Self {
                mode: SchemaMode::Explicit(load_explicit_schema(path)?),
            }),
        }
    }

    /// Returns selected schema mode.
    pub fn mode(&self) -> &SchemaMode {
        &self.mode
    }
}

/// Map a DuckDB-style type string to a Polars [`DataType`].
///
/// Case-insensitive; tolerates `DECIMAL(p,s)` and `NUMERIC(p,s)` parameterised forms.
/// Returns [`DtooError::Schema`] for unknown or unsupported type names.
pub fn duckdb_type_to_polars(data_type: &str) -> Result<DataType, DtooError> {
    let upper = data_type.trim().to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or("").trim();
    let dt = match base {
        "INTEGER" | "INT" | "INT4" => DataType::Int32,
        "BIGINT" | "INT8" | "LONG" => DataType::Int64,
        "SMALLINT" | "INT2" => DataType::Int16,
        "TINYINT" => DataType::Int8,
        "DOUBLE" | "FLOAT8" => DataType::Float64,
        "REAL" | "FLOAT" | "FLOAT4" => DataType::Float32,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" => DataType::String,
        "DATE" => DataType::Date,
        "TIMESTAMP" | "DATETIME" => DataType::Datetime(TimeUnit::Microseconds, None),
        "DECIMAL" | "NUMERIC" => {
            let (p, s) = parse_decimal_params(&upper);
            // DuckDB bare DECIMAL defaults to DECIMAL(18,3); explicit params override.
            DataType::Decimal(p.unwrap_or(18), s.unwrap_or(3))
        }
        _ => {
            return Err(DtooError::Schema {
                message: format!("unsupported schema type `{data_type}`"),
            });
        }
    };
    Ok(dt)
}

fn parse_decimal_params(upper: &str) -> (Option<usize>, Option<usize>) {
    if let Some(open) = upper.find('(')
        && let Some(close) = upper[open..].find(')')
    {
        let inner = &upper[open + 1..open + close];
        let mut parts = inner.split(',').map(|x| x.trim().parse::<usize>().ok());
        let p = parts.next().flatten();
        let s = parts.next().flatten();
        return (p, s);
    }
    (None, None)
}

/// Project a [`LazyFrame`] to the declared columns, casting present columns to
/// declared Polars types and filling absent columns with typed `NULL`s.
///
/// - Output columns appear in exactly the declared order.
/// - Source columns not present in `columns` are dropped.
/// - A declared column absent from the source produces a `NULL` column of the declared type.
pub fn coerce_to_schema(lf: LazyFrame, columns: &[SchemaColumn]) -> Result<LazyFrame, DtooError> {
    let schema = lf.clone().collect_schema().map_err(|e| DtooError::Schema {
        message: e.to_string(),
    })?;

    // Build a map from lowercased source column name → actual source column name
    // so that declared columns match source columns case-insensitively (parity
    // with `projected_query_for_schema` which lowercases both sides).
    let lower_to_actual: HashMap<String, String> = schema
        .iter_names()
        .map(|n: &PlSmallStr| (n.to_ascii_lowercase(), n.to_string()))
        .collect();

    let mut exprs: Vec<Expr> = Vec::with_capacity(columns.len());
    for c in columns {
        let dt = duckdb_type_to_polars(&c.data_type)?;
        let e = if let Some(actual) = lower_to_actual.get(&c.name.to_ascii_lowercase()) {
            col(actual.as_str()).cast(dt).alias(c.name.as_str())
        } else {
            lit(LiteralValue::untyped_null())
                .cast(dt)
                .alias(c.name.as_str())
        };
        exprs.push(e);
    }
    Ok(lf.select(exprs))
}

fn load_explicit_schema(path: &Path) -> Result<ExplicitSchema, DtooError> {
    if !path.exists() {
        return Err(DtooError::Config {
            message: format!("schema file not found: {}", path.display()),
        });
    }

    let contents = fs::read_to_string(path).map_err(|source| DtooError::FileRead {
        path: path.display().to_string(),
        source: Box::new(source),
    })?;

    let parsed: SchemaFile =
        serde_yaml::from_str(&contents).map_err(|source| DtooError::Config {
            message: format!("invalid schema YAML at {}: {source}", path.display()),
        })?;

    if parsed.columns.is_empty() {
        return Err(DtooError::Schema {
            message: "schema must include at least one column".to_string(),
        });
    }

    let mut seen = HashSet::new();
    let mut columns = Vec::with_capacity(parsed.columns.len());

    for col in parsed.columns {
        let name = col.name.trim();
        let data_type = col.data_type.trim();

        if name.is_empty() || data_type.is_empty() {
            return Err(DtooError::Schema {
                message: "every schema column requires non-empty `name` and `type`".to_string(),
            });
        }

        if !is_valid_duckdb_identifier(name) {
            return Err(DtooError::Schema {
                message: format!("invalid column name `{name}`"),
            });
        }

        let lowered = name.to_ascii_lowercase();
        if !seen.insert(lowered) {
            return Err(DtooError::Schema {
                message: format!("duplicate schema column `{name}`"),
            });
        }

        columns.push(SchemaColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
        });
    }

    Ok(ExplicitSchema { columns })
}

fn is_valid_duckdb_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn invalid_yaml_returns_config_error() {
        let schema_file = temp_schema("columns: [\n");
        let err = SchemaManager::from_schema_path(Some(&schema_file)).expect_err("should fail");
        assert!(matches!(err, DtooError::Config { .. }));
        let _ = fs::remove_file(schema_file);
    }

    #[test]
    fn duplicate_columns_rejected() {
        let schema_file = temp_schema(
            "columns:\n  - name: id\n    type: INTEGER\n  - name: ID\n    type: BIGINT\n",
        );
        let err = SchemaManager::from_schema_path(Some(&schema_file)).expect_err("should fail");
        assert!(matches!(err, DtooError::Schema { .. }));
        let _ = fs::remove_file(schema_file);
    }

    #[test]
    fn invalid_column_identifier_rejected() {
        let schema_file = temp_schema("columns:\n  - name: 9id\n    type: INTEGER\n");
        let err = SchemaManager::from_schema_path(Some(&schema_file)).expect_err("should fail");
        assert!(matches!(err, DtooError::Schema { .. }));
        let _ = fs::remove_file(schema_file);
    }

    #[test]
    fn duckdb_type_maps_to_polars() {
        use polars::prelude::*;
        assert_eq!(duckdb_type_to_polars("INTEGER").unwrap(), DataType::Int32);
        assert_eq!(duckdb_type_to_polars("BIGINT").unwrap(), DataType::Int64);
        assert_eq!(duckdb_type_to_polars("VARCHAR").unwrap(), DataType::String);
        assert_eq!(duckdb_type_to_polars("BOOLEAN").unwrap(), DataType::Boolean);
        assert!(duckdb_type_to_polars("NOPE_TYPE").is_err());
    }

    #[test]
    fn coerce_projects_casts_and_nulls_missing() {
        use crate::types::SchemaColumn;
        use polars::prelude::*;
        let lf = df!["id" => ["1"], "extra" => ["x"]].unwrap().lazy();
        let cols = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "INTEGER".into(),
            },
            SchemaColumn {
                name: "name".into(),
                data_type: "VARCHAR".into(),
            },
        ];
        let out = coerce_to_schema(lf, &cols).unwrap().collect().unwrap();
        assert_eq!(
            out.get_column_names()
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name"]
        ); // declared order, extra dropped
        assert_eq!(out.column("id").unwrap().dtype(), &DataType::Int32);
        assert_eq!(out.column("name").unwrap().null_count(), 1); // missing -> null
    }

    #[test]
    fn coerce_matches_source_columns_case_insensitively() {
        // Source has "Name" (capital N); declared schema uses "name" (lowercase).
        // Must match case-insensitively and produce "alice", not NULL.
        use crate::types::SchemaColumn;
        use polars::prelude::*;
        let lf = df!["Name" => ["alice"]].unwrap().lazy();
        let cols = vec![SchemaColumn {
            name: "name".into(),
            data_type: "VARCHAR".into(),
        }];
        let out = coerce_to_schema(lf, &cols).unwrap().collect().unwrap();
        let col_name = out.column("name").expect("output column 'name' must exist");
        assert_eq!(col_name.null_count(), 0, "matched column must not be null");
        assert_eq!(
            col_name.str().unwrap().get(0).expect("first value"),
            "alice",
            "case-insensitive match must preserve source value"
        );
    }

    #[test]
    fn decimal_bare_defaults_to_18_3() {
        use polars::prelude::*;
        // Bare DECIMAL (no params) must default to DuckDB's DECIMAL(18,3).
        assert_eq!(
            duckdb_type_to_polars("DECIMAL").unwrap(),
            DataType::Decimal(18, 3),
            "bare DECIMAL must map to Decimal(18,3)"
        );
        // Explicit DECIMAL(10,2) must not be overridden.
        assert_eq!(
            duckdb_type_to_polars("DECIMAL(10,2)").unwrap(),
            DataType::Decimal(10, 2),
            "explicit DECIMAL(10,2) must map to Decimal(10,2)"
        );
    }

    fn temp_schema(contents: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("dtoo-schema-{}.yaml", unique_suffix()));
        fs::write(&path, contents).expect("write schema file");
        path
    }

    fn unique_suffix() -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let counter = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{nanos}-{counter}")
    }
}
