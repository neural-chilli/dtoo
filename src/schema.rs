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

/// Parse a Polars dtype name from a `--schema` file into a [`DataType`].
///
/// Type names are the Polars dtype names (case-insensitive): `Int8`/`Int16`/`Int32`/
/// `Int64`, `UInt8`/`UInt16`/`UInt32`/`UInt64`, `Float32`/`Float64`, `Boolean`,
/// `String`, `Date`, `Datetime`, `Time`, and `Decimal(p,s)`. A bare `Decimal`
/// defaults to `Decimal(18, 3)`. Returns [`DtooError::Schema`] for unknown names.
pub fn parse_schema_type(data_type: &str) -> Result<DataType, DtooError> {
    let upper = data_type.trim().to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or("").trim();
    let dt = match base {
        "INT8" => DataType::Int8,
        "INT16" => DataType::Int16,
        "INT32" => DataType::Int32,
        "INT64" => DataType::Int64,
        "UINT8" => DataType::UInt8,
        "UINT16" => DataType::UInt16,
        "UINT32" => DataType::UInt32,
        "UINT64" => DataType::UInt64,
        "FLOAT32" => DataType::Float32,
        "FLOAT64" => DataType::Float64,
        "BOOLEAN" => DataType::Boolean,
        "STRING" => DataType::String,
        "DATE" => DataType::Date,
        "DATETIME" => DataType::Datetime(TimeUnit::Microseconds, None),
        "TIME" => DataType::Time,
        "DECIMAL" => {
            let (p, s) = parse_decimal_params(&upper);
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
        let dt = parse_schema_type(&c.data_type)?;
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

        if !is_valid_column_identifier(name) {
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

fn is_valid_column_identifier(name: &str) -> bool {
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
    fn schema_type_maps_to_polars() {
        use polars::prelude::*;
        assert_eq!(parse_schema_type("Int32").unwrap(), DataType::Int32);
        assert_eq!(parse_schema_type("Int64").unwrap(), DataType::Int64);
        assert_eq!(parse_schema_type("String").unwrap(), DataType::String);
        assert_eq!(parse_schema_type("Boolean").unwrap(), DataType::Boolean);
        assert_eq!(parse_schema_type("Float64").unwrap(), DataType::Float64);
        // Case-insensitive.
        assert_eq!(parse_schema_type("int64").unwrap(), DataType::Int64);
        // DuckDB/SQL names are no longer accepted.
        assert!(parse_schema_type("VARCHAR").is_err());
        assert!(parse_schema_type("INTEGER").is_err());
        assert!(parse_schema_type("NOPE_TYPE").is_err());
    }

    #[test]
    fn coerce_projects_casts_and_nulls_missing() {
        use crate::types::SchemaColumn;
        use polars::prelude::*;
        let lf = df!["id" => ["1"], "extra" => ["x"]].unwrap().lazy();
        let cols = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int32".into(),
            },
            SchemaColumn {
                name: "name".into(),
                data_type: "String".into(),
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
            data_type: "String".into(),
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
        // Bare Decimal (no params) defaults to Decimal(18,3).
        assert_eq!(
            parse_schema_type("Decimal").unwrap(),
            DataType::Decimal(18, 3),
            "bare Decimal must map to Decimal(18,3)"
        );
        // Explicit Decimal(10,2) must not be overridden.
        assert_eq!(
            parse_schema_type("Decimal(10,2)").unwrap(),
            DataType::Decimal(10, 2),
            "explicit Decimal(10,2) must map to Decimal(10,2)"
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
