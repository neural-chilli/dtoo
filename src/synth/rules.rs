//! Intra-row rules: constraint filtering and derived columns via Polars SQL.
#![allow(dead_code)] // consumed by Task 14

use polars::prelude::*;

use crate::{error::DtooError, polars_engine::PolarsEngine};

fn config_err(message: String) -> DtooError {
    DtooError::Config { message }
}

/// Splits a derive rule `name = expr` on the FIRST `=`.
pub fn parse_derive(rule: &str) -> Result<(String, String), DtooError> {
    let Some(pos) = rule.find('=') else {
        return Err(config_err(format!(
            "derive rule `{rule}` must be in `column = expression` form"
        )));
    };
    let name = rule[..pos].trim().to_string();
    let expr = rule[pos + 1..].trim().to_string();
    if name.is_empty() || expr.is_empty() {
        return Err(config_err(format!(
            "derive rule `{rule}` must be in `column = expression` form"
        )));
    }
    Ok((name, expr))
}

/// Filters a batch by ALL constraints (ANDed) and returns per-constraint
/// individual pass counts (each evaluated against the full input batch).
pub fn filter_constraints(
    engine: &PolarsEngine,
    df: DataFrame,
    constraints: &[String],
) -> Result<(DataFrame, Vec<(String, usize)>), DtooError> {
    let mut counts = Vec::with_capacity(constraints.len());
    for c in constraints {
        let sql = format!("SELECT * FROM _ WHERE ({c})");
        let passed = engine
            .collect(engine.run_sql(df.clone().lazy(), &[], &sql)?)?
            .height();
        counts.push((c.clone(), passed));
    }
    let combined = constraints
        .iter()
        .map(|c| format!("({c})"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!("SELECT * FROM _ WHERE {combined}");
    let kept = engine.collect(engine.run_sql(df.lazy(), &[], &sql)?)?;
    Ok((kept, counts))
}

/// Applies derive rules in order. Existing columns are replaced.
pub fn apply_derives(
    engine: &PolarsEngine,
    mut df: DataFrame,
    derives: &[String],
) -> Result<DataFrame, DtooError> {
    for rule in derives {
        let (name, expr) = parse_derive(rule)?;
        if df.get_column_names().iter().any(|c| c.as_str() == name) {
            df = df
                .drop(&name)
                .map_err(|e| config_err(format!("derive `{rule}`: {e}")))?;
        }
        let sql = format!("SELECT *, ({expr}) AS \"{name}\" FROM _");
        df = engine.collect(engine.run_sql(df.lazy(), &[], &sql)?)?;
    }
    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_derive_splits_on_first_equals() {
        let (name, expr) = parse_derive("total = amount * 2").expect("parse");
        assert_eq!(name, "total");
        assert_eq!(expr, "amount * 2");
        assert!(parse_derive("no equals here").is_err());
        assert!(parse_derive("= expr").is_err());
    }

    #[test]
    fn constraints_filter_and_count_per_rule() {
        let engine = crate::polars_engine::PolarsEngine::new();
        let df = df!["a" => [1i64, -2, 3, -4, 5]].unwrap();
        let constraints = vec!["a > 0".to_string()];
        let (kept, counts) = filter_constraints(&engine, df, &constraints).expect("filter");
        assert_eq!(kept.height(), 3);
        assert_eq!(counts, vec![("a > 0".to_string(), 3usize)]);
    }

    #[test]
    fn derive_adds_and_replaces_columns() {
        let engine = crate::polars_engine::PolarsEngine::new();
        let df = df!["amount" => [2i64, 3], "total" => [0i64, 0]].unwrap();
        let derives = vec![
            "total = amount * 2".to_string(),
            "flag = amount > 2".to_string(),
        ];
        let out = apply_derives(&engine, df, &derives).expect("derive");
        let totals: Vec<i64> = out
            .column("total")
            .unwrap()
            .i64()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert_eq!(totals, vec![4, 6]);
        assert_eq!(out.column("flag").unwrap().dtype(), &DataType::Boolean);
    }

    #[test]
    fn bad_rule_sql_is_a_clear_error() {
        let engine = crate::polars_engine::PolarsEngine::new();
        let df = df!["a" => [1i64]].unwrap();
        let err = apply_derives(&engine, df, &["x = nonexistent_col * 2".to_string()])
            .expect_err("bad sql");
        assert!(err.to_string().contains("nonexistent_col") || err.to_string().contains("x ="));
    }
}
