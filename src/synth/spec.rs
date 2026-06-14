//! Synth spec (YAML): schema, validation, and generation ordering.

// Items are consumed by future tasks; suppress dead_code until then.
#![allow(dead_code)]

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::error::DtooError;

/// Top-level synth spec describing a family of tables to generate.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SynthSpec {
    #[serde(default)]
    pub seed: u64,
    pub tables: BTreeMap<String, TableSpec>,
}

/// One table: its source profile, target size, structure, and output.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSpec {
    pub profile: PathBuf,
    pub rows: usize,
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeySpec>,
    #[serde(default)]
    pub rules: Vec<RuleSpec>,
    pub output: PathBuf,
    #[serde(default)]
    pub output_format: Option<String>,
}

/// A foreign-key declaration: this table's column references parent.key.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKeySpec {
    pub column: String,
    pub references: String,
    #[serde(default)]
    pub fan_out: Option<FanOutSpec>,
}

/// Raw YAML form of fan_out: either a bare string or `{distribution: ...}`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum FanOutSpec {
    Named(String),
    Dist { distribution: String },
}

/// One intra-row rule: exactly one of `derive` / `constraint` must be set.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleSpec {
    #[serde(default)]
    pub derive: Option<String>,
    #[serde(default)]
    pub constraint: Option<String>,
}

/// Normalized fan-out mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanOut {
    FromProfile,
    Uniform,
}

/// A parsed `table.column` FK reference.
#[derive(Debug, Clone)]
pub struct FkRef {
    pub table: String,
    pub column: String,
}

fn config_err(message: String) -> DtooError {
    DtooError::Config { message }
}

/// Loads and parses a spec file. Paths inside remain relative to the spec dir.
pub fn load_spec(path: &Path) -> Result<SynthSpec, DtooError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| config_err(format!("cannot read synth spec {}: {e}", path.display())))?;
    serde_yaml::from_str(&raw)
        .map_err(|e| config_err(format!("invalid synth spec {}: {e}", path.display())))
}

/// Parses `table.column` (exactly one dot).
pub fn parse_reference(reference: &str) -> Result<FkRef, DtooError> {
    let mut parts = reference.splitn(2, '.');
    let (Some(table), Some(column)) = (parts.next(), parts.next()) else {
        return Err(config_err(format!(
            "foreign key reference `{reference}` must be in table.column form"
        )));
    };
    if table.is_empty() || column.is_empty() || column.contains('.') {
        return Err(config_err(format!(
            "foreign key reference `{reference}` must be in table.column form"
        )));
    }
    Ok(FkRef {
        table: table.to_string(),
        column: column.to_string(),
    })
}

/// Normalizes a fan_out spec value; default is FromProfile.
pub fn fan_out(fk: &ForeignKeySpec) -> Result<FanOut, DtooError> {
    let name = match &fk.fan_out {
        None => return Ok(FanOut::FromProfile),
        Some(FanOutSpec::Named(n)) => n.clone(),
        Some(FanOutSpec::Dist { distribution }) => distribution.clone(),
    };
    match name.as_str() {
        "from_profile" => Ok(FanOut::FromProfile),
        "uniform" => Ok(FanOut::Uniform),
        other => Err(config_err(format!(
            "fan_out must be `from_profile` or `uniform`, got `{other}`"
        ))),
    }
}

/// Validates cross-table references, rule shape, and basic sanity.
pub fn validate(spec: &SynthSpec) -> Result<(), DtooError> {
    if spec.tables.is_empty() {
        return Err(config_err("synth spec has no tables".to_string()));
    }
    for (name, table) in &spec.tables {
        for fk in &table.foreign_keys {
            let r = parse_reference(&fk.references)?;
            if r.table == *name {
                return Err(config_err(format!(
                    "table `{name}` has a foreign key referencing itself (`{}`); self-referential foreign keys are not supported",
                    fk.references
                )));
            }
            let Some(parent) = spec.tables.get(&r.table) else {
                return Err(config_err(format!(
                    "table `{name}` foreign key references unknown target `{}`",
                    fk.references
                )));
            };
            if !parent.keys.contains(&r.column) {
                return Err(config_err(format!(
                    "table `{name}` foreign key references `{}`, which is not listed in `{}`'s keys",
                    fk.references, r.table
                )));
            }
            if table.keys.contains(&fk.column) {
                return Err(config_err(format!(
                    "table `{name}` column `{}` is listed as both a key and a foreign key",
                    fk.column
                )));
            }
            fan_out(fk)?;
        }
        for rule in &table.rules {
            if rule.derive.is_some() == rule.constraint.is_some() {
                return Err(config_err(format!(
                    "table `{name}`: each rule must set exactly one of `derive` or `constraint`"
                )));
            }
        }
    }
    Ok(())
}

/// Topological order over FK dependencies (Kahn). Alphabetical tie-break via
/// BTreeMap iteration, so the order is deterministic. Errors on cycles.
pub fn generation_order(spec: &SynthSpec) -> Result<Vec<String>, DtooError> {
    let mut deps: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for (name, table) in &spec.tables {
        let mut parents = Vec::new();
        for fk in &table.foreign_keys {
            parents.push(parse_reference(&fk.references)?.table);
        }
        deps.insert(name, parents);
    }
    let mut order = Vec::new();
    while order.len() < spec.tables.len() {
        let mut progressed = false;
        for (name, parents) in &deps {
            if order.iter().any(|d| d == name) {
                continue;
            }
            if parents.iter().all(|p| order.iter().any(|d| d == p)) {
                order.push(name.to_string());
                progressed = true;
            }
        }
        if !progressed {
            let remaining: Vec<&str> = deps
                .keys()
                .filter(|n| !order.iter().any(|d| d == **n))
                .copied()
                .collect();
            return Err(config_err(format!(
                "foreign key dependency cycle involving: {}",
                remaining.join(", ")
            )));
        }
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn two_table_yaml() -> &'static str {
        r#"
seed: 42
tables:
  customers:
    profile: profiles/customers.json
    rows: 100
    keys: [customer_id]
    output: out/customers.parquet
  orders:
    profile: profiles/orders.json
    rows: 500
    foreign_keys:
      - column: customer_id
        references: customers.customer_id
    rules:
      - constraint: "amount > 0"
      - derive: "total = amount * 2"
    output: out/orders.csv
"#
    }

    #[test]
    fn parses_and_validates_two_table_spec() {
        let spec: SynthSpec = serde_yaml::from_str(two_table_yaml()).expect("parse");
        assert_eq!(spec.seed, 42);
        validate(&spec).expect("valid");
        let orders = &spec.tables["orders"];
        assert_eq!(orders.foreign_keys[0].references, "customers.customer_id");
        assert!(matches!(
            fan_out(&orders.foreign_keys[0]).unwrap(),
            FanOut::FromProfile
        ));
    }

    #[test]
    fn generation_order_puts_parents_first() {
        let spec: SynthSpec = serde_yaml::from_str(two_table_yaml()).expect("parse");
        assert_eq!(
            generation_order(&spec).unwrap(),
            vec!["customers", "orders"]
        );
    }

    #[test]
    fn rejects_unknown_fk_reference() {
        let yaml = r#"
tables:
  orders:
    profile: p.json
    rows: 10
    foreign_keys:
      - column: x
        references: missing.id
    output: o.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        let err = validate(&spec).expect_err("invalid");
        assert!(err.to_string().contains("missing.id"));
    }

    #[test]
    fn rejects_fk_to_non_key_column() {
        let yaml = r#"
tables:
  customers:
    profile: c.json
    rows: 10
    keys: [customer_id]
    output: c.csv
  orders:
    profile: o.json
    rows: 10
    foreign_keys:
      - column: cid
        references: customers.name
    output: o.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        let err = validate(&spec).expect_err("invalid");
        assert!(err.to_string().contains("customers.name"));
    }

    #[test]
    fn detects_dependency_cycle() {
        let yaml = r#"
tables:
  a:
    profile: a.json
    rows: 10
    keys: [id]
    foreign_keys: [{column: bid, references: b.id}]
    output: a.csv
  b:
    profile: b.json
    rows: 10
    keys: [id]
    foreign_keys: [{column: aid, references: a.id}]
    output: b.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        let err = generation_order(&spec).expect_err("cycle");
        assert!(err.to_string().contains("cycle"));
    }

    #[test]
    fn rejects_rule_with_both_or_neither_kind() {
        let yaml = r#"
tables:
  t:
    profile: t.json
    rows: 10
    rules:
      - derive: "x = 1"
        constraint: "x > 0"
    output: t.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        let err = validate(&spec).expect_err("invalid rule");
        assert!(err.to_string().contains("exactly one"));
    }

    #[test]
    fn rejects_self_referential_foreign_key() {
        let yaml = r#"
tables:
  t:
    profile: t.json
    rows: 10
    keys: [id]
    foreign_keys:
      - column: parent_id
        references: t.id
    output: t.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        let err = validate(&spec).expect_err("self-ref");
        assert!(err.to_string().contains("itself"));
    }

    #[test]
    fn rejects_three_part_reference() {
        let err = parse_reference("a.b.c").expect_err("3-part");
        assert!(err.to_string().contains("table.column"));
    }

    #[test]
    fn rejects_column_that_is_both_key_and_foreign_key() {
        let yaml = r#"
tables:
  parent:
    profile: p.json
    rows: 10
    keys: [id]
    output: p.csv
  child:
    profile: c.json
    rows: 10
    keys: [shared]
    foreign_keys:
      - column: shared
        references: parent.id
    output: c.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        let err = validate(&spec).expect_err("key+fk conflict");
        assert!(err.to_string().contains("both a key and a foreign key"));
    }

    #[test]
    fn parses_bare_string_uniform_fan_out() {
        let yaml = r#"
tables:
  c:
    profile: c.json
    rows: 10
    keys: [id]
    output: c.csv
  o:
    profile: o.json
    rows: 10
    foreign_keys:
      - column: cid
        references: c.id
        fan_out: uniform
    output: o.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        validate(&spec).expect("valid");
        assert!(matches!(
            fan_out(&spec.tables["o"].foreign_keys[0]).unwrap(),
            FanOut::Uniform
        ));
    }

    #[test]
    fn rejects_invalid_fan_out_string() {
        let yaml = r#"
tables:
  c:
    profile: c.json
    rows: 10
    keys: [id]
    output: c.csv
  o:
    profile: o.json
    rows: 10
    foreign_keys:
      - column: cid
        references: c.id
        fan_out: wibble
    output: o.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        let err = validate(&spec).expect_err("bad fan_out");
        assert!(err.to_string().contains("wibble"));
    }

    #[test]
    fn parses_uniform_fan_out_mapping_form() {
        let yaml = r#"
tables:
  c:
    profile: c.json
    rows: 10
    keys: [id]
    output: c.csv
  o:
    profile: o.json
    rows: 10
    foreign_keys:
      - column: cid
        references: c.id
        fan_out: {distribution: uniform}
    output: o.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        validate(&spec).expect("valid");
        assert!(matches!(
            fan_out(&spec.tables["o"].foreign_keys[0]).unwrap(),
            FanOut::Uniform
        ));
    }
}
