//! Synth orchestration: batch generation, FK fan-out, spec execution.
#![allow(dead_code)] // generate_batch and helpers consumed by Tasks 13/14

use std::collections::HashMap;

use polars::prelude::*;
use rand::Rng;
use rand_chacha::ChaCha8Rng;

use crate::{
    cli::SynthArgs,
    error::DtooError,
    synth::{
        copula,
        keys::{self, KeyKind},
        profile_input::{SynthColumn, SynthProfile},
        samplers,
        spec::FanOut,
    },
};

/// Generated parent key columns, addressed as "table.column".
#[derive(Default)]
pub struct ParentKeys(HashMap<String, Series>);

impl ParentKeys {
    pub fn insert(&mut self, reference: String, keys: Series) {
        self.0.insert(reference, keys);
    }
    pub fn get(&self, reference: &str) -> Option<&Series> {
        self.0.get(reference)
    }
}

/// A foreign key resolved against the spec (validated to exist).
pub struct ResolvedFk {
    pub column: String,
    pub parent_key: String,
    pub fan_out: FanOut,
}

/// Everything needed to generate batches for one table.
pub struct TableGenContext<'a> {
    pub name: &'a str,
    pub profile: &'a SynthProfile,
    pub keys: &'a [String],
    pub fks: &'a [ResolvedFk],
    pub seed: u64,
}

fn config_err(message: String) -> DtooError {
    DtooError::Config { message }
}

fn is_temporal(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Date | DataType::Datetime(_, _) | DataType::Time
    )
}

fn is_marginal_numeric(dt: &DataType) -> bool {
    dt.is_primitive_numeric() || matches!(dt, DataType::Decimal(_, _)) || is_temporal(dt)
}

/// Converts a physical f64 into a Series-buildable representation per dtype.
/// Integers/temporal round; Float stays. Returns builders' raw vectors.
enum PhysVec {
    F64(Vec<Option<f64>>),
    I64(Vec<Option<i64>>),
    I32(Vec<Option<i32>>),
}

fn build_numeric_series(
    name: &str,
    dtype: &DataType,
    values: Vec<Option<f64>>,
) -> Result<Series, DtooError> {
    let phys = match dtype {
        DataType::Float32 | DataType::Float64 | DataType::Decimal(_, _) => PhysVec::F64(values),
        DataType::Date
        | DataType::Int32
        | DataType::Int16
        | DataType::Int8
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => PhysVec::I32(
            values
                .into_iter()
                .map(|v| v.map(|f| f.round() as i32))
                .collect(),
        ),
        _ => PhysVec::I64(
            values
                .into_iter()
                .map(|v| v.map(|f| f.round() as i64))
                .collect(),
        ),
    };
    let base = match phys {
        PhysVec::F64(v) => Series::new(name.into(), v),
        PhysVec::I64(v) => Series::new(name.into(), v),
        PhysVec::I32(v) => Series::new(name.into(), v),
    };
    match base.cast(dtype) {
        Ok(s) => Ok(s),
        Err(_) => {
            eprintln!(
                "Warning: column `{name}` could not be cast to {dtype:?}; keeping {:?}",
                base.dtype()
            );
            Ok(base)
        }
    }
}

/// Generates one batch of `batch_rows` rows for the table.
/// `round` decorrelates re-generation under constraints; `offset` keeps key
/// uniqueness across rounds.
pub fn generate_batch(
    ctx: &TableGenContext,
    round: u64,
    batch_rows: usize,
    offset: usize,
    parents: &ParentKeys,
) -> Result<DataFrame, DtooError> {
    // 1. Classify columns and pre-compute the copula group.
    let key_set: Vec<&str> = ctx.keys.iter().map(String::as_str).collect();
    let fk_map: HashMap<&str, &ResolvedFk> =
        ctx.fks.iter().map(|fk| (fk.column.as_str(), fk)).collect();

    let copula_cols: Vec<&SynthColumn> = match &ctx.profile.correlation {
        Some(m) => ctx
            .profile
            .columns
            .iter()
            .filter(|c| {
                m.columns.contains(&c.name)
                    && is_marginal_numeric(&c.dtype)
                    && c.histogram.is_some()
                    && !key_set.contains(&c.name.as_str())
                    && !fk_map.contains_key(c.name.as_str())
            })
            .collect(),
        None => Vec::new(),
    };

    let copula_uniforms: Option<(Vec<&SynthColumn>, Vec<Vec<f64>>)> = if copula_cols.len() >= 2 {
        let matrix = ctx.profile.correlation.as_ref().expect("checked above");
        let idx: Vec<usize> = copula_cols
            .iter()
            .map(|c| {
                matrix
                    .columns
                    .iter()
                    .position(|n| *n == c.name)
                    .expect("present")
            })
            .collect();
        let k = idx.len();
        let mut sub = vec![vec![0.0; k]; k];
        for (a, &ia) in idx.iter().enumerate() {
            for (b, &ib) in idx.iter().enumerate() {
                sub[a][b] = matrix.data[ia][ib];
            }
        }
        copula::psd_repair(&mut sub);
        let chol = copula::cholesky(&sub).ok_or_else(|| {
            config_err(format!(
                "table `{}`: correlation matrix could not be factorized",
                ctx.name
            ))
        })?;
        let mut rng = samplers::stream_rng(ctx.seed, ctx.name, "__copula__", round);
        Some((
            copula_cols.clone(),
            copula::correlated_uniforms(&chol, batch_rows, &mut rng),
        ))
    } else {
        None
    };

    // 2. Generate every column in profile order.
    let mut columns: Vec<Column> = Vec::with_capacity(ctx.profile.columns.len());
    for col in &ctx.profile.columns {
        let mut rng = samplers::stream_rng(ctx.seed, ctx.name, &col.name, round);

        let series = if key_set.contains(&col.name.as_str()) {
            generate_key_series(col, batch_rows, offset, &mut rng)?
        } else if let Some(fk) = fk_map.get(col.name.as_str()) {
            generate_fk_series(ctx, col, fk, batch_rows, parents, &mut rng)?
        } else if let Some((cols, uniforms)) = copula_uniforms
            .as_ref()
            .filter(|(cols, _)| cols.iter().any(|c| c.name == col.name))
        {
            let pos = cols
                .iter()
                .position(|c| c.name == col.name)
                .expect("present");
            let hist = col.histogram.as_ref().expect("copula needs histogram");
            let values: Vec<Option<f64>> = uniforms
                .iter()
                .map(|row| {
                    if samplers::is_null_draw(col.null_percentage, &mut rng) {
                        None
                    } else {
                        Some(samplers::histogram_quantile(hist, row[pos]))
                    }
                })
                .collect();
            build_numeric_series(&col.name, &col.dtype, values)?
        } else {
            generate_independent_series(ctx, col, batch_rows, &mut rng)?
        };
        columns.push(series.into_column());
    }

    DataFrame::new(batch_rows, columns).map_err(|e| config_err(format!("assembling batch: {e}")))
}

fn generate_key_series(
    col: &SynthColumn,
    rows: usize,
    offset: usize,
    rng: &mut ChaCha8Rng,
) -> Result<Series, DtooError> {
    if col.unique_ratio < 1.0 {
        eprintln!(
            "Warning: key column `{}` is not unique in the profiled data (unique_ratio {:.3}); synthetic keys WILL be unique",
            col.name, col.unique_ratio
        );
    }
    let kind = keys::detect_key_kind(col);
    if keys::is_numeric_kind(&kind) {
        let KeyKind::SequentialInt { start } = kind else {
            unreachable!()
        };
        let values: Vec<i64> = (0..rows).map(|i| start + (offset + i) as i64).collect();
        let base = Series::new(col.name.as_str().into(), values);
        return Ok(base.cast(&col.dtype).unwrap_or(base));
    }
    let values: Vec<String> = (0..rows)
        .map(|i| keys::key_string(&kind, offset + i, rng))
        .collect();
    Ok(Series::new(col.name.as_str().into(), values))
}

fn generate_fk_series(
    ctx: &TableGenContext,
    col: &SynthColumn,
    fk: &ResolvedFk,
    rows: usize,
    parents: &ParentKeys,
    rng: &mut ChaCha8Rng,
) -> Result<Series, DtooError> {
    let parent = parents.get(&fk.parent_key).ok_or_else(|| {
        config_err(format!(
            "table `{}`: parent keys `{}` not generated yet (internal ordering bug)",
            ctx.name, fk.parent_key
        ))
    })?;
    let n_parents = parent.len();
    if n_parents == 0 {
        return Err(config_err(format!(
            "table `{}`: parent `{}` generated zero keys; cannot sample foreign keys",
            ctx.name, fk.parent_key
        )));
    }

    let indices: Vec<u32> = match fk.fan_out {
        FanOut::Uniform => (0..rows)
            .map(|_| rng.gen_range(0..n_parents) as u32)
            .collect(),
        FanOut::FromProfile => {
            // Distinct parents used ≈ rows × unique_ratio, capped at parent count.
            let uncapped = (rows as f64 * col.unique_ratio).round() as usize;
            let used = uncapped.clamp(1, n_parents);
            if uncapped > n_parents {
                eprintln!(
                    "Warning: table `{}` column `{}`: profile implies {} distinct parents but only {} exist; fan-out will be denser than profiled",
                    ctx.name, col.name, uncapped, n_parents
                );
            }
            // Skew: top-K frequencies rank-matched onto the first `used` parents;
            // the tail shares the average remaining frequency.
            let top: Vec<f64> = col.top_values.iter().map(|v| v.freq as f64).collect();
            let covered: f64 = top.iter().sum();
            let total = col.non_null_count.max(1) as f64;
            let tail_count = col.distinct_count.saturating_sub(top.len());
            let tail_avg = if tail_count > 0 {
                ((total - covered) / tail_count as f64).max(0.0)
            } else {
                0.0
            };
            let weights: Vec<f64> = (0..used)
                .map(|i| top.get(i).copied().unwrap_or(tail_avg).max(1e-12))
                .collect();
            let cumulative: Vec<f64> = weights
                .iter()
                .scan(0.0, |acc, w| {
                    *acc += w;
                    Some(*acc)
                })
                .collect();
            let total_w = *cumulative.last().expect("non-empty");
            (0..rows)
                .map(|_| {
                    let t = rng.r#gen::<f64>() * total_w;
                    cumulative.partition_point(|c| *c < t).min(used - 1) as u32
                })
                .collect()
        }
    };

    let idx = IdxCa::from_vec("idx".into(), indices);
    let mut s = parent
        .take(&idx)
        .map_err(|e| config_err(format!("sampling foreign keys: {e}")))?;
    s.rename(col.name.as_str().into());
    Ok(s)
}

fn generate_independent_series(
    ctx: &TableGenContext,
    col: &SynthColumn,
    rows: usize,
    rng: &mut ChaCha8Rng,
) -> Result<Series, DtooError> {
    if is_marginal_numeric(&col.dtype) {
        if col.histogram.is_none() && !ctx.profile.synth_detail {
            eprintln!(
                "Warning: column `{}` generated from 5-point quantiles only; re-profile with --detail synth for full fidelity",
                col.name
            );
        }
        let values: Vec<Option<f64>> = (0..rows)
            .map(|_| {
                if samplers::is_null_draw(col.null_percentage, rng) {
                    return None;
                }
                Some(match (&col.histogram, &col.quantiles) {
                    (Some(h), _) => samplers::sample_histogram(h, rng),
                    (None, Some(q)) => samplers::quantiles_quantile(q, rng.r#gen()),
                    (None, None) => 0.0,
                })
            })
            .collect();
        return build_numeric_series(&col.name, &col.dtype, values);
    }

    if col.dtype == DataType::Boolean {
        let true_freq = col
            .top_values
            .iter()
            .find(|v| v.value == "true")
            .map(|v| v.freq as f64)
            .unwrap_or(0.5 * col.non_null_count.max(1) as f64);
        let p = true_freq / col.non_null_count.max(1) as f64;
        let values: Vec<Option<bool>> = (0..rows)
            .map(|_| {
                if samplers::is_null_draw(col.null_percentage, rng) {
                    None
                } else {
                    Some(rng.r#gen::<f64>() < p)
                }
            })
            .collect();
        return Ok(Series::new(col.name.as_str().into(), values));
    }

    // String path: weighted top-K for covered mass, pattern filler for tail.
    let covered: usize = col.top_values.iter().map(|v| v.freq).sum();
    let coverage = covered as f64 / col.non_null_count.max(1) as f64;
    let values: Vec<Option<String>> = (0..rows)
        .map(|_| {
            if samplers::is_null_draw(col.null_percentage, rng) {
                return None;
            }
            let from_top = !col.top_values.is_empty()
                && (coverage >= 0.995
                    || rng.r#gen::<f64>() < coverage
                    || col.pattern_sample.is_empty());
            if from_top {
                let i = samplers::sample_weighted_index(&col.top_values, rng);
                Some(col.top_values[i].value.clone())
            } else if !col.pattern_sample.is_empty() {
                let i = samplers::sample_weighted_index(&col.pattern_sample, rng);
                Some(samplers::generate_from_pattern(
                    &col.pattern_sample[i].value,
                    col.min_length,
                    col.max_length,
                    rng,
                ))
            } else {
                Some(String::new())
            }
        })
        .collect();
    Ok(Series::new(col.name.as_str().into(), values))
}

/// Entry point for `dtoo synth` (filled in by the orchestration task).
pub fn run(_args: &SynthArgs) -> Result<(), DtooError> {
    Err(DtooError::Config {
        message: "synth is not implemented yet".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiler::{CorrelationMatrix, HistogramBucket, ValueFrequency};
    use crate::synth::profile_input::{SynthColumn, SynthProfile};
    use crate::synth::spec::FanOut;

    fn numeric_col(name: &str) -> SynthColumn {
        SynthColumn {
            name: name.into(),
            dtype: DataType::Float64,
            null_percentage: 0.0,
            non_null_count: 100,
            distinct_count: 100,
            unique_ratio: 1.0,
            histogram: Some(vec![
                HistogramBucket {
                    lo: 0.0,
                    hi: 50.0,
                    count: 50,
                },
                HistogramBucket {
                    lo: 50.0,
                    hi: 100.0,
                    count: 50,
                },
            ]),
            quantiles: Some(vec![0.0, 25.0, 50.0, 75.0, 100.0]),
            top_values: vec![],
            pattern_sample: vec![],
            min_length: 1,
            max_length: 1,
        }
    }

    fn profile_of(
        columns: Vec<SynthColumn>,
        correlation: Option<CorrelationMatrix>,
    ) -> SynthProfile {
        SynthProfile {
            synth_detail: true,
            row_count: 100,
            columns,
            correlation,
        }
    }

    #[test]
    fn generates_requested_rows_with_profile_schema() {
        let profile = profile_of(
            vec![
                numeric_col("amount"),
                SynthColumn {
                    name: "status".into(),
                    dtype: DataType::String,
                    null_percentage: 0.0,
                    non_null_count: 100,
                    distinct_count: 2,
                    unique_ratio: 0.02,
                    histogram: None,
                    quantiles: None,
                    top_values: vec![
                        ValueFrequency {
                            value: "active".into(),
                            freq: 70,
                        },
                        ValueFrequency {
                            value: "closed".into(),
                            freq: 30,
                        },
                    ],
                    pattern_sample: vec![ValueFrequency {
                        value: "aaaaaa".into(),
                        freq: 100,
                    }],
                    min_length: 6,
                    max_length: 6,
                },
            ],
            None,
        );
        let ctx = TableGenContext {
            name: "t",
            profile: &profile,
            keys: &[],
            fks: &[],
            seed: 42,
        };
        let df = generate_batch(&ctx, 0, 200, 0, &ParentKeys::default()).expect("generate");
        assert_eq!(df.height(), 200);
        assert_eq!(df.get_column_names(), &["amount", "status"]);
        assert_eq!(df.column("amount").unwrap().dtype(), &DataType::Float64);
        // status values come from top_values (coverage = 100%)
        let s = df.column("status").unwrap().str().unwrap();
        for v in s.iter().flatten() {
            assert!(v == "active" || v == "closed");
        }
    }

    #[test]
    fn generation_is_deterministic_per_seed() {
        let profile = profile_of(vec![numeric_col("v")], None);
        let ctx = TableGenContext {
            name: "t",
            profile: &profile,
            keys: &[],
            fks: &[],
            seed: 7,
        };
        let a = generate_batch(&ctx, 0, 50, 0, &ParentKeys::default()).unwrap();
        let b = generate_batch(&ctx, 0, 50, 0, &ParentKeys::default()).unwrap();
        assert!(a.equals_missing(&b), "same seed, same data");
        let ctx2 = TableGenContext {
            name: "t",
            profile: &profile,
            keys: &[],
            fks: &[],
            seed: 8,
        };
        let c = generate_batch(&ctx2, 0, 50, 0, &ParentKeys::default()).unwrap();
        assert!(!a.equals_missing(&c), "different seed, different data");
    }

    #[test]
    fn null_percentage_is_respected() {
        let mut col = numeric_col("v");
        col.null_percentage = 30.0;
        let profile = profile_of(vec![col], None);
        let ctx = TableGenContext {
            name: "t",
            profile: &profile,
            keys: &[],
            fks: &[],
            seed: 1,
        };
        let df = generate_batch(&ctx, 0, 2000, 0, &ParentKeys::default()).unwrap();
        let nulls = df.column("v").unwrap().null_count();
        assert!((480..=720).contains(&nulls), "≈30% nulls, got {nulls}");
    }

    #[test]
    fn key_columns_are_unique() {
        let mut col = numeric_col("id");
        col.dtype = DataType::Int64;
        let profile = profile_of(vec![col], None);
        let keys = vec!["id".to_string()];
        let ctx = TableGenContext {
            name: "t",
            profile: &profile,
            keys: &keys,
            fks: &[],
            seed: 1,
        };
        let df = generate_batch(&ctx, 0, 500, 0, &ParentKeys::default()).unwrap();
        let id = df.column("id").unwrap();
        assert_eq!(id.n_unique().unwrap(), 500);
        assert_eq!(id.null_count(), 0, "keys are never null");
    }

    #[test]
    fn fk_values_come_from_parent_keys() {
        let parent = Series::new("customer_id".into(), (0..100i64).collect::<Vec<_>>());
        let mut parents = ParentKeys::default();
        parents.insert("customers.customer_id".into(), parent);

        let mut fk_col = numeric_col("customer_id");
        fk_col.dtype = DataType::Int64;
        fk_col.unique_ratio = 0.1; // mean fan-out 10
        let profile = profile_of(vec![fk_col], None);
        let fks = vec![ResolvedFk {
            column: "customer_id".into(),
            parent_key: "customers.customer_id".into(),
            fan_out: FanOut::FromProfile,
        }];
        let ctx = TableGenContext {
            name: "orders",
            profile: &profile,
            keys: &[],
            fks: &fks,
            seed: 3,
        };
        let df = generate_batch(&ctx, 0, 1000, 0, &parents).unwrap();
        let fk = df.column("customer_id").unwrap().i64().unwrap();
        for v in fk.iter().flatten() {
            assert!((0..100).contains(&v), "FK {v} must exist in parent");
        }
        // distinct parents used ≈ rows × unique_ratio = 100
        let used = df.column("customer_id").unwrap().n_unique().unwrap();
        assert!((60..=100).contains(&used), "distinct parents used: {used}");
    }

    #[test]
    fn copula_preserves_correlation_between_columns() {
        let profile = profile_of(
            vec![numeric_col("x"), numeric_col("y")],
            Some(CorrelationMatrix {
                columns: vec!["x".into(), "y".into()],
                data: vec![vec![1.0, 0.9], vec![0.9, 1.0]],
            }),
        );
        let ctx = TableGenContext {
            name: "t",
            profile: &profile,
            keys: &[],
            fks: &[],
            seed: 11,
        };
        let df = generate_batch(&ctx, 0, 5000, 0, &ParentKeys::default()).unwrap();
        let xs: Vec<f64> = df
            .column("x")
            .unwrap()
            .f64()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        let ys: Vec<f64> = df
            .column("y")
            .unwrap()
            .f64()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        let n = xs.len() as f64;
        let mx = xs.iter().sum::<f64>() / n;
        let my = ys.iter().sum::<f64>() / n;
        let (mut c, mut vx, mut vy) = (0.0, 0.0, 0.0);
        for (x, y) in xs.iter().zip(&ys) {
            c += (x - mx) * (y - my);
            vx += (x - mx) * (x - mx);
            vy += (y - my) * (y - my);
        }
        let r = c / (vx.sqrt() * vy.sqrt());
        assert!(r > 0.7, "expected strong positive correlation, got {r}");
    }
}
