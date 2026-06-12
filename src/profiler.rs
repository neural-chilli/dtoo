use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use chrono::Utc;
use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    cli::{ProfileDetail, ProfileFormat},
    error::DtooError,
};

/// Value of [`ProfileReport::detail`] when a profile was generated at synth detail.
pub const SYNTH_DETAIL: &str = "synth";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValueFrequency {
    pub value: String,
    pub freq: usize,
}

/// One bucket of a quantile-spaced histogram over a column's physical values.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub lo: f64,
    pub hi: f64,
    pub count: u64,
}

/// Pairwise Spearman correlation matrix over numeric/temporal columns.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorrelationMatrix {
    pub columns: Vec<String>,
    pub data: Vec<Vec<f64>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnProfile {
    pub name: String,
    pub data_type: String,
    pub count: usize,
    pub null_count: usize,
    pub null_percentage: f64,
    pub distinct_count: usize,
    pub min: Option<String>,
    pub max: Option<String>,
    pub mean: Option<String>,
    pub stddev: Option<String>,
    pub median: Option<String>,
    pub p25: Option<String>,
    pub p75: Option<String>,
    pub min_length: Option<String>,
    pub max_length: Option<String>,
    pub avg_length: Option<String>,
    pub top_5_values: Vec<ValueFrequency>,
    pub pattern_sample: Vec<ValueFrequency>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub histogram: Option<Vec<HistogramBucket>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub top_values: Option<Vec<ValueFrequency>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub unique_ratio: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileReport {
    pub row_count: usize,
    pub sample_percentage: u8,
    pub generated_at: String,
    pub columns: Vec<ColumnProfile>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub correlation_matrix: Option<CorrelationMatrix>,
}

/// Profile generation options for query pipeline profile output.
#[derive(Clone, Debug)]
pub struct ProfileOptions {
    pub path: PathBuf,
    pub format: ProfileFormat,
    pub sample_percentage: u8,
    pub detail: ProfileDetail,
    pub top_k: usize,
}

/// Computes and renders profile reports from a [`DataFrame`].
pub struct Profiler;

impl Profiler {
    /// Compute a profile report from a Polars [`DataFrame`] and write it according to `options`.
    ///
    /// Sampling uses `df.head(n)` (deterministic, take the first N rows) when
    /// `sample_percentage < 100`.
    pub fn generate(df: &DataFrame, options: &ProfileOptions) -> Result<(), DtooError> {
        if options.sample_percentage == 0 || options.sample_percentage > 100 {
            return Err(DtooError::Config {
                message: "--profile-sample must be between 1 and 100".to_string(),
            });
        }

        if options.detail == ProfileDetail::Synth && options.format != ProfileFormat::Json {
            return Err(DtooError::Config {
                message: "synth profile detail requires JSON profile format".to_string(),
            });
        }

        let sampled: DataFrame;
        let source: &DataFrame = if options.sample_percentage < 100 {
            let n = ((df.height() as f64 * options.sample_percentage as f64 / 100.0).round()
                as usize)
                .max(1);
            sampled = df.head(Some(n));
            &sampled
        } else {
            df
        };

        let report = build_report(
            source,
            options.sample_percentage,
            options.detail,
            options.top_k,
        )?;
        write_report(options, &report)
    }
}

// ── Polars-based report builder ───────────────────────────────────────────────

fn build_report(
    df: &DataFrame,
    sample_percentage: u8,
    detail: ProfileDetail,
    top_k: usize,
) -> Result<ProfileReport, DtooError> {
    let row_count = df.height();
    let mut columns = Vec::with_capacity(df.width());

    for col in df.columns() {
        columns.push(profile_column(col, row_count, detail, top_k)?);
    }

    Ok(ProfileReport {
        row_count,
        sample_percentage,
        generated_at: Utc::now().to_rfc3339(),
        columns,
        detail: (detail == ProfileDetail::Synth).then(|| SYNTH_DETAIL.to_string()),
        correlation_matrix: None,
    })
}

fn profile_column(
    series: &Column,
    total_rows: usize,
    detail: ProfileDetail,
    top_k: usize,
) -> Result<ColumnProfile, DtooError> {
    let name = series.name().to_string();
    let dtype = series.dtype().clone();
    let data_type = format!("{dtype:?}");

    let null_count = series.null_count();
    let count = total_rows;
    let null_percentage = if count == 0 {
        0.0
    } else {
        (100.0 * null_count as f64 / count as f64 * 100.0).round() / 100.0
    };
    let distinct_count = series
        .as_materialized_series()
        .drop_nulls()
        .n_unique()
        .map_err(polars_err)?;

    let top_5 = top_values(series, 5)?;

    let mut profile = ColumnProfile {
        name,
        data_type,
        count,
        null_count,
        null_percentage,
        distinct_count,
        min: None,
        max: None,
        mean: None,
        stddev: None,
        median: None,
        p25: None,
        p75: None,
        min_length: None,
        max_length: None,
        avg_length: None,
        top_5_values: top_5,
        pattern_sample: Vec::new(),
        histogram: None,
        top_values: None,
        unique_ratio: None,
    };

    if is_numeric_dtype(&dtype) {
        profile.min = scalar_to_opt_string(series.min_reduce().map_err(polars_err)?);
        profile.max = scalar_to_opt_string(series.max_reduce().map_err(polars_err)?);
        profile.mean = scalar_to_opt_string(series.mean_reduce().map_err(polars_err)?);
        profile.stddev = scalar_to_opt_string(series.std_reduce(1).map_err(polars_err)?);
        profile.median = scalar_to_opt_string(series.median_reduce().map_err(polars_err)?);
        profile.p25 = scalar_to_opt_string(
            series
                .quantile_reduce(0.25, QuantileMethod::Linear)
                .map_err(polars_err)?,
        );
        profile.p75 = scalar_to_opt_string(
            series
                .quantile_reduce(0.75, QuantileMethod::Linear)
                .map_err(polars_err)?,
        );
    } else if is_text_dtype(&dtype) {
        let lengths = string_char_lengths(series)?;
        if !lengths.is_empty() {
            let min_l = lengths.iter().copied().min().unwrap_or(0);
            let max_l = lengths.iter().copied().max().unwrap_or(0);
            let avg_l = lengths.iter().copied().sum::<u32>() as f64 / lengths.len() as f64;
            profile.min_length = Some(min_l.to_string());
            profile.max_length = Some(max_l.to_string());
            profile.avg_length = Some(format!("{avg_l:.2}"));
        }
        profile.pattern_sample = text_patterns(series)?;
    } else if is_date_like_dtype(&dtype) {
        // For date/time types, cast to String for human-readable output before reducing.
        let as_str = series.cast(&DataType::String).map_err(polars_err)?;
        profile.min = scalar_to_opt_string(as_str.min_reduce().map_err(polars_err)?);
        profile.max = scalar_to_opt_string(as_str.max_reduce().map_err(polars_err)?);
    }

    if detail == ProfileDetail::Synth {
        profile.top_values = Some(top_values(series, top_k)?);
        profile.unique_ratio = Some(if count == 0 {
            0.0
        } else {
            distinct_count as f64 / count as f64
        });
        if is_numeric_dtype(&dtype) || is_date_like_dtype(&dtype) {
            profile.histogram = numeric_histogram(series)?;
        }
    }

    Ok(profile)
}

/// Returns the character lengths of all non-null string values.
fn string_char_lengths(series: &Column) -> Result<Vec<u32>, DtooError> {
    let as_str = series.cast(&DataType::String).map_err(polars_err)?;
    let ca = as_str.str().map_err(polars_err)?;
    let lengths: Vec<u32> = ca
        .str_len_chars()
        .iter()
        .flatten() // drops None (nulls)
        .collect();
    Ok(lengths)
}

/// Returns the top-`limit` most frequent non-null values, sorted descending by count.
fn top_values(series: &Column, limit: usize) -> Result<Vec<ValueFrequency>, DtooError> {
    // Cast to String so all types produce a uniform representation.
    let as_str = series.cast(&DataType::String).map_err(polars_err)?;
    let non_null = as_str.drop_nulls();

    // value_counts(sort=true, parallel=false, name="count", normalize=false)
    let vc_df = non_null
        .as_materialized_series()
        .value_counts(true, false, "count".into(), false)
        .map_err(polars_err)?;

    // Columns: [series_name (String), "count" (UInt32)]
    let values_col = vc_df.column(series.name()).map_err(polars_err)?;
    let counts_col = vc_df.column("count").map_err(polars_err)?;

    let values_ca = values_col.str().map_err(polars_err)?;
    let counts_ca = counts_col.cast(&DataType::UInt64).map_err(polars_err)?;
    let counts_u64 = counts_ca.u64().map_err(polars_err)?;

    let mut pairs: Vec<ValueFrequency> = values_ca
        .iter()
        .zip(counts_u64.iter())
        .filter_map(|(v, c)| {
            Some(ValueFrequency {
                value: v?.to_string(),
                freq: c? as usize,
            })
        })
        .collect();

    // value_counts with sort=true returns descending; take at most `limit`.
    pairs.truncate(limit);
    Ok(pairs)
}

/// Computes text patterns: replace digits→`d`, letters→`a`, collapse `d+`→`N`;
/// returns the top-5 patterns by frequency.
fn text_patterns(series: &Column) -> Result<Vec<ValueFrequency>, DtooError> {
    let as_str = series.cast(&DataType::String).map_err(polars_err)?;
    let ca = as_str.str().map_err(polars_err)?;

    let mut freq: HashMap<String, usize> = HashMap::new();
    for v in ca.iter().flatten() {
        let pattern = make_pattern(v);
        *freq.entry(pattern).or_insert(0) += 1;
    }

    let mut pairs: Vec<ValueFrequency> = freq
        .into_iter()
        .map(|(value, freq)| ValueFrequency { value, freq })
        .collect();
    pairs.sort_by_key(|b| std::cmp::Reverse(b.freq));
    pairs.truncate(5);
    Ok(pairs)
}

/// Replace each digit with `d`, each ASCII letter with `a`, then collapse
/// runs of consecutive `d` characters into the single token `N`.
fn make_pattern(s: &str) -> String {
    // Step 1: replace each digit → 'd', each ASCII letter → 'a'
    let replaced: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_digit() {
                'd'
            } else if c.is_ascii_alphabetic() {
                'a'
            } else {
                c
            }
        })
        .collect();

    // Step 2: collapse runs of 'd' → 'N'
    let mut result = String::with_capacity(replaced.len());
    let mut in_digit_run = false;
    for c in replaced.chars() {
        if c == 'd' {
            if !in_digit_run {
                result.push('N');
                in_digit_run = true;
            }
        } else {
            in_digit_run = false;
            result.push(c);
        }
    }
    result
}

fn scalar_to_opt_string(scalar: Scalar) -> Option<String> {
    let av = scalar.value();
    if matches!(av, AnyValue::Null) {
        None
    } else {
        Some(format!("{av}"))
    }
}

fn polars_err(e: PolarsError) -> DtooError {
    DtooError::Config {
        message: format!("profiler: {e}"),
    }
}

/// Extracts a column's non-null values as f64 in logical (value) space.
///
/// For most types this goes through the physical representation (Date → days since
/// epoch, Datetime → its time-unit integer, Time → nanoseconds). Decimal is
/// special: `to_physical_repr()` yields the scaled integer (value × 10^scale), but
/// histograms must live in value space to match the min/max strings and to be
/// useful for downstream synth generation. We cast Decimal directly to Float64,
/// which Polars implements as value-space conversion.
fn physical_f64_values(series: &Column) -> Result<Vec<f64>, DtooError> {
    let s = series.as_materialized_series();
    let as_f64 = if matches!(s.dtype(), DataType::Decimal(_, _)) {
        // Decimal's physical repr is the scaled integer (value × 10^scale); cast the
        // logical value directly so histograms live in value space like min/max.
        s.cast(&DataType::Float64).map_err(polars_err)?
    } else {
        s.to_physical_repr()
            .into_owned()
            .cast(&DataType::Float64)
            .map_err(polars_err)?
    };
    Ok(as_f64
        .f64()
        .map_err(polars_err)?
        .iter()
        .flatten()
        .filter(|v| v.is_finite())
        .collect())
}

/// Builds a quantile-spaced histogram (up to 20 buckets) over non-null finite values.
///
/// **Point-mass handling.** When multiple of the 21 raw quantile positions collapse
/// onto the same edge value `v` (extreme skew), `v` is a *point mass*. We emit a
/// dedicated point-mass bucket `{lo: v, hi: v}` for it; if a next distinct edge
/// exists, a continuous bucket `{lo: v, hi: next}` follows. This ensures tied
/// values route to their own bucket and are not smeared into a neighbouring wide
/// continuous bucket.
///
/// **Two-pass counting.**
/// Pass 1: run-length encode sorted `vals` → `(value, run_count)` pairs.
/// Pass 2: route each pair with a forward pointer over the (sorted) bucket list:
///   - If the current bucket is a point mass (`lo == hi`) and `value == lo` → add
///     `run_count` there.
///   - Otherwise advance past buckets whose `hi < value` (strict; point-mass
///     buckets where `lo == value` were already checked) and add `run_count` to
///     the first bucket with `hi >= value`. The last bucket absorbs any remainder.
///
/// Zero-count buckets are pruned after counting.
///
/// **Invariants preserved:**
/// - `counts.sum() == total finite values`
/// - buckets are ordered: `w[i].hi <= w[i+1].lo` for every consecutive pair
/// - `buckets[0].lo == min`, `buckets.last().hi == max`
/// - every bucket: `lo <= hi`
fn numeric_histogram(series: &Column) -> Result<Option<Vec<HistogramBucket>>, DtooError> {
    let mut vals = physical_f64_values(series)?;
    if vals.is_empty() {
        return Ok(None);
    }
    // All values are finite (guaranteed by physical_f64_values), so partial_cmp always succeeds.
    vals.sort_unstable_by(|a, b| a.partial_cmp(b).expect("finite values compare"));

    let n_buckets = 20usize;
    // Collect 21 raw quantile edge positions (boundaries for 20 buckets).
    let raw_edges: Vec<f64> = (0..=n_buckets)
        .map(|i| {
            let p = i as f64 / n_buckets as f64;
            let idx = ((vals.len() - 1) as f64 * p).round() as usize;
            vals[idx]
        })
        .collect();

    // Count how many of the 21 raw positions collapsed onto each distinct edge value.
    // Edges come from the same sorted array, so exact equality is correct here —
    // using an absolute epsilon risks merging genuinely distinct tiny values.
    let mut edge_counts: Vec<(f64, usize)> = Vec::new();
    for v in &raw_edges {
        if let Some(last) = edge_counts.last_mut()
            && last.0 == *v
        {
            last.1 += 1;
            continue;
        }
        edge_counts.push((*v, 1));
    }

    if edge_counts.len() < 2 {
        // All values identical: a single degenerate point-mass bucket.
        return Ok(Some(vec![HistogramBucket {
            lo: edge_counts[0].0,
            hi: edge_counts[0].0,
            count: vals.len() as u64,
        }]));
    }

    // Build the ordered bucket list.
    //
    // For each deduped edge `e[i]` in order:
    //   • if `e[i]` is a point mass (collapsed >1 raw positions) → push a
    //     point-mass bucket `{lo: e[i], hi: e[i]}`.
    //   • if a next distinct edge `e[i+1]` exists → push a continuous bucket
    //     `{lo: e[i], hi: e[i+1]}`.
    //
    // A point-mass bucket always precedes the continuous bucket that starts at
    // the same value, so tied values route to the point-mass bucket first.
    let deduped_edges: Vec<f64> = edge_counts.iter().map(|(v, _)| *v).collect();
    let mut buckets: Vec<HistogramBucket> = Vec::new();
    for i in 0..deduped_edges.len() {
        let lo = deduped_edges[i];
        let is_point_mass = edge_counts[i].1 > 1;
        if is_point_mass {
            buckets.push(HistogramBucket {
                lo,
                hi: lo,
                count: 0,
            });
        }
        if i + 1 < deduped_edges.len() {
            let hi = deduped_edges[i + 1];
            buckets.push(HistogramBucket { lo, hi, count: 0 });
        }
    }

    // Pass 1: run-length encode sorted vals into (value, run_count) pairs.
    let mut rle: Vec<(f64, u64)> = Vec::new();
    for &v in &vals {
        if let Some(last) = rle.last_mut()
            && last.0 == v
        {
            last.1 += 1;
        } else {
            rle.push((v, 1));
        }
    }

    // Pass 2: route each (value, run_count) pair to the correct bucket using a
    // forward pointer. Both `rle` and `buckets` are sorted, so this is O(n + k).
    //
    // Routing rule:
    //   • If current bucket is a point mass (`lo == hi`) and `lo == value` → route here.
    //   • Otherwise advance while the current bucket cannot contain the value:
    //     - A continuous bucket (`lo < hi`) cannot contain `value` when:
    //         `hi < value` (value is beyond this bucket), OR
    //         `hi == value` AND the next bucket is a point mass for the same value
    //         (the point-mass bucket takes precedence for exact ties at the boundary).
    //     - A point-mass bucket (`lo == hi`) cannot contain `value` when `lo < value`.
    //   • After advancing, add run_count to the current bucket (or the last bucket as
    //     a catch-all if no bucket strictly satisfies the condition).
    let mut b = 0usize;
    for (value, run_count) in &rle {
        // Advance forward while the current bucket cannot hold this value.
        while b + 1 < buckets.len() {
            let bk = &buckets[b];
            let is_pm = bk.lo == bk.hi;
            if is_pm {
                // Point-mass bucket: only exact match belongs here.
                if bk.lo < *value {
                    b += 1;
                    continue;
                }
            } else {
                // Continuous bucket: advance when hi < value, OR when hi == value
                // and the immediately following bucket is a point-mass for that value.
                let next_is_pm_for_value = {
                    let nb = &buckets[b + 1];
                    nb.lo == nb.hi && nb.lo == *value
                };
                if bk.hi < *value || (bk.hi == *value && next_is_pm_for_value) {
                    b += 1;
                    continue;
                }
            }
            break;
        }
        buckets[b].count += run_count;
    }

    // Prune zero-count buckets (they are dead weight for weighted sampling).
    // The invariants survive pruning because min and max always land in some bucket.
    buckets.retain(|b| b.count > 0);

    Ok(Some(buckets))
}

fn is_numeric_dtype(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal(_, _)
    )
}

fn is_text_dtype(dt: &DataType) -> bool {
    matches!(dt, DataType::String)
}

fn is_date_like_dtype(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Date | DataType::Datetime(_, _) | DataType::Time | DataType::Duration(_)
    )
}

// ── Shared rendering (UNCHANGED) ─────────────────────────────────────────────

fn write_report(options: &ProfileOptions, report: &ProfileReport) -> Result<(), DtooError> {
    let content = match options.format {
        ProfileFormat::Json => {
            serde_json::to_string_pretty(report).map_err(|source| DtooError::Output {
                message: format!("failed to serialize JSON profile: {source}"),
            })?
        }
        ProfileFormat::Csv => render_csv(report),
        ProfileFormat::Html => render_html(report),
    };

    if options.path == Path::new("-") {
        println!("{content}");
        return Ok(());
    }

    if let Some(parent) = options.path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        return Err(DtooError::Output {
            message: format!(
                "profile output directory does not exist: {}",
                parent.display()
            ),
        });
    }

    fs::write(&options.path, content).map_err(|source| DtooError::Output {
        message: format!(
            "failed to write profile report to {}: {source}",
            options.path.display()
        ),
    })
}

fn render_csv(report: &ProfileReport) -> String {
    let mut out = String::from(
        "column_name,type,count,null_count,null_pct,distinct_count,min,max,mean,stddev,median,p25,p75,min_length,max_length,avg_length\n",
    );
    for c in &report.columns {
        let row = [
            c.name.as_str(),
            c.data_type.as_str(),
            &c.count.to_string(),
            &c.null_count.to_string(),
            &format!("{:.2}", c.null_percentage),
            &c.distinct_count.to_string(),
            c.min.as_deref().unwrap_or(""),
            c.max.as_deref().unwrap_or(""),
            c.mean.as_deref().unwrap_or(""),
            c.stddev.as_deref().unwrap_or(""),
            c.median.as_deref().unwrap_or(""),
            c.p25.as_deref().unwrap_or(""),
            c.p75.as_deref().unwrap_or(""),
            c.min_length.as_deref().unwrap_or(""),
            c.max_length.as_deref().unwrap_or(""),
            c.avg_length.as_deref().unwrap_or(""),
        ];
        out.push_str(
            &row.iter()
                .map(|v| csv_escape(v))
                .collect::<Vec<_>>()
                .join(","),
        );
        out.push('\n');
    }
    out
}

const HTML_TEMPLATE: &str = r#"<!doctype html>
<html><head><meta charset="utf-8"><title>dtoo profile</title>
<style>
body{font-family:system-ui;margin:24px}
table{width:100%;border-collapse:collapse}
th,td{border:1px solid #ddd;padding:8px;vertical-align:top}
tr:nth-child(even){background:#f8f8f8}
th{cursor:pointer;user-select:none}
.good{color:#0a7d2c}.warn{color:#a57a00}.bad{color:#c62828}
.details{display:none;background:#fcfcff}
button{padding:4px 8px}
</style>
<script>
function sortTable(colIdx){
  const table=document.getElementById('profile-table');
  const rows=[...table.tBodies[0].rows].filter(r=>!r.classList.contains('details'));
  rows.sort((a,b)=>a.cells[colIdx].innerText.localeCompare(b.cells[colIdx].innerText, undefined, {numeric:true}));
  const tbody=table.tBodies[0];
  rows.forEach(r=>{
    const detail=document.getElementById('detail-'+r.dataset.idx);
    tbody.appendChild(r);
    if(detail) tbody.appendChild(detail);
  });
}
function toggleDetails(idx){
  const row=document.getElementById('detail-'+idx);
  row.style.display = row.style.display === 'table-row' ? 'none' : 'table-row';
}
</script>
</head><body>
<h1>Data Profile</h1>
<p>rows: __ROW_COUNT__ | sample: __SAMPLE__% | columns: __COL_COUNT__ | generated: __GENERATED__</p>
<table id="profile-table">
<thead>
<tr>
<th onclick="sortTable(0)">Column</th>
<th onclick="sortTable(1)">Type</th>
<th onclick="sortTable(2)">Count</th>
<th onclick="sortTable(3)">Null %</th>
<th onclick="sortTable(4)">Distinct</th>
<th onclick="sortTable(5)">Min</th>
<th onclick="sortTable(6)">Max</th>
<th>Details</th>
</tr>
</thead>
<tbody>__ROWS__</tbody>
</table>
</body></html>"#;

fn render_html(report: &ProfileReport) -> String {
    let mut rows = String::new();
    for (idx, c) in report.columns.iter().enumerate() {
        let class = if c.null_percentage < 5.0 {
            "good"
        } else if c.null_percentage <= 20.0 {
            "warn"
        } else {
            "bad"
        };
        rows.push_str(&format!(
            "<tr data-idx=\"{idx}\"><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{:.2}%</td><td>{}</td><td>{}</td><td>{}</td><td><button onclick=\"toggleDetails({idx})\">Toggle</button></td></tr>",
            html_escape(&c.name),
            html_escape(&c.data_type),
            c.count,
            class,
            c.null_percentage,
            c.distinct_count,
            html_escape(c.min.as_deref().unwrap_or("")),
            html_escape(c.max.as_deref().unwrap_or("")),
        ));
        let top_values = c
            .top_5_values
            .iter()
            .map(|v| format!("{} ({})", html_escape(&v.value), v.freq))
            .collect::<Vec<_>>()
            .join("<br>");
        let patterns = c
            .pattern_sample
            .iter()
            .map(|v| format!("{} ({})", html_escape(&v.value), v.freq))
            .collect::<Vec<_>>()
            .join("<br>");
        rows.push_str(&format!(
            "<tr id=\"detail-{idx}\" class=\"details\"><td colspan=\"8\"><strong>Top 5:</strong> {}<br><strong>Patterns:</strong> {}<br><strong>Mean:</strong> {} <strong>Stddev:</strong> {} <strong>Median:</strong> {} <strong>P25:</strong> {} <strong>P75:</strong> {}</td></tr>",
            if top_values.is_empty() { "-".to_string() } else { top_values },
            if patterns.is_empty() { "-".to_string() } else { patterns },
            html_escape(c.mean.as_deref().unwrap_or("-")),
            html_escape(c.stddev.as_deref().unwrap_or("-")),
            html_escape(c.median.as_deref().unwrap_or("-")),
            html_escape(c.p25.as_deref().unwrap_or("-")),
            html_escape(c.p75.as_deref().unwrap_or("-")),
        ));
    }
    HTML_TEMPLATE
        .replace("__ROW_COUNT__", &report.row_count.to_string())
        .replace("__SAMPLE__", &report.sample_percentage.to_string())
        .replace("__COL_COUNT__", &report.columns.len().to_string())
        .replace("__GENERATED__", &html_escape(&report.generated_at))
        .replace("__ROWS__", &rows)
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('\"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_json_profile_file() {
        let df = df![
            "id" => [Some(1i64), Some(2)],
            "email" => [Some("a@example.com"), None::<&str>]
        ]
        .unwrap();
        let path = std::env::temp_dir().join(format!(
            "dtoo-profile-{}.json",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        Profiler::generate(
            &df,
            &ProfileOptions {
                path: path.clone(),
                format: ProfileFormat::Json,
                sample_percentage: 100,
                detail: ProfileDetail::Standard,
                top_k: 1000,
            },
        )
        .expect("generate profile");
        let contents = fs::read_to_string(&path).expect("read profile");
        assert!(contents.contains("\"row_count\": 2"));
        assert!(contents.contains("\"columns\""));
        fs::remove_file(path).ok();
    }

    #[test]
    fn generates_html_profile_file() {
        let df = df![
            "id" => [1i64, 2, 3]
        ]
        .unwrap();
        let path = std::env::temp_dir().join(format!(
            "dtoo-profile-{}.html",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        Profiler::generate(
            &df,
            &ProfileOptions {
                path: path.clone(),
                format: ProfileFormat::Html,
                sample_percentage: 100,
                detail: ProfileDetail::Standard,
                top_k: 1000,
            },
        )
        .expect("generate html profile");
        let contents = fs::read_to_string(&path).expect("read profile");
        assert!(contents.contains("<html>"));
        assert!(contents.contains("sortTable"));
        assert!(contents.contains("Toggle"));
        fs::remove_file(path).ok();
    }

    #[test]
    fn csv_renderer_escapes_commas_and_quotes() {
        let report = ProfileReport {
            row_count: 1,
            sample_percentage: 100,
            generated_at: "x".to_string(),
            columns: vec![ColumnProfile {
                name: "name".to_string(),
                data_type: "VARCHAR".to_string(),
                count: 1,
                null_count: 0,
                null_percentage: 0.0,
                distinct_count: 1,
                min: Some("a,b".to_string()),
                max: Some("a\"b".to_string()),
                mean: None,
                stddev: None,
                median: None,
                p25: None,
                p75: None,
                min_length: None,
                max_length: None,
                avg_length: None,
                top_5_values: Vec::new(),
                pattern_sample: Vec::new(),
                histogram: None,
                top_values: None,
                unique_ratio: None,
            }],
            detail: None,
            correlation_matrix: None,
        };
        let csv = render_csv(&report);
        assert!(csv.contains("\"a,b\""));
        assert!(csv.contains("\"a\"\"b\""));
    }

    #[test]
    fn distinct_count_excludes_nulls() {
        // DuckDB COUNT(DISTINCT col) excludes NULLs; Polars n_unique() does not.
        // "a", "a", NULL → 1 distinct non-null value.
        let df = df!["c" => [Some("a"), Some("a"), None::<&str>]].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Standard, 1000).expect("build report");
        assert_eq!(
            report.columns[0].distinct_count, 1,
            "distinct_count must exclude NULLs (parity with DuckDB COUNT(DISTINCT))"
        );
    }

    #[test]
    fn make_pattern_replaces_digits_and_letters() {
        assert_eq!(make_pattern("abc123def"), "aaaNaaa");
        assert_eq!(make_pattern("a@b.com"), "a@a.aaa");
        assert_eq!(make_pattern("2024-01-15"), "N-N-N");
        assert_eq!(make_pattern("hello"), "aaaaa");
        assert_eq!(make_pattern("42"), "N");
    }

    #[test]
    fn profile_numeric_column_fills_stats() {
        let df = df!["val" => [1.0f64, 2.0, 3.0, 4.0, 5.0]].unwrap();
        let path = std::env::temp_dir().join(format!(
            "dtoo-profile-numeric-{}.json",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Profiler::generate(
            &df,
            &ProfileOptions {
                path: path.clone(),
                format: ProfileFormat::Json,
                sample_percentage: 100,
                detail: ProfileDetail::Standard,
                top_k: 1000,
            },
        )
        .unwrap();
        let contents = fs::read_to_string(&path).unwrap();
        assert!(contents.contains("\"mean\""));
        assert!(contents.contains("\"stddev\""));
        assert!(contents.contains("\"median\""));
        fs::remove_file(path).ok();
    }

    #[test]
    fn profile_string_column_fills_lengths_and_patterns() {
        let df = df!["email" => ["a@example.com", "b@test.org", "c@foo.net"]].unwrap();
        let path = std::env::temp_dir().join(format!(
            "dtoo-profile-str-{}.json",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Profiler::generate(
            &df,
            &ProfileOptions {
                path: path.clone(),
                format: ProfileFormat::Json,
                sample_percentage: 100,
                detail: ProfileDetail::Standard,
                top_k: 1000,
            },
        )
        .unwrap();
        let contents = fs::read_to_string(&path).unwrap();
        assert!(contents.contains("\"min_length\""));
        assert!(contents.contains("\"pattern_sample\""));
        fs::remove_file(path).ok();
    }

    #[test]
    fn standard_detail_json_has_no_synth_fields() {
        let df = df!["id" => [1i64, 2, 3]].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Standard, 1000).expect("report");
        let json = serde_json::to_string_pretty(&report).unwrap();
        assert!(!json.contains("\"histogram\""));
        assert!(!json.contains("\"top_values\""));
        assert!(!json.contains("\"unique_ratio\""));
        assert!(!json.contains("\"detail\""));
        assert!(!json.contains("\"correlation_matrix\""));
    }

    #[test]
    fn profile_report_round_trips_through_json() {
        let df = df!["id" => [1i64, 2, 3]].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Standard, 1000).expect("report");
        let json = serde_json::to_string(&report).unwrap();
        let back: ProfileReport = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.row_count, 3);
        assert_eq!(back.columns[0].name, "id");
        assert!(back.columns[0].histogram.is_none());
    }

    #[test]
    fn synth_detail_adds_numeric_histogram() {
        let vals: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let df = df!["v" => vals].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let hist = report.columns[0].histogram.as_ref().expect("histogram");
        assert_eq!(hist.len(), 20);
        let total: u64 = hist.iter().map(|b| b.count).sum();
        assert_eq!(total, 1000);
        assert!(hist[0].lo <= 0.0 + f64::EPSILON);
        assert!((hist[19].hi - 999.0).abs() < 1e-9);
        for w in hist.windows(2) {
            assert!(w[0].hi <= w[1].lo + 1e-9, "buckets must be ordered");
        }
    }

    #[test]
    fn synth_detail_histogram_handles_low_cardinality() {
        // [1, 1, 2, 2, 3]: 1 and 2 are point masses; the lone 3 lands in a
        // continuous bucket.  After pruning, zero-count buckets are removed.
        let df = df!["v" => [1i64, 1, 2, 2, 3]].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let hist = report.columns[0].histogram.as_ref().expect("histogram");
        assert!(
            hist.len() <= 5,
            "expected <= 5 buckets with point-mass logic, got {}",
            hist.len()
        );
        assert_eq!(hist.iter().map(|b| b.count).sum::<u64>(), 5);
        // Point-mass bucket for 1 must carry exactly 2.
        let pm1 = hist
            .iter()
            .find(|b| b.lo == 1.0 && b.hi == 1.0)
            .expect("point-mass bucket for 1.0");
        assert_eq!(pm1.count, 2, "point-mass at 1.0 should have count 2");
        // Point-mass bucket for 2 must carry exactly 2.
        let pm2 = hist
            .iter()
            .find(|b| b.lo == 2.0 && b.hi == 2.0)
            .expect("point-mass bucket for 2.0");
        assert_eq!(pm2.count, 2, "point-mass at 2.0 should have count 2");
    }

    #[test]
    fn point_mass_at_maximum_gets_its_own_bucket() {
        let mut vals: Vec<f64> = (1..=10).map(|i| i as f64).collect();
        vals.extend(std::iter::repeat_n(100.0, 990));
        let df = df!["v" => vals].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let hist = report.columns[0].histogram.as_ref().expect("histogram");
        assert_eq!(hist.iter().map(|b| b.count).sum::<u64>(), 1000);
        let pm = hist
            .iter()
            .find(|b| b.lo == 100.0 && b.hi == 100.0)
            .expect("point mass at max");
        assert_eq!(pm.count, 990, "all ties belong to the point-mass bucket");
        for w in hist.windows(2) {
            assert!(w[0].hi <= w[1].lo + 1e-9);
        }
    }

    #[test]
    fn point_mass_in_interior_gets_its_own_bucket() {
        let mut vals: Vec<f64> = (0..100).map(|i| i as f64).collect();
        vals.extend(std::iter::repeat_n(50.0, 800));
        vals.extend((100..200).map(|i| i as f64));
        let df = df!["v" => vals].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let hist = report.columns[0].histogram.as_ref().expect("histogram");
        assert_eq!(hist.iter().map(|b| b.count).sum::<u64>(), 1000);
        let pm = hist
            .iter()
            .find(|b| b.lo == 50.0 && b.hi == 50.0)
            .expect("interior point mass");
        assert!(
            pm.count >= 800,
            "ties route to the point bucket, got {}",
            pm.count
        );
    }

    #[test]
    fn no_zero_count_buckets_survive() {
        let df = df!["v" => [1.0f64, 1.0, 2.0, 2.0, 3.0]].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let hist = report.columns[0].histogram.as_ref().expect("histogram");
        assert!(
            hist.iter().all(|b| b.count > 0),
            "zero-count buckets must be pruned: {hist:?}"
        );
        assert_eq!(hist.iter().map(|b| b.count).sum::<u64>(), 5);
    }

    #[test]
    fn synth_detail_adds_date_histogram_with_physical_values() {
        use chrono::NaiveDate;
        let dates: Vec<NaiveDate> = (0..100)
            .map(|i| NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i))
            .collect();
        let s = Series::new("d".into(), dates);
        let col = s.into_column();
        let n = col.len();
        let df = DataFrame::new(n, vec![col]).unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let hist = report.columns[0]
            .histogram
            .as_ref()
            .expect("date histogram");
        // Date physical repr is days since epoch; 2024-01-01 = 19723.
        assert!((hist[0].lo - 19723.0).abs() < 1.0);
    }

    #[test]
    fn standard_detail_has_no_histogram() {
        let df = df!["v" => [1.0f64, 2.0, 3.0]].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Standard, 1000).expect("report");
        assert!(report.columns[0].histogram.is_none());
    }

    // ── Fix 1: Decimal histograms must live in value space ───────────────────

    #[test]
    fn decimal_histogram_buckets_are_in_value_space() {
        // Build a column of Decimal values representing 11111.0 and 22222.0.
        // Cast Float64 → Decimal(10, 2) so physical repr would be 1111100 / 2222200
        // (value × 10^2), but the histogram lo/hi should still be ≈ 11111.0 / 22222.0.
        let s = Series::new("d".into(), &[11111.0f64, 22222.0f64])
            .cast(&DataType::Decimal(10, 2))
            .expect("cast to decimal");
        let col = s.into_column();
        let n = col.len();
        let df = DataFrame::new(n, vec![col]).unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let hist = report.columns[0].histogram.as_ref().expect("histogram");
        // The first bucket's lo should be close to 11111.0, NOT 1111100.0.
        assert!(
            (hist[0].lo - 11111.0).abs() < 1.0,
            "expected lo ≈ 11111.0 (value space), got {}",
            hist[0].lo
        );
    }

    // ── Fix 2: Point-mass buckets under extreme skew ─────────────────────────

    #[test]
    fn skewed_column_has_point_mass_bucket_for_zeros() {
        // 990 zeros + 10 values spread across 1..=100.
        let mut data: Vec<i64> = vec![0i64; 990];
        data.extend((1i64..=10).map(|i| i * 10));
        let s = Series::new("v".into(), data);
        let col = s.into_column();
        let n = col.len();
        let df = DataFrame::new(n, vec![col]).unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let hist = report.columns[0].histogram.as_ref().expect("histogram");

        // (a) counts sum to total rows
        let total: u64 = hist.iter().map(|b| b.count).sum();
        assert_eq!(total, 1000, "counts must sum to 1000");

        // (b) there is a point-mass bucket at 0.0 carrying >= 950 values
        let zero_bucket = hist.iter().find(|b| b.lo == 0.0 && b.hi == 0.0);
        assert!(
            zero_bucket.is_some(),
            "expected a point-mass bucket at 0.0; buckets: {hist:?}"
        );
        assert!(
            zero_bucket.unwrap().count >= 950,
            "point-mass bucket at 0.0 should carry >= 950, got {}",
            zero_bucket.unwrap().count
        );

        // (c) buckets are ordered
        for w in hist.windows(2) {
            assert!(
                w[0].hi <= w[1].lo + 1e-9,
                "buckets out of order: {:?} then {:?}",
                w[0],
                w[1]
            );
        }
    }

    // ── Fix 3: Degenerate branches ────────────────────────────────────────────

    #[test]
    fn all_identical_column_produces_single_point_mass_bucket() {
        let df = df!["v" => [5i64, 5, 5]].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let hist = report.columns[0].histogram.as_ref().expect("histogram");
        assert_eq!(hist.len(), 1, "single bucket expected; got {hist:?}");
        assert_eq!(hist[0].lo, 5.0);
        assert_eq!(hist[0].hi, 5.0);
        assert_eq!(hist[0].count, 3);
    }

    #[test]
    fn all_null_column_produces_no_histogram() {
        let s = Series::new("v".into(), &[None::<f64>, None::<f64>, None::<f64>]);
        let col = s.into_column();
        let n = col.len();
        let df = DataFrame::new(n, vec![col]).unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        assert!(
            report.columns[0].histogram.is_none(),
            "all-null column should yield no histogram"
        );
    }

    #[test]
    fn populated_synth_report_round_trips_through_json() {
        let df = df!["v" => (0..100).map(|i| i as f64).collect::<Vec<_>>()].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let json = serde_json::to_string(&report).unwrap();
        let back: ProfileReport =
            serde_json::from_str(&json).expect("deserialize populated report");
        assert_eq!(
            back.columns[0].histogram.as_ref().unwrap().len(),
            report.columns[0].histogram.as_ref().unwrap().len()
        );
        assert_eq!(back.detail.as_deref(), Some(SYNTH_DETAIL));
    }

    #[test]
    fn synth_detail_adds_top_k_and_unique_ratio() {
        let vals: Vec<String> = (0..50).map(|i| format!("v{}", i % 10)).collect();
        let df = df!["c" => vals].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 7).expect("report");
        let col = &report.columns[0];
        let top = col.top_values.as_ref().expect("top_values");
        assert_eq!(top.len(), 7, "truncated to top_k");
        assert_eq!(top[0].freq, 5);
        assert!((col.unique_ratio.unwrap() - 10.0 / 50.0).abs() < 1e-9);
        // top_5_values retained for backward compatibility
        assert_eq!(col.top_5_values.len(), 5);
    }

    #[test]
    fn unique_ratio_is_zero_for_empty_frame() {
        let col = Series::new_empty("c".into(), &DataType::String).into_column();
        let df = DataFrame::new(0, vec![col]).unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        assert_eq!(report.columns[0].unique_ratio, Some(0.0));
    }
}
