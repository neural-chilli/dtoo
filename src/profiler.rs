use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use chrono::Utc;
use polars::prelude::*;
use serde::Serialize;

use crate::{cli::ProfileFormat, error::DtooError};

#[derive(Clone, Debug, Serialize)]
pub struct ValueFrequency {
    pub value: String,
    pub freq: usize,
}

#[derive(Clone, Debug, Serialize)]
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
}

#[derive(Clone, Debug, Serialize)]
pub struct ProfileReport {
    pub row_count: usize,
    pub sample_percentage: u8,
    pub generated_at: String,
    pub columns: Vec<ColumnProfile>,
}

/// Profile generation options for query pipeline profile output.
#[derive(Clone, Debug)]
pub struct ProfileOptions {
    pub path: PathBuf,
    pub format: ProfileFormat,
    pub sample_percentage: u8,
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

        let report = build_report(source, options.sample_percentage)?;
        write_report(options, &report)
    }
}

// ── Polars-based report builder ───────────────────────────────────────────────

fn build_report(df: &DataFrame, sample_percentage: u8) -> Result<ProfileReport, DtooError> {
    let row_count = df.height();
    let mut columns = Vec::with_capacity(df.width());

    for col in df.columns() {
        columns.push(profile_column(col, row_count)?);
    }

    Ok(ProfileReport {
        row_count,
        sample_percentage,
        generated_at: Utc::now().to_rfc3339(),
        columns,
    })
}

fn profile_column(series: &Column, total_rows: usize) -> Result<ColumnProfile, DtooError> {
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

    let top_5 = top_values(series)?;

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

/// Returns the top-5 most frequent non-null values, sorted descending by count.
fn top_values(series: &Column) -> Result<Vec<ValueFrequency>, DtooError> {
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

    // value_counts with sort=true returns descending; take at most 5.
    pairs.truncate(5);
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
            }],
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
        let report = build_report(&df, 100).expect("build report");
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
            },
        )
        .unwrap();
        let contents = fs::read_to_string(&path).unwrap();
        assert!(contents.contains("\"min_length\""));
        assert!(contents.contains("\"pattern_sample\""));
        fs::remove_file(path).ok();
    }
}
