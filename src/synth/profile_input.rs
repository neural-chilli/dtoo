//! Loads dtoo profile JSON into the model the synth engine consumes.

#![allow(dead_code)]

use std::path::Path;

use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::{DataType, TimeUnit};

use crate::{
    error::DtooError,
    profiler::{CorrelationMatrix, HistogramBucket, ProfileReport, ValueFrequency},
};

/// A profile loaded for generation.
#[derive(Debug)]
pub struct SynthProfile {
    pub synth_detail: bool,
    pub row_count: usize,
    pub columns: Vec<SynthColumn>,
    pub correlation: Option<CorrelationMatrix>,
}

/// Per-column statistics in generation-ready form.
#[derive(Debug)]
pub struct SynthColumn {
    pub name: String,
    pub dtype: DataType,
    pub null_percentage: f64,
    pub non_null_count: usize,
    pub distinct_count: usize,
    pub unique_ratio: f64,
    pub histogram: Option<Vec<HistogramBucket>>,
    /// Fallback marginal: [min, p25, median, p75, max] as physical f64.
    pub quantiles: Option<Vec<f64>>,
    pub top_values: Vec<ValueFrequency>,
    pub pattern_sample: Vec<ValueFrequency>,
    pub min_length: usize,
    pub max_length: usize,
}

fn config_err(message: String) -> DtooError {
    DtooError::Config { message }
}

/// Parses the Debug-formatted Polars dtype names that profiles record.
/// Unknown/exotic dtypes degrade to String (pattern-based generation).
pub fn parse_dtype(s: &str) -> DataType {
    match s {
        "Int8" => DataType::Int8,
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "UInt8" => DataType::UInt8,
        "UInt16" => DataType::UInt16,
        "UInt32" => DataType::UInt32,
        "UInt64" => DataType::UInt64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "String" => DataType::String,
        "Boolean" => DataType::Boolean,
        "Date" => DataType::Date,
        "Time" => DataType::Time,
        s if s.starts_with("Datetime") => {
            let unit = if s.contains("'ns'") {
                TimeUnit::Nanoseconds
            } else if s.contains("'ms'") {
                TimeUnit::Milliseconds
            } else {
                TimeUnit::Microseconds // "'μs'", "'us'", or anything unrecognized
            };
            DataType::Datetime(unit, None)
        }
        s if s.starts_with("Decimal") => {
            let nums: Vec<usize> = s
                .split(|c: char| !c.is_ascii_digit())
                .filter(|t| !t.is_empty())
                .filter_map(|t| t.parse().ok())
                .collect();
            match nums.as_slice() {
                // In polars 0.54 Decimal takes (usize, usize), not Option
                [p, sc, ..] => DataType::Decimal(*p, *sc),
                _ => DataType::Decimal(18, 3),
            }
        }
        _ => DataType::String,
    }
}

/// Parses a profile min/max string into physical f64 for the dtype.
pub fn parse_bound(dtype: &DataType, raw: &str) -> Option<f64> {
    match dtype {
        DataType::Date => NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok().map(|d| {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            (d - epoch).num_days() as f64
        }),
        DataType::Datetime(unit, _) => {
            let dt = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f"))
                .ok()?;
            let micros = dt.and_utc().timestamp_micros() as f64;
            Some(match unit {
                TimeUnit::Nanoseconds => micros * 1_000.0,
                TimeUnit::Microseconds => micros,
                TimeUnit::Milliseconds => micros / 1_000.0,
            })
        }
        _ => raw.parse::<f64>().ok().filter(|v| v.is_finite()),
    }
}

/// Loads a dtoo profile JSON for generation.
pub fn load_profile(path: &Path) -> Result<SynthProfile, DtooError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| config_err(format!("cannot read profile {}: {e}", path.display())))?;
    let report: ProfileReport = serde_json::from_str(&raw).map_err(|e| {
        config_err(format!(
            "{} is not a dtoo profile JSON: {e}",
            path.display()
        ))
    })?;

    let synth_detail = report.detail.as_deref() == Some(crate::profiler::SYNTH_DETAIL);
    let columns = report
        .columns
        .iter()
        .map(|c| {
            let dtype = parse_dtype(&c.data_type);
            let quantiles = build_quantiles(&dtype, c);
            let parse_len = |v: &Option<String>| v.as_deref().and_then(|s| s.parse::<usize>().ok());
            SynthColumn {
                name: c.name.clone(),
                dtype,
                null_percentage: c.null_percentage,
                non_null_count: c.count.saturating_sub(c.null_count),
                distinct_count: c.distinct_count,
                unique_ratio: c.unique_ratio.unwrap_or_else(|| {
                    if c.count == 0 {
                        0.0
                    } else {
                        c.distinct_count as f64 / c.count as f64
                    }
                }),
                histogram: c.histogram.clone(),
                quantiles,
                top_values: c
                    .top_values
                    .clone()
                    .unwrap_or_else(|| c.top_5_values.clone()),
                pattern_sample: c.pattern_sample.clone(),
                min_length: parse_len(&c.min_length).unwrap_or(1),
                max_length: parse_len(&c.max_length).unwrap_or(12),
            }
        })
        .collect();

    Ok(SynthProfile {
        synth_detail,
        row_count: report.row_count,
        columns,
        correlation: report.correlation_matrix.clone(),
    })
}

/// Builds the 5-point fallback marginal [min, p25, median, p75, max].
/// Temporal columns only record min/max, so interior points are interpolated.
fn build_quantiles(dtype: &DataType, c: &crate::profiler::ColumnProfile) -> Option<Vec<f64>> {
    let min = parse_bound(dtype, c.min.as_deref()?)?;
    let max = parse_bound(dtype, c.max.as_deref()?)?;
    let mid = |p: f64, raw: &Option<String>| {
        raw.as_deref()
            .and_then(|s| parse_bound(dtype, s))
            .unwrap_or(min + (max - min) * p)
    };
    let mut q = vec![
        min,
        mid(0.25, &c.p25),
        mid(0.50, &c.median),
        mid(0.75, &c.p75),
        max,
    ];
    // Enforce monotonicity defensively (string-parsed stats can be jumbled).
    for i in 1..q.len() {
        if q[i] < q[i - 1] {
            q[i] = q[i - 1];
        }
    }
    Some(q)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_dtypes() {
        assert_eq!(parse_dtype("Int64"), DataType::Int64);
        assert_eq!(parse_dtype("Float64"), DataType::Float64);
        assert_eq!(parse_dtype("String"), DataType::String);
        assert_eq!(parse_dtype("Boolean"), DataType::Boolean);
        assert_eq!(parse_dtype("Date"), DataType::Date);
        assert!(matches!(
            parse_dtype("Datetime('μs')"),
            DataType::Datetime(_, _)
        ));
        assert!(matches!(
            parse_dtype("Decimal(10, 2)"),
            DataType::Decimal(_, _)
        ));
        // Unknown dtypes degrade to String (pattern sampling).
        assert_eq!(parse_dtype("List(Int64)"), DataType::String);
    }

    #[test]
    fn parses_datetime_time_units_from_debug_strings() {
        assert!(matches!(
            parse_dtype("Datetime('ns')"),
            DataType::Datetime(TimeUnit::Nanoseconds, _)
        ));
        assert!(matches!(
            parse_dtype("Datetime('us')"),
            DataType::Datetime(TimeUnit::Microseconds, _)
        ));
        assert!(matches!(
            parse_dtype("Datetime('μs')"),
            DataType::Datetime(TimeUnit::Microseconds, _)
        ));
        assert!(matches!(
            parse_dtype("Datetime('ms')"),
            DataType::Datetime(TimeUnit::Milliseconds, _)
        ));
    }

    #[test]
    fn parses_temporal_bounds_to_physical_f64() {
        let days = parse_bound(&DataType::Date, "2024-01-01").expect("date");
        assert!((days - 19723.0).abs() < 1.0);
        let micros = parse_bound(
            &DataType::Datetime(TimeUnit::Microseconds, None),
            "2024-01-01 00:00:00",
        )
        .expect("datetime");
        assert!((micros - 19723.0 * 86_400.0 * 1_000_000.0).abs() < 1e9);
        assert!((parse_bound(&DataType::Int64, "42").unwrap() - 42.0).abs() < 1e-9);
        assert!(parse_bound(&DataType::Int64, "garbage").is_none());
    }

    #[test]
    fn loads_synth_profile_json() {
        let json = r#"{
            "row_count": 100,
            "sample_percentage": 100,
            "generated_at": "x",
            "detail": "synth",
            "columns": [{
                "name": "amount",
                "data_type": "Float64",
                "count": 100, "null_count": 10, "null_percentage": 10.0,
                "distinct_count": 90,
                "min": "1.5", "max": "99.5", "mean": "50", "stddev": "10",
                "median": "50", "p25": "25", "p75": "75",
                "min_length": null, "max_length": null, "avg_length": null,
                "top_5_values": [], "pattern_sample": [],
                "histogram": [{"lo": 1.5, "hi": 50.0, "count": 50}, {"lo": 50.0, "hi": 99.5, "count": 40}],
                "top_values": [{"value": "7.0", "freq": 3}],
                "unique_ratio": 0.9
            }],
            "correlation_matrix": {"columns": ["amount"], "data": [[1.0]]}
        }"#;
        let dir = std::env::temp_dir().join(format!(
            "dtoo-synthprof-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("p.json");
        std::fs::write(&path, json).unwrap();

        let profile = load_profile(&path).expect("load");
        assert!(profile.synth_detail);
        let col = &profile.columns[0];
        assert_eq!(col.name, "amount");
        assert_eq!(col.dtype, DataType::Float64);
        assert!((col.null_percentage - 10.0).abs() < 1e-9);
        assert!((col.unique_ratio - 0.9).abs() < 1e-9);
        assert_eq!(col.histogram.as_ref().unwrap().len(), 2);
        assert!(profile.correlation.is_some());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn standard_profile_builds_fallback_quantiles() {
        let json = r#"{
            "row_count": 10, "sample_percentage": 100, "generated_at": "x",
            "columns": [{
                "name": "v", "data_type": "Int64",
                "count": 10, "null_count": 0, "null_percentage": 0.0,
                "distinct_count": 10,
                "min": "0", "max": "100", "mean": "50", "stddev": "30",
                "median": "50", "p25": "25", "p75": "75",
                "min_length": null, "max_length": null, "avg_length": null,
                "top_5_values": [], "pattern_sample": []
            }]
        }"#;
        let path = std::env::temp_dir().join(format!(
            "dtoo-synthprof-std-{}.json",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, json).unwrap();
        let profile = load_profile(&path).expect("load");
        assert!(!profile.synth_detail);
        let col = &profile.columns[0];
        assert!(col.histogram.is_none());
        assert_eq!(col.quantiles, Some(vec![0.0, 25.0, 50.0, 75.0, 100.0]));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn missing_profile_is_a_clear_error() {
        let err =
            load_profile(Path::new("/tmp/definitely-missing-dtoo-prof.json")).expect_err("missing");
        assert!(
            err.to_string()
                .contains("definitely-missing-dtoo-prof.json")
        );
    }
}
