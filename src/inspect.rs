use comfy_table::{Cell, ContentArrangement, Row, Table, presets::UTF8_FULL};
use polars::prelude::DataType;

use crate::{
    cli::InspectArgs, crypto, error::DtooError, path_utils::split_excel_sheet_from_path,
    polars_engine::PolarsEngine, types::InputFormat,
};

pub fn run(args: &InspectArgs) -> Result<(), DtooError> {
    let target = args.path.to_string_lossy();
    let (path, sheet) = split_excel_sheet_from_path(&target);
    let format = detect_format(&path)?;

    let input_format = to_input_format(&format, sheet.as_deref(), &path, args.delimiter);

    let engine = PolarsEngine::new();
    let lf = engine.scan(&path, &input_format)?;

    let row_count = engine.row_count(lf.clone())?;
    let schema = engine.schema_of(&lf)?;
    let df = engine.collect(lf)?;
    let preview = df.head(Some(args.rows));

    if args.crypto_discover {
        let rows = if let Some(profile_name) = args.crypto_profile.as_deref() {
            let profile =
                crypto::resolve_profile(profile_name, args.crypto_profiles_file.as_deref(), None)?;
            crypto::discover_wrapped_values_df(&df, &profile.detection, &profile.columns)?
        } else {
            crypto::discover_wrapped_values_df(&df, &crypto::DetectionConfig::default(), &[])?
        };

        println!();
        println!("Detected wrapped encrypted values:");
        for row in rows {
            let percent = if row.total == 0 {
                0.0
            } else {
                (row.encrypted as f64 * 100.0) / row.total as f64
            };
            let noise_hint = if row.total > 0 && percent < 5.0 {
                " — likely noise"
            } else {
                ""
            };
            println!(
                "  {}: {:.1}% ({}/{}){}",
                row.column, percent, row.encrypted, row.total, noise_hint
            );
        }
    }

    println!("File: {}", args.path.display());
    println!("Format: {}", format_label(&format));
    println!("Rows: {}", format_count(row_count));
    println!("Columns: {}", schema.len());
    println!();
    println!("Schema:");
    for (name, dtype) in &schema {
        println!("  {name:<14} {dtype}");
    }
    println!();
    println!("Preview (first {} rows):", args.rows);
    println!("{}", render_preview_table(&schema, &preview));

    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum InspectFormat {
    Csv,
    Parquet,
    Ndjson,
    Excel,
}

fn detect_format(path: &str) -> Result<InspectFormat, DtooError> {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".csv") || lower.ends_with(".tsv") {
        return Ok(InspectFormat::Csv);
    }
    if lower.ends_with(".parquet") {
        return Ok(InspectFormat::Parquet);
    }
    if lower.ends_with(".ndjson") || lower.ends_with(".jsonl") {
        return Ok(InspectFormat::Ndjson);
    }
    if lower.ends_with(".xlsx") || lower.ends_with(".xls") {
        return Ok(InspectFormat::Excel);
    }

    Err(DtooError::Config {
        message: format!("unsupported inspect file format for path `{path}`"),
    })
}

fn to_input_format(
    format: &InspectFormat,
    sheet: Option<&str>,
    path: &str,
    delimiter: char,
) -> InputFormat {
    match format {
        InspectFormat::Csv => {
            let delim = if path.to_ascii_lowercase().ends_with(".tsv") {
                '\t'
            } else {
                delimiter
            };
            InputFormat::Csv { delimiter: delim }
        }
        InspectFormat::Parquet => InputFormat::Parquet,
        InspectFormat::Ndjson => InputFormat::Ndjson,
        InspectFormat::Excel => InputFormat::Excel {
            sheet: sheet.map(ToString::to_string),
        },
    }
}

fn render_preview_table(
    schema: &[(String, DataType)],
    preview: &polars::frame::DataFrame,
) -> Table {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);

    let header = schema
        .iter()
        .map(|(name, _)| Cell::new(name))
        .collect::<Vec<_>>();
    table.set_header(header);

    // Build per-column string vectors to avoid repeated schema lookups
    let col_strings: Vec<Vec<Option<String>>> = schema
        .iter()
        .map(|(name, _)| {
            preview
                .column(name)
                .ok()
                .and_then(|col| col.cast(&DataType::String).ok())
                .and_then(|col| {
                    col.str().ok().map(|ca| {
                        (0..preview.height())
                            .map(|i| ca.get(i).map(|s| s.to_string()))
                            .collect()
                    })
                })
                .unwrap_or_else(|| vec![None; preview.height()])
        })
        .collect();

    for row_idx in 0..preview.height() {
        let cells: Vec<Cell> = col_strings
            .iter()
            .map(|col| Cell::new(col.get(row_idx).and_then(|s| s.as_deref()).unwrap_or("")))
            .collect();
        table.add_row(Row::from(cells));
    }

    table
}

fn format_label(format: &InspectFormat) -> &'static str {
    match format {
        InspectFormat::Csv => "CSV",
        InspectFormat::Parquet => "Parquet",
        InspectFormat::Ndjson => "NDJSON",
        InspectFormat::Excel => "Excel",
    }
}

fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (idx, ch) in digits.chars().enumerate() {
        if idx > 0 && (digits.len() - idx).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path_utils::is_cloud_path;

    #[test]
    fn split_excel_sheet_parses_colon_syntax() {
        let (path, sheet) = split_excel_sheet_from_path("sales.xlsx:Sheet2");
        assert_eq!(path, "sales.xlsx");
        assert_eq!(sheet.as_deref(), Some("Sheet2"));
    }

    #[test]
    fn detect_format_rejects_unknown_extension() {
        let err = detect_format("data.unknown").expect_err("must reject unsupported format");
        assert!(err.to_string().contains("unsupported inspect file format"));
    }

    #[test]
    fn cloud_path_supports_abfss() {
        assert!(is_cloud_path("abfss://container/data.parquet"));
    }
}
