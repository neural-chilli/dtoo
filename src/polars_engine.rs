//! Pure-Rust data engine built on Polars. See docs/specs/34-polars-engine.md.

use std::path::Path;

use polars::prelude::*;

use crate::error::DtooError;
use crate::types::{CompressionCodec, ExportFormat, InputFormat};

/// Stateless handle for Polars-backed data operations.
pub struct PolarsEngine;

impl Default for PolarsEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl PolarsEngine {
    /// Construct a new engine handle.
    pub fn new() -> Self {
        Self
    }

    /// Lazily scan an input file by format. Glob patterns are supported by the
    /// underlying Polars scanners for Parquet/CSV/NDJSON.
    pub fn scan(&self, path: &str, format: &InputFormat) -> Result<LazyFrame, DtooError> {
        reject_cloud(path)?;
        match format {
            InputFormat::Csv { delimiter } => LazyCsvReader::new(path.into())
                .with_separator(*delimiter as u8)
                .with_has_header(true)
                .with_ignore_errors(false) // explicit: ragged/unparseable rows must error, not silently drop
                .finish()
                .map_err(|e| read_err(path, e)),
            InputFormat::Ndjson => LazyJsonLineReader::new(path.into())
                .finish()
                .map_err(|e| read_err(path, e)),
            InputFormat::Parquet => {
                LazyFrame::scan_parquet(path.into(), ScanArgsParquet::default())
                    .map_err(|e| read_err(path, e))
            }
            InputFormat::Excel { sheet } => read_excel(path, sheet.as_deref()),
        }
    }

    /// Materialize a LazyFrame into a DataFrame.
    pub fn collect(&self, lf: LazyFrame) -> Result<DataFrame, DtooError> {
        lf.collect().map_err(|e| read_err("(query)", e))
    }

    /// Concatenate frames union-by-name (diagonal), filling missing columns with null.
    pub fn concat_by_name(&self, frames: Vec<LazyFrame>) -> Result<LazyFrame, DtooError> {
        if frames.is_empty() {
            return Err(read_err("(concat)", "no frames to concatenate"));
        }
        concat_lf_diagonal(frames, UnionArgs::default()).map_err(|e| read_err("(concat)", e))
    }

    /// Count rows without retaining the materialized frame.
    pub fn row_count(&self, lf: LazyFrame) -> Result<usize, DtooError> {
        Ok(self.collect(lf)?.height())
    }

    /// Run user SQL with `base` registered as the magic table `_` and each ref
    /// registered under its name. Returns the resulting LazyFrame.
    pub fn run_sql(
        &self,
        base: LazyFrame,
        refs: &[(String, LazyFrame)],
        sql: &str,
    ) -> Result<LazyFrame, DtooError> {
        let mut ctx = polars::sql::SQLContext::new();
        ctx.register("_", base);
        for (name, lf) in refs {
            ctx.register(name, lf.clone());
        }
        ctx.execute(sql).map_err(|e| sql_err(sql, e))
    }

    /// Return (column name, dtype) pairs without materializing the frame.
    pub fn schema_of(&self, lf: &LazyFrame) -> Result<Vec<(String, DataType)>, DtooError> {
        let schema = lf.clone().collect_schema().map_err(|e| DtooError::Schema {
            message: e.to_string(),
        })?;
        Ok(schema
            .iter()
            .map(|(name, dtype)| (name.to_string(), dtype.clone()))
            .collect())
    }

    /// Write a DataFrame to a file path or, when `dest` is None, to stdout.
    pub fn write(
        &self,
        mut df: DataFrame,
        dest: Option<&Path>,
        format: ExportFormat,
        header: bool,
        delimiter: char,
        compression: Option<CompressionCodec>,
    ) -> Result<(), DtooError> {
        let sink = open_sink(dest)?;
        match format {
            ExportFormat::Csv => write_with_optional_compression(sink, compression, |w| {
                CsvWriter::new(w)
                    .include_header(header)
                    .with_separator(delimiter as u8)
                    .finish(&mut df)
                    .map(|_| ())
                    .map_err(write_err)
            }),
            ExportFormat::Parquet => {
                let codec = match compression {
                    Some(CompressionCodec::Gzip) => ParquetCompression::Gzip(None),
                    Some(CompressionCodec::Zstd) => ParquetCompression::Zstd(None),
                    None => ParquetCompression::default(),
                };
                ParquetWriter::new(sink)
                    .with_compression(codec)
                    .finish(&mut df)
                    .map(|_| ())
                    .map_err(write_err)
            }
            ExportFormat::Ndjson => write_with_optional_compression(sink, compression, |w| {
                JsonWriter::new(w)
                    .with_json_format(JsonFormat::JsonLines)
                    .finish(&mut df)
                    .map(|_| ())
                    .map_err(write_err)
            }),
        }
    }
}

fn read_err(path: &str, source: impl std::fmt::Display) -> DtooError {
    DtooError::FileProcess {
        path: path.to_string(),
        message: source.to_string(),
    }
}

fn write_err(source: impl std::fmt::Display) -> DtooError {
    DtooError::Output {
        message: source.to_string(),
    }
}

fn reject_cloud(path: &str) -> Result<(), DtooError> {
    if crate::path_utils::is_cloud_path(path) {
        return Err(DtooError::Config {
            message: format!("cloud storage ({path}) is not supported in this build yet"),
        });
    }
    Ok(())
}

fn open_sink(dest: Option<&Path>) -> Result<Box<dyn std::io::Write>, DtooError> {
    match dest {
        Some(path) => {
            let file = std::fs::File::create(path)
                .map_err(|e| write_err(format!("{}: {e}", path.display())))?;
            Ok(Box::new(file))
        }
        None => Ok(Box::new(std::io::stdout())),
    }
}

/// Run `write_body` against a (possibly compressed) sink, then explicitly
/// finalize the compression frame so a failed final flush is surfaced as an
/// error rather than silently truncating the output.
fn write_with_optional_compression<F>(
    sink: Box<dyn std::io::Write>,
    compression: Option<CompressionCodec>,
    write_body: F,
) -> Result<(), DtooError>
where
    F: FnOnce(&mut dyn std::io::Write) -> Result<(), DtooError>,
{
    match compression {
        None => {
            let mut sink = sink;
            write_body(&mut sink)?;
            sink.flush().map_err(write_err)
        }
        Some(CompressionCodec::Gzip) => {
            let mut encoder = flate2::write::GzEncoder::new(sink, flate2::Compression::default());
            write_body(&mut encoder)?;
            encoder.finish().map_err(write_err)?;
            Ok(())
        }
        Some(CompressionCodec::Zstd) => {
            let mut encoder = zstd::stream::write::Encoder::new(sink, 0).map_err(write_err)?;
            write_body(&mut encoder)?;
            encoder.finish().map_err(write_err)?;
            Ok(())
        }
    }
}

fn read_excel(path: &str, sheet: Option<&str>) -> Result<LazyFrame, DtooError> {
    use calamine::{Reader, open_workbook_auto};

    let mut workbook = open_workbook_auto(path).map_err(|e| read_err(path, e))?;
    let sheet_name = match sheet {
        Some(name) => name.to_string(),
        None => workbook
            .sheet_names()
            .first()
            .cloned()
            .ok_or_else(|| read_err(path, "workbook has no sheets"))?,
    };
    let range = workbook
        .worksheet_range(&sheet_name)
        .map_err(|e| read_err(path, e))?;

    let mut rows = range.rows();

    // Calamine 0.35 pads every row to the sheet's maximum used width, so an
    // over-wide data row causes calamine to also widen the header row — but
    // the extra header cells are `Empty` (serialised to "").  We detect this
    // by checking that every header cell has a non-empty name.  A trailing
    // empty-string header means a data row had more cells than the user
    // declared columns, which is a data-integrity error: the value would
    // otherwise be silently ingested under an unnamed column.
    let headers: Vec<String> = match rows.next() {
        Some(first) => {
            // Strip any trailing empty-string columns that calamine appended
            // purely as padding (they have no header name).  After stripping,
            // if the rightmost *named* column is genuinely empty that is also
            // an error — we only strip pure trailing padding.
            let all: Vec<String> = first.iter().map(cell_to_string).collect();
            // Find how many trailing columns are empty (calamine padding).
            let trailing_empty = all.iter().rev().take_while(|s| s.is_empty()).count();
            let named_count = all.len() - trailing_empty;
            if named_count == 0 {
                return Ok(DataFrame::empty().lazy());
            }
            // If any non-trailing header cell is empty the sheet is malformed.
            for (i, h) in all[..named_count].iter().enumerate() {
                if h.is_empty() {
                    return Err(read_err(
                        path,
                        format!("header column {i} has an empty name — sheet is malformed"),
                    ));
                }
            }
            // If there were trailing-empty padding columns, that means at
            // least one data row had more cells than declared headers.  Error
            // now rather than silently ingesting garbage columns.
            if trailing_empty > 0 {
                return Err(read_err(
                    path,
                    format!(
                        "at least one data row has {} extra cell(s) beyond the {} declared \
                         header columns — refusing to silently drop data",
                        trailing_empty, named_count
                    ),
                ));
            }
            all
        }
        None => return Ok(DataFrame::empty().lazy()),
    };

    let mut columns: Vec<Vec<String>> = vec![Vec::new(); headers.len()];
    for row in rows {
        // Calamine pads all rows to the sheet width, so row.len() always
        // equals headers.len() here.  We iterate only up to headers.len()
        // as a defensive measure in case of short rows (null-fill behaviour).
        for (idx, col) in columns.iter_mut().enumerate() {
            let cell = row.get(idx).map(cell_to_string).unwrap_or_default();
            col.push(cell);
        }
    }

    let height = columns.first().map(|c| c.len()).unwrap_or(0);
    let series: Vec<Column> = headers
        .into_iter()
        .zip(columns)
        .map(|(name, values)| Series::new(name.into(), values).into_column())
        .collect();

    DataFrame::new(height, series)
        .map(|df| df.lazy())
        .map_err(|e| read_err(path, e))
}

fn cell_to_string(cell: &calamine::Data) -> String {
    cell.to_string()
}

fn sql_err(sql: &str, source: PolarsError) -> DtooError {
    DtooError::Sql {
        context: "polars-sql".to_string(),
        sql: sql.to_string(),
        source: Box::new(std::io::Error::other(source.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str, ext: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("dtoo-pe-{name}-{nanos}.{ext}"))
    }

    fn write_test_xlsx(path: &std::path::Path, sheet: &str) {
        use rust_xlsxwriter::Workbook;
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();
        worksheet.set_name(sheet).unwrap();
        worksheet.write_string(0, 0, "id").unwrap();
        worksheet.write_string(0, 1, "name").unwrap();
        worksheet.write_string(1, 0, "1").unwrap();
        worksheet.write_string(1, 1, "alice").unwrap();
        worksheet.write_string(2, 0, "2").unwrap();
        worksheet.write_string(2, 1, "bob").unwrap();
        workbook.save(path).unwrap();
    }

    #[test]
    fn scan_excel_reads_default_sheet() {
        let path = tmp("xlsx-default", "xlsx");
        write_test_xlsx(&path, "Sheet1");
        let engine = PolarsEngine::new();

        let lf = engine
            .scan(path.to_str().unwrap(), &InputFormat::Excel { sheet: None })
            .unwrap();
        let df = engine.collect(lf).unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.get_column_names(), vec!["id", "name"]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn scan_excel_reads_named_sheet() {
        let path = tmp("xlsx-named", "xlsx");
        write_test_xlsx(&path, "Data");
        let engine = PolarsEngine::new();

        let lf = engine
            .scan(
                path.to_str().unwrap(),
                &InputFormat::Excel {
                    sheet: Some("Data".to_string()),
                },
            )
            .unwrap();
        let df = engine.collect(lf).unwrap();

        assert_eq!(df.height(), 2);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn scan_ndjson_reads_rows() {
        let path = tmp("ndjson", "ndjson");
        std::fs::write(
            &path,
            "{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n",
        )
        .unwrap();
        let engine = PolarsEngine::new();

        let lf = engine
            .scan(path.to_str().unwrap(), &InputFormat::Ndjson)
            .unwrap();
        assert_eq!(engine.row_count(lf).unwrap(), 2);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn scan_csv_reads_rows_and_columns() {
        let path = tmp("csv", "csv");
        std::fs::write(&path, "id,name\n1,alice\n2,bob\n").unwrap();
        let engine = PolarsEngine::new();

        let lf = engine
            .scan(path.to_str().unwrap(), &InputFormat::Csv { delimiter: ',' })
            .unwrap();
        let count = engine.row_count(lf.clone()).unwrap();
        let df = engine.collect(lf).unwrap();

        assert_eq!(count, 2);
        assert_eq!(df.get_column_names(), vec!["id", "name"]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn parquet_roundtrips() {
        let src = tmp("pq-src", "csv");
        std::fs::write(&src, "id,name\n1,alice\n2,bob\n").unwrap();
        let pq = tmp("pq-mid", "parquet");
        let engine = PolarsEngine::new();

        let df = engine
            .collect(
                engine
                    .scan(src.to_str().unwrap(), &InputFormat::Csv { delimiter: ',' })
                    .unwrap(),
            )
            .unwrap();
        engine
            .write(
                df,
                Some(pq.as_path()),
                ExportFormat::Parquet,
                true,
                ',',
                None,
            )
            .unwrap();

        let back = engine
            .scan(pq.to_str().unwrap(), &InputFormat::Parquet)
            .unwrap();
        assert_eq!(engine.row_count(back).unwrap(), 2);
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(pq);
    }

    #[test]
    fn ndjson_write_roundtrips() {
        let src = tmp("wnd-src", "csv");
        std::fs::write(&src, "id,name\n1,alice\n").unwrap();
        let dst = tmp("wnd-dst", "ndjson");
        let engine = PolarsEngine::new();

        let df = engine
            .collect(
                engine
                    .scan(src.to_str().unwrap(), &InputFormat::Csv { delimiter: ',' })
                    .unwrap(),
            )
            .unwrap();
        engine
            .write(
                df,
                Some(dst.as_path()),
                ExportFormat::Ndjson,
                true,
                ',',
                None,
            )
            .unwrap();

        let back = engine
            .scan(dst.to_str().unwrap(), &InputFormat::Ndjson)
            .unwrap();
        assert_eq!(engine.row_count(back).unwrap(), 1);
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn write_csv_roundtrips() {
        let src = tmp("wcsv-src", "csv");
        std::fs::write(&src, "id,name\n1,alice\n").unwrap();
        let dst = tmp("wcsv-dst", "csv");
        let engine = PolarsEngine::new();

        let df = engine
            .collect(
                engine
                    .scan(src.to_str().unwrap(), &InputFormat::Csv { delimiter: ',' })
                    .unwrap(),
            )
            .unwrap();
        engine
            .write(df, Some(dst.as_path()), ExportFormat::Csv, true, ',', None)
            .unwrap();

        let written = std::fs::read_to_string(&dst).unwrap();
        assert!(written.contains("id,name"));
        assert!(written.contains("1,alice"));
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn write_csv_without_header_omits_header_row() {
        let src = tmp("noh-src", "csv");
        std::fs::write(&src, "id,name\n1,alice\n").unwrap();
        let dst = tmp("noh-dst", "csv");
        let engine = PolarsEngine::new();
        let df = engine
            .collect(
                engine
                    .scan(src.to_str().unwrap(), &InputFormat::Csv { delimiter: ',' })
                    .unwrap(),
            )
            .unwrap();
        engine
            .write(df, Some(dst.as_path()), ExportFormat::Csv, false, ',', None)
            .unwrap();
        let written = std::fs::read_to_string(&dst).unwrap();
        assert!(!written.contains("id,name"));
        assert!(written.contains("1,alice"));
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn csv_gzip_output_is_decompressible() {
        let src = tmp("gz-src", "csv");
        std::fs::write(&src, "id,name\n1,alice\n").unwrap();
        let dst = tmp("gz-dst", "csv.gz");
        let engine = PolarsEngine::new();

        let df = engine
            .collect(
                engine
                    .scan(src.to_str().unwrap(), &InputFormat::Csv { delimiter: ',' })
                    .unwrap(),
            )
            .unwrap();
        engine
            .write(
                df,
                Some(dst.as_path()),
                ExportFormat::Csv,
                true,
                ',',
                Some(CompressionCodec::Gzip),
            )
            .unwrap();

        let bytes = std::fs::read(&dst).unwrap();
        let mut decoder = flate2::read::GzDecoder::new(&bytes[..]);
        let mut text = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut text).unwrap();
        assert!(text.contains("1,alice"));
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn concat_by_name_aligns_differing_columns() {
        let engine = PolarsEngine::new();
        let a = df!["id" => [1i64], "name" => ["alice"]].unwrap().lazy();
        let b = df!["id" => [2i64], "extra" => ["x"]].unwrap().lazy();

        let merged = engine.concat_by_name(vec![a, b]).unwrap();
        let df = engine.collect(merged).unwrap();

        assert_eq!(df.height(), 2);
        let mut names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec!["extra".to_string(), "id".to_string(), "name".to_string()]
        );
    }

    #[test]
    fn run_sql_filters_and_joins() {
        let engine = PolarsEngine::new();
        let base = df!["id" => [1i64, 2], "region_id" => [10i64, 20]]
            .unwrap()
            .lazy();
        let regions = df!["id" => [10i64, 20], "region_name" => ["EMEA", "APAC"]]
            .unwrap()
            .lazy();

        let out = engine
            .run_sql(
                base,
                &[("regions".to_string(), regions)],
                "SELECT _.id, r.region_name FROM _ JOIN regions r ON _.region_id = r.id WHERE _.id = 1",
            )
            .unwrap();
        let df = engine.collect(out).unwrap();

        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column("region_name").unwrap().str().unwrap().get(0),
            Some("EMEA")
        );
    }

    #[test]
    fn run_sql_returns_error_on_unsupported_sql_without_hanging() {
        // In Polars 0.54, DELETE is actually implemented (inverted filter), so
        // it silently succeeds.  INSERT is unhandled and hits the catch-all
        // `_ => polars_bail!` branch in SQLContext::execute, making it the
        // canonical unsupported-statement test.  The non-negotiable requirement
        // is that an error is returned as a Result (no panic, no hang).
        let engine = PolarsEngine::new();
        let base = df!["id" => [1i64]].unwrap().lazy();
        let result = engine.run_sql(base, &[], "INSERT INTO _ VALUES (2)");
        assert!(matches!(result, Err(DtooError::Sql { .. })));
    }

    #[test]
    fn schema_of_returns_names_and_types() {
        let engine = PolarsEngine::new();
        let lf = df!["id" => [1i64], "name" => ["alice"]].unwrap().lazy();

        let schema = engine.schema_of(&lf).unwrap();
        let names: Vec<&str> = schema.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["id", "name"]);
        assert_eq!(schema[0].1, DataType::Int64);
    }

    #[test]
    fn malformed_csv_surfaces_error_not_hang() {
        // Regression for the motivating bug: the old DuckDB engine silently hung
        // on a malformed CSV field, costing the maintainer half a day.
        //
        // This exercises ragged rows: the header declares 2 columns but one data
        // row has 4 fields.  `.with_ignore_errors(false)` on the LazyCsvReader
        // makes this an explicit contract — malformed input must surface as
        // Err(DtooError::FileProcess), never a silent Ok or a hang.
        let path = tmp("bad", "csv");
        std::fs::write(&path, "id,name\n1,alice\n2,bob,extra,boom\n").unwrap();
        let engine = PolarsEngine::new();

        let result = engine
            .scan(path.to_str().unwrap(), &InputFormat::Csv { delimiter: ',' })
            .and_then(|lf| engine.collect(lf));

        assert!(
            matches!(result, Err(DtooError::FileProcess { .. })),
            "expected a FileProcess error for malformed CSV, got: {result:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cloud_paths_are_rejected_with_clear_error() {
        let engine = PolarsEngine::new();
        let result = engine.scan("s3://bucket/data.parquet", &InputFormat::Parquet);
        match result {
            Err(DtooError::Config { message }) => {
                assert!(message.contains("cloud storage"));
                assert!(message.contains("not supported"));
            }
            Err(other) => panic!("expected Config error, got a different error: {other}"),
            Ok(_) => panic!("expected Config error, got Ok"),
        }
    }

    // -----------------------------------------------------------------------
    // New hardening tests (Fix 1 + four coverage tests)
    // -----------------------------------------------------------------------

    /// Calamine 0.35 pads all rows to the sheet's maximum used width, so an
    /// over-wide data row makes the header row also wider — but the extra
    /// header cells are Empty ("").  `read_excel` detects trailing empty
    /// header columns and returns `FileProcess` rather than silently ingesting
    /// data under unnamed columns.
    #[test]
    fn scan_excel_over_wide_row_errors() {
        use rust_xlsxwriter::Workbook;
        let path = tmp("xlsx-wide", "xlsx");
        let mut workbook = Workbook::new();
        let ws = workbook.add_worksheet();
        ws.write_string(0, 0, "id").unwrap();
        ws.write_string(0, 1, "name").unwrap();
        ws.write_string(1, 0, "1").unwrap();
        ws.write_string(1, 1, "alice").unwrap();
        ws.write_string(1, 2, "EXTRA").unwrap(); // third cell, no header
        workbook.save(&path).unwrap();

        let engine = PolarsEngine::new();
        let result = engine
            .scan(path.to_str().unwrap(), &InputFormat::Excel { sheet: None })
            .and_then(|lf| engine.collect(lf));
        assert!(
            matches!(result, Err(DtooError::FileProcess { .. })),
            "expected FileProcess error for over-wide Excel row, got: {result:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    /// Requesting a named sheet that does not exist must yield a Result error,
    /// never a panic.  Calamine 0.35 returns Err(WorksheetNotFound(…)) which
    /// is already mapped via `.map_err(|e| read_err(path, e))`.
    #[test]
    fn scan_excel_missing_named_sheet_errors() {
        let path = tmp("xlsx-missing-sheet", "xlsx");
        write_test_xlsx(&path, "Sheet1");
        let engine = PolarsEngine::new();
        let result = engine.scan(
            path.to_str().unwrap(),
            &InputFormat::Excel {
                sheet: Some("DoesNotExist".to_string()),
            },
        );
        // scan may error eagerly, or error on collect — either way it must be
        // a Result error, never a panic.
        let result = result.and_then(|lf| engine.collect(lf));
        assert!(
            matches!(result, Err(DtooError::FileProcess { .. })),
            "expected FileProcess error for missing sheet, got: {result:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    /// The `open_sink(None)` → stdout branch was previously untested.
    /// Writing CSV to stdout (dest = None) must complete without error.
    #[test]
    fn write_csv_to_stdout_sink_succeeds() {
        let src = tmp("stdout-src", "csv");
        std::fs::write(&src, "id,name\n1,alice\n").unwrap();
        let engine = PolarsEngine::new();
        let df = engine
            .collect(
                engine
                    .scan(src.to_str().unwrap(), &InputFormat::Csv { delimiter: ',' })
                    .unwrap(),
            )
            .unwrap();
        // dest = None writes to stdout; we only assert it completes without error.
        engine
            .write(df, None, ExportFormat::Csv, true, ',', None)
            .unwrap();
        let _ = std::fs::remove_file(src);
    }

    /// Only CSV compression was previously tested; NDJSON shares the
    /// write_with_optional_compression path but was unverified.
    #[test]
    fn ndjson_gzip_output_is_decompressible() {
        let src = tmp("ndgz-src", "csv");
        std::fs::write(&src, "id,name\n1,alice\n").unwrap();
        let dst = tmp("ndgz-dst", "ndjson.gz");
        let engine = PolarsEngine::new();
        let df = engine
            .collect(
                engine
                    .scan(src.to_str().unwrap(), &InputFormat::Csv { delimiter: ',' })
                    .unwrap(),
            )
            .unwrap();
        engine
            .write(
                df,
                Some(dst.as_path()),
                ExportFormat::Ndjson,
                true,
                ',',
                Some(CompressionCodec::Gzip),
            )
            .unwrap();
        let bytes = std::fs::read(&dst).unwrap();
        let mut decoder = flate2::read::GzDecoder::new(&bytes[..]);
        let mut text = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut text).unwrap();
        assert!(text.contains("alice"));
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn csv_zstd_output_is_decompressible() {
        let src = tmp("zs-src", "csv");
        std::fs::write(&src, "id,name\n1,alice\n").unwrap();
        let dst = tmp("zs-dst", "csv.zst");
        let engine = PolarsEngine::new();
        let df = engine
            .collect(
                engine
                    .scan(src.to_str().unwrap(), &InputFormat::Csv { delimiter: ',' })
                    .unwrap(),
            )
            .unwrap();
        engine
            .write(
                df,
                Some(dst.as_path()),
                ExportFormat::Csv,
                true,
                ',',
                Some(CompressionCodec::Zstd),
            )
            .unwrap();

        let bytes = std::fs::read(&dst).unwrap();
        let text = String::from_utf8(zstd::stream::decode_all(&bytes[..]).unwrap()).unwrap();
        assert!(text.contains("1,alice"));
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }
}
