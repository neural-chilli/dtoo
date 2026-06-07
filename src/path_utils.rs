use std::path::Path;

/// Returns true when a path points to supported cloud storage.
pub fn is_cloud_path(path: &str) -> bool {
    path.starts_with("s3://")
        || path.starts_with("gs://")
        || path.starts_with("az://")
        || path.starts_with("abfss://")
}

/// Returns true when a filesystem path string points to a cloud URI.
pub fn is_cloud_path_buf(path: &Path) -> bool {
    is_cloud_path(path.to_string_lossy().as_ref())
}

/// Parse optional `:SheetName` suffix from Excel paths.
pub fn split_excel_sheet_from_path(path: &str) -> (String, Option<String>) {
    let Some(colon_idx) = path.rfind(':') else {
        return (path.to_string(), None);
    };

    let (left, right_with_colon) = path.split_at(colon_idx);
    let sheet = right_with_colon.trim_start_matches(':').trim();
    let left_lower = left.to_ascii_lowercase();
    if sheet.is_empty() || !(left_lower.ends_with(".xlsx") || left_lower.ends_with(".xls")) {
        return (path.to_string(), None);
    }

    (left.to_string(), Some(sheet.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_path_detection_handles_abfss() {
        assert!(is_cloud_path("abfss://container/data.parquet"));
        assert!(!is_cloud_path("/tmp/data.parquet"));
    }

    #[test]
    fn split_excel_sheet_parses_suffix() {
        let (path, sheet) = split_excel_sheet_from_path("book.xlsx:Sheet2");
        assert_eq!(path, "book.xlsx");
        assert_eq!(sheet.as_deref(), Some("Sheet2"));
    }
}
