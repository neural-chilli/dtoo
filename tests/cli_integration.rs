use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn dtoo_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dtoo")
}

fn temp_path(prefix: &str, ext: &str) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis();
    let seq = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("dtoo-it-{prefix}-{millis}-{seq}.{ext}"))
}

#[test]
fn query_count_outputs_rows_and_exit_zero() {
    let input = temp_path("count-input", "csv");
    fs::write(&input, "id\n1\n2\n3\n").expect("write input");

    let output = Command::new(dtoo_bin())
        .arg("query")
        .arg(input.to_string_lossy().as_ref())
        .arg("--count")
        .output()
        .expect("run dtoo query");

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "3");

    let _ = fs::remove_file(input);
}

#[test]
fn query_on_error_skip_returns_partial_failure_exit_code() {
    let input = temp_path("skip-input", "csv");
    fs::write(&input, "id\n1\n").expect("write input");
    let bad_dir = temp_path("skip-dir", "tmpdir");
    fs::create_dir_all(&bad_dir).expect("create bad directory");

    let output = Command::new(dtoo_bin())
        .arg("query")
        .arg(input.to_string_lossy().as_ref())
        .arg(bad_dir.to_string_lossy().as_ref())
        .arg("--on-error")
        .arg("skip")
        .arg("--count")
        .output()
        .expect("run dtoo query");

    assert_eq!(output.status.code(), Some(3));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Warning: Skipping"));

    let _ = fs::remove_file(input);
    let _ = fs::remove_dir_all(bad_dir);
}

#[test]
fn inspect_unknown_extension_returns_error_exit_code() {
    let bogus = temp_path("inspect-unknown", "foo");
    fs::write(&bogus, "whatever").expect("write input");

    let output = Command::new(dtoo_bin())
        .arg("inspect")
        .arg(bogus.to_string_lossy().as_ref())
        .output()
        .expect("run dtoo inspect");

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unsupported inspect file format"));

    let _ = fs::remove_file(bogus);
}
