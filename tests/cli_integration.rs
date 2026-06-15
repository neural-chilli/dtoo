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
fn where_clause_can_reference_a_ref_table_subquery() {
    // DESIGN.md: reference tables are available by name in all SQL contexts,
    // including --where. Repro from review: a subquery against a --ref table.
    let input = temp_path("where-ref-in", "csv");
    let regions = temp_path("where-ref-regions", "csv");
    fs::write(&input, "id,v\n1,10\n2,20\n3,30\n4,40\n").expect("write input");
    fs::write(&regions, "id\n2\n4\n").expect("write regions");

    let ref_arg = format!("regions={}", regions.display());
    let output = Command::new(dtoo_bin())
        .arg("query")
        .arg(input.to_string_lossy().as_ref())
        .arg("--ref")
        .arg(&ref_arg)
        .arg("--where")
        .arg("id IN (SELECT id FROM regions)")
        .arg("--count")
        .output()
        .expect("run dtoo query");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "2");

    let _ = fs::remove_file(input);
    let _ = fs::remove_file(regions);
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

#[test]
fn profile_detail_synth_emits_histogram_and_matrix() {
    let input = temp_path("synth-detail-in", "csv");
    let output = temp_path("synth-detail-out", "json");
    let mut body = String::from("a,b\n");
    for i in 0..100 {
        body.push_str(&format!("{},{}\n", i, i * 2));
    }
    fs::write(&input, body).expect("write input");

    let out = Command::new(dtoo_bin())
        .args([
            "profile",
            input.to_string_lossy().as_ref(),
            "--detail",
            "synth",
            "--output",
            output.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run profile");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let json = fs::read_to_string(&output).expect("read profile");
    assert!(json.contains("\"detail\": \"synth\""));
    assert!(json.contains("\"histogram\""));
    assert!(json.contains("\"unique_ratio\""));
    assert!(json.contains("\"correlation_matrix\""));

    let _ = fs::remove_file(input);
    let _ = fs::remove_file(output);
}

#[test]
fn profile_detail_synth_rejects_html_format() {
    let input = temp_path("synth-detail-html", "csv");
    fs::write(&input, "a\n1\n").expect("write input");

    let out = Command::new(dtoo_bin())
        .args([
            "profile",
            input.to_string_lossy().as_ref(),
            "--detail",
            "synth",
            "--format",
            "html",
        ])
        .output()
        .expect("run profile");
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("JSON"));

    let _ = fs::remove_file(input);
}
