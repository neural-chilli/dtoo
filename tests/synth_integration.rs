use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn dtoo_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dtoo")
}

fn temp_dir(prefix: &str) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis();
    let seq = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("dtoo-synth-{prefix}-{millis}-{seq}"));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Writes real source CSVs, profiles them at synth detail, returns profile paths.
fn make_profiles(dir: &Path) -> (PathBuf, PathBuf) {
    let customers_csv = dir.join("customers.csv");
    let mut body = String::from("customer_id,region\n");
    for i in 0..50 {
        body.push_str(&format!(
            "{},{}\n",
            1000 + i,
            if i % 3 == 0 { "EU" } else { "US" }
        ));
    }
    fs::write(&customers_csv, body).expect("write customers");

    let orders_csv = dir.join("orders.csv");
    let mut body = String::from("order_id,customer_id,amount\n");
    for i in 0..500 {
        // Skewed fan-out: low customer ids get more orders.
        let cust = 1000 + ((i * i) % 50);
        body.push_str(&format!("{},{},{}\n", i + 1, cust, (i % 90) + 10));
    }
    fs::write(&orders_csv, body).expect("write orders");

    let cust_profile = dir.join("customers.json");
    let ord_profile = dir.join("orders.json");
    for (input, output) in [(&customers_csv, &cust_profile), (&orders_csv, &ord_profile)] {
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
            .expect("profile");
        assert!(
            out.status.success(),
            "profile failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    (cust_profile, ord_profile)
}

fn write_spec(dir: &Path) -> PathBuf {
    let spec = dir.join("synth.yaml");
    fs::write(
        &spec,
        r#"
seed: 42
tables:
  customers:
    profile: customers.json
    rows: 200
    keys: [customer_id]
    output: out/customers.csv
  orders:
    profile: orders.json
    rows: 2000
    foreign_keys:
      - column: customer_id
        references: customers.customer_id
    rules:
      - constraint: "amount > 0"
      - derive: "amount_doubled = amount * 2"
    output: out/orders.csv
"#,
    )
    .expect("write spec");
    fs::create_dir_all(dir.join("out")).expect("create out dir");
    spec
}

#[test]
fn spec_mode_generates_tables_with_fk_integrity() {
    let dir = temp_dir("fk");
    make_profiles(&dir);
    let spec = write_spec(&dir);

    let out = Command::new(dtoo_bin())
        .args([
            "synth",
            "--spec",
            spec.to_string_lossy().as_ref(),
            "--verbose",
        ])
        .output()
        .expect("run synth");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let customers = fs::read_to_string(dir.join("out/customers.csv")).expect("customers out");
    let orders = fs::read_to_string(dir.join("out/orders.csv")).expect("orders out");
    assert_eq!(customers.lines().count(), 201, "header + 200 rows");
    assert_eq!(orders.lines().count(), 2001, "header + 2000 rows");

    // FK integrity: every order's customer_id exists in customers.
    let cust_header: Vec<&str> = customers.lines().next().unwrap().split(',').collect();
    let cust_id_idx = cust_header
        .iter()
        .position(|c| *c == "customer_id")
        .unwrap();
    let cust_ids: std::collections::HashSet<String> = customers
        .lines()
        .skip(1)
        .map(|l| l.split(',').nth(cust_id_idx).unwrap().to_string())
        .collect();
    assert_eq!(cust_ids.len(), 200, "keys are unique");

    let ord_header: Vec<&str> = orders.lines().next().unwrap().split(',').collect();
    let fk_idx = ord_header.iter().position(|c| *c == "customer_id").unwrap();
    let amount_idx = ord_header.iter().position(|c| *c == "amount").unwrap();
    let doubled_idx = ord_header
        .iter()
        .position(|c| *c == "amount_doubled")
        .unwrap();
    for line in orders.lines().skip(1) {
        let fields: Vec<&str> = line.split(',').collect();
        assert!(
            cust_ids.contains(fields[fk_idx]),
            "FK {} not found in parent keys",
            fields[fk_idx]
        );
        let amount: f64 = fields[amount_idx].parse().expect("amount numeric");
        assert!(amount > 0.0, "constraint must hold: {amount}");
        let doubled: f64 = fields[doubled_idx].parse().expect("doubled numeric");
        assert!((doubled - amount * 2.0).abs() < 1e-6, "derive must hold");
    }
}

#[test]
fn same_seed_produces_identical_output() {
    let dir = temp_dir("repro");
    make_profiles(&dir);
    let spec = write_spec(&dir);

    let run = || {
        let out = Command::new(dtoo_bin())
            .args(["synth", "--spec", spec.to_string_lossy().as_ref()])
            .output()
            .expect("run synth");
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        (
            fs::read(dir.join("out/customers.csv")).unwrap(),
            fs::read(dir.join("out/orders.csv")).unwrap(),
        )
    };
    let (c1, o1) = run();
    let (c2, o2) = run();
    assert_eq!(c1, c2, "customers byte-identical across runs");
    assert_eq!(o1, o2, "orders byte-identical across runs");

    // Different seed differs.
    let out = Command::new(dtoo_bin())
        .args([
            "synth",
            "--spec",
            spec.to_string_lossy().as_ref(),
            "--seed",
            "7",
        ])
        .output()
        .expect("run synth");
    assert!(out.status.success());
    let o3 = fs::read(dir.join("out/orders.csv")).unwrap();
    assert_ne!(o1, o3, "different seed, different data");
}

#[test]
fn single_table_mode_writes_to_stdout() {
    let dir = temp_dir("single");
    let (cust_profile, _) = make_profiles(&dir);

    let out = Command::new(dtoo_bin())
        .args([
            "synth",
            "--profile",
            cust_profile.to_string_lossy().as_ref(),
            "--rows",
            "25",
        ])
        .output()
        .expect("run synth");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(stdout.lines().count(), 26, "header + 25 rows");
    assert!(stdout.lines().next().unwrap().contains("customer_id"));
}

#[test]
fn standard_profile_degrades_with_warning() {
    let dir = temp_dir("degrade");
    let input = dir.join("data.csv");
    fs::write(&input, "v\n1\n2\n3\n4\n5\n").expect("write input");
    let profile = dir.join("std.json");
    let out = Command::new(dtoo_bin())
        .args([
            "profile",
            input.to_string_lossy().as_ref(),
            "--output",
            profile.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("profile standard");
    assert!(out.status.success());

    let out = Command::new(dtoo_bin())
        .args([
            "synth",
            "--profile",
            profile.to_string_lossy().as_ref(),
            "--rows",
            "10",
        ])
        .output()
        .expect("run synth");
    assert!(out.status.success(), "degraded mode still succeeds");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--detail synth"),
        "warns to re-profile"
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).lines().count(), 11);
}

#[test]
fn dry_run_generates_nothing() {
    let dir = temp_dir("dryrun");
    make_profiles(&dir);
    let spec = write_spec(&dir);

    let out = Command::new(dtoo_bin())
        .args([
            "synth",
            "--spec",
            spec.to_string_lossy().as_ref(),
            "--dry-run",
        ])
        .output()
        .expect("run synth");
    assert!(out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("Generation order: customers, orders"));
    assert!(
        !dir.join("out/customers.csv").exists(),
        "dry run writes nothing"
    );
}

#[test]
fn unknown_fk_reference_exits_one_with_message() {
    let dir = temp_dir("badref");
    make_profiles(&dir);
    let spec = dir.join("bad.yaml");
    fs::write(
        &spec,
        r#"
tables:
  orders:
    profile: orders.json
    rows: 10
    foreign_keys:
      - column: customer_id
        references: missing.customer_id
    output: out/orders.csv
"#,
    )
    .expect("write spec");

    let out = Command::new(dtoo_bin())
        .args(["synth", "--spec", spec.to_string_lossy().as_ref()])
        .output()
        .expect("run synth");
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("missing.customer_id"));
}
