# Synthetic Data Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement spec 35 (`docs/specs/35-synthetic-data.md`): a `--detail synth` profile mode that captures histograms, top-K values, unique ratios, and a Spearman correlation matrix, plus a `dtoo synth` subcommand that generates realistic synthetic data from those profiles with referential integrity, fan-out realism, copula-preserved correlations, and SQL-based intra-row rules.

**Architecture:** Extend `src/profiler.rs` additively (new optional serde fields, skip-serialized at standard detail so existing output is byte-identical). New `src/synth/` module: `spec.rs` (YAML spec + topo sort), `profile_input.rs` (load profile JSON back in), `samplers.rs` (seeded per-column value generation), `keys.rs` (unique key synthesis), `copula.rs` (PSD repair + Cholesky + Gaussian copula), `rules.rs` (constraint filter + derives via the existing `PolarsEngine::run_sql` and the `_` magic table), `engine.rs` (orchestration). Output goes through the existing `OutputWriter`.

**Tech Stack:** Rust, Polars 0.54 (`SQLContext` via `PolarsEngine`), `rand` 0.8 + `rand_chacha` 0.3 (new deps — seeded ChaCha8 streams), existing `sha2` for stream derivation, `serde`/`serde_yaml`/`serde_json`.

**Conventions for every task:** run `cargo fmt` before each commit; `cargo clippy --all-targets -- -D warnings` must pass; commit messages are plain imperative (repo style: "Add …", not "feat: …"). All public functions get doc comments. Errors are `DtooError` variants — never panic on bad user input. Tests never write to the repo tree (use `std::env::temp_dir()` patterns already in the codebase).

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `Cargo.toml` | Modify | Add `rand = "0.8"`, `rand_chacha = "0.3"` |
| `src/profiler.rs` | Modify | New structs `HistogramBucket`, `CorrelationMatrix`; new optional fields on `ColumnProfile`/`ProfileReport`; `Deserialize` derives; histogram/top-K/unique-ratio/Spearman computation gated on `ProfileDetail::Synth` |
| `src/cli.rs` | Modify | `ProfileDetail` ValueEnum; `--detail`/`--top-k` on `ProfileArgs`; `--profile-detail`/`--top-k` on `QueryArgs`; new `Synth(SynthArgs)` subcommand + validation |
| `src/profile_command.rs` | Modify | Pass detail/top-k into `ProfileOptions` |
| `src/query_pipeline.rs` | Modify | Pass detail/top-k into `ProfileOptions` (construction at ~line 556) |
| `src/main.rs` | Modify | `mod synth;` + dispatch arm |
| `src/synth/mod.rs` | Create | Module wiring, `pub use engine::run` |
| `src/synth/spec.rs` | Create | Spec YAML parse, validation, FK reference parsing, topological generation order |
| `src/synth/profile_input.rs` | Create | Load profile JSON → `SynthProfile`/`SynthColumn`; dtype string parsing; numeric/temporal bound parsing; standard-detail fallback quantiles |
| `src/synth/samplers.rs` | Create | Seeded stream derivation; histogram/quantile/categorical/pattern/null samplers |
| `src/synth/keys.rs` | Create | Key format detection and unique key synthesis |
| `src/synth/copula.rs` | Create | Jacobi eigendecomposition, PSD repair, Cholesky, Box-Muller, normal CDF, correlated uniforms |
| `src/synth/rules.rs` | Create | Constraint filtering with per-constraint pass counts; derived columns |
| `src/synth/engine.rs` | Create | Batch generation, FK/fan-out sampling, oversample-and-filter loop, spec + single-table orchestration, dry-run, verbose |
| `tests/synth_integration.rs` | Create | End-to-end CLI tests (FK integrity, reproducibility, degradation, dry-run, errors) |
| `docs/USER_GUIDE.md` | Modify | New "Synthetic data" section |

Dtype support matrix (referenced by several tasks): Int8–64, UInt8–64, Float32/64 (numeric); Date, Datetime, Time (temporal — sampled via physical representation: Date = i32 days, Datetime = i64 in its time unit, Time = i64 ns); String; Boolean; Decimal (generated as f64, cast best-effort — on cast failure keep Float64 and warn). Any other dtype string in a profile falls back to String-with-pattern sampling.

---

### Task 1: Dependencies and module scaffold

**Files:**
- Modify: `Cargo.toml`
- Create: `src/synth/mod.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Add dependencies**

In `Cargo.toml` `[dependencies]`, after the `zstd` line add:

```toml
rand = "0.8"
rand_chacha = "0.3"
```

- [ ] **Step 2: Create the module skeleton**

Create `src/synth/mod.rs`:

```rust
//! Synthetic data generation from statistical profiles (spec 35).

pub mod copula;
pub mod engine;
pub mod keys;
pub mod profile_input;
pub mod rules;
pub mod samplers;
pub mod spec;

pub use engine::run;
```

Create empty placeholder files so the tree compiles (each will be filled by a later task): `src/synth/copula.rs`, `src/synth/engine.rs`, `src/synth/keys.rs`, `src/synth/profile_input.rs`, `src/synth/rules.rs`, `src/synth/samplers.rs`, each containing only a module doc comment for now, e.g.:

```rust
//! Gaussian copula utilities for correlation-preserving sampling.
```

`src/synth/engine.rs` additionally needs a stub `run` so `mod.rs` compiles:

```rust
//! Synth orchestration: spec execution and single-table generation.

use crate::{cli::SynthArgs, error::DtooError};

/// Entry point for `dtoo synth` (filled in by the orchestration task).
pub fn run(_args: &SynthArgs) -> Result<(), DtooError> {
    Err(DtooError::Config {
        message: "synth is not implemented yet".to_string(),
    })
}
```

This references `cli::SynthArgs` which doesn't exist yet — so in THIS task also add the minimal `SynthArgs` to `src/cli.rs` (full flags arrive in Task 6's CLI work; add it complete now to avoid churn). In `src/cli.rs`, after `FingerprintArgs`:

```rust
/// Arguments for `dtoo synth`.
#[derive(Debug, Parser)]
pub struct SynthArgs {
    #[arg(long)]
    pub spec: Option<PathBuf>,

    #[arg(long)]
    pub profile: Option<PathBuf>,

    #[arg(long)]
    pub rows: Option<usize>,

    #[arg(long)]
    pub output: Option<PathBuf>,

    #[arg(long = "output-format", default_value = "csv")]
    pub output_format: OutputFormat,

    #[arg(long, default_value = ",")]
    pub delimiter: char,

    #[arg(long)]
    pub compress: Option<CompressMethod>,

    #[arg(long = "no-header")]
    pub no_header: bool,

    #[arg(long)]
    pub seed: Option<u64>,

    #[arg(long = "dry-run")]
    pub dry_run: bool,

    #[arg(long)]
    pub verbose: bool,
}
```

Add the variant to `Commands`:

```rust
    Synth(SynthArgs),
```

- [ ] **Step 3: Wire into main.rs**

In `src/main.rs`: add `mod synth;` to the module list (alphabetical position, no `#[allow(dead_code)]`), and a dispatch arm:

```rust
        Commands::Synth(args) => synth::run(&args),
```

- [ ] **Step 4: Verify it builds and existing tests pass**

Run: `cargo build && cargo test`
Expected: builds; all existing tests PASS. `dtoo synth --profile x.json` would error with "not implemented yet" (don't test this; it's removed in Task 14).

- [ ] **Step 5: Clippy, fmt, commit**

Run: `cargo fmt && cargo clippy --all-targets -- -D warnings`
Expected: no warnings.

```bash
git add Cargo.toml Cargo.lock src/synth src/main.rs src/cli.rs
git commit -m "Add synth module scaffold and rand dependencies"
```

---

### Task 2: Profiler — serde round-trip and additive optional fields

**Files:**
- Modify: `src/profiler.rs`
- Modify: `src/cli.rs` (add `ProfileDetail`)
- Modify: `src/profile_command.rs`, `src/query_pipeline.rs` (ProfileOptions construction)

- [ ] **Step 1: Write the failing regression + round-trip tests**

In `src/profiler.rs` `#[cfg(test)] mod tests`, add:

```rust
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
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo profiler 2>&1 | tail -20`
Expected: COMPILE FAILURE (`ProfileDetail` not found, `build_report` takes 2 args, no `Deserialize` on `ProfileReport`).

- [ ] **Step 3: Implement**

In `src/cli.rs`, next to `ProfileFormat`:

```rust
#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum ProfileDetail {
    Standard,
    Synth,
}
```

In `src/profiler.rs`:

1. Change the serde import to `use serde::{Deserialize, Serialize};` and add `ProfileDetail` to the cli import.
2. Add `Deserialize` to the derives on `ValueFrequency`, `ColumnProfile`, and `ProfileReport`.
3. Add new structs:

```rust
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
```

4. Append to `ColumnProfile` (after `pattern_sample`):

```rust
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub histogram: Option<Vec<HistogramBucket>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub top_values: Option<Vec<ValueFrequency>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub unique_ratio: Option<f64>,
```

5. Append to `ProfileReport` (after `columns`):

```rust
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub correlation_matrix: Option<CorrelationMatrix>,
```

6. Extend `ProfileOptions`:

```rust
    pub detail: ProfileDetail,
    pub top_k: usize,
```

7. Change signatures and thread the new params (no synth computation yet — that's Tasks 3–5):

```rust
fn build_report(
    df: &DataFrame,
    sample_percentage: u8,
    detail: ProfileDetail,
    top_k: usize,
) -> Result<ProfileReport, DtooError> {
```

Set `detail: (detail == ProfileDetail::Synth).then(|| "synth".to_string())` and `correlation_matrix: None` in the constructed report; initialize the three new `ColumnProfile` fields to `None` in `profile_column` (whose signature becomes `profile_column(series: &Column, total_rows: usize, detail: ProfileDetail, top_k: usize)` — params unused for now, prefix with `_` until Tasks 3–4 use them... actually keep them named and add `let _ = (detail, top_k);` temporarily to avoid clippy unused warnings, removed in Task 3).

8. In `Profiler::generate`, before building the report, reject synth + non-JSON and pass the params through:

```rust
        if options.detail == ProfileDetail::Synth && options.format != ProfileFormat::Json {
            return Err(DtooError::Config {
                message: "--detail synth requires JSON profile format".to_string(),
            });
        }
        // ...
        let report = build_report(source, options.sample_percentage, options.detail, options.top_k)?;
```

9. Fix all `ProfileOptions` construction sites to add `detail: ProfileDetail::Standard, top_k: 1000`:
   - `src/profile_command.rs` (`run`, ~line 46) — uses `args.detail` / `args.top_k` from Task 6 onward; for now hardcode `ProfileDetail::Standard` and `1000` ONLY if Task 6 isn't done; since Task 6 in this plan adds the flags, just hardcode here and Task 6 replaces it.
   - `src/query_pipeline.rs` (~line 556) — same hardcoded defaults; Task 6 replaces.
   - All `ProfileOptions { .. }` literals in `src/profiler.rs` tests and `src/profile_command.rs` tests.

Run `grep -rn "ProfileOptions {" src/` to catch every site.

10. Existing tests that call `build_report(&df, 100)` directly (e.g. `distinct_count_excludes_nulls`) must become `build_report(&df, 100, ProfileDetail::Standard, 1000)` — the compiler will list them.

- [ ] **Step 4: Run tests**

Run: `cargo test`
Expected: ALL PASS, including the two new tests and every pre-existing profiler/profile_command/query test (proves standard output unchanged).

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/profiler.rs src/cli.rs src/profile_command.rs src/query_pipeline.rs
git commit -m "Add additive synth-detail fields and serde round-trip to profiler"
```

---

### Task 3: Profiler — histograms at synth detail

**Files:**
- Modify: `src/profiler.rs`

- [ ] **Step 1: Write the failing tests**

```rust
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
        let df = df!["v" => [1i64, 1, 2, 2, 3]].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let hist = report.columns[0].histogram.as_ref().expect("histogram");
        assert!(hist.len() <= 3);
        assert_eq!(hist.iter().map(|b| b.count).sum::<u64>(), 5);
    }

    #[test]
    fn synth_detail_adds_date_histogram_with_physical_values() {
        use chrono::NaiveDate;
        let dates: Vec<NaiveDate> = (0..100)
            .map(|i| NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i))
            .collect();
        let df = df!["d" => dates].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let hist = report.columns[0].histogram.as_ref().expect("date histogram");
        // Date physical repr is days since epoch; 2024-01-01 = 19723.
        assert!((hist[0].lo - 19723.0).abs() < 1.0);
    }

    #[test]
    fn standard_detail_has_no_histogram() {
        let df = df!["v" => [1.0f64, 2.0, 3.0]].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Standard, 1000).expect("report");
        assert!(report.columns[0].histogram.is_none());
    }
```

Note: `df!` with `NaiveDate` requires the polars `temporal`/`dtype-date` capability — `dtype-full` (already enabled) covers it. If `df!["d" => dates]` fails to compile, build via `Series::new("d".into(), dates)` then `DataFrame::new(vec![s.into_column()])`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo synth_detail 2>&1 | tail -20`
Expected: FAIL — histogram is `None`.

- [ ] **Step 3: Implement**

In `src/profiler.rs` add:

```rust
/// Extracts a column's non-null values as f64 of the physical representation
/// (Date → days, Datetime → its unit, Time → ns, Decimal → scaled int as f64).
fn physical_f64_values(series: &Column) -> Result<Vec<f64>, DtooError> {
    let phys = series.as_materialized_series().to_physical_repr().into_owned();
    let as_f64 = phys.cast(&DataType::Float64).map_err(polars_err)?;
    Ok(as_f64
        .f64()
        .map_err(polars_err)?
        .iter()
        .flatten()
        .filter(|v| v.is_finite())
        .collect())
}

/// Builds a quantile-spaced histogram (up to 20 buckets) over non-null values.
fn numeric_histogram(series: &Column) -> Result<Option<Vec<HistogramBucket>>, DtooError> {
    let mut vals = physical_f64_values(series)?;
    if vals.is_empty() {
        return Ok(None);
    }
    vals.sort_by(|a, b| a.partial_cmp(b).expect("finite values compare"));

    let n_buckets = 20usize;
    // Quantile-spaced edges; dedup keeps them strictly increasing under heavy ties.
    let mut edges: Vec<f64> = (0..=n_buckets)
        .map(|i| {
            let p = i as f64 / n_buckets as f64;
            let idx = ((vals.len() - 1) as f64 * p).round() as usize;
            vals[idx]
        })
        .collect();
    edges.dedup();
    if edges.len() < 2 {
        // All values identical: a single degenerate bucket.
        return Ok(Some(vec![HistogramBucket {
            lo: edges[0],
            hi: edges[0],
            count: vals.len() as u64,
        }]));
    }

    let mut buckets: Vec<HistogramBucket> = edges
        .windows(2)
        .map(|w| HistogramBucket { lo: w[0], hi: w[1], count: 0 })
        .collect();
    // Count each value into the first bucket whose hi bounds it (last bucket inclusive).
    let mut b = 0usize;
    for v in &vals {
        while b + 1 < buckets.len() && *v > buckets[b].hi {
            b += 1;
        }
        buckets[b].count += 1;
    }
    Ok(Some(buckets))
}
```

In `profile_column`, remove the temporary `let _ = (detail, top_k);` and gate on detail. Histograms apply to numeric AND date-like dtypes:

```rust
    if detail == ProfileDetail::Synth
        && (is_numeric_dtype(&dtype) || is_date_like_dtype(&dtype))
    {
        profile.histogram = numeric_histogram(series)?;
    }
```

(Keep `top_k` threaded but unused until Task 4 — reference it as `let _ = top_k;` if needed for one commit.)

- [ ] **Step 4: Run tests**

Run: `cargo test`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/profiler.rs
git commit -m "Add quantile-spaced histograms to synth-detail profiles"
```

---

### Task 4: Profiler — top-K values and unique_ratio

**Files:**
- Modify: `src/profiler.rs`

- [ ] **Step 1: Write the failing tests**

```rust
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
        let df = DataFrame::new(vec![Series::new_empty("c".into(), &DataType::String).into_column()])
            .unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        assert_eq!(report.columns[0].unique_ratio, Some(0.0));
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo top_k 2>&1 | tail -10` and `cargo test -p dtoo unique_ratio 2>&1 | tail -10`
Expected: FAIL — fields are `None`.

- [ ] **Step 3: Implement**

Generalize `top_values` to take a limit. Rename the existing function to `top_values_limited` with a `limit: usize` param (`pairs.truncate(limit);`), keep a thin `top_values(series)` wrapper calling `top_values_limited(series, 5)` so existing call sites stay simple — or just update the single call site; prefer updating the call site and having ONE function:

```rust
/// Returns the top-`limit` most frequent non-null values, sorted descending by count.
fn top_values(series: &Column, limit: usize) -> Result<Vec<ValueFrequency>, DtooError> {
    // body unchanged except: pairs.truncate(limit);
}
```

Existing call becomes `top_values(series, 5)?`.

In `profile_column`, in the synth gate (extends Task 3's block):

```rust
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
```

- [ ] **Step 4: Run tests**

Run: `cargo test`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/profiler.rs
git commit -m "Add top-K values and unique ratio to synth-detail profiles"
```

---

### Task 5: Profiler — Spearman correlation matrix

**Files:**
- Modify: `src/profiler.rs`

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn ranks_average_ties() {
        assert_eq!(rank_values(&[10.0, 20.0, 20.0, 30.0]), vec![1.0, 2.5, 2.5, 4.0]);
    }

    #[test]
    fn synth_detail_adds_correlation_matrix() {
        let x: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let y: Vec<f64> = (0..100).map(|i| (i * 2) as f64).collect(); // perfectly monotone with x
        let z: Vec<f64> = (0..100).map(|i| ((i * 7919) % 100) as f64). collect(); // scrambled
        let df = df!["x" => x, "y" => y, "z" => z, "s" => vec!["a"; 100]].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        let m = report.correlation_matrix.as_ref().expect("matrix");
        assert_eq!(m.columns, vec!["x", "y", "z"]); // string column excluded
        assert!((m.data[0][0] - 1.0).abs() < 1e-9);
        assert!((m.data[0][1] - 1.0).abs() < 1e-6, "x~y Spearman = 1");
        assert_eq!(m.data[0][1], m.data[1][0], "symmetric");
        assert!(m.data[0][2].abs() < 0.3, "x~z near zero");
    }

    #[test]
    fn correlation_matrix_omitted_with_fewer_than_two_numeric_columns() {
        let df = df!["x" => [1.0f64, 2.0], "s" => ["a", "b"]].unwrap();
        let report = build_report(&df, 100, ProfileDetail::Synth, 1000).expect("report");
        assert!(report.correlation_matrix.is_none());
    }
```

(Note `(i * 7919) % 100` — a fixed permutation, deterministic, no RNG in tests.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo correlation 2>&1 | tail -10`
Expected: COMPILE FAIL (`rank_values` missing) then logic fail.

- [ ] **Step 3: Implement**

```rust
/// Average ranks (1-based) with ties sharing their mean rank.
fn rank_values(values: &[f64]) -> Vec<f64> {
    let mut idx: Vec<usize> = (0..values.len()).collect();
    idx.sort_by(|&a, &b| values[a].partial_cmp(&values[b]).expect("finite"));
    let mut ranks = vec![0.0; values.len()];
    let mut i = 0;
    while i < idx.len() {
        let mut j = i;
        while j + 1 < idx.len() && values[idx[j + 1]] == values[idx[i]] {
            j += 1;
        }
        let avg = (i + j + 2) as f64 / 2.0; // mean of 1-based ranks i+1..=j+1
        for k in i..=j {
            ranks[idx[k]] = avg;
        }
        i = j + 1;
    }
    ranks
}

/// Pearson correlation; 0.0 when either side has zero variance.
fn pearson(xs: &[f64], ys: &[f64]) -> f64 {
    let n = xs.len() as f64;
    let mx = xs.iter().sum::<f64>() / n;
    let my = ys.iter().sum::<f64>() / n;
    let mut cov = 0.0;
    let mut vx = 0.0;
    let mut vy = 0.0;
    for (x, y) in xs.iter().zip(ys) {
        cov += (x - mx) * (y - my);
        vx += (x - mx) * (x - mx);
        vy += (y - my) * (y - my);
    }
    if vx <= 0.0 || vy <= 0.0 {
        0.0
    } else {
        cov / (vx.sqrt() * vy.sqrt())
    }
}

/// Spearman over rows where BOTH columns are non-null; 0.0 for <2 pairs.
fn spearman_pair(xs: &[Option<f64>], ys: &[Option<f64>]) -> f64 {
    let pairs: Vec<(f64, f64)> = xs
        .iter()
        .zip(ys)
        .filter_map(|(x, y)| Some(((*x)?, (*y)?)))
        .collect();
    if pairs.len() < 2 {
        return 0.0;
    }
    let rx = rank_values(&pairs.iter().map(|p| p.0).collect::<Vec<_>>());
    let ry = rank_values(&pairs.iter().map(|p| p.1).collect::<Vec<_>>());
    pearson(&rx, &ry)
}

fn corr_eligible(dt: &DataType) -> bool {
    is_numeric_dtype(dt) || matches!(dt, DataType::Date | DataType::Datetime(_, _))
}

/// Extracts physical f64 values KEEPING null positions (unlike physical_f64_values).
fn physical_f64_with_nulls(series: &Column) -> Result<Vec<Option<f64>>, DtooError> {
    let phys = series.as_materialized_series().to_physical_repr().into_owned();
    let as_f64 = phys.cast(&DataType::Float64).map_err(polars_err)?;
    Ok(as_f64.f64().map_err(polars_err)?.iter().collect())
}

/// Spearman correlation matrix over all eligible columns; None when < 2.
fn spearman_matrix(df: &DataFrame) -> Result<Option<CorrelationMatrix>, DtooError> {
    let mut names = Vec::new();
    let mut cols: Vec<Vec<Option<f64>>> = Vec::new();
    for col in df.columns() {
        if corr_eligible(col.dtype()) {
            names.push(col.name().to_string());
            cols.push(physical_f64_with_nulls(col)?);
        }
    }
    if names.len() < 2 {
        return Ok(None);
    }
    let k = names.len();
    let mut data = vec![vec![0.0; k]; k];
    for i in 0..k {
        data[i][i] = 1.0;
        for j in (i + 1)..k {
            let r = spearman_pair(&cols[i], &cols[j]);
            data[i][j] = r;
            data[j][i] = r;
        }
    }
    Ok(Some(CorrelationMatrix { columns: names, data }))
}
```

In `build_report`, populate at synth detail:

```rust
    let correlation_matrix = if detail == ProfileDetail::Synth {
        spearman_matrix(df)?
    } else {
        None
    };
```

- [ ] **Step 4: Run tests**

Run: `cargo test`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/profiler.rs
git commit -m "Add Spearman correlation matrix to synth-detail profiles"
```

---

### Task 6: CLI flags for profile detail

**Files:**
- Modify: `src/cli.rs`, `src/profile_command.rs`, `src/query_pipeline.rs`
- Test: `tests/cli_integration.rs`

- [ ] **Step 1: Write the failing tests**

In `src/cli.rs` tests:

```rust
    #[test]
    fn parses_profile_detail_and_top_k() {
        let cli = parse([
            "dtoo", "profile", "input.csv", "--detail", "synth", "--top-k", "500",
        ]);
        match cli.command {
            Commands::Profile(args) => {
                assert_eq!(args.detail, ProfileDetail::Synth);
                assert_eq!(args.top_k, 500);
            }
            _ => panic!("expected profile command"),
        }
    }

    #[test]
    fn parses_query_profile_detail() {
        let cli = parse([
            "dtoo", "query", "input.csv", "--profile", "p.json", "--profile-detail", "synth",
        ]);
        match cli.command {
            Commands::Query(args) => {
                assert_eq!(args.profile_detail, ProfileDetail::Synth);
                assert_eq!(args.top_k, 1000);
            }
            _ => panic!("expected query command"),
        }
    }
```

In `tests/cli_integration.rs` (uses the file's existing `dtoo_bin()`/`temp_path()` helpers):

```rust
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
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));

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
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test 2>&1 | tail -20`
Expected: COMPILE FAIL (`args.detail` etc. missing).

- [ ] **Step 3: Implement**

`ProfileArgs` gains (after `sample`):

```rust
    #[arg(long, default_value = "standard")]
    pub detail: ProfileDetail,

    #[arg(long = "top-k", default_value_t = 1000)]
    pub top_k: usize,
```

`QueryArgs` gains (after `profile_sample`):

```rust
    #[arg(long = "profile-detail", default_value = "standard")]
    pub profile_detail: ProfileDetail,

    #[arg(long = "top-k", default_value_t = 1000)]
    pub top_k: usize,
```

Replace the hardcoded `detail`/`top_k` in `src/profile_command.rs` with `args.detail` / `args.top_k`, and in `src/query_pipeline.rs` with `args.profile_detail` / `args.top_k` (the `QueryArgs` value is in scope at the ProfileOptions construction — check the surrounding function's parameter name with `grep -n "fn.*Args" src/query_pipeline.rs` and use the local binding).

- [ ] **Step 4: Run tests**

Run: `cargo test`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/cli.rs src/profile_command.rs src/query_pipeline.rs tests/cli_integration.rs
git commit -m "Add --detail synth and --top-k profile flags"
```

---

### Task 7: Synth spec parsing, validation, and generation order

**Files:**
- Create: `src/synth/spec.rs` (replace placeholder)

- [ ] **Step 1: Write the failing tests**

`src/synth/spec.rs` with a `#[cfg(test)]` module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn two_table_yaml() -> &'static str {
        r#"
seed: 42
tables:
  customers:
    profile: profiles/customers.json
    rows: 100
    keys: [customer_id]
    output: out/customers.parquet
  orders:
    profile: profiles/orders.json
    rows: 500
    foreign_keys:
      - column: customer_id
        references: customers.customer_id
    rules:
      - constraint: "amount > 0"
      - derive: "total = amount * 2"
    output: out/orders.csv
"#
    }

    #[test]
    fn parses_and_validates_two_table_spec() {
        let spec: SynthSpec = serde_yaml::from_str(two_table_yaml()).expect("parse");
        assert_eq!(spec.seed, 42);
        validate(&spec).expect("valid");
        let orders = &spec.tables["orders"];
        assert_eq!(orders.foreign_keys[0].references, "customers.customer_id");
        assert!(matches!(fan_out(&orders.foreign_keys[0]).unwrap(), FanOut::FromProfile));
    }

    #[test]
    fn generation_order_puts_parents_first() {
        let spec: SynthSpec = serde_yaml::from_str(two_table_yaml()).expect("parse");
        assert_eq!(generation_order(&spec).unwrap(), vec!["customers", "orders"]);
    }

    #[test]
    fn rejects_unknown_fk_reference() {
        let yaml = r#"
tables:
  orders:
    profile: p.json
    rows: 10
    foreign_keys:
      - column: x
        references: missing.id
    output: o.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        let err = validate(&spec).expect_err("invalid");
        assert!(err.to_string().contains("missing.id"));
    }

    #[test]
    fn rejects_fk_to_non_key_column() {
        let yaml = r#"
tables:
  customers:
    profile: c.json
    rows: 10
    keys: [customer_id]
    output: c.csv
  orders:
    profile: o.json
    rows: 10
    foreign_keys:
      - column: cid
        references: customers.name
    output: o.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        let err = validate(&spec).expect_err("invalid");
        assert!(err.to_string().contains("customers.name"));
    }

    #[test]
    fn detects_dependency_cycle() {
        let yaml = r#"
tables:
  a:
    profile: a.json
    rows: 10
    keys: [id]
    foreign_keys: [{column: bid, references: b.id}]
    output: a.csv
  b:
    profile: b.json
    rows: 10
    keys: [id]
    foreign_keys: [{column: aid, references: a.id}]
    output: b.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        let err = generation_order(&spec).expect_err("cycle");
        assert!(err.to_string().contains("cycle"));
    }

    #[test]
    fn rejects_rule_with_both_or_neither_kind() {
        let yaml = r#"
tables:
  t:
    profile: t.json
    rows: 10
    rules:
      - derive: "x = 1"
        constraint: "x > 0"
    output: t.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        let err = validate(&spec).expect_err("invalid rule");
        assert!(err.to_string().contains("exactly one"));
    }

    #[test]
    fn parses_uniform_fan_out_mapping_form() {
        let yaml = r#"
tables:
  c:
    profile: c.json
    rows: 10
    keys: [id]
    output: c.csv
  o:
    profile: o.json
    rows: 10
    foreign_keys:
      - column: cid
        references: c.id
        fan_out: {distribution: uniform}
    output: o.csv
"#;
        let spec: SynthSpec = serde_yaml::from_str(yaml).expect("parse");
        validate(&spec).expect("valid");
        assert!(matches!(fan_out(&spec.tables["o"].foreign_keys[0]).unwrap(), FanOut::Uniform));
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo synth::spec 2>&1 | tail -10`
Expected: COMPILE FAIL — types missing.

- [ ] **Step 3: Implement**

```rust
//! Synth spec (YAML): schema, validation, and generation ordering.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::error::DtooError;

/// Top-level synth spec describing a family of tables to generate.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SynthSpec {
    #[serde(default)]
    pub seed: u64,
    pub tables: BTreeMap<String, TableSpec>,
}

/// One table: its source profile, target size, structure, and output.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSpec {
    pub profile: PathBuf,
    pub rows: usize,
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeySpec>,
    #[serde(default)]
    pub rules: Vec<RuleSpec>,
    pub output: PathBuf,
    #[serde(default)]
    pub output_format: Option<String>,
}

/// A foreign-key declaration: this table's column references parent.key.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKeySpec {
    pub column: String,
    pub references: String,
    #[serde(default)]
    pub fan_out: Option<FanOutSpec>,
}

/// Raw YAML form of fan_out: either a bare string or `{distribution: ...}`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum FanOutSpec {
    Named(String),
    Dist { distribution: String },
}

/// One intra-row rule: exactly one of `derive` / `constraint` must be set.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleSpec {
    #[serde(default)]
    pub derive: Option<String>,
    #[serde(default)]
    pub constraint: Option<String>,
}

/// Normalized fan-out mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanOut {
    FromProfile,
    Uniform,
}

/// A parsed `table.column` FK reference.
#[derive(Debug, Clone)]
pub struct FkRef {
    pub table: String,
    pub column: String,
}

fn config_err(message: String) -> DtooError {
    DtooError::Config { message }
}

/// Loads and parses a spec file. Paths inside remain relative to the spec dir.
pub fn load_spec(path: &Path) -> Result<SynthSpec, DtooError> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        config_err(format!("cannot read synth spec {}: {e}", path.display()))
    })?;
    serde_yaml::from_str(&raw)
        .map_err(|e| config_err(format!("invalid synth spec {}: {e}", path.display())))
}

/// Parses `table.column` (exactly one dot).
pub fn parse_reference(reference: &str) -> Result<FkRef, DtooError> {
    let mut parts = reference.splitn(2, '.');
    let (Some(table), Some(column)) = (parts.next(), parts.next()) else {
        return Err(config_err(format!(
            "foreign key reference `{reference}` must be in table.column form"
        )));
    };
    if table.is_empty() || column.is_empty() {
        return Err(config_err(format!(
            "foreign key reference `{reference}` must be in table.column form"
        )));
    }
    Ok(FkRef { table: table.to_string(), column: column.to_string() })
}

/// Normalizes a fan_out spec value; default is FromProfile.
pub fn fan_out(fk: &ForeignKeySpec) -> Result<FanOut, DtooError> {
    let name = match &fk.fan_out {
        None => return Ok(FanOut::FromProfile),
        Some(FanOutSpec::Named(n)) => n.clone(),
        Some(FanOutSpec::Dist { distribution }) => distribution.clone(),
    };
    match name.as_str() {
        "from_profile" => Ok(FanOut::FromProfile),
        "uniform" => Ok(FanOut::Uniform),
        other => Err(config_err(format!(
            "fan_out must be `from_profile` or `uniform`, got `{other}`"
        ))),
    }
}

/// Validates cross-table references, rule shape, and basic sanity.
pub fn validate(spec: &SynthSpec) -> Result<(), DtooError> {
    if spec.tables.is_empty() {
        return Err(config_err("synth spec has no tables".to_string()));
    }
    for (name, table) in &spec.tables {
        for fk in &table.foreign_keys {
            let r = parse_reference(&fk.references)?;
            let Some(parent) = spec.tables.get(&r.table) else {
                return Err(config_err(format!(
                    "table `{name}` foreign key references unknown target `{}`",
                    fk.references
                )));
            };
            if !parent.keys.contains(&r.column) {
                return Err(config_err(format!(
                    "table `{name}` foreign key references `{}`, which is not listed in `{}`'s keys",
                    fk.references, r.table
                )));
            }
            fan_out(fk)?;
        }
        for rule in &table.rules {
            if rule.derive.is_some() == rule.constraint.is_some() {
                return Err(config_err(format!(
                    "table `{name}`: each rule must set exactly one of `derive` or `constraint`"
                )));
            }
        }
    }
    Ok(())
}

/// Topological order over FK dependencies (Kahn). Alphabetical tie-break via
/// BTreeMap iteration, so the order is deterministic. Errors on cycles.
pub fn generation_order(spec: &SynthSpec) -> Result<Vec<String>, DtooError> {
    let mut deps: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for (name, table) in &spec.tables {
        let mut parents = Vec::new();
        for fk in &table.foreign_keys {
            parents.push(parse_reference(&fk.references)?.table);
        }
        deps.insert(name, parents);
    }
    let mut order = Vec::new();
    let mut done: Vec<String> = Vec::new();
    while order.len() < spec.tables.len() {
        let mut progressed = false;
        for (name, parents) in &deps {
            if done.iter().any(|d| d == name) {
                continue;
            }
            if parents.iter().all(|p| done.iter().any(|d| d == p)) {
                order.push(name.to_string());
                done.push(name.to_string());
                progressed = true;
            }
        }
        if !progressed {
            let remaining: Vec<&str> = deps
                .keys()
                .filter(|n| !done.iter().any(|d| d == **n))
                .copied()
                .collect();
            return Err(config_err(format!(
                "foreign key dependency cycle involving: {}",
                remaining.join(", ")
            )));
        }
    }
    Ok(order)
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p dtoo synth::spec`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/synth/spec.rs
git commit -m "Add synth spec parsing, validation, and generation ordering"
```

---

### Task 8: Profile input model for synth

**Files:**
- Create: `src/synth/profile_input.rs` (replace placeholder)

- [ ] **Step 1: Write the failing tests**

```rust
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
        assert!(matches!(parse_dtype("Datetime(Microseconds, None)"), DataType::Datetime(_, _)));
        assert!(matches!(parse_dtype("Decimal(Some(10), Some(2))"), DataType::Decimal(_, _)));
        // Unknown dtypes degrade to String (pattern sampling).
        assert_eq!(parse_dtype("List(Int64)"), DataType::String);
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
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
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
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
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
        let err = load_profile(Path::new("/tmp/definitely-missing-dtoo-prof.json"))
            .expect_err("missing");
        assert!(err.to_string().contains("definitely-missing-dtoo-prof.json"));
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo synth::profile_input 2>&1 | tail -10`
Expected: COMPILE FAIL.

- [ ] **Step 3: Implement**

```rust
//! Loads dtoo profile JSON into the model the synth engine consumes.

use std::path::Path;

use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::{DataType, TimeUnit};

use crate::{
    error::DtooError,
    profiler::{CorrelationMatrix, HistogramBucket, ProfileReport, ValueFrequency},
};

/// A profile loaded for generation.
pub struct SynthProfile {
    pub synth_detail: bool,
    pub row_count: usize,
    pub columns: Vec<SynthColumn>,
    pub correlation: Option<CorrelationMatrix>,
}

/// Per-column statistics in generation-ready form.
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
            let unit = if s.contains("Nanoseconds") {
                TimeUnit::Nanoseconds
            } else if s.contains("Milliseconds") {
                TimeUnit::Milliseconds
            } else {
                TimeUnit::Microseconds
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
                [p, sc, ..] => DataType::Decimal(Some(*p), Some(*sc)),
                _ => DataType::Decimal(Some(18), Some(3)),
            }
        }
        _ => DataType::String,
    }
}

/// Parses a profile min/max string into physical f64 for the dtype.
pub fn parse_bound(dtype: &DataType, raw: &str) -> Option<f64> {
    match dtype {
        DataType::Date => NaiveDate::parse_from_str(raw, "%Y-%m-%d")
            .ok()
            .map(|d| {
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

    let synth_detail = report.detail.as_deref() == Some("synth");
    let columns = report
        .columns
        .iter()
        .map(|c| {
            let dtype = parse_dtype(&c.data_type);
            let quantiles = build_quantiles(&dtype, c);
            let parse_len = |v: &Option<String>| {
                v.as_deref().and_then(|s| s.parse::<f64>().ok()).map(|f| f as usize)
            };
            SynthColumn {
                name: c.name.clone(),
                dtype,
                null_percentage: c.null_percentage,
                non_null_count: c.count.saturating_sub(c.null_count),
                distinct_count: c.distinct_count,
                unique_ratio: c.unique_ratio.unwrap_or_else(|| {
                    if c.count == 0 { 0.0 } else { c.distinct_count as f64 / c.count as f64 }
                }),
                histogram: c.histogram.clone(),
                quantiles,
                top_values: c.top_values.clone().unwrap_or_else(|| c.top_5_values.clone()),
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
```

Note: `build_quantiles` needs `ColumnProfile` fields public — they already are.

- [ ] **Step 4: Run tests**

Run: `cargo test -p dtoo synth::profile_input`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/synth/profile_input.rs
git commit -m "Add synth profile input model with dtype and bound parsing"
```

---

### Task 9: Seeded samplers

**Files:**
- Create: `src/synth/samplers.rs` (replace placeholder)

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiler::{HistogramBucket, ValueFrequency};

    #[test]
    fn stream_rng_is_deterministic_and_column_isolated() {
        let mut a1 = stream_rng(42, "t", "a", 0);
        let mut a2 = stream_rng(42, "t", "a", 0);
        let mut b = stream_rng(42, "t", "b", 0);
        let mut a_round1 = stream_rng(42, "t", "a", 1);
        let v1: f64 = a1.r#gen();
        assert_eq!(v1, a2.r#gen::<f64>(), "same stream reproduces");
        assert_ne!(v1, b.r#gen::<f64>(), "different column, different stream");
        assert_ne!(v1, a_round1.r#gen::<f64>(), "different round, different stream");
    }

    #[test]
    fn histogram_sampling_respects_bucket_weights_and_bounds() {
        let buckets = vec![
            HistogramBucket { lo: 0.0, hi: 10.0, count: 90 },
            HistogramBucket { lo: 10.0, hi: 100.0, count: 10 },
        ];
        let mut rng = stream_rng(1, "t", "c", 0);
        let mut low = 0;
        for _ in 0..1000 {
            let v = sample_histogram(&buckets, &mut rng);
            assert!((0.0..=100.0).contains(&v));
            if v <= 10.0 {
                low += 1;
            }
        }
        assert!((800..=980).contains(&low), "≈90% in first bucket, got {low}");
    }

    #[test]
    fn histogram_quantile_maps_uniform_through_cdf() {
        let buckets = vec![
            HistogramBucket { lo: 0.0, hi: 10.0, count: 50 },
            HistogramBucket { lo: 10.0, hi: 20.0, count: 50 },
        ];
        assert!((histogram_quantile(&buckets, 0.0) - 0.0).abs() < 1e-9);
        assert!((histogram_quantile(&buckets, 0.5) - 10.0).abs() < 1e-9);
        assert!((histogram_quantile(&buckets, 1.0) - 20.0).abs() < 1e-9);
        assert!((histogram_quantile(&buckets, 0.25) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn quantile_fallback_interpolates_five_points() {
        let q = vec![0.0, 25.0, 50.0, 75.0, 100.0];
        assert!((quantiles_quantile(&q, 0.0) - 0.0).abs() < 1e-9);
        assert!((quantiles_quantile(&q, 0.5) - 50.0).abs() < 1e-9);
        assert!((quantiles_quantile(&q, 0.125) - 12.5).abs() < 1e-9);
        assert!((quantiles_quantile(&q, 1.0) - 100.0).abs() < 1e-9);
    }

    #[test]
    fn weighted_value_sampling_follows_frequencies() {
        let values = vec![
            ValueFrequency { value: "a".into(), freq: 90 },
            ValueFrequency { value: "b".into(), freq: 10 },
        ];
        let mut rng = stream_rng(7, "t", "c", 0);
        let a_count = (0..1000)
            .filter(|_| values[sample_weighted_index(&values, &mut rng)].value == "a")
            .count();
        assert!((830..=960).contains(&a_count), "≈90% a, got {a_count}");
    }

    #[test]
    fn pattern_generation_matches_shape() {
        let mut rng = stream_rng(3, "t", "c", 0);
        for _ in 0..100 {
            let s = generate_from_pattern("aaa-N", 5, 8, &mut rng);
            assert!(s.len() >= 5 && s.len() <= 8, "length {} of {s}", s.len());
            let bytes = s.as_bytes();
            assert!(bytes[0].is_ascii_lowercase());
            assert!(bytes[1].is_ascii_lowercase());
            assert!(bytes[2].is_ascii_lowercase());
            assert_eq!(bytes[3], b'-');
            assert!(bytes[4..].iter().all(u8::is_ascii_digit));
        }
        // Pattern with literal-only content just reproduces literals.
        assert_eq!(generate_from_pattern("--", 2, 2, &mut rng), "--");
    }

    #[test]
    fn null_draws_match_percentage() {
        let mut rng = stream_rng(9, "t", "c", 0);
        let nulls = (0..10_000).filter(|_| is_null_draw(25.0, &mut rng)).count();
        assert!((2200..=2800).contains(&nulls), "≈25% nulls, got {nulls}");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo synth::samplers 2>&1 | tail -10`
Expected: COMPILE FAIL.

- [ ] **Step 3: Implement**

```rust
//! Deterministic seeded samplers for synthetic value generation.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sha2::{Digest, Sha256};

use crate::profiler::{HistogramBucket, ValueFrequency};

/// Derives an isolated, reproducible RNG stream for (seed, table, column, round).
/// Adding or removing other columns never perturbs this stream.
pub fn stream_rng(seed: u64, table: &str, column: &str, round: u64) -> ChaCha8Rng {
    let mut hasher = Sha256::new();
    hasher.update(seed.to_le_bytes());
    hasher.update(table.as_bytes());
    hasher.update([0u8]);
    hasher.update(column.as_bytes());
    hasher.update([0u8]);
    hasher.update(round.to_le_bytes());
    let digest = hasher.finalize();
    let mut seed_bytes = [0u8; 32];
    seed_bytes.copy_from_slice(&digest);
    ChaCha8Rng::from_seed(seed_bytes)
}

/// Samples a value from a histogram: weighted bucket pick, uniform within.
pub fn sample_histogram(buckets: &[HistogramBucket], rng: &mut ChaCha8Rng) -> f64 {
    let total: u64 = buckets.iter().map(|b| b.count).sum();
    if total == 0 {
        return buckets.first().map(|b| b.lo).unwrap_or(0.0);
    }
    let mut target = rng.gen_range(0..total);
    for b in buckets {
        if target < b.count {
            return b.lo + (b.hi - b.lo) * rng.r#gen::<f64>();
        }
        target -= b.count;
    }
    buckets.last().map(|b| b.hi).unwrap_or(0.0)
}

/// Maps a uniform u ∈ [0,1] through the histogram's empirical CDF (for copula).
pub fn histogram_quantile(buckets: &[HistogramBucket], u: f64) -> f64 {
    let total: u64 = buckets.iter().map(|b| b.count).sum();
    if total == 0 {
        return buckets.first().map(|b| b.lo).unwrap_or(0.0);
    }
    let target = u.clamp(0.0, 1.0) * total as f64;
    let mut cum = 0.0;
    for b in buckets {
        let next = cum + b.count as f64;
        if target <= next && b.count > 0 {
            let frac = ((target - cum) / b.count as f64).clamp(0.0, 1.0);
            return b.lo + (b.hi - b.lo) * frac;
        }
        cum = next;
    }
    buckets.last().map(|b| b.hi).unwrap_or(0.0)
}

/// Maps a uniform u through a piecewise-linear CDF over [min,p25,median,p75,max].
pub fn quantiles_quantile(q: &[f64], u: f64) -> f64 {
    debug_assert_eq!(q.len(), 5);
    let u = u.clamp(0.0, 1.0);
    let seg = (u * 4.0).floor().min(3.0) as usize; // 0..=3
    let frac = u * 4.0 - seg as f64;
    q[seg] + (q[seg + 1] - q[seg]) * frac
}

/// Picks an index into `values` weighted by frequency.
pub fn sample_weighted_index(values: &[ValueFrequency], rng: &mut ChaCha8Rng) -> usize {
    let total: usize = values.iter().map(|v| v.freq).sum();
    if total == 0 || values.is_empty() {
        return 0;
    }
    let mut target = rng.gen_range(0..total);
    for (i, v) in values.iter().enumerate() {
        if target < v.freq {
            return i;
        }
        target -= v.freq;
    }
    values.len() - 1
}

/// Generates a string matching a dtoo pattern (`a`=letter, `d`=digit,
/// `N`=digit run, everything else literal), targeting the observed length range.
pub fn generate_from_pattern(
    pattern: &str,
    min_len: usize,
    max_len: usize,
    rng: &mut ChaCha8Rng,
) -> String {
    let n_runs = pattern.chars().filter(|c| *c == 'N').count();
    let fixed: usize = pattern.chars().filter(|c| *c != 'N').count();
    let mut run_lengths = vec![0usize; n_runs];
    if n_runs > 0 {
        let budget_min = min_len.saturating_sub(fixed).max(n_runs);
        let budget_max = max_len.saturating_sub(fixed).max(budget_min);
        let total_digits = if budget_min == budget_max {
            budget_min
        } else {
            rng.gen_range(budget_min..=budget_max)
        };
        let base = total_digits / n_runs;
        let extra = total_digits % n_runs;
        for (i, len) in run_lengths.iter_mut().enumerate() {
            *len = base + usize::from(i < extra);
        }
    }
    let mut out = String::new();
    let mut run_idx = 0;
    for c in pattern.chars() {
        match c {
            'a' => out.push((b'a' + rng.gen_range(0..26u8)) as char),
            'd' => out.push((b'0' + rng.gen_range(0..10u8)) as char),
            'N' => {
                for _ in 0..run_lengths[run_idx].max(1) {
                    out.push((b'0' + rng.gen_range(0..10u8)) as char);
                }
                run_idx += 1;
            }
            other => out.push(other),
        }
    }
    out
}

/// Decides nullness for one value at the observed null percentage.
pub fn is_null_draw(null_percentage: f64, rng: &mut ChaCha8Rng) -> bool {
    null_percentage > 0.0 && rng.r#gen::<f64>() * 100.0 < null_percentage
}
```

(Note: `gen` is a reserved keyword in Rust 2024 — call it as `rng.r#gen::<f64>()`. `gen_range` is unaffected.)

- [ ] **Step 4: Run tests**

Run: `cargo test -p dtoo synth::samplers`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/synth/samplers.rs
git commit -m "Add deterministic seeded samplers for synth generation"
```

---

### Task 10: Key synthesis

**Files:**
- Create: `src/synth/keys.rs` (replace placeholder)

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{profiler::ValueFrequency, synth::samplers::stream_rng};
    use polars::prelude::DataType;

    fn col(dtype: DataType, pattern: &str, min_len: usize, max_len: usize, min: Option<&str>) -> crate::synth::profile_input::SynthColumn {
        crate::synth::profile_input::SynthColumn {
            name: "k".into(),
            dtype,
            null_percentage: 0.0,
            non_null_count: 100,
            distinct_count: 100,
            unique_ratio: 1.0,
            histogram: None,
            quantiles: min.map(|m| {
                let v = m.parse::<f64>().unwrap();
                vec![v, v, v, v, v + 100.0]
            }),
            top_values: vec![],
            pattern_sample: if pattern.is_empty() {
                vec![]
            } else {
                vec![ValueFrequency { value: pattern.into(), freq: 100 }]
            },
            min_length: min_len,
            max_length: max_len,
        }
    }

    #[test]
    fn integer_keys_are_sequential_from_observed_min() {
        let c = col(DataType::Int64, "", 1, 1, Some("1000"));
        let kind = detect_key_kind(&c);
        assert!(matches!(kind, KeyKind::SequentialInt { start: 1000 }));
        let mut rng = stream_rng(1, "t", "k", 0);
        assert_eq!(key_string(&kind, 0, &mut rng), "1000");
        assert_eq!(key_string(&kind, 5, &mut rng), "1005");
    }

    #[test]
    fn padded_digit_keys_keep_width() {
        let c = col(DataType::String, "N", 8, 8, None);
        let kind = detect_key_kind(&c);
        assert!(matches!(kind, KeyKind::PaddedDigits { width: 8 }));
        let mut rng = stream_rng(1, "t", "k", 0);
        assert_eq!(key_string(&kind, 41, &mut rng), "00000042"); // 1-based to avoid all-zero key
    }

    #[test]
    fn uuid_shaped_keys_are_valid_and_deterministic() {
        let c = col(DataType::String, "NaN-aNaN-Na-aNa-NaNa", 36, 36, None);
        let kind = detect_key_kind(&c);
        assert!(matches!(kind, KeyKind::UuidLike));
        let mut r1 = stream_rng(1, "t", "k", 0);
        let mut r2 = stream_rng(1, "t", "k", 0);
        let a = key_string(&kind, 0, &mut r1);
        let b = key_string(&kind, 0, &mut r2);
        assert_eq!(a, b, "deterministic");
        assert_eq!(a.len(), 36);
        assert_eq!(a.matches('-').count(), 4);
    }

    #[test]
    fn pattern_keys_embed_index_for_uniqueness() {
        let c = col(DataType::String, "aaa-N", 7, 9, None);
        let kind = detect_key_kind(&c);
        let mut rng = stream_rng(1, "t", "k", 0);
        let mut seen = std::collections::HashSet::new();
        for i in 0..1000 {
            let k = key_string(&kind, i, &mut rng);
            assert!(seen.insert(k.clone()), "duplicate key {k} at {i}");
        }
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo synth::keys 2>&1 | tail -10`
Expected: COMPILE FAIL.

- [ ] **Step 3: Implement**

```rust
//! Unique key synthesis formatted to match observed data.

use polars::prelude::DataType;
use rand::Rng;
use rand_chacha::ChaCha8Rng;

use crate::synth::profile_input::SynthColumn;

/// How key values for a column are constructed.
#[derive(Debug)]
pub enum KeyKind {
    SequentialInt { start: i64 },
    PaddedDigits { width: usize },
    UuidLike,
    PatternCounter { pattern: String },
}

/// Detects the key construction strategy from the column's profile.
pub fn detect_key_kind(col: &SynthColumn) -> KeyKind {
    if col.dtype.is_primitive_numeric() || matches!(col.dtype, DataType::Decimal(_, _)) {
        let start = col
            .quantiles
            .as_ref()
            .map(|q| q[0] as i64)
            .unwrap_or(1);
        return KeyKind::SequentialInt { start };
    }
    let top_pattern = col.pattern_sample.first().map(|p| p.value.as_str()).unwrap_or("");
    if col.min_length == 36 && col.max_length == 36 && top_pattern.matches('-').count() == 4 {
        return KeyKind::UuidLike;
    }
    if top_pattern == "N" && col.min_length == col.max_length && col.min_length > 0 {
        return KeyKind::PaddedDigits { width: col.min_length };
    }
    if top_pattern.is_empty() {
        return KeyKind::PaddedDigits { width: 8 };
    }
    KeyKind::PatternCounter { pattern: top_pattern.to_string() }
}

/// Produces the key value for a row index. Unique by construction for every
/// kind: ints/padded embed the index, UUIDs consume 16 rng bytes per index in
/// stream order, pattern keys replace their last digit run with the index.
pub fn key_string(kind: &KeyKind, index: usize, rng: &mut ChaCha8Rng) -> String {
    match kind {
        KeyKind::SequentialInt { start } => (start + index as i64).to_string(),
        KeyKind::PaddedDigits { width } => format!("{:0width$}", index + 1, width = width),
        KeyKind::UuidLike => {
            let mut bytes = [0u8; 16];
            rng.fill(&mut bytes);
            uuid::Builder::from_random_bytes(bytes).into_uuid().to_string()
        }
        KeyKind::PatternCounter { pattern } => {
            let idx_str = (index + 1).to_string();
            // Replace the LAST digit-run token with the padded index; if the
            // pattern has no digit run, append the index.
            if let Some(pos) = pattern.rfind('N') {
                let mut out = String::new();
                for (i, c) in pattern.chars().enumerate() {
                    match c {
                        'N' if i == pos => out.push_str(&idx_str),
                        'N' => out.push('0'),
                        'a' => out.push((b'a' + (index % 26) as u8) as char),
                        'd' => out.push('0'),
                        other => out.push(other),
                    }
                }
                out
            } else {
                format!("{pattern}{idx_str}")
            }
        }
    }
}

/// True when generated key strings should be parsed back to integers
/// (numeric key columns build an integer Series, not strings).
pub fn is_numeric_kind(kind: &KeyKind) -> bool {
    matches!(kind, KeyKind::SequentialInt { .. })
}
```

(If `is_primitive_numeric()` doesn't exist on `DataType` in polars 0.54, use `col.dtype.is_numeric()` — check with `cargo doc` or compile error; one of the two exists.)

- [ ] **Step 4: Run tests**

Run: `cargo test -p dtoo synth::keys`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/synth/keys.rs
git commit -m "Add key synthesis with format detection"
```

---

### Task 11: Copula

**Files:**
- Create: `src/synth/copula.rs` (replace placeholder)

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::synth::samplers::stream_rng;

    #[test]
    fn normal_cdf_known_values() {
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-6);
        assert!((normal_cdf(1.96) - 0.975).abs() < 1e-3);
        assert!((normal_cdf(-1.96) - 0.025).abs() < 1e-3);
    }

    #[test]
    fn psd_repair_fixes_invalid_matrix() {
        // This matrix is NOT positive semi-definite (eigenvalue < 0).
        let mut m = vec![
            vec![1.0, 0.9, 0.9],
            vec![0.9, 1.0, -0.9],
            vec![0.9, -0.9, 1.0],
        ];
        psd_repair(&mut m);
        // After repair: symmetric, unit diagonal, Cholesky succeeds.
        for i in 0..3 {
            assert!((m[i][i] - 1.0).abs() < 1e-9);
            for j in 0..3 {
                assert!((m[i][j] - m[j][i]).abs() < 1e-9);
            }
        }
        assert!(cholesky(&m).is_some(), "repaired matrix must factor");
    }

    #[test]
    fn cholesky_of_identity_is_identity() {
        let m = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        let l = cholesky(&m).expect("identity factors");
        assert!((l[0][0] - 1.0).abs() < 1e-9);
        assert!((l[1][0]).abs() < 1e-9);
        assert!((l[1][1] - 1.0).abs() < 1e-9);
    }

    #[test]
    fn correlated_uniforms_reproduce_target_correlation() {
        let mut m = vec![vec![1.0, 0.8], vec![0.8, 1.0]];
        psd_repair(&mut m);
        let l = cholesky(&m).expect("factor");
        let mut rng = stream_rng(5, "t", "__copula__", 0);
        let rows = correlated_uniforms(&l, 5000, &mut rng);
        assert_eq!(rows.len(), 5000);
        assert_eq!(rows[0].len(), 2);
        for row in &rows {
            for u in row {
                assert!((0.0..=1.0).contains(u));
            }
        }
        // Spearman of uniforms ≈ rank correlation of the Gaussian copula:
        // 6/π·asin(ρ/2) ≈ 0.786 for ρ=0.8. Accept a generous band.
        let xs: Vec<f64> = rows.iter().map(|r| r[0]).collect();
        let ys: Vec<f64> = rows.iter().map(|r| r[1]).collect();
        let r = sample_pearson(&xs, &ys);
        assert!((0.68..=0.88).contains(&r), "got correlation {r}");
    }

    fn sample_pearson(xs: &[f64], ys: &[f64]) -> f64 {
        let n = xs.len() as f64;
        let mx = xs.iter().sum::<f64>() / n;
        let my = ys.iter().sum::<f64>() / n;
        let (mut c, mut vx, mut vy) = (0.0, 0.0, 0.0);
        for (x, y) in xs.iter().zip(ys) {
            c += (x - mx) * (y - my);
            vx += (x - mx) * (x - mx);
            vy += (y - my) * (y - my);
        }
        c / (vx.sqrt() * vy.sqrt())
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo synth::copula 2>&1 | tail -10`
Expected: COMPILE FAIL.

- [ ] **Step 3: Implement**

```rust
//! Gaussian copula: PSD repair, Cholesky, correlated uniform generation.
//! Matrices here are k×k for k profiled numeric columns — small — so the
//! linear algebra is implemented in-module rather than adding a crate.

use rand::Rng;
use rand_chacha::ChaCha8Rng;

/// Cyclic Jacobi eigendecomposition of a symmetric matrix.
/// Returns (eigenvalues, eigenvectors-as-columns).
pub fn jacobi_eigen(matrix: &[Vec<f64>]) -> (Vec<f64>, Vec<Vec<f64>>) {
    let n = matrix.len();
    let mut a: Vec<Vec<f64>> = matrix.to_vec();
    let mut v = vec![vec![0.0; n]; n];
    for (i, row) in v.iter_mut().enumerate() {
        row[i] = 1.0;
    }
    for _sweep in 0..100 {
        let mut off = 0.0;
        for i in 0..n {
            for j in (i + 1)..n {
                off += a[i][j] * a[i][j];
            }
        }
        if off < 1e-18 {
            break;
        }
        for p in 0..n {
            for q in (p + 1)..n {
                if a[p][q].abs() < 1e-15 {
                    continue;
                }
                let theta = (a[q][q] - a[p][p]) / (2.0 * a[p][q]);
                let t = theta.signum() / (theta.abs() + (theta * theta + 1.0).sqrt());
                let c = 1.0 / (t * t + 1.0).sqrt();
                let s = t * c;
                for k in 0..n {
                    let akp = a[k][p];
                    let akq = a[k][q];
                    a[k][p] = c * akp - s * akq;
                    a[k][q] = s * akp + c * akq;
                }
                for k in 0..n {
                    let apk = a[p][k];
                    let aqk = a[q][k];
                    a[p][k] = c * apk - s * aqk;
                    a[q][k] = s * apk + c * aqk;
                }
                for k in 0..n {
                    let vkp = v[k][p];
                    let vkq = v[k][q];
                    v[k][p] = c * vkp - s * vkq;
                    v[k][q] = s * vkp + c * vkq;
                }
            }
        }
    }
    let values = (0..n).map(|i| a[i][i]).collect();
    (values, v)
}

/// Repairs a sampled correlation matrix to positive definite in place:
/// clamp eigenvalues at ε, reconstruct, re-normalize diagonal to 1.
pub fn psd_repair(matrix: &mut Vec<Vec<f64>>) {
    let n = matrix.len();
    let (mut values, vectors) = jacobi_eigen(matrix);
    for v in &mut values {
        *v = v.max(1e-10);
    }
    let mut out = vec![vec![0.0; n]; n];
    for i in 0..n {
        for j in 0..n {
            let mut s = 0.0;
            for (k, lambda) in values.iter().enumerate() {
                s += vectors[i][k] * lambda * vectors[j][k];
            }
            out[i][j] = s;
        }
    }
    // Re-normalize to a correlation matrix (unit diagonal, symmetric).
    for i in 0..n {
        for j in 0..n {
            let d = (out[i][i] * out[j][j]).sqrt();
            matrix[i][j] = if d > 0.0 { out[i][j] / d } else { 0.0 };
        }
    }
    for i in 0..n {
        matrix[i][i] = 1.0;
        for j in 0..i {
            let avg = (matrix[i][j] + matrix[j][i]) / 2.0;
            matrix[i][j] = avg;
            matrix[j][i] = avg;
        }
    }
}

/// Cholesky factorization (lower-triangular). None if not positive definite.
pub fn cholesky(matrix: &[Vec<f64>]) -> Option<Vec<Vec<f64>>> {
    let n = matrix.len();
    let mut l = vec![vec![0.0; n]; n];
    for i in 0..n {
        for j in 0..=i {
            let mut s = matrix[i][j];
            for k in 0..j {
                s -= l[i][k] * l[j][k];
            }
            if i == j {
                if s <= 0.0 {
                    return None;
                }
                l[i][j] = s.sqrt();
            } else {
                l[i][j] = s / l[j][j];
            }
        }
    }
    Some(l)
}

/// One standard normal via Box-Muller.
fn standard_normal(rng: &mut ChaCha8Rng) -> f64 {
    let u1: f64 = rng.r#gen::<f64>().max(1e-12);
    let u2: f64 = rng.r#gen();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

/// Error function approximation (Abramowitz & Stegun 7.1.26, |err| < 1.5e-7).
fn erf(x: f64) -> f64 {
    let sign = x.signum();
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let y = 1.0
        - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t
            + 0.254829592)
            * t
            * (-x * x).exp();
    sign * y
}

/// Standard normal CDF Φ.
pub fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Generates `rows` vectors of k correlated uniforms from a Cholesky factor.
pub fn correlated_uniforms(
    chol: &[Vec<f64>],
    rows: usize,
    rng: &mut ChaCha8Rng,
) -> Vec<Vec<f64>> {
    let k = chol.len();
    let mut out = Vec::with_capacity(rows);
    for _ in 0..rows {
        let z: Vec<f64> = (0..k).map(|_| standard_normal(rng)).collect();
        let mut row = Vec::with_capacity(k);
        for (i, l_row) in chol.iter().enumerate() {
            let mut s = 0.0;
            for j in 0..=i {
                s += l_row[j] * z[j];
            }
            row.push(normal_cdf(s));
        }
        out.push(row);
    }
    out
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p dtoo synth::copula`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/synth/copula.rs
git commit -m "Add Gaussian copula with PSD repair and Cholesky factorization"
```

---

### Task 12: Engine — batch generation and FK fan-out

**Files:**
- Modify: `src/synth/engine.rs` (replace stub body; keep `run` stub at bottom until Task 14)

This task builds `generate_batch` — one batch of rows for one table, no rules — plus the FK sampler. Everything is driven by a `TableGenContext`.

- [ ] **Step 1: Write the failing tests**

In `src/synth/engine.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiler::{CorrelationMatrix, HistogramBucket, ValueFrequency};
    use crate::synth::profile_input::{SynthColumn, SynthProfile};
    use crate::synth::spec::FanOut;
    use polars::prelude::*;

    fn numeric_col(name: &str) -> SynthColumn {
        SynthColumn {
            name: name.into(),
            dtype: DataType::Float64,
            null_percentage: 0.0,
            non_null_count: 100,
            distinct_count: 100,
            unique_ratio: 1.0,
            histogram: Some(vec![
                HistogramBucket { lo: 0.0, hi: 50.0, count: 50 },
                HistogramBucket { lo: 50.0, hi: 100.0, count: 50 },
            ]),
            quantiles: Some(vec![0.0, 25.0, 50.0, 75.0, 100.0]),
            top_values: vec![],
            pattern_sample: vec![],
            min_length: 1,
            max_length: 1,
        }
    }

    fn profile_of(columns: Vec<SynthColumn>, correlation: Option<CorrelationMatrix>) -> SynthProfile {
        SynthProfile { synth_detail: true, row_count: 100, columns, correlation }
    }

    #[test]
    fn generates_requested_rows_with_profile_schema() {
        let profile = profile_of(
            vec![numeric_col("amount"), SynthColumn {
                name: "status".into(),
                dtype: DataType::String,
                null_percentage: 0.0,
                non_null_count: 100,
                distinct_count: 2,
                unique_ratio: 0.02,
                histogram: None,
                quantiles: None,
                top_values: vec![
                    ValueFrequency { value: "active".into(), freq: 70 },
                    ValueFrequency { value: "closed".into(), freq: 30 },
                ],
                pattern_sample: vec![ValueFrequency { value: "aaaaaa".into(), freq: 100 }],
                min_length: 6,
                max_length: 6,
            }],
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
        let ctx = TableGenContext { name: "t", profile: &profile, keys: &[], fks: &[], seed: 7 };
        let a = generate_batch(&ctx, 0, 50, 0, &ParentKeys::default()).unwrap();
        let b = generate_batch(&ctx, 0, 50, 0, &ParentKeys::default()).unwrap();
        assert!(a.equals_missing(&b), "same seed, same data");
        let ctx2 = TableGenContext { name: "t", profile: &profile, keys: &[], fks: &[], seed: 8 };
        let c = generate_batch(&ctx2, 0, 50, 0, &ParentKeys::default()).unwrap();
        assert!(!a.equals_missing(&c), "different seed, different data");
    }

    #[test]
    fn null_percentage_is_respected() {
        let mut col = numeric_col("v");
        col.null_percentage = 30.0;
        let profile = profile_of(vec![col], None);
        let ctx = TableGenContext { name: "t", profile: &profile, keys: &[], fks: &[], seed: 1 };
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
        let ctx = TableGenContext { name: "t", profile: &profile, keys: &keys, fks: &[], seed: 1 };
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
        let ctx = TableGenContext { name: "orders", profile: &profile, keys: &[], fks: &fks, seed: 3 };
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
        let ctx = TableGenContext { name: "t", profile: &profile, keys: &[], fks: &[], seed: 11 };
        let df = generate_batch(&ctx, 0, 5000, 0, &ParentKeys::default()).unwrap();
        let xs: Vec<f64> = df.column("x").unwrap().f64().unwrap().iter().flatten().collect();
        let ys: Vec<f64> = df.column("y").unwrap().f64().unwrap().iter().flatten().collect();
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
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo synth::engine 2>&1 | tail -10`
Expected: COMPILE FAIL.

- [ ] **Step 3: Implement**

Replace `src/synth/engine.rs` content (keep the `pub fn run` stub from Task 1 at the bottom — Task 14 fills it):

```rust
//! Synth orchestration: batch generation, FK fan-out, spec execution.

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
    matches!(dt, DataType::Date | DataType::Datetime(_, _) | DataType::Time)
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
        DataType::Date | DataType::Int32 | DataType::Int16 | DataType::Int8
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
            PhysVec::I32(values.into_iter().map(|v| v.map(|f| f.round() as i32)).collect())
        }
        _ => PhysVec::I64(values.into_iter().map(|v| v.map(|f| f.round() as i64)).collect()),
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
            .map(|c| matrix.columns.iter().position(|n| *n == c.name).expect("present"))
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
        Some((copula_cols.clone(), copula::correlated_uniforms(&chol, batch_rows, &mut rng)))
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
            let pos = cols.iter().position(|c| c.name == col.name).expect("present");
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

    DataFrame::new(columns).map_err(|e| config_err(format!("assembling batch: {e}")))
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
        let KeyKind::SequentialInt { start } = kind else { unreachable!() };
        let values: Vec<i64> = (0..rows).map(|i| start + (offset + i) as i64).collect();
        let base = Series::new(col.name.as_str().into(), values);
        return Ok(base.cast(&col.dtype).unwrap_or(base));
    }
    let values: Vec<String> = (0..rows).map(|i| keys::key_string(&kind, offset + i, rng)).collect();
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
        FanOut::Uniform => (0..rows).map(|_| rng.gen_range(0..n_parents) as u32).collect(),
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
                && (coverage >= 0.995 || rng.r#gen::<f64>() < coverage || col.pattern_sample.is_empty());
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
```

(Compile notes for the implementer: `Series::into_column()` exists in polars 0.54 — if not, use `Column::from(series)`. `IdxCa::from_vec` takes `PlSmallStr` — `.into()` on `&str` provides it. If `dt.is_primitive_numeric()` is missing use `dt.is_numeric()`. `DataFrame::equals_missing` is the null-aware equality used in tests. `get_column_names()` returns `Vec<&PlSmallStr>` in 0.54 — if the `&["amount", "status"]` comparison doesn't compile, map to `&str` first: `df.get_column_names().iter().map(|s| s.as_str()).collect::<Vec<_>>()`.)

- [ ] **Step 4: Run tests**

Run: `cargo test -p dtoo synth::engine`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/synth/engine.rs
git commit -m "Add synth batch generation with keys, fan-out FKs, and copula"
```

---

### Task 13: Rules — constraints (oversample-and-filter) and derives

**Files:**
- Create: `src/synth/rules.rs` (replace placeholder)
- Modify: `src/synth/engine.rs` (add `generate_table` wrapping the rounds loop)

- [ ] **Step 1: Write the failing tests**

`src/synth/rules.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn parse_derive_splits_on_first_equals() {
        let (name, expr) = parse_derive("total = amount * 2").expect("parse");
        assert_eq!(name, "total");
        assert_eq!(expr, "amount * 2");
        assert!(parse_derive("no equals here").is_err());
        assert!(parse_derive("= expr").is_err());
    }

    #[test]
    fn constraints_filter_and_count_per_rule() {
        let engine = crate::polars_engine::PolarsEngine::new();
        let df = df!["a" => [1i64, -2, 3, -4, 5]].unwrap();
        let constraints = vec!["a > 0".to_string()];
        let (kept, counts) = filter_constraints(&engine, df, &constraints).expect("filter");
        assert_eq!(kept.height(), 3);
        assert_eq!(counts, vec![("a > 0".to_string(), 3usize)]);
    }

    #[test]
    fn derive_adds_and_replaces_columns() {
        let engine = crate::polars_engine::PolarsEngine::new();
        let df = df!["amount" => [2i64, 3], "total" => [0i64, 0]].unwrap();
        let derives = vec!["total = amount * 2".to_string(), "flag = amount > 2".to_string()];
        let out = apply_derives(&engine, df, &derives).expect("derive");
        let totals: Vec<i64> = out.column("total").unwrap().i64().unwrap().iter().flatten().collect();
        assert_eq!(totals, vec![4, 6]);
        assert_eq!(out.column("flag").unwrap().dtype(), &DataType::Boolean);
    }

    #[test]
    fn bad_rule_sql_is_a_clear_error() {
        let engine = crate::polars_engine::PolarsEngine::new();
        let df = df!["a" => [1i64]].unwrap();
        let err = apply_derives(&engine, df, &["x = nonexistent_col * 2".to_string()])
            .expect_err("bad sql");
        assert!(err.to_string().contains("nonexistent_col") || err.to_string().contains("x ="));
    }
}
```

`src/synth/engine.rs` tests (append):

```rust
    #[test]
    fn generate_table_satisfies_constraints_and_target_rows() {
        let profile = profile_of(vec![numeric_col("v")], None);
        let ctx = TableGenContext { name: "t", profile: &profile, keys: &[], fks: &[], seed: 2 };
        let rules = TableRules {
            constraints: vec!["v > 20".to_string()],
            derives: vec!["doubled = v * 2".to_string()],
        };
        let engine = crate::polars_engine::PolarsEngine::new();
        let df = generate_table(&engine, &ctx, 300, &rules, &ParentKeys::default()).expect("generate");
        assert_eq!(df.height(), 300);
        let v = df.column("v").unwrap().f64().unwrap();
        assert!(v.iter().flatten().all(|x| x > 20.0));
        assert!(df.column("doubled").is_ok());
    }

    #[test]
    fn impossible_constraint_fails_with_named_constraint() {
        let profile = profile_of(vec![numeric_col("v")], None);
        let ctx = TableGenContext { name: "t", profile: &profile, keys: &[], fks: &[], seed: 2 };
        let rules = TableRules {
            constraints: vec!["v > 1000000".to_string()],
            derives: vec![],
        };
        let engine = crate::polars_engine::PolarsEngine::new();
        let err = generate_table(&engine, &ctx, 100, &rules, &ParentKeys::default())
            .expect_err("unsatisfiable");
        assert!(err.to_string().contains("v > 1000000"));
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo synth:: 2>&1 | tail -10`
Expected: COMPILE FAIL.

- [ ] **Step 3: Implement rules.rs**

```rust
//! Intra-row rules: constraint filtering and derived columns via Polars SQL.

use polars::prelude::*;

use crate::{error::DtooError, polars_engine::PolarsEngine};

fn config_err(message: String) -> DtooError {
    DtooError::Config { message }
}

/// Splits a derive rule `name = expr` on the FIRST `=`.
pub fn parse_derive(rule: &str) -> Result<(String, String), DtooError> {
    let Some(pos) = rule.find('=') else {
        return Err(config_err(format!(
            "derive rule `{rule}` must be in `column = expression` form"
        )));
    };
    let name = rule[..pos].trim().to_string();
    let expr = rule[pos + 1..].trim().to_string();
    if name.is_empty() || expr.is_empty() {
        return Err(config_err(format!(
            "derive rule `{rule}` must be in `column = expression` form"
        )));
    }
    Ok((name, expr))
}

/// Filters a batch by ALL constraints (ANDed) and returns per-constraint
/// individual pass counts (each evaluated against the full input batch).
pub fn filter_constraints(
    engine: &PolarsEngine,
    df: DataFrame,
    constraints: &[String],
) -> Result<(DataFrame, Vec<(String, usize)>), DtooError> {
    let mut counts = Vec::with_capacity(constraints.len());
    for c in constraints {
        let sql = format!("SELECT * FROM _ WHERE ({c})");
        let passed = engine
            .collect(engine.run_sql(df.clone().lazy(), &[], &sql)?)?
            .height();
        counts.push((c.clone(), passed));
    }
    let combined = constraints
        .iter()
        .map(|c| format!("({c})"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!("SELECT * FROM _ WHERE {combined}");
    let kept = engine.collect(engine.run_sql(df.lazy(), &[], &sql)?)?;
    Ok((kept, counts))
}

/// Applies derive rules in order. Existing columns are replaced.
pub fn apply_derives(
    engine: &PolarsEngine,
    mut df: DataFrame,
    derives: &[String],
) -> Result<DataFrame, DtooError> {
    for rule in derives {
        let (name, expr) = parse_derive(rule)?;
        if df.get_column_names().iter().any(|c| c.as_str() == name) {
            df = df
                .drop(&name)
                .map_err(|e| config_err(format!("derive `{rule}`: {e}")))?;
        }
        let sql = format!("SELECT *, ({expr}) AS \"{name}\" FROM _");
        df = engine.collect(engine.run_sql(df.lazy(), &[], &sql)?)?;
    }
    Ok(df)
}
```

**Implement `generate_table` in engine.rs** (add after `generate_batch`; add `use crate::polars_engine::PolarsEngine;` and `use crate::synth::rules;`):

```rust
/// Rules attached to a table, pre-split by kind.
pub struct TableRules {
    pub constraints: Vec<String>,
    pub derives: Vec<String>,
}

/// Generates a full table: oversample-and-filter under constraints (10%
/// acceptance floor checked after 5 rounds), then derives, then truncation.
pub fn generate_table(
    engine: &PolarsEngine,
    ctx: &TableGenContext,
    rows: usize,
    rules: &TableRules,
    parents: &ParentKeys,
) -> Result<DataFrame, DtooError> {
    // rows: 0 is valid per the spec: empty output with the correct schema.
    // A zero-row batch produces empty columns of the right dtypes.
    if rows == 0 {
        let empty = generate_batch(ctx, 0, 0, 0, parents)?;
        return if rules.derives.is_empty() {
            Ok(empty)
        } else {
            rules::apply_derives(engine, empty, &rules.derives)
        };
    }

    let mut kept: Option<DataFrame> = None;
    let mut kept_rows = 0usize;
    let mut generated_rows = 0usize;
    let mut worst: Option<(String, f64)> = None;
    let mut round: u64 = 0;

    while kept_rows < rows {
        let remaining = rows - kept_rows;
        let batch_rows = if rules.constraints.is_empty() {
            remaining
        } else {
            (remaining * 3 / 2).max(1)
        };
        let batch = generate_batch(ctx, round, batch_rows, generated_rows, parents)?;
        generated_rows += batch.height();

        let filtered = if rules.constraints.is_empty() {
            batch
        } else {
            let (f, counts) = rules::filter_constraints(engine, batch, &rules.constraints)?;
            for (c, passed) in counts {
                let rate = passed as f64 / batch_rows as f64;
                if worst.as_ref().is_none_or(|(_, w)| rate < *w) {
                    worst = Some((c, rate));
                }
            }
            f
        };
        kept_rows += filtered.height();
        kept = Some(match kept {
            None => filtered,
            Some(acc) => acc
                .vstack(&filtered)
                .map_err(|e| config_err(format!("accumulating batches: {e}")))?,
        });

        round += 1;
        if kept_rows < rows && round >= 5 {
            let acceptance = kept_rows as f64 / generated_rows as f64;
            if acceptance < 0.10 {
                let (constraint, rate) = worst.expect("constraints ran");
                return Err(config_err(format!(
                    "table `{}`: constraint `{constraint}` accepts only {:.1}% of generated rows after {round} rounds; it appears unsatisfiable against the profile",
                    ctx.name,
                    rate * 100.0
                )));
            }
        }
    }

    let mut df = kept.unwrap_or_default().head(Some(rows));
    if !rules.derives.is_empty() {
        df = rules::apply_derives(engine, df, &rules.derives)?;
    }
    Ok(df)
}
```

(Note `is_none_or` needs Rust 1.82+; the repo pins a stable toolchain in `rust-toolchain.toml` — if it's older, use `worst.as_ref().map_or(true, |(_, w)| rate < *w)`.)

- [ ] **Step 4: Run tests**

Run: `cargo test -p dtoo synth::`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/synth/rules.rs src/synth/engine.rs
git commit -m "Add constraint filtering and derived columns to synth generation"
```

---

### Task 14: CLI validation, orchestration, dry-run, verbose

**Files:**
- Modify: `src/cli.rs` (synth validation), `src/synth/engine.rs` (real `run`)

- [ ] **Step 1: Write the failing CLI validation tests**

`src/cli.rs` tests:

```rust
    #[test]
    fn synth_requires_spec_or_profile() {
        let err = parse_err(["dtoo", "synth"]);
        assert!(err.to_string().contains("--spec or --profile"));
    }

    #[test]
    fn synth_rejects_spec_and_profile_together() {
        let err = parse_err([
            "dtoo", "synth", "--spec", "s.yaml", "--profile", "p.json",
        ]);
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn synth_profile_mode_requires_rows() {
        let err = parse_err(["dtoo", "synth", "--profile", "p.json"]);
        assert!(err.to_string().contains("--rows"));
    }

    #[test]
    fn synth_spec_mode_rejects_rows_and_output() {
        let err = parse_err(["dtoo", "synth", "--spec", "s.yaml", "--rows", "10"]);
        assert!(err.to_string().contains("--profile"));
    }

    #[test]
    fn synth_parses_valid_invocations() {
        let cli = parse(["dtoo", "synth", "--profile", "p.json", "--rows", "100", "--seed", "9"]);
        match cli.command {
            Commands::Synth(args) => {
                assert_eq!(args.rows, Some(100));
                assert_eq!(args.seed, Some(9));
            }
            _ => panic!("expected synth command"),
        }
        parse(["dtoo", "synth", "--spec", "s.yaml", "--dry-run", "--verbose"]);
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dtoo cli 2>&1 | tail -10`
Expected: FAIL — no validation exists.

- [ ] **Step 3: Implement CLI validation**

In `Cli::parse_and_validate_from`, after the query block:

```rust
        if let Commands::Synth(synth) = &cli.command {
            synth
                .validate()
                .map_err(|message| Self::command().error(ErrorKind::ValueValidation, message))?;
        }
```

Add to `impl SynthArgs` (new impl block near `impl QueryArgs`):

```rust
impl SynthArgs {
    fn validate(&self) -> Result<(), String> {
        match (&self.spec, &self.profile) {
            (Some(_), Some(_)) => {
                Err("--spec and --profile are mutually exclusive".to_string())
            }
            (None, None) => Err("synth requires --spec or --profile".to_string()),
            (Some(_), None) => {
                if self.rows.is_some() || self.output.is_some() {
                    Err("--rows and --output are only valid with --profile (spec mode reads them from the spec)".to_string())
                } else {
                    Ok(())
                }
            }
            (None, Some(_)) => {
                if self.rows.is_none() {
                    Err("--profile mode requires --rows".to_string())
                } else {
                    Ok(())
                }
            }
        }
    }
}
```

- [ ] **Step 4: Implement `run` in engine.rs**

Replace the stub `run` with:

```rust
/// Entry point for `dtoo synth`.
pub fn run(args: &SynthArgs) -> Result<(), DtooError> {
    let engine = PolarsEngine::new();
    match (&args.spec, &args.profile) {
        (Some(spec_path), None) => run_spec_mode(&engine, args, spec_path),
        (None, Some(profile_path)) => run_single_mode(&engine, args, profile_path),
        _ => unreachable!("CLI validation enforces exactly one mode"),
    }
}

fn run_single_mode(
    engine: &PolarsEngine,
    args: &SynthArgs,
    profile_path: &std::path::Path,
) -> Result<(), DtooError> {
    use crate::output_writer::{OutputWriter, OutputWriterConfig};

    let profile = crate::synth::profile_input::load_profile(profile_path)?;
    let rows = args.rows.expect("CLI validation guarantees --rows");
    let seed = args.seed.unwrap_or(0);

    if args.dry_run {
        eprintln!("Synth Plan\n==========");
        eprintln!("Seed: {seed}");
        eprintln!(
            "  synth: {rows} rows -> {} ({:?})",
            args.output
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "stdout".to_string()),
            args.output_format
        );
        return Ok(());
    }

    let ctx = TableGenContext { name: "synth", profile: &profile, keys: &[], fks: &[], seed };
    let rules = TableRules { constraints: vec![], derives: vec![] };
    let df = generate_table(engine, &ctx, rows, &rules, &ParentKeys::default())?;
    if args.verbose {
        eprintln!("[1/1] synth — {} rows generated", df.height());
    }

    let writer = OutputWriter::new(OutputWriterConfig {
        output: args.output.clone(),
        format: export_format(args.output_format),
        header: !args.no_header,
        delimiter: args.delimiter,
        compression: args.compress.map(compression_codec),
    });
    writer.write(engine, df)
}

fn run_spec_mode(
    engine: &PolarsEngine,
    args: &SynthArgs,
    spec_path: &std::path::Path,
) -> Result<(), DtooError> {
    use crate::output_writer::{OutputWriter, OutputWriterConfig};
    use crate::synth::spec;

    let synth_spec = spec::load_spec(spec_path)?;
    spec::validate(&synth_spec)?;
    let order = spec::generation_order(&synth_spec)?;
    let seed = args.seed.unwrap_or(synth_spec.seed);
    let base_dir = spec_path.parent().unwrap_or(std::path::Path::new("."));

    if args.dry_run {
        eprintln!("Synth Plan\n==========");
        eprintln!("Seed: {seed}");
        eprintln!("Generation order: {}", order.join(", "));
        for name in &order {
            let t = &synth_spec.tables[name];
            let n_c = t.rules.iter().filter(|r| r.constraint.is_some()).count();
            let n_d = t.rules.iter().filter(|r| r.derive.is_some()).count();
            eprintln!(
                "  {name}: {} rows -> {} ({} fks, {n_c} constraints, {n_d} derives)",
                t.rows,
                t.output.display(),
                t.foreign_keys.len()
            );
        }
        return Ok(());
    }

    let mut parents = ParentKeys::default();
    for (i, name) in order.iter().enumerate() {
        let table = &synth_spec.tables[name];
        let profile = crate::synth::profile_input::load_profile(&base_dir.join(&table.profile))?;

        let fks: Vec<ResolvedFk> = table
            .foreign_keys
            .iter()
            .map(|fk| {
                Ok(ResolvedFk {
                    column: fk.column.clone(),
                    parent_key: fk.references.clone(),
                    fan_out: spec::fan_out(fk)?,
                })
            })
            .collect::<Result<_, DtooError>>()?;
        let rules = TableRules {
            constraints: table.rules.iter().filter_map(|r| r.constraint.clone()).collect(),
            derives: table.rules.iter().filter_map(|r| r.derive.clone()).collect(),
        };

        let ctx = TableGenContext {
            name,
            profile: &profile,
            keys: &table.keys,
            fks: &fks,
            seed,
        };
        let df = generate_table(engine, &ctx, table.rows, &rules, &parents)?;
        if args.verbose {
            eprintln!(
                "[{}/{}] {name} — {} rows -> {}",
                i + 1,
                order.len(),
                df.height(),
                table.output.display()
            );
        }

        // Retain key columns referenced by children.
        for key in &table.keys {
            if let Ok(col) = df.column(key) {
                parents.insert(format!("{name}.{key}"), col.as_materialized_series().clone());
            }
        }

        let output = base_dir.join(&table.output);
        let format = match table.output_format.as_deref() {
            Some("csv") => crate::types::ExportFormat::Csv,
            Some("parquet") => crate::types::ExportFormat::Parquet,
            Some("ndjson") => crate::types::ExportFormat::Ndjson,
            Some(other) => {
                return Err(config_err(format!(
                    "table `{name}`: output_format must be csv, parquet, or ndjson, got `{other}`"
                )));
            }
            None => infer_format(&output),
        };
        let writer = OutputWriter::new(OutputWriterConfig {
            output: Some(output),
            format,
            header: true,
            delimiter: ',',
            compression: None,
        });
        writer.write(engine, df)?;
    }
    Ok(())
}

fn export_format(f: crate::cli::OutputFormat) -> crate::types::ExportFormat {
    match f {
        crate::cli::OutputFormat::Csv => crate::types::ExportFormat::Csv,
        crate::cli::OutputFormat::Parquet => crate::types::ExportFormat::Parquet,
        crate::cli::OutputFormat::Ndjson => crate::types::ExportFormat::Ndjson,
    }
}

fn compression_codec(c: crate::cli::CompressMethod) -> crate::types::CompressionCodec {
    match c {
        crate::cli::CompressMethod::Gzip => crate::types::CompressionCodec::Gzip,
        crate::cli::CompressMethod::Zstd => crate::types::CompressionCodec::Zstd,
    }
}

fn infer_format(path: &std::path::Path) -> crate::types::ExportFormat {
    match path.extension().and_then(|e| e.to_str()) {
        Some("parquet") => crate::types::ExportFormat::Parquet,
        Some("ndjson") | Some("jsonl") => crate::types::ExportFormat::Ndjson,
        _ => crate::types::ExportFormat::Csv,
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test`
Expected: ALL PASS (CLI validation tests now pass; everything else still green).

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/cli.rs src/synth/engine.rs
git commit -m "Add dtoo synth CLI validation and orchestration"
```

---

### Task 15: End-to-end integration tests

**Files:**
- Create: `tests/synth_integration.rs`

- [ ] **Step 1: Write the integration tests**

```rust
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
fn make_profiles(dir: &PathBuf) -> (PathBuf, PathBuf) {
    let customers_csv = dir.join("customers.csv");
    let mut body = String::from("customer_id,region\n");
    for i in 0..50 {
        body.push_str(&format!("{},{}\n", 1000 + i, if i % 3 == 0 { "EU" } else { "US" }));
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
        assert!(out.status.success(), "profile failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    (cust_profile, ord_profile)
}

fn write_spec(dir: &PathBuf) -> PathBuf {
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
        .args(["synth", "--spec", spec.to_string_lossy().as_ref(), "--verbose"])
        .output()
        .expect("run synth");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));

    let customers = fs::read_to_string(dir.join("out/customers.csv")).expect("customers out");
    let orders = fs::read_to_string(dir.join("out/orders.csv")).expect("orders out");
    assert_eq!(customers.lines().count(), 201, "header + 200 rows");
    assert_eq!(orders.lines().count(), 2001, "header + 2000 rows");

    // FK integrity: every order's customer_id exists in customers.
    let cust_header: Vec<&str> = customers.lines().next().unwrap().split(',').collect();
    let cust_id_idx = cust_header.iter().position(|c| *c == "customer_id").unwrap();
    let cust_ids: std::collections::HashSet<String> = customers
        .lines()
        .skip(1)
        .map(|l| l.split(',').nth(cust_id_idx).unwrap().to_string())
        .collect();
    assert_eq!(cust_ids.len(), 200, "keys are unique");

    let ord_header: Vec<&str> = orders.lines().next().unwrap().split(',').collect();
    let fk_idx = ord_header.iter().position(|c| *c == "customer_id").unwrap();
    let amount_idx = ord_header.iter().position(|c| *c == "amount").unwrap();
    let doubled_idx = ord_header.iter().position(|c| *c == "amount_doubled").unwrap();
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
        assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
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
        .args(["synth", "--spec", spec.to_string_lossy().as_ref(), "--seed", "7"])
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
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
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
        .args(["synth", "--spec", spec.to_string_lossy().as_ref(), "--dry-run"])
        .output()
        .expect("run synth");
    assert!(out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("Generation order: customers, orders"));
    assert!(!dir.join("out/customers.csv").exists(), "dry run writes nothing");
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
```

- [ ] **Step 2: Run them**

Run: `cargo test --test synth_integration`
Expected: ALL PASS. If `spec_mode_generates_tables_with_fk_integrity` fails on row counts, debug `generate_table`'s truncation; if FK integrity fails, debug `generate_fk_series` parent indexing.

- [ ] **Step 3: Run the whole suite**

Run: `cargo test`
Expected: ALL PASS.

- [ ] **Step 4: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add tests/synth_integration.rs
git commit -m "Add end-to-end integration tests for dtoo synth"
```

---

### Task 16: Documentation and final verification

**Files:**
- Modify: `docs/USER_GUIDE.md`

- [ ] **Step 1: Add a "Synthetic data" section to USER_GUIDE.md**

Append (adjusting heading level to match the file's existing structure — check with `head -40 docs/USER_GUIDE.md`):

```markdown
## Synthetic data

Generate realistic, privacy-preserving synthetic data from profiles of real
sources. The profile JSON is the only artifact you need — profile inside a
secure environment, generate anywhere.

### 1. Profile the real data at synth detail

```bash
dtoo profile real/customers.csv --detail synth --output profiles/customers.json
dtoo profile real/orders.csv   --detail synth --output profiles/orders.json
```

Synth detail adds histograms, top-K value frequencies (`--top-k`, default
1000), unique ratios, and a Spearman correlation matrix. It requires JSON
format. Standard profiles also work for generation, with reduced fidelity.

### 2a. Quick single-table generation

```bash
dtoo synth --profile profiles/customers.json --rows 100000 --seed 42 \
  --output synth/customers.parquet --output-format parquet
```

### 2b. Multi-table generation with referential integrity

```yaml
# synth.yaml
seed: 42
tables:
  customers:
    profile: profiles/customers.json
    rows: 10000
    keys: [customer_id]
    output: synth/customers.parquet
  orders:
    profile: profiles/orders.json
    rows: 250000
    foreign_keys:
      - column: customer_id
        references: customers.customer_id
        fan_out: from_profile      # realistic orders-per-customer skew
    rules:
      - constraint: "amount > 0"
      - derive: "total = quantity * unit_price"
    output: synth/orders.parquet
```

```bash
dtoo synth --spec synth.yaml --dry-run    # show the plan
dtoo synth --spec synth.yaml --verbose    # generate
```

Guarantees: same spec + same seed → byte-identical output; every foreign key
value exists in its parent; constraints hold on every output row; numeric
correlations from the profile are preserved via a Gaussian copula. Paths in
the spec are relative to the spec file. Practical ceiling is ~10M rows per
table (in-memory generation).
```

- [ ] **Step 2: Final full verification**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test`
Expected: clean fmt, zero warnings, ALL tests pass.

- [ ] **Step 3: Commit**

```bash
git add docs/USER_GUIDE.md
git commit -m "Document synthetic data generation in user guide"
```

---

## Spec Coverage Checklist (self-review aid)

| Spec section | Task(s) |
|---|---|
| `--detail synth` / `--profile-detail` / `--top-k` CLI | 6 |
| Histogram (20 quantile buckets, temporal physical) | 3 |
| top_values / unique_ratio | 4 |
| correlation_matrix (Spearman, pairwise-null exclusion) | 5 |
| Standard output byte-identical | 2 (regression test) |
| Synth detail rejects csv/html | 2 (check), 6 (integration test) |
| Spec YAML + validation + topo order + cycle error | 7 |
| `--spec`/`--profile` mutual exclusion, `--rows`, `--seed`, `--dry-run`, `--verbose` | 14 |
| Per-dtype samplers + null injection + pattern vocabulary | 9, 12 |
| Standard-profile degradation + warning | 8 (quantiles), 12 (warning), 15 (test) |
| Keys (sequential/padded/UUID/pattern; non-unique warning) | 10, 12 |
| FK integrity + fan-out from_profile/uniform + cap warning | 12, 15 |
| Copula + PSD repair | 11, 12 |
| Rules: derive replace/append, constraint oversample-filter, 10%/5-round floor | 13 |
| Output via existing writer, format inference, stdout | 14, 15 |
| Reproducibility guarantee | 12 (unit), 15 (integration) |
| Error cases table | 7, 8, 13, 14, 15 |
| USER_GUIDE | 16 |
