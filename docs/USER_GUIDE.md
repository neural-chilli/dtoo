# dtoo User Guide

This guide covers day-to-day usage of `dtoo`, from first run through practical workflows.

## What is dtoo?

`dtoo` is a Rust CLI for querying and profiling data files. It is built on **Polars** (pure Rust) and is designed for fast local analytics and reproducible pipelines across file trees.

Core capabilities:
- Query many files using SQL
- Read CSV, Parquet, NDJSON, and Excel (`.xlsx`, `.xls`)
- Join reference tables into your query
- Add lineage, masking, profiling, fingerprinting, and manifests
- Read from local files (cloud paths S3/GCS/Azure are deferred — they return a clear error in this build)

## Install and Build

### Prerequisites
- Rust toolchain (stable)

### Build

```bash
cargo build --release
```

Binary path:

```bash
./target/release/dtoo
```

## Command Overview

```bash
dtoo <COMMAND>
```

Commands:
- `query` - main data pipeline command
- `inspect` - preview rows from one file
- `profile` - profile one file
- `fingerprint` - compute a file fingerprint

## `query` Essentials

`query` is the primary command.

```bash
dtoo query [OPTIONS] [PATH]...
```

### Input Selection

Use one input source style at a time:
- Positional paths
- `--glob "pattern"`
- `--pipe file` (read list of paths from stdin)
- `--pipe data --stdin-format <csv|parquet|ndjson>` (read raw data from stdin)

Examples:

```bash
# One file
dtoo query data/trips.parquet

# Recursive tree scan
dtoo query --glob "testdata/**/*"

# Path list from stdin
printf "data/a.csv\ndata/b.csv\n" | dtoo query --pipe file
```

### Supported Formats

Auto-detected by extension:
- `.parquet`
- `.csv`, `.tsv`, `.txt`
- `.ndjson`, `.jsonl`
- `.xlsx`, `.xls`

### SQL Stages (`_` table)

`dtoo` registers each input file as `_` and processes it through SQL stages:
- `--where` for simple filtering
- `--filter-sql` for per-file SQL against `_`
- `--post-sql` for SQL after all files are combined

Example:

```bash
dtoo query --glob "data/**/*.parquet" \
  --where "total_amount > 0" \
  --post-sql "SELECT passenger_count, COUNT(*) AS trips FROM _ GROUP BY 1"
```

**SQL limitations (Polars `SQLContext`):** `SELECT`, `WHERE`, `GROUP BY`, `JOIN`, `ORDER BY`, `LIMIT`, CTEs, `UNION`/`UNION ALL`, subqueries, and common string/date functions are supported. Known gaps:

- **Window functions** (`OVER (PARTITION BY … ORDER BY …)`) have correctness issues — avoid them.
- **`DELETE`/`UPDATE`** are treated as row-filtering transforms, not DML mutations. Do not rely on DML semantics in `--post-sql`.

### Excel Sheet Selection

You can control sheet selection in two ways:
- Global sheet for Excel inputs: `--sheet "SheetName"`
- Per-file sheet on paths/refs: `file.xlsx:SheetName`

If no sheet is provided, `dtoo` reads the first sheet.

Examples:

```bash
# Global sheet
dtoo query --glob "testdata/**/*.xlsx" --sheet "trips"

# Per-file sheet
dtoo query sales.xlsx:Q1
```

### Reference Tables

Load lookup data with `--ref` and join it in SQL.

Format:

```text
--ref name=path
--ref name=path.xlsx:SheetName
```

Example:

```bash
dtoo query trips.parquet \
  --ref zones=zones.csv \
  --post-sql "SELECT t.*, z.borough FROM _ t LEFT JOIN zones z USING (LocationID)"
```

### Output

Default output format is CSV.

Options:
- `--output out/result.csv`
- `--output-format csv|parquet|ndjson`
- `--compress gzip|zstd`
- `--no-header` (CSV)

Examples:

```bash
# Convert parquet to csv
dtoo query testdata/trips.parquet --output out/trips.csv

# Write parquet
dtoo query --glob "data/**/*.csv" --output-format parquet --output out/all.parquet
```

### Output Controls

- `--limit N` limit output rows
- `--count` emit only row count
- `--expect-at-least N` fail if output row count is below N

Example:

```bash
dtoo query --glob "data/**/*.csv" --count --expect-at-least 1
```

### Error Handling and Visibility

- `--on-error fail` (default): stop on first file error
- `--on-error skip`: warn and continue
- `--verbose`: print progress/timing logs to stderr
- `--dry-run`: print execution plan without reading data

Example:

```bash
dtoo query --glob "data/**/*" --on-error skip --dry-run
```

### Schema, Lineage, Masking, Profiling, Fingerprint, Manifest

- `--schema <PATH>` use explicit schema
- `--lineage <PATH>` write lineage output
- `--mask <RULES>` and `--mask-salt <SALT>`
- `--profile <PATH>` with `--profile-format json|csv|html` and `--profile-sample`
- `--fingerprint` add output fingerprinting
- `--manifest <PATH>` write run manifest

## `inspect` Command

Quickly preview rows from a single file.

```bash
dtoo inspect data/trips.parquet --rows 20
```

Useful options:
- `--rows <N>` (default 10)
- `--delimiter <CHAR>` for CSV-like inputs
- cloud flags: `--s3-region`, `--s3-profile`, `--gcs-project`, `--azure-account`

## `profile` Command

Profile one file directly.

```bash
dtoo profile data/trips.parquet --format html --output out/profile.html
```

Options:
- `--format json|csv|html`
- `--sample <N>`
- `--output <PATH>`
- `--delimiter <CHAR>`
- cloud flags

## `fingerprint` Command

Compute a fingerprint for a single file.

```bash
dtoo fingerprint data/trips.parquet
```

## Cloud Paths

**Cloud storage is deferred in this build.** Paths beginning with `s3://`, `gs://`, or `az://` return an explicit error:

```
cloud storage (s3://…) is not supported in this build yet
```

The cloud CLI flags (`--s3-region`, `--s3-profile`, `--gcs-project`, `--azure-account`) still parse so that config files written for a future cloud-enabled build remain valid.

## Practical Recipes

### 1. Convert Parquet to CSV

```bash
dtoo query testdata/trips.parquet --output out/trips.csv
```

### 2. Aggregate Across a Folder Recursively

```bash
dtoo query \
  --glob "testdata/**/*" \
  --on-error skip \
  --post-sql "SELECT passenger_count, COUNT(*) AS trips, AVG(trip_distance) AS avg_distance, SUM(total_amount) AS total_amount FROM _ GROUP BY 1 ORDER BY 2 DESC" \
  --output out/agg.csv
```

### 3. Aggregate Excel with Explicit Sheet

```bash
dtoo query \
  --glob "testdata/**/*.xlsx" \
  --sheet "trips" \
  --post-sql "SELECT passenger_count, COUNT(*) AS trips FROM _ GROUP BY 1 ORDER BY 2 DESC" \
  --output out/agg_xlsx.csv
```

## Troubleshooting

### "No files matched glob pattern"
- Check shell quoting: always quote globs (`"data/**/*"`)
- Confirm files exist and extensions are supported

### Excel: "Sheet not found"
- Use the actual sheet name from the workbook
- Or omit `--sheet` to use the first sheet

### Skipped files with `--on-error skip`
- Inspect warnings on stderr
- Re-run a failing file directly with `inspect` for diagnosis

### Mixed file trees
- Prefer recursive glob plus `--on-error skip`
- Keep non-data files out of input trees when possible

## Performance Tips

- Prefer Parquet for large datasets
- Use `--where` or `--filter-sql` to reduce rows early
- Use `--dry-run` before expensive scans
- Write Parquet output for downstream jobs

## Exit Behavior

At a high level:
- Success: command completes and outputs expected result
- Partial success (`--on-error skip`): warnings plus summary
- Failure: configuration, SQL, or IO errors stop execution

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
format (`--format json`). Standard profiles also work for generation, with
reduced fidelity (a warning is emitted).

### 2a. Quick single-table generation

```bash
dtoo synth --profile profiles/customers.json --rows 100000 --seed 42 \
  --output synth/customers.parquet --output-format parquet
```

### 2b. Multi-table generation with referential integrity

Write a spec YAML that declares generation order, key columns, foreign keys, and
intra-row rules:

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

Preview the plan, then generate:

```bash
dtoo synth --spec synth.yaml --dry-run    # show the plan, write nothing
dtoo synth --spec synth.yaml --verbose    # generate with progress on stderr
```

### Guarantees

- **Reproducible:** same spec + same seed produces byte-identical output.
- **FK integrity:** every foreign key value exists in its parent table's key column.
- **Constraints hold:** intra-row `constraint` rules filter the generated batch;
  rows that cannot satisfy a constraint after repeated oversampling cause a
  named error.
- **Copula correlations:** numeric correlations captured in the profile are
  preserved via a Gaussian copula, so column relationships remain realistic.
- **Spec-relative paths:** `profile` and `output` paths in the spec are resolved
  relative to the spec file, not the working directory.
- **Scale:** practical ceiling is approximately 10 million rows per table
  (in-memory generation via Polars).

## Additional References

- Architecture and full spec: [DESIGN.md](DESIGN.md)
- Contributor workflow: [CONTRIBUTING.md](../CONTRIBUTING.md)
- License: [LICENSE.md](../LICENSE.md)
