# dtoo - Design Specification

A Rust CLI tool for data engineers to query, profile, and transform data across file trees.

## Core Engine

**Polars** (pure Rust, `polars` crate ~0.54.x) — provides SQL execution via `SQLContext`, format readers (Parquet, CSV/TSV, NDJSON, and Excel via the pure-Rust `calamine` backend), glob resolution, type-safe lazy evaluation, and the `concat_lf_diagonal` union-by-name schema evolution. There is no bundled C++ toolchain and no runtime extension downloads.

**Cloud storage** (S3/GCS/Azure) is **deferred in this build**: cloud paths (`s3://`, `gs://`, `az://`) return a clear `"cloud storage … is not supported in this build yet"` error. The cloud CLI flags (`--s3-region`, `--s3-profile`, `--gcs-project`, `--azure-account`) still parse for forward compatibility.

### SQL Surface Limitations

User-facing SQL (`--where`, `--filter-sql`, `--post-sql`, reference-table JOINs) runs on Polars `SQLContext`:

- **`DELETE` / `UPDATE` statements are treated as transforms**, not mutations. For example, `DELETE FROM _ WHERE x` drops matching rows and returns a result set rather than raising an error. Do not rely on DML semantics in `--post-sql`.
- **Window functions** (`OVER (PARTITION BY … ORDER BY …)`) have known correctness issues in Polars SQL. Avoid them; dtoo does not attempt to detect or warn about their use.
- **Narrower function library**: some exotic date, regex, and string functions are absent. Unsupported SQL returns a clear error (it never silently hangs — which was the motivation for the migration).
- Errors are always explicit `Result` values; the engine never hangs on malformed input.

---

## Subcommands

### `dtoo query`

The main workhorse. Scans files, applies SQL, accumulates results, emits output.

### `dtoo profile`

Profiles an existing file directly (shortcut — no query pipeline needed).

### `dtoo inspect`

Quick schema, row count, and preview of a file.

### `dtoo fingerprint`

SHA256 hash of an existing file. Also available as `--fingerprint` flag on `query`.

---

## Query Pipeline

Execution order within `dtoo query`:

```
1.  Resolve file list (glob pattern / pipe input / explicit paths)
2.  Apply --exclude patterns to filter file list
3.  If --dry-run: display plan and exit
4.  Init Polars engine; scan reference table files into named LazyFrames
5.  Scan each input file as a LazyFrame; register refs and _ in SQLContext
6.  Create accumulated result (LazyFrame; schema from first file or explicit --schema)
7.  For each file:
    a. Register file as `_` (the magic table name)
    b. Apply --where clause if specified:  SELECT * FROM _ WHERE {where}
    c. Apply --filter-sql if specified:    {filter-sql} (user writes full SELECT from _)
       - If both --where and --filter-sql, --where is applied first
       - (effectively: filter-sql runs against the already-filtered result)
    d. INSERT matching rows into temp_results
    e. Track origin_file metadata for lineage
    f. If --verbose: log file name, rows matched, running total to stderr
8.  Apply --post-sql against temp_results (user writes full SELECT from _)
    - _ now refers to the accumulated temp_results table
    - Reference tables are available for JOINs
9.  Apply column masking
10. Add lineage columns
11. Apply --limit if specified
12. Validate --expect-at-least
13. If --count: emit row count to stdout
14. Write output (file or stdout), applying --compress and --no-header
15. Generate profile report if requested
16. Generate fingerprint if requested
17. Write manifest if requested
```

### The Magic Table: `_`

In all SQL contexts (`--filter-sql`, `--post-sql`), the underscore `_` refers to the current data:
- In `--filter-sql`: `_` is the current file being processed
- In `--post-sql`: `_` is the accumulated temp_results table

Reference tables are available by their assigned names in all SQL contexts.

---

## CLI Interface

### dtoo query

```
dtoo query [OPTIONS]

INPUT:
  --glob <PATTERN>            Glob pattern for input files (e.g. "data/**/*.parquet")
  --exclude <PATTERN>...      Glob pattern(s) to exclude from file list
  --pipe <MODE>               Shell pipe mode: file (stdin = file paths) | data (stdin = records)
  --stdin-format <FORMAT>     Format for pipe data mode [default: csv]
  --sheet <NAME>              Excel sheet name for globbed .xlsx files [default: first sheet]

QUERY:
  --where <CLAUSE>            Simple WHERE clause applied first (e.g. "amount > 100")
  --filter-sql <SQL>          Full SELECT against _ per file (e.g. "SELECT id, name FROM _")
  --post-sql <SQL>            Full SELECT against accumulated _ after all files processed
  --ref <NAME=PATH>...        Reference tables (e.g. --ref regions=ref/regions.csv)

SCHEMA:
  --schema <PATH>             Explicit schema definition file (YAML)

OUTPUT:
  --output <PATH>             Output file path [default: stdout]
  --output-format <FORMAT>    csv | parquet | ndjson [default: csv]
  --delimiter <CHAR>          Delimiter for CSV output [default: ,]

LINEAGE:
  --lineage <COLUMNS>         Add lineage columns: all | comma-separated list of:
                              batch_id, record_id, batch_timestamp, batch_hash, origin_file

MASKING:
  --mask <COLUMNS>            Comma-separated columns to mask
  --mask-salt <STRING>        Salt for deterministic masking [default: ""]

PROFILING:
  --profile <PATH>            Generate profile report at path (- for stdout after output)
  --profile-format <FORMAT>   json | csv | html [default: json]
  --profile-sample <PERCENT>  Sampling percentage for profiling [default: 100]

OUTPUT CONTROL:
  --limit <N>                 Cap output to N rows
  --no-header                 Suppress header row in CSV output
  --compress <METHOD>         Compress output: gzip | zstd

VALIDATION:
  --expect-at-least <N>       Minimum expected record count (exit non-zero if fewer)
  --fingerprint               Generate SHA256 fingerprint of output
  --count                     Emit only the row count (no data output)

EXECUTION:
  --dry-run                   Show resolved file list, schema, and SQL plan without executing
  --verbose                   Log progress to stderr (files processed, row counts, timing)
  --on-error <MODE>           skip | fail [default: fail]

MANIFEST:
  --manifest <PATH>           Write run metadata to YAML sidecar file

CLOUD:
  --s3-region <REGION>        AWS region for S3 paths
  --s3-profile <PROFILE>      AWS profile for credentials
  --gcs-project <PROJECT>     GCP project for GCS paths
  --azure-account <ACCOUNT>   Azure storage account name

CONFIG:
  --config <PATH>             Load options from YAML config file
```

### dtoo profile

```
dtoo profile <PATH> [OPTIONS]
  --format <FORMAT>           json | csv | html [default: json]
  --output <PATH>             Output file path [default: stdout]
  --sample <PERCENT>          Sampling percentage [default: 100]
  --delimiter <CHAR>          Delimiter if input is CSV [default: ,]
```

### dtoo inspect

```
dtoo inspect <PATH> [OPTIONS]
  --rows <N>                  Number of preview rows [default: 10]
  --delimiter <CHAR>          Delimiter if input is CSV [default: ,]
```

### dtoo fingerprint

```
dtoo fingerprint <PATH>
```

---

## Detailed Feature Design

### File Resolution

Files are resolved from one of three sources (mutually exclusive):

1. **--glob**: Glob pattern. Supports `**` for recursive matching (resolved by Polars native glob scan).
2. **--pipe file**: Newline-delimited file paths from stdin.
3. **--pipe data**: Raw data stream from stdin (requires `--stdin-format`).

Cloud paths (`s3://`, `gs://`, `az://`) are **not supported in this build** — they return an explicit error. See Core Engine above.

Format is auto-detected from file extension:
- `.parquet` — Parquet
- `.csv`, `.tsv`, `.txt` — Delimited text (uses `--delimiter`)
- `.ndjson`, `.jsonl` — Newline-delimited JSON
- `.xlsx`, `.xls` — Excel (sheet selection, see below)

**Excel sheet selection** (precedence, highest first):
1. **Colon syntax** on explicit paths: `file.xlsx:SheetName` — for individual files and ref tables
2. **`--sheet` flag**: applies to all `.xlsx` files matched by glob or pipe
3. **Default**: first sheet

**Excel reading behavior (calamine):** Every cell is read as a string; type inference is deferred to downstream SQL casts or explicit `--schema`. A data row wider than the header row is an error (no silent data loss).

Examples:
```bash
# All xlsx files, same sheet
dtoo query --glob "data/**/*.xlsx" --sheet "Sales"

# Ref table with specific sheet
dtoo query --glob "*.parquet" --ref prices=catalog.xlsx:Pricing

# Colon syntax overrides --sheet for that specific file
find . -name "*.xlsx" | dtoo query --pipe file --sheet "Data" 
# (all files use "Data" sheet)
```

### Schema Handling

**Default (no --schema):** Union-by-name with type promotion. The accumulated result schema evolves as new columns are encountered. Polars `concat_lf_diagonal` handles type promotion (e.g., Int32 → Int64 → Float64) and fills missing columns with `null`.

**Explicit (--schema):** Schema file defines the target columns and types. Files are coerced to match. Extra columns in source files are ignored; missing columns become NULL.

Schema file format (YAML). Type strings use DuckDB-style names (e.g. `INTEGER`, `VARCHAR`, `DECIMAL(10,2)`, `TIMESTAMP`) which are mapped to Polars dtypes at load time. Bare `DECIMAL` defaults to `DECIMAL(18,3)`.

```yaml
columns:
  - name: id
    type: INTEGER
  - name: name
    type: VARCHAR
  - name: amount
    type: DECIMAL(10,2)
  - name: created_at
    type: TIMESTAMP
```

### Where + Filter-SQL Interaction

When both are specified, `--where` is applied first as a pre-filter, then `--filter-sql` operates on the result:

```
-- Internal execution when both specified:
-- Step 1: Scan file → LazyFrame; register as _; execute: SELECT * FROM _ WHERE {where_clause}
-- Step 2: Apply --filter-sql against the filtered LazyFrame (user's SELECT from _)
-- Step 3: Concatenate result into the accumulated LazyFrame (union-by-name)
```

When only `--where` is specified, it's equivalent to `--filter-sql "SELECT * FROM _ WHERE {clause}"`.

When only `--filter-sql` is specified, it runs directly against the file.

### Reference Tables

Loaded once at startup into named LazyFrames registered in the `SQLContext`:

```
--ref regions=ref/regions.parquet --ref products=lookups/products.csv
```

Available in both filter-sql and post-sql:
```sql
--post-sql "SELECT _.*, r.region_name FROM _ JOIN regions r ON _.region_id = r.id"
```

### Lineage Columns

Added after post-sql, before output:

| Column | Description | Value |
|--------|-------------|-------|
| `batch_id` | UUID v4 | Same for all records in this run |
| `record_id` | UUID v4 | Unique per record |
| `batch_timestamp` | UTC timestamp | Same for all records in this run |
| `batch_hash` | SHA256 hex | Deterministic: hash of (filter-sql + post-sql + sorted input file list + schema) |
| `origin_file` | VARCHAR | Source file path the record came from |

`--lineage all` adds all five. `--lineage batch_id,origin_file` adds only those specified.

### Column Masking

Deterministic, irreversible hashing using HMAC-SHA256:

```
masked_value = hex(hmac_sha256(salt + column_name, original_value))
```

- **Deterministic**: Same input always produces same output (within same salt).
- **Referential integrity**: The value "john@example.com" in the email column will always hash to the same value across different runs (with same salt), so JOINs across masked datasets still work.
- **Column-namespaced**: Salt includes column name, so the same value in different columns produces different hashes (prevents cross-column correlation attacks).

NULL values remain NULL (not masked).

### Profiling

Produces per-column statistics using Polars aggregate expressions:

| Metric | Applies To |
|--------|-----------|
| count | All |
| null_count | All |
| null_percentage | All |
| distinct_count | All |
| min | Numeric, Date, Timestamp |
| max | Numeric, Date, Timestamp |
| mean | Numeric |
| stddev | Numeric |
| median | Numeric |
| p25, p75 | Numeric |
| min_length, max_length, avg_length | VARCHAR |
| top_5_values | All (with frequencies) |
| pattern_sample | VARCHAR (regex pattern inference) |

**HTML output**: Self-contained single file with inline CSS. Clean, modern styling. Sortable columns. Expandable detail rows for top values and patterns.

### Fingerprinting

SHA256 hash of the output file bytes. Printed to stderr (or included in a sidecar `.sha256` file).

```
$ dtoo query --glob "*.parquet" --output result.parquet --fingerprint
sha256:a1b2c3d4...  result.parquet
```

### Error Handling

`--on-error skip`: Log warning to stderr, continue processing remaining files. Summary at end: "Processed 95/100 files (5 skipped)".

`--on-error fail`: Exit immediately with non-zero status and error message.

### Expect-at-least

After accumulation and post-sql, check row count. If fewer than N:

```
Error: Expected at least 1000 records, got 42
```

Exit code: non-zero (distinct from other errors, e.g., exit code 2).

### Dry Run

`--dry-run` stops before file processing and outputs a plan to stderr:

```
$ dtoo query --glob "data/**/*.csv" --where "amount > 100" --post-sql "SELECT * FROM _" --dry-run

Dry Run
=======
Files matched: 47
  data/2024-01/sales.csv
  data/2024-01/returns.csv
  ...
Format: CSV (delimiter: ,)
Schema: auto-detect (union-by-name)
Where: amount > 100
Filter SQL: (none)
Post SQL: SELECT * FROM _
Reference tables: (none)
Output: stdout (csv)
Lineage: (none)
Masking: (none)
```

No data is read or processed. Useful for validating glob patterns and SQL before committing to a long run.

### Manifest

`--manifest <PATH>` writes a YAML sidecar file containing full run metadata:

```yaml
# output.manifest.yaml
batch_id: "550e8400-e29b-41d4-a716-446655440000"
batch_hash: "a1b2c3d4..."
batch_timestamp: "2024-03-15T14:30:00Z"
command:
  glob: "data/**/*.parquet"
  where: "amount > 100"
  post_sql: "SELECT region, SUM(amount) FROM _ GROUP BY region"
  output_format: parquet
files:
  total: 100
  processed: 95
  skipped: 5
  details:
    - path: "data/2024-01/sales.parquet"
      rows_matched: 1423
      status: ok
    - path: "data/2024-01/corrupt.parquet"
      rows_matched: 0
      status: skipped
      error: "Invalid Parquet footer"
output:
  path: "results/output.parquet"
  rows: 48721
  fingerprint: "sha256:e3b0c44298..."
  compressed: false
timing:
  started: "2024-03-15T14:30:00Z"
  finished: "2024-03-15T14:30:47Z"
  duration_seconds: 47.2
```

The manifest ties together lineage (batch_id, batch_hash), validation (row counts), and auditability (what ran, what happened) in a single file that pipeline orchestrators can consume.

### Output Controls

**`--limit <N>`**: Appends `LIMIT N` to the final query. Applied after post-sql, masking, and lineage. Useful for sampling or capping output size.

**`--count`**: Emits only the row count to stdout (as a plain integer). No data is written. Useful for validation scripts:

```bash
count=$(dtoo query --glob "data/**/*.csv" --where "status='active'" --count)
if [ "$count" -lt 1000 ]; then echo "Alert: only $count active records"; fi
```

When `--count` is used with `--output`, the count goes to stdout and data still goes to the file.

**`--no-header`**: Suppresses the CSV header row. Only applies to CSV output format. Useful when appending to existing files or piping into tools that don't expect headers.

**`--compress <gzip|zstd>`**: Compresses the output.
- For Parquet: sets the internal compression codec (Parquet is already a binary format).
- For CSV/NDJSON: wraps output in gzip or zstd compression. Output filename should reflect this (e.g., `output.csv.gz`).
- Works with stdout output for pipe chains.

### Verbose Mode

`--verbose` logs progress to stderr so it doesn't interfere with stdout data output:

```
[00:00.0] Starting dtoo query
[00:00.0] Resolved 47 files from glob "data/**/*.csv"
[00:00.1] Loading ref table: regions (ref/regions.parquet) — 250 rows
[00:00.2] [1/47] data/2024-01/sales.csv — 1,423 rows matched
[00:00.3] [2/47] data/2024-01/returns.csv — 89 rows matched
...
[00:12.4] [47/47] data/2024-12/sales.csv — 2,100 rows matched
[00:12.4] Accumulation complete: 48,721 rows
[00:12.5] Applying post-sql...
[00:12.8] Post-sql complete: 1,204 rows
[00:12.8] Applying masking: [email, phone]
[00:12.9] Adding lineage columns: [batch_id, origin_file]
[00:12.9] Writing output: results/output.parquet (parquet)
[00:13.1] Fingerprint: sha256:a1b2c3d4...
[00:13.1] Done. 1,204 rows written in 13.1s
```

Without `--verbose`, dtoo is silent on stderr (except errors and `--on-error skip` warnings). This preserves clean pipe behaviour.

### Exclude Pattern

`--exclude <PATTERN>` filters out files matching the glob pattern during file resolution:

```bash
# Process all CSVs except temp files and backups
dtoo query --glob "data/**/*.csv" --exclude "**/tmp_*" --exclude "**/*.bak.csv"
```

Multiple `--exclude` flags can be specified. Applied after `--glob` resolution, before processing.

### Shell Pipe Behavior

dtoo follows the pattern of `sort` and `uniq` — it accumulates all input before emitting output:

```bash
# File mode: find feeds file paths
find /data -name "*.csv" -mtime -1 | dtoo query --pipe file --where "status='active'" --output-format ndjson

# Data mode: upstream tool feeds records
cat events.ndjson | dtoo query --pipe data --stdin-format ndjson --post-sql "SELECT * FROM _ WHERE ts > '2024-01-01'"

# Chaining
dtoo query --glob "raw/**/*.csv" --where "amount > 0" --output-format ndjson | \
  dtoo query --pipe data --stdin-format ndjson --post-sql "SELECT region, SUM(amount) FROM _ GROUP BY region"
```

---

## Config File

For complex pipelines, a YAML config file avoids unwieldy CLI invocations:

```yaml
# pipeline.yaml
glob: "data/**/*.parquet"
exclude:
  - "**/tmp_*"
  - "**/*.bak.*"
where: "status = 'active'"
filter_sql: "SELECT id, name, amount, region_id FROM _"
post_sql: |
  SELECT _.*,  r.region_name
  FROM _ JOIN regions r ON _.region_id = r.id
  ORDER BY amount DESC

ref:
  regions: ref/regions.parquet
  products: ref/products.csv

schema: schema.yaml

output: results/output.parquet
output_format: parquet
compress: zstd
limit: null
no_header: false

lineage: all
mask:
  columns: [email, phone, ssn]
  salt: "project-x-2024"

profile:
  path: results/profile.html
  format: html
  sample: 10

fingerprint: true
manifest: results/output.manifest.yaml
expect_at_least: 1000
on_error: skip
verbose: true

cloud:
  s3_region: us-east-1
  s3_profile: data-team
```

CLI flags override config file values. Config file can be combined with CLI flags.

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error (IO, SQL syntax, invalid args) |
| 2 | Validation failure (expect-at-least not met) |
| 3 | Partial failure (some files skipped in skip mode) |

---

## Rust Crate Dependencies (Key)

| Crate | Purpose |
|-------|---------|
| `polars` | Core engine: SQL execution (`SQLContext`), lazy evaluation, format readers (Parquet, CSV, NDJSON) |
| `calamine` | Excel reader (pure Rust; bundled via Polars `excel` feature) |
| `flate2` | gzip output wrapping for CSV/NDJSON (Polars has no native text-output compression) |
| `zstd` | zstd output wrapping for CSV/NDJSON |
| `clap` | CLI argument parsing (derive API) |
| `serde` + `serde_yaml` | Config file and schema parsing |
| `uuid` | Lineage UUID generation |
| `sha2` + `hmac` | Column masking and file fingerprinting |
| `chrono` | Timestamp handling |
| `indicatif` | Progress bars for file processing |
| `comfy-table` | Terminal table output for inspect |

---

## v1.0 Scope (Open Source)

Everything described above. **Cloud storage (S3, GCS, Azure) is deferred** in the current build — cloud paths return an explicit error. Cloud support will be re-enabled in a follow-up once the Polars engine is proven in local-file deployments.

---

## Licensing Model

| | Open Source | Pro | Enterprise |
|---|---|---|---|
| **Price** | Free | ~$30/user/month | Custom |
| Core query/transform | Yes | Yes | Yes |
| Profile/inspect/fingerprint | Yes | Yes | Yes |
| Lineage, masking, pipe mode | Yes | Yes | Yes |
| Cloud storage (S3/GCS/Azure) | Deferred | Deferred | Deferred |
| Config files, manifests | Yes | Yes | Yes |
| Data quality assertions | - | Yes | Yes |
| Incremental processing | - | Yes | Yes |
| Schema drift detection | - | Yes | Yes |
| Database sinks | - | Yes | Yes |
| Output partitioning | - | Yes | Yes |
| Encrypted output | - | - | Yes |
| Pipeline DAGs | - | - | Yes |
| Audit log / run history | - | - | Yes |
| Webhooks / notifications | - | - | Yes |
| Parallel processing | - | - | Yes |

The open-source version is generous enough that individual data engineers love it and advocate
for it at work. Pro catches teams that need production-grade features. Enterprise catches orgs
with compliance/governance requirements.

---

## Pro Features

### Data Quality Assertions (`--assertions`)

A rules engine beyond `--expect-at-least`. Define column-level and table-level assertions in YAML:

```yaml
# assertions.yaml
assertions:
  - column: email
    rule: not_null

  - column: amount
    rule: between
    min: 0
    max: 1000000

  - column: country_code
    rule: in_set
    values: [US, GB, DE, FR, JP]

  - column: created_at
    rule: freshness
    max_age: "48h"

  - column: order_id
    rule: unique

  - rule: row_count
    min: 1000
    max: 500000

  - column: customer_id
    rule: referential_integrity
    ref_table: customers
    ref_column: id
```

Usage:
```bash
dtoo query --glob "data/**/*.parquet" --assertions quality_rules.yaml --assertion-report report.json
```

**Assertion report** includes: pass/fail per rule, failure counts, sample violations (up to N rows),
overall pass/fail status. Exit code 4 on assertion failure.

**Supported rules:**
| Rule | Description |
|------|-------------|
| `not_null` | No NULL values allowed |
| `unique` | All values must be unique |
| `between` | Numeric values within min/max range |
| `in_set` | Values must be in allowed set |
| `not_in_set` | Values must not be in disallowed set |
| `matches` | Values must match regex pattern |
| `freshness` | Max age of timestamp values relative to now |
| `row_count` | Table-level min/max row count |
| `referential_integrity` | All values exist in a reference table column |
| `custom_sql` | User-provided SQL returning violation rows |

Implementation: each rule translates to a query (via Polars `SQLContext`) against the accumulated result. Runs after post-sql but before output. `custom_sql` allows arbitrary validation:

```yaml
  - rule: custom_sql
    name: "no_duplicate_orders_per_day"
    sql: |
      SELECT order_id, order_date, COUNT(*) as cnt
      FROM _ GROUP BY order_id, order_date HAVING cnt > 1
```

### Incremental Processing (`--checkpoint`)

Track which files have been processed via a persistent checkpoint file. Only process
new/changed files on subsequent runs:

```bash
dtoo query --glob "data/**/*.parquet" --checkpoint .dtoo_checkpoint.yaml --where "..."
# First run: processes all 1000 files
# Second run: processes only 12 new files since last run
```

Checkpoint file (YAML):
```yaml
# .dtoo_checkpoint.yaml (auto-managed by dtoo)
last_run: "2024-03-15T14:30:00Z"
batch_hash: "a1b2c3d4..."
files:
  "data/2024-01/sales.parquet":
    modified: "2024-01-15T10:00:00Z"
    size: 1048576
    content_hash: "sha256:abc123..."
    last_processed: "2024-03-15T14:30:00Z"
  "data/2024-02/sales.parquet":
    modified: "2024-02-15T10:00:00Z"
    size: 2097152
    content_hash: "sha256:def456..."
    last_processed: "2024-03-15T14:30:00Z"
```

**Change detection** (in order of cost):
1. File path exists in checkpoint? If not — new file, process it.
2. Modification time changed? If not — skip.
3. File size changed? If not — skip.
4. Content hash changed? If not — skip. (Only computed when mtime/size match but
   `--checkpoint-verify` is set for paranoid mode.)

**Modes:**
- `--checkpoint-mode append`: New/changed file results are appended to existing output.
- `--checkpoint-mode replace`: Full output is rewritten from all checkpoint + new data.
  (Requires re-reading previously processed files, but uses checkpoint to skip unchanged ones.)

**Reset:** `--checkpoint-reset` clears the checkpoint and reprocesses everything.

### Schema Drift Detection (`--schema-check`)

Compare current run's detected schema against a baseline and report changes:

```bash
# Establish baseline
dtoo query --glob "data/**/*.csv" --schema-check baseline.yaml --schema-check-init

# Subsequent runs detect drift
dtoo query --glob "data/**/*.csv" --schema-check baseline.yaml --schema-check-mode warn|fail
```

**Detects:**
| Change | Example | Severity |
|--------|---------|----------|
| New column | `phone_number` appeared | Warning |
| Removed column | `fax_number` gone | Error |
| Type change | `amount` was INTEGER, now VARCHAR | Error |
| Nullability change | `email` was NOT NULL, now nullable | Warning |
| Column order change | Columns reordered | Info |

**Drift report** (JSON):
```json
{
  "status": "drift_detected",
  "baseline_file": "baseline.yaml",
  "changes": [
    {
      "type": "new_column",
      "column": "phone_number",
      "detected_type": "VARCHAR",
      "severity": "warning",
      "first_seen_in": "data/2024-03/contacts.csv"
    },
    {
      "type": "type_change",
      "column": "amount",
      "baseline_type": "INTEGER",
      "detected_type": "VARCHAR",
      "severity": "error",
      "first_seen_in": "data/2024-03/sales.csv"
    }
  ]
}
```

`--schema-check-mode warn`: Log drift to stderr, continue processing.
`--schema-check-mode fail`: Exit with code 5 on any error-severity drift.

### Database Sinks (`--sink`)

Write output directly to databases instead of files:

```bash
# Append to existing table
dtoo query --glob "data/**/*.parquet" \
  --sink "postgres://user:pass@host/db?table=results" \
  --sink-mode append

# Replace table contents
dtoo query --glob "data/**/*.csv" \
  --sink "postgres://host/db?table=staging" \
  --sink-mode replace

# Upsert (idempotent loads)
dtoo query --glob "data/**/*.parquet" \
  --sink "postgres://host/db?table=dim_customers" \
  --sink-mode upsert \
  --sink-key customer_id
```

**Supported databases:**
| Database | Connection String |
|----------|------------------|
| PostgreSQL | `postgres://...` |
| MySQL | `mysql://...` |
| SQLite | `sqlite:///path/to/db` |
| Snowflake | `snowflake://account/db/schema?table=t` |
| BigQuery | `bigquery://project/dataset?table=t` |
| Redshift | `redshift://...` (Postgres wire protocol) |

**Sink modes:**
- `append`: INSERT INTO target table. Schema must be compatible.
- `replace`: DROP + CREATE + INSERT (or TRUNCATE + INSERT with `--sink-preserve-table`).
- `upsert`: INSERT ... ON CONFLICT (key) DO UPDATE. Requires `--sink-key`.

**Credentials:** Connection strings can reference environment variables: `postgres://$PG_USER:$PG_PASS@host/db`.

Sink output can be combined with `--output` — write to both file and database in one run.

### Output Partitioning (`--partition-by`)

Write output split by column values (Hive-style partitioning):

```bash
dtoo query --glob "data/**/*.csv" \
  --output "output/" \
  --partition-by region,year \
  --output-format parquet
```

Produces:
```
output/region=US/year=2024/part-0.parquet
output/region=US/year=2025/part-0.parquet
output/region=GB/year=2024/part-0.parquet
...
```

Output is split by writing each partition subset to its own path.

Partition columns are removed from the data by default (they're encoded in the path).
Use `--partition-keep-columns` to retain them in the data as well.

---

## Enterprise Features

### Encrypted Output (`--encrypt`)

AES-256-GCM encryption for output files:

```bash
# File-based key
dtoo query --glob "data/**/*.csv" \
  --output results.parquet.enc \
  --encrypt \
  --encrypt-key-file key.pem

# AWS KMS
dtoo query --glob "data/**/*.csv" \
  --output results.parquet.enc \
  --encrypt \
  --kms "aws:arn:aws:kms:us-east-1:123456:key/abc-123"

# GCP KMS
dtoo query --glob "data/**/*.csv" \
  --output results.parquet.enc \
  --encrypt \
  --kms "gcp:projects/my-project/locations/global/keyRings/my-ring/cryptoKeys/my-key"

# Azure Key Vault
dtoo query --glob "data/**/*.csv" \
  --output results.parquet.enc \
  --encrypt \
  --kms "azure:https://my-vault.vault.azure.net/keys/my-key"
```

**Envelope encryption:** A random data encryption key (DEK) encrypts the file. The DEK is
encrypted by the KMS key (KEK) and stored in the file header / sidecar. This is the standard
pattern used by AWS S3 SSE, GCP CMEK, etc.

**Decryption:**
```bash
dtoo decrypt results.parquet.enc --output results.parquet --encrypt-key-file key.pem
```

The manifest records encryption metadata (algorithm, KMS key ARN, encrypted DEK hash) for
audit purposes.

### Pipeline DAGs (`dtoo pipeline`)

Multi-step pipelines with dependency ordering:

```yaml
# pipeline.yaml
name: daily_sales_pipeline
description: Extract, enrich, and validate daily sales data

steps:
  - name: extract_sales
    glob: "raw/sales/**/*.csv"
    filter_sql: "SELECT * FROM _ WHERE amount > 0"
    output: staging/sales.parquet
    on_error: fail

  - name: extract_returns
    glob: "raw/returns/**/*.csv"
    output: staging/returns.parquet
    on_error: fail

  - name: enrich
    depends_on: [extract_sales, extract_returns]
    glob: "staging/*.parquet"
    ref:
      regions: ref/regions.parquet
    post_sql: |
      SELECT s.*, r.region_name
      FROM _ s LEFT JOIN regions r ON s.region_id = r.id
    output: output/enriched.parquet
    lineage: all

  - name: validate
    depends_on: [enrich]
    glob: "output/enriched.parquet"
    assertions: quality_rules.yaml
    profile:
      path: output/profile.html
      format: html
    expect_at_least: 1000

  - name: publish
    depends_on: [validate]
    glob: "output/enriched.parquet"
    sink: "postgres://$PG_HOST/warehouse?table=daily_sales"
    sink_mode: append
    fingerprint: true
    manifest: output/manifest.yaml
```

Usage:
```bash
# Run full pipeline
dtoo pipeline run pipeline.yaml

# Run from a specific step (re-run after fixing validation)
dtoo pipeline run pipeline.yaml --from validate

# Dry run
dtoo pipeline run pipeline.yaml --dry-run

# Visualise dependency graph
dtoo pipeline graph pipeline.yaml
```

**Execution:**
- Steps with no dependencies or whose dependencies are met run in parallel.
- Step failure stops dependent steps. Independent branches continue (unless `--pipeline-fail-fast`).
- Each step is effectively a `dtoo query` invocation with its own config.
- Step outputs become inputs for dependent steps.

**Pipeline report** (JSON/YAML): status per step, timing, row counts, assertion results.

### Audit Log / Run History (`--audit-log`)

Persistent, append-only log of all dtoo runs stored in a SQLite database:

```bash
# Enable audit logging (appends to SQLite DB)
dtoo query --glob "data/**/*.csv" --audit-log .dtoo_audit.db --where "..."

# Search audit history
dtoo audit search --log .dtoo_audit.db --after "2024-03-01" --status failed
dtoo audit search --log .dtoo_audit.db --glob "*sales*"

# Compare two runs
dtoo audit diff --log .dtoo_audit.db --run-id abc123 --run-id def456

# Export audit log
dtoo audit export --log .dtoo_audit.db --format csv --output audit_report.csv
```

**Audit record schema:**
```sql
CREATE TABLE audit_runs (
    run_id          TEXT PRIMARY KEY,   -- UUID
    batch_id        TEXT,               -- Matches lineage batch_id
    batch_hash      TEXT,               -- Deterministic run hash
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    duration_ms     INTEGER,
    status          TEXT,               -- success | failed | partial | validation_failed
    exit_code       INTEGER,
    command_line    TEXT,               -- Full CLI invocation (secrets redacted)
    config_file     TEXT,               -- Config YAML content if used
    glob_pattern    TEXT,
    files_total     INTEGER,
    files_processed INTEGER,
    files_skipped   INTEGER,
    rows_input      INTEGER,
    rows_output     INTEGER,
    output_path     TEXT,
    output_format   TEXT,
    fingerprint     TEXT,
    assertion_pass  INTEGER,
    assertion_fail  INTEGER,
    schema_drift    BOOLEAN,
    error_message   TEXT,
    username        TEXT,               -- From $USER / whoami
    hostname        TEXT
);

CREATE TABLE audit_file_details (
    run_id          TEXT REFERENCES audit_runs(run_id),
    file_path       TEXT,
    rows_matched    INTEGER,
    status          TEXT,               -- ok | skipped | error
    error_message   TEXT
);
```

`dtoo audit diff` compares: row counts, schema changes, file list changes, assertion result
changes between two runs. Useful for investigating "what changed between yesterday's run and
today's?".

### Webhooks / Notifications (`--notify`)

Post-run notifications for pipeline monitoring:

```bash
# Slack notification on failure
dtoo query --glob "data/**/*.csv" \
  --notify "slack:#data-alerts" \
  --notify-on fail,validation

# Generic webhook
dtoo query --glob "data/**/*.csv" \
  --notify "https://hooks.example.com/dtoo" \
  --notify-on always

# PagerDuty on assertion failure
dtoo query --glob "data/**/*.csv" \
  --assertions rules.yaml \
  --notify "pagerduty:service-key-123" \
  --notify-on assertion_fail
```

**Notification payload** (JSON):
```json
{
  "tool": "dtoo",
  "version": "1.0.0",
  "run_id": "550e8400-...",
  "batch_hash": "a1b2c3...",
  "status": "validation_failed",
  "exit_code": 2,
  "summary": "Expected at least 1000 records, got 42",
  "rows_output": 42,
  "files_processed": 95,
  "files_total": 100,
  "duration_seconds": 47.2,
  "timestamp": "2024-03-15T14:30:47Z",
  "assertions": {
    "passed": 8,
    "failed": 2,
    "failures": ["email:not_null (12 violations)", "amount:between (3 violations)"]
  }
}
```

**Trigger conditions:**
| Condition | Fires when |
|-----------|-----------|
| `always` | Every run |
| `success` | Exit code 0 |
| `fail` | Any non-zero exit code |
| `validation` | expect-at-least not met (exit code 2) |
| `assertion_fail` | Assertion failures (exit code 4) |
| `schema_drift` | Schema drift detected (exit code 5) |
| `partial` | Files skipped (exit code 3) |

**Supported channels:**
| Channel | Format |
|---------|--------|
| Slack | `slack:#channel` or `slack:webhook-url` |
| Email | `email:addr@example.com` (via SMTP config in config file) |
| PagerDuty | `pagerduty:service-key` |
| Generic webhook | `https://...` (POST JSON payload) |

Config file support for notifications:
```yaml
notify:
  - channel: "slack:#data-alerts"
    on: [fail, validation, assertion_fail]
  - channel: "email:oncall@company.com"
    on: [fail]
  - channel: "https://monitoring.internal/webhooks/dtoo"
    on: [always]
```

### Parallel File Processing (`--parallel`)

Process files across multiple threads for large file sets:

```bash
dtoo query --glob "data/**/*.parquet" --parallel 8 --where "amount > 100"
```

**Implementation:** Spawn N worker threads, each scanning files from a shared queue into thread-local `LazyFrame`s. After all files are processed, merge per-worker frames into the final accumulated result via `concat_lf_diagonal`.

**Considerations:**
- Default: sequential (1 thread). Enterprise unlocks `--parallel N`.
- Memory usage scales with N (each connection has its own buffer pool).
- `--parallel auto` uses number of CPU cores.
- Progress reporting (`--verbose`) remains coherent — workers report to a shared progress bar.
- File order is not guaranteed when parallel. Use `--post-sql` with ORDER BY if order matters.

---

## Exit Codes (Complete)

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error (IO, SQL syntax, invalid args) |
| 2 | Validation failure (expect-at-least not met) |
| 3 | Partial failure (some files skipped in skip mode) |
| 4 | Assertion failure (Pro/Enterprise) |
| 5 | Schema drift detected in fail mode (Pro/Enterprise) |

---

## Future Considerations (v2+)

- **Watch mode**: Re-run on file changes
- **Plugin system**: Custom format readers
- **SQLite input**: Read from SQLite files (would require a pure-Rust SQLite reader crate)
- **Avro input**: Via Rust avro crate as preprocessor
- **Web UI**: Browser-based dashboard for audit logs, pipeline status, and profiling reports
- **dtoo server**: Long-running daemon mode for API-driven pipeline execution
