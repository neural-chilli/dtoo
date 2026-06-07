<img src="docs/logo.png" alt="dtoo" height="96" align="left" hspace="16" />

<h3>dtoo</h3>

[![CI](https://img.shields.io/github/actions/workflow/status/neural-chilli/dtoo/ci.yml?label=CI&logo=github)](https://github.com/neural-chilli/dtoo/actions)

<br clear="left"/>

A fast, ergonomic CLI tool for data engineers. Query across file trees using SQL, profile data, add lineage, and more.

Built in Rust on top of DuckDB.

## Quick Start

### 1. Build

```bash
cargo build --release
```

### 2. Run your first query

```bash
./target/release/dtoo query testdata/trips.parquet --output out/trips.csv
```

### 3. Query recursively across a directory tree

```bash
./target/release/dtoo query \
  --glob "testdata/**/*" \
  --on-error skip \
  --post-sql "SELECT passenger_count, COUNT(*) AS trips FROM _ GROUP BY 1 ORDER BY 2 DESC" \
  --output out/agg.csv
```

### 4. Inspect and profile

```bash
./target/release/dtoo inspect testdata/trips.parquet --rows 10
./target/release/dtoo profile testdata/trips.parquet --format html --output out/profile.html
```

### 5. Get command help

```bash
./target/release/dtoo --help
./target/release/dtoo query --help
```

For full usage, options, and recipes, see [USER_GUIDE.md](docs/USER_GUIDE.md).

See [DESIGN.md](docs/DESIGN.md) for the full specification.
