# dtoo — task runner (https://github.com/casey/just)
# Run `just` with no args to list everything.

# Show all available recipes.
default:
    @just --list

# --- Build & run -----------------------------------------------------------

# Compile the debug binary.
build:
    cargo build

# Compile the optimised release binary.
release:
    cargo build --release

# Run dtoo, forwarding any args:  just run query data.csv --where "amount > 100"
run *args:
    cargo run -- {{args}}

# Run the release binary, forwarding any args.
run-release *args:
    cargo run --release -- {{args}}

# Install dtoo onto your PATH (~/.cargo/bin) from source.
install:
    cargo install --path .

# --- Quality gates ---------------------------------------------------------

# Run the test suite (optionally filtered):  just test schema
test *args:
    cargo test {{args}}

# Format the code in place.
fmt:
    cargo fmt

# Verify formatting without changing files (CI mode).
fmt-check:
    cargo fmt --check

# Lint with clippy, failing on any warning.
lint:
    cargo clippy --all-targets -- -D warnings

# Auto-fix what cargo/clippy can, then format.
fix:
    cargo clippy --fix --allow-dirty --allow-staged --all-targets
    cargo fmt

# The full pre-push gate: formatting, lints, and tests must all pass.
check: fmt-check lint test

# Alias for `check` — run before pushing.
ci: check

# --- Docs & housekeeping ---------------------------------------------------

# Build and open the API docs in a browser.
doc:
    cargo doc --no-deps --open

# Update dependency versions within semver constraints.
update:
    cargo update

# Remove build artifacts.
clean:
    cargo clean

# --- Optional extras (each needs a one-time `cargo install <tool>`) ---------

# Re-run tests on file change. Needs: cargo install cargo-watch
watch:
    cargo watch -x test

# Audit dependencies for known vulnerabilities. Needs: cargo install cargo-audit
audit:
    cargo audit

# Line coverage report. Needs: cargo install cargo-llvm-cov
coverage:
    cargo llvm-cov --all-features

# --- Demo ------------------------------------------------------------------

# Exercise every subcommand end-to-end against a throwaway sample file.
demo:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    printf 'id,region,amount\n1,EMEA,100\n2,APAC,250\n3,EMEA,50\n' > "$tmp/sales.csv"
    echo "── inspect ─────────────────────────────"
    cargo run -q -- inspect "$tmp/sales.csv"
    echo "── query (amount > 80, sum by region) ──"
    cargo run -q -- query "$tmp/sales.csv" --where "amount > 80" \
        --post-sql "SELECT region, SUM(amount) AS total FROM _ GROUP BY region"
    echo "── profile ─────────────────────────────"
    cargo run -q -- profile "$tmp/sales.csv"
    echo "── fingerprint ─────────────────────────"
    cargo run -q -- fingerprint "$tmp/sales.csv"
    echo "── convert csv → parquet ───────────────"
    cargo run -q -- convert "$tmp/sales.csv" --output "$tmp/sales.parquet" --output-format parquet
    echo "wrote $tmp/sales.parquet"
