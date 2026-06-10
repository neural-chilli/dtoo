# Synthetic Data Generation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this spec.

**Goal:** Generate realistic, privacy-preserving synthetic data from statistical profiles of real sources — never from the real rows themselves. Multi-table generation preserves referential integrity, join fan-out, and numeric correlation structure, so synthetic datasets behave like the real thing under analytical queries.

**Architecture:** Two halves. (1) An extended profile detail level (`--detail synth`) that enriches the existing JSON profile with histograms, top-K value frequencies, unique ratios, and a Spearman correlation matrix. (2) A new `dtoo synth` subcommand that reads those profiles — plus an optional multi-table YAML spec declaring keys, foreign keys, and rules — and generates data through a deterministic seeded engine, writing output via the existing output writer.

**Tech Stack:** Rust, Polars engine (spec 34), `rand` + `rand_chacha` (new dependencies: seeded, reproducible RNG)

**Tier:** Open Source

---

## Motivation

The profile JSON is the only artifact that crosses a trust boundary: profile real data inside a secure environment (local files; later GCS/BigQuery), carry out a file containing only aggregates, and generate synthetic data anywhere from it. Use cases, in priority order:

1. **Test fixtures** — analysts and pipelines need data with the right shape, types, and plausible values.
2. **Realistic analytics / load testing** — downstream queries and joins must behave like the real thing: FK integrity holds, join result sizes are realistic, distributions and correlations match.

## CLI Usage

### Extended profiling

```
dtoo profile data.parquet --detail synth --output profiles/data.json
dtoo query --glob "data/**/*.parquet" --profile profiles/data.json --profile-detail synth
```

- `--detail` / `--profile-detail`: `standard` (default) | `synth`
- `--top-k <N>`: number of top values captured at synth detail (default: 1000)
- Synth detail applies to JSON format only; `--detail synth` with csv/html output is exit 1 with a clear error.

### Generation

```
# Multi-table, spec-driven
dtoo synth --spec synth.yaml [--seed <N>] [--dry-run] [--verbose]

# Single-table quick mode (no spec file)
dtoo synth --profile profiles/customers.json --rows 100000 \
  --output out.parquet [--output-format parquet] [--seed <N>] [--verbose]
```

- `--spec` and `--profile` are mutually exclusive; exactly one is required.
- `--seed`: u64; overrides the spec's `seed`; default 0. Same spec + same seed → byte-identical output.
- `--dry-run`: print the plan to stderr (generation order, row counts, rules, outputs) without generating.
- Single-table mode supports `--output`, `--output-format` (csv | parquet | ndjson), `--delimiter`, `--compress`, `--no-header` with the same semantics as `dtoo query`. Default output: stdout, csv.
- `--verbose` logs per-table progress to stderr in the established `[mm:ss.t]` style.

## Extended Profile (`--detail synth`)

Additive only: every existing `ColumnProfile` field is unchanged; standard-detail output is byte-for-byte identical to today's. Synth detail adds:

### Per column

| Field | Type | Applies to | Description |
|-------|------|-----------|-------------|
| `histogram` | array of `{lo, hi, count}` | Numeric, Date, Datetime, Time | 20 quantile-spaced buckets (fewer if distinct_count < 20). Bucket edges from `quantile_reduce` at evenly spaced probabilities; counts from a single pass. |
| `top_values` | array of `{value, count}` | All | Top-K values by frequency (K from `--top-k`, default 1000). Superset of `top_5_values`, which is retained. |
| `unique_ratio` | float | All | `distinct_count / count` (0 when count = 0). Used for key detection and fan-out derivation. |

### Per report

| Field | Type | Description |
|-------|------|-------------|
| `detail` | string | `"standard"` (absent today; absent = standard) or `"synth"` |
| `correlation_matrix` | `{columns: [names], data: [[f64]]}` | Spearman pairwise correlation over numeric/Date/Datetime columns (dates ranked via their physical representation). Symmetric, 1.0 diagonal. NULL pairs excluded pairwise. Omitted when fewer than 2 eligible columns. |

Spearman (rank) correlation is used rather than Pearson because generation maps through empirical CDFs — rank correlation is exactly what the copula preserves.

## Synth Spec (YAML)

```yaml
# synth.yaml
seed: 42                          # optional, default 0; --seed overrides
tables:
  customers:
    profile: profiles/customers.json
    rows: 10000
    keys: [customer_id]           # generated unique, formatted per observed pattern
    output: synth/customers.parquet
    output_format: parquet        # optional; inferred from extension when omitted
  orders:
    profile: profiles/orders.json
    rows: 250000
    foreign_keys:
      - column: customer_id
        references: customers.customer_id
        fan_out: from_profile     # default; or {distribution: uniform}
    rules:
      - derive: "total = quantity * unit_price"
      - constraint: "ship_date >= order_date"
    output: synth/orders.csv
```

- Paths are relative to the spec file's directory.
- `references` must name a `tables` entry and one of its `keys` columns; anything else is exit 1 with a specific error.
- Generation order is a topological sort of FK dependencies. A cycle is exit 1, naming the cycle.
- Per-table `output` is required in spec mode (multi-table stdout is ambiguous).

## Generation Engine

New `src/synth/` module: `spec.rs` (parse/validate), `engine.rs` (orchestration, topo sort), `samplers.rs` (per-dtype column samplers), `copula.rs`, `keys.rs`, `rules.rs`.

### Determinism

One `ChaCha8Rng` seeded from the run seed. Each (table, column) derives its own stream seeded by `hash(seed, table_name, column_name)` so adding a column never perturbs other columns' output. Reproducibility (same spec + seed → identical bytes) is a tested guarantee.

### Per-dtype samplers

Column dtypes come from the profile's recorded `type`. Sampling strategy:

| Profile shape | Strategy |
|---------------|----------|
| Numeric/temporal with `histogram` | Pick bucket weighted by count; piecewise-linear within `[lo, hi]`. Integer dtypes round; Decimal respects scale. |
| Numeric/temporal, standard profile only | Piecewise-linear empirical CDF through min/p25/median/p75/max (temporal: min/max uniform). Warn on stderr that fidelity is reduced; recommend re-profiling with `--detail synth`. |
| Low cardinality (`top_values` covers ≥ 99.5% of rows) | Weighted sampling from `top_values` only. |
| Higher cardinality strings | Weighted sampling from `top_values` for the covered mass; remaining mass generated from `pattern_sample` patterns (weighted by pattern frequency). |
| Boolean | Weighted by observed frequencies. |

- **Nulls:** injected per column at `null_percentage`, via the column's own RNG stream.
- **Pattern generation:** `pattern_sample` uses dtoo's own vocabulary (`a` = letter, `d` = digit, `N` = digit run, punctuation literal — spec 10). The generator covers exactly this vocabulary: `a` → random lowercase letter, `d` → random digit, `N` → digit run with length sampled between observed `min_length`/`max_length` bounds. No general regex-generation dependency.

### Keys

For each column listed in `keys`: values are a deterministic function of (table, column, row index), guaranteed unique by construction, formatted to match the observed data:

- Integer dtype → sequential from observed `min`.
- String matching a digit pattern → zero-padded counter at observed width.
- String matching a UUID-shaped pattern → UUIDv4 from the column's seeded RNG stream.
- Other string patterns → pattern-generated prefix + counter suffix, respecting length bounds.

A `keys` column whose profile shows `unique_ratio < 1.0` produces a stderr warning (the real column isn't unique; synth will make it unique).

### Foreign keys and fan-out

Child FK values are drawn from the parent's generated key set (parent is always generated first; keys for FK sampling are regenerated from the deterministic key function, not retained in memory beyond what's needed).

`fan_out: from_profile` derives the assignment distribution from the **child** profile's FK column: in the real data, mean rows per distinct FK value = `1 / unique_ratio`, and the skew of that distribution is shaped by the column's `top_values` frequencies (the top-K frequency histogram is rank-matched onto generated parent keys). The number of distinct parent keys referenced ≈ `child.rows × unique_ratio`, capped at the parent's row count — if the uncapped value exceeds the parent's row count, warn on stderr that fan-out will be denser than profiled. Result: "orders per customer" has realistic mean and skew, so join result sizes behave like real data. `fan_out: {distribution: uniform}` assigns FKs uniformly at random.

Integrity guarantee (tested): every generated FK value exists in the parent's generated key column.

### Copula (correlation preservation)

When the profile carries a `correlation_matrix`, numeric/temporal columns are sampled jointly:

1. Take the submatrix for columns being generated (excluding keys/FKs — their values are structurally determined).
2. Repair to positive semi-definite: eigen-decompose, clamp eigenvalues at ε = 1e-10, reconstruct, re-normalise diagonal to 1.0. (Sampled correlation matrices are routinely slightly non-PSD; this is the standard fix.)
3. Cholesky-factor; draw correlated standard normals; map through Φ to uniforms; map each uniform through that column's empirical CDF (histogram).

Marginals are exactly as profiled; Spearman correlation structure is preserved on top. Columns absent from the matrix, or profiles without one (standard detail), fall back to independent sampling. Eigen/Cholesky is implemented in-module over `Vec<f64>` (matrices are k×k for k profiled numeric columns — small); no linear-algebra crate is added.

### Rules

Applied after sampling, before output, using the existing Polars `SQLContext` with the generated frame registered as `_` (the established magic-table convention):

- **`derive: "col = expr"`** — runs `SELECT *, (expr) AS col FROM _`. The column is replaced if it exists (profiled but recomputed) or appended. Derives run in spec order.
- **`constraint: "expr"`** — enforced by oversample-and-filter: generate a batch (target rows × 1.5), keep rows where all constraints hold, repeat until the target count is reached, then truncate. Deterministic under the seeded streams. If cumulative acceptance falls below 10% after 5 rounds, exit 1 naming the failing constraint — never loop indefinitely.

Execution order is fixed: sample → constraints (oversample/filter) → derives (in spec order) → output. Consequently a `constraint` may only reference sampled columns; one that references a derived column fails with a column-not-found SQL error. Derived columns are not constraint-checked in v1.

SQL errors in rules surface as exit 1 with the rule text and the underlying Polars error.

## Memory Model

In-memory generation: each table is materialised as a single DataFrame and written once via the existing `output_writer`. Practical ceiling ~10M rows per table (documented). Chunked/streaming generation is out of scope (see below) but the seeded design — row *i* independent of row *i−1* — deliberately keeps it cheap to add.

## Error Cases

| Case | Behaviour |
|------|-----------|
| Spec unreadable / invalid YAML / unknown field | Exit 1, message names file and field |
| Profile missing, unreadable, or not a dtoo profile JSON | Exit 1, names the path |
| Standard-detail profile supplied | Generate with reduced fidelity; warn per affected column on stderr |
| `references` to unknown table/column | Exit 1, names the reference |
| FK dependency cycle | Exit 1, names the cycle |
| Constraint acceptance below floor | Exit 1, names the constraint |
| Rule SQL error | Exit 1, includes rule text and Polars error |
| Output path not writable | Exit 1 |
| `rows: 0` | Valid: empty output with correct schema |

All errors are `thiserror` variants on `DtooError`. No panics on bad input.

## Testing Requirements

- **Samplers (unit):** with a fixed seed, generated marginals fall within tolerance of profiled stats (null %, histogram bucket occupancy, top-value frequencies, min/max bounds); integer/decimal dtype fidelity.
- **Keys (unit):** uniqueness across full table size; format matches observed pattern cases (int, padded, UUID, prefixed).
- **Copula (unit):** PSD repair on a deliberately non-PSD matrix; generated Spearman correlation within tolerance of target; fallback to independent when matrix absent.
- **Spec (unit):** parse/validate happy path and every error case above; topo sort incl. cycle detection.
- **Integration (CLI, temp dirs only):** two-table spec → every child FK exists in parent output; fan-out mean/skew within tolerance; constraints hold on every output row; derives computed correctly; reproducibility (same seed → byte-identical files); `--dry-run` generates nothing; single-table mode to stdout and to file; standard-profile degradation warns but succeeds.
- **Profiler (unit + integration):** standard-detail output unchanged (regression); synth detail adds expected fields; correlation matrix correctness on a known dataset; `--detail synth` rejected for csv/html.

## Out of Scope (deferred)

- **Chunked/streaming generation** for >10M-row tables (design permits; add when needed).
- **Categorical functional-dependency detection** (`city` → `state` conditional maps).
- **Cross-profile FK inference** (value sketches) and `dtoo synth init` spec scaffolding.
- **Intra-row temporal relation detection** — ordering invariants remain declared `constraint` rules by design: the user knows which invariants are load-bearing; a profiler can only guess.
- Cloud paths for profiles/outputs (follows the engine-wide cloud deferral).
