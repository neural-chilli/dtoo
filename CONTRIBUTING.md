# Contributing to dtoo

Thanks for your interest in improving `dtoo`.

This document covers how to propose changes that are practical, reviewable, and likely to merge.

## License and Contribution Terms

This project is licensed under BSL 1.1 (see [LICENSE.md](LICENSE.md)).
By submitting a contribution (code, docs, tests, or other material), you agree your contribution is provided under the same project license terms.

## Before You Start

Read these first:
- [DESIGN.md](docs/DESIGN.md) for architecture and behavior expectations
- [CLAUDE.md](CLAUDE.md) for coding standards and PR quality bar
- Relevant spec in `specs/` for the feature/area you are changing

## Development Setup

Prerequisites:
- Rust stable toolchain

Build:

```bash
cargo build --release
```

Core checks:

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

All three should pass before opening or updating a PR.

## Scope and Quality Expectations

- One focused problem per PR
- Do not bundle unrelated changes
- Avoid speculative fixes without a concrete user-facing issue
- Keep dependencies lean; justify any new crate
- Add tests for both happy path and failure paths

## Working Style

- Use clear commit messages
- Keep diffs small and easy to review
- Prefer explicit errors and `Result`-based handling over panics
- Update docs/help text when behavior or flags change

## Pull Request Checklist

Before opening a PR:

1. Confirm the problem is real and reproducible.
2. Search for related open/closed PRs to avoid duplicates.
3. Rebase or merge latest `main` as needed.
4. Run format/lint/tests locally.
5. Fill out the PR template completely.
6. Include a clear problem statement and testing evidence.

## Testing Guidance

At minimum, include tests that cover:
- Expected success path
- Error handling path (`--on-error fail` and/or `--on-error skip` where relevant)
- Regressions for the bug you fixed

Use temporary directories/files for tests. Do not write test artifacts into repo paths.

## Documentation Contributions

Docs improvements are welcome and encouraged.
If command behavior changes, update:
- `README.md` (quick discoverability)
- `USER_GUIDE.md` (end-user details)
- `DESIGN.md` (if architecture/contract changes)

## Reporting Issues

When filing an issue, include:
- Exact command run
- Expected behavior
- Actual behavior
- Error output
- Minimal repro input if possible

## Questions

If you are unsure whether something belongs in core, open an issue or draft PR first and ask.
