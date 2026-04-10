---
id: contributing
title: Contributing
sidebar_position: 1
---

# Contributing

## Local setup

### Prerequisites

- **Rust** stable toolchain (install via [rustup](https://rustup.rs))
- **Node.js** 20+ and npm (for the web UI)
- **Podman** or Docker with Compose
- **Python 3.8+** (for smoke tests)

### Bootstrap

```bash
git clone https://github.com/suryachereddy/Astra.git
cd Astra

# Start local infrastructure
podman compose -f deploy/docker-compose/docker-compose.yml up -d

# Build everything
cargo build

# Run all tests
cargo test --workspace

# Build the web UI
cd apps/web && npm install && npm run build && cd ../..
```

## Development workflow

### Rust

```bash
# Build
cargo build

# Test all crates
cargo test --workspace

# Test a single crate
cargo test -p astra-yaml

# Run a specific test
cargo test test_name

# Lint
cargo clippy --workspace -- -D warnings

# Format — always run before committing
cargo fmt --all
```

:::warning
Always run `cargo fmt --all` before committing. The CI will fail if formatting is not applied.
:::

### Web UI

```bash
cd apps/web
npm install
npm run dev        # Vite dev server at http://127.0.0.1:4173
npm run build      # Production build
npm run lint       # TypeScript type check
```

### Smoke test

The end-to-end smoke test requires the local Postgres stack to be running:

```bash
podman compose -f deploy/docker-compose/docker-compose.yml up -d
python3 scripts/yaml_contract_smoke.py
```

## Project structure

See [Crate Guide →](./crate-guide.md) for a description of each crate's responsibility.

## Adding a feature

1. Find or create the relevant crate
2. Write the code
3. Write tests (unit tests in the same file, integration tests in `tests/`)
4. Run `cargo fmt --all` and `cargo clippy --workspace`
5. Update or add documentation in `website/docs/`
6. Open a PR with a description following the PR template

## Definition of done

A feature is done when:

- [ ] Code compiles without warnings (`cargo check --workspace`)
- [ ] Clippy passes (`cargo clippy --workspace -- -D warnings`)
- [ ] All tests pass (`cargo test --workspace`)
- [ ] Code is formatted (`cargo fmt --all --check`)
- [ ] New public APIs have documentation comments
- [ ] CHANGELOG.md is updated

## Testing philosophy

- Unit tests live in the same file as the code under test (`#[cfg(test)]` modules)
- Integration tests live in `tests/` within each crate or app
- The e2e smoke test (`scripts/yaml_contract_smoke.py`) validates the full pipeline end-to-end
- Don't mock the database for integration tests — the Docker Compose stack provides a real Postgres instance

## Code expectations

- No `unsafe` code (enforced via `unsafe_code = forbid` in workspace `Cargo.toml`)
- Errors use `anyhow` for application code, `thiserror` for library code
- Async code uses `tokio`
- Logging uses `tracing` (not `println!` or `eprintln!`)
- Edition 2021
