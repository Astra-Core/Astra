# Astra Web Shell

Temporary UI scaffold for Astra's product surface.

## What exists

- onboarding shell view
- job status shell view
- YAML preview wired to the canonical example spec at `examples/postgres-to-warehouse.astra.yaml`
- pipeline list wired to the control-plane API

## Run it

From the repo root:

```bash
cargo run -p astra-control-plane
```

Then open <http://127.0.0.1:8080>.

If you only want to inspect the static files without the Rust backend, from `apps/web/` run:

```bash
python3 -m http.server 4173
```

That static mode is just for eyeballing markup/styles. The pipeline and YAML API calls will fail there unless you separately proxy the control-plane routes.

## Why this is still a shell

This is intentionally lightweight. The long-term UI direction is React + TypeScript, tracked in issue #26.
