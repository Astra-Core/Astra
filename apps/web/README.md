# Astra Web App

Astra's web surface now lives in a proper Vite + React + TypeScript app instead of the earlier static shell.

## What exists

- app shell for Astra control-plane surfaces
- onboarding view stubbed into reusable React layout/components
- job status view wired to `GET /api/v1/pipelines`
- YAML studio seeded from `examples/postgres-to-warehouse.astra.yaml`
- apply action wired to `POST /api/v1/specs/apply`
- Vite proxy config so local UI work talks to the Rust control plane without weird hacks

## Local development

Run the control plane from the repo root:

```bash
cargo run -p astra-control-plane
```

Then in `apps/web/` install dependencies and start Vite:

```bash
npm install
npm run dev
```

Open <http://127.0.0.1:4173>.

## Self-hosted build

Build the frontend in `apps/web/`:

```bash
npm install
npm run build
```

Then run the control plane from the repo root:

```bash
cargo run -p astra-control-plane
```

When `apps/web/dist` exists, the Rust app serves that build at <http://127.0.0.1:8080>.

## Why this is still a foundation

This lands the real app skeleton and migrates the current surfaces without pretending the full product is done. Real routing, forms, richer job detail, and auth can layer on top of this without rewriting from scratch.
