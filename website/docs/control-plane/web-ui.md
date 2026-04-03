---
id: web-ui
title: Web UI
sidebar_position: 2
---

# Web UI

The Astra web UI is a React 18 + TypeScript + Vite application served directly by the control plane binary at the root path (`/`). No separate web server is needed.

## Access

Start the control plane and open [http://127.0.0.1:8080](http://127.0.0.1:8080).

## Features

### Pipeline Inventory

The main view lists all registered pipelines with:

- Pipeline name and ID
- Mode (snapshot / incremental / CDC)
- Schedule (manual / continuous / cron)
- Current status (active / paused / error)
- Last run timestamp and outcome

From here you can:
- Click a pipeline to view its detail page
- Trigger a manual run
- Delete a pipeline

### Run History

Each pipeline's detail page shows a chronological list of runs:

- Run ID and trigger time
- Duration
- Status (started / completed / failed)
- Link to per-table execution details

### Table Drill-Down

Clicking a run shows per-table execution records:

- Table name
- Rows captured and chunks staged
- Status per table
- Start and end timestamps

Useful for debugging partial failures where one table in a multi-table pipeline fails.

### YAML Studio

The YAML Studio allows you to:

1. Paste or type a YAML pipeline spec
2. Validate it against the `v1alpha1` schema in real-time
3. Apply it to the control plane with one click

This is equivalent to `POST /api/v1/specs/apply` but with a live editor and inline error messages.

## Development

The web app lives in `apps/web`. To run it in development mode with hot module reload:

```bash
cd apps/web
npm install
npm run dev
# Vite dev server starts at http://127.0.0.1:4173
# API calls proxy to http://127.0.0.1:8080
```

Build for production (output goes to `apps/web/dist`, embedded in the control plane binary):

```bash
npm run build
```

Type-check without emitting:

```bash
npm run lint
```

### Tech stack

| Layer | Library |
|---|---|
| Framework | React 18 |
| Language | TypeScript 5 |
| Build | Vite 6 |
| UI primitives | Radix UI |
| Styling | Tailwind CSS 3 |
| Icons | Lucide React |

### API client

`apps/web/src/api.ts` contains the typed HTTP client that calls the control-plane REST API. All API calls use the browser's native `fetch`.

### Types

`apps/web/src/types.ts` mirrors the control-plane response shapes. Keep these in sync when adding new API endpoints.
