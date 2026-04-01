import type { WizardState } from '@/types';

export function runStatusVariant(
  status: string
): 'default' | 'secondary' | 'destructive' | 'outline' | 'success' | 'muted' {
  const s = status.toLowerCase();
  if (s === 'succeeded') return 'success';
  if (s === 'failed' || s === 'error') return 'destructive';
  if (s === 'running') return 'default';
  return 'muted';
}

export function tableStatusVariant(
  status: string
): 'default' | 'secondary' | 'destructive' | 'outline' | 'success' | 'muted' {
  const s = status.toLowerCase();
  if (s === 'applied') return 'success';
  if (s === 'failed') return 'destructive';
  if (s === 'snapshot') return 'default';
  return 'muted';
}

export function formatRowProgress(rowsProcessed: number, rowsTotal: number | null): string {
  if (rowsTotal != null) {
    return `${rowsProcessed.toLocaleString()} / ${rowsTotal.toLocaleString()} rows`;
  }
  return `${rowsProcessed.toLocaleString()} rows`;
}

export function formatDuration(startedAt: string, finishedAt: string | null): string {
  if (!finishedAt) return '—';
  const ms = new Date(finishedAt).getTime() - new Date(startedAt).getTime();
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${Math.round(ms / 1000)}s`;
  return `${Math.round(ms / 60_000)}m`;
}

export function formatTimestamp(ts: string): string {
  return new Date(ts).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function generateWizardYaml(w: WizardState): string {
  const tables = w.source.tables
    .split(/[\n,]/)
    .map((t) => t.trim())
    .filter(Boolean);
  const tableLines = tables.map((t) => `      - ${t}`).join('\n');
  return `version: v1alpha1
pipeline:
  name: ${w.pipelineName || 'my-pipeline'}
  mode: snapshot
source:
  kind: postgres
  connection:
    host: ${w.source.host}
    port: ${w.source.port}
    database: ${w.source.database}
    username: ${w.source.username}
    passwordRef: env:${w.source.passwordRef}
  capture:
    tables:
${tableLines}
    snapshot:
      mode: incremental
      chunkSize: 50000
destination:
  kind: postgres
  connection:
    host: ${w.destination.host}
    port: ${w.destination.port}
    database: ${w.destination.database}
    username: ${w.destination.username}
    passwordRef: env:${w.destination.passwordRef}
  write:
    mode: append
    batchSize: 10000
runtime:
  parallelism:
    tables: 1
  checkpointing:
    intervalSeconds: 30
`;
}
