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
  if (s === 'applied' || s === 'snapshot_complete') return 'success';
  if (s === 'failed' || s === 'error') return 'destructive';
  if (s === 'snapshot' || s === 'snapshot_running' || s === 'running') return 'default';
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

// YAML single-quoted scalar: safe for any string — only ' needs escaping (doubled).
function yamlQuote(s: string): string {
  return `'${s.replace(/'/g, "''")}'`;
}

export function generateWizardYaml(w: WizardState): string {
  const pipelineName = w.pipelineName.trim() || 'my-pipeline';
  const tables = w.source.tables
    .split(/[\n,]/)
    .map((t) => t.trim())
    .filter(Boolean);
  const tableLines = tables.map((t) => `      - ${yamlQuote(t)}`).join('\n');

  const { mode, cursorField, chunkSize } = w.snapshot;
  const chunkSizeNum = parseInt(chunkSize, 10);

  let snapshotBlock = '';
  if (mode !== 'none') {
    const lines = [`      mode: ${mode}`];
    if (mode === 'incremental' && cursorField.trim()) {
      lines.push(`      cursorField: ${yamlQuote(cursorField.trim())}`);
    }
    if (!isNaN(chunkSizeNum) && chunkSizeNum > 0) {
      lines.push(`      chunkSize: ${chunkSizeNum}`);
    }
    snapshotBlock = `    snapshot:\n${lines.join('\n')}\n`;
  }

  const sourceConn =
    w.source.useSavedConnection && w.source.connectionRef
      ? `  connectionRef: ${yamlQuote(w.source.connectionRef)}`
      : `  connection:
    host: ${yamlQuote(w.source.host)}
    port: ${w.source.port}
    database: ${yamlQuote(w.source.database)}
    username: ${yamlQuote(w.source.username)}
    passwordRef: env:${w.source.passwordRef}`;

  const destConn =
    w.destination.useSavedConnection && w.destination.connectionRef
      ? `  connectionRef: ${yamlQuote(w.destination.connectionRef)}`
      : `  connection:
    host: ${yamlQuote(w.destination.host)}
    port: ${w.destination.port}
    database: ${yamlQuote(w.destination.database)}
    username: ${yamlQuote(w.destination.username)}
    passwordRef: env:${w.destination.passwordRef}`;

  return `version: v1alpha1
pipeline:
  name: ${yamlQuote(pipelineName)}
  mode: snapshot
  schedule: manual
source:
  kind: postgres
${sourceConn}
  capture:
    tables:
${tableLines}
${snapshotBlock}destination:
  kind: postgres
${destConn}
  staging:
    kind: local
    bucket: astra-staging
    prefix: ${yamlQuote(`${pipelineName}/`)}
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
