import { useEffect, useMemo, useState } from 'react';

type ViewKey = 'overview' | 'onboarding' | 'jobs' | 'yaml';

type Pipeline = {
  name: string;
  source_kind: string;
  destination_kind: string;
  status: string;
  spec_version: number;
};

type PipelinesResponse = {
  pipelines: Pipeline[];
};

type PipelineRun = {
  id: string;
  pipeline_name: string;
  trigger_mode: string;
  status: string;
  worker_id: string | null;
  started_at: string;
  finished_at: string | null;
  created_at: string;
  updated_at: string;
  stats_json: Record<string, unknown> | null;
};

type PipelineRunsResponse = {
  runs: PipelineRun[];
};

type RunHistoryState = {
  loading: boolean;
  error: string | null;
  runs: PipelineRun[];
};

type ApplySpecResponse = {
  pipeline_name: string;
  spec_version: number;
  content_hash: string;
  message: string;
};

type TableExecution = {
  id: string;
  stream_name: string;
  status: string;
  rows_processed: number;
  rows_total: number | null;
  error_summary: string | null;
  checkpoint_completed: boolean;
  started_at: string;
  finished_at: string | null;
  updated_at: string;
};

type TableExecutionsResponse = {
  tables: TableExecution[];
};

type TableExecutionState = {
  loading: boolean;
  error: string | null;
  tables: TableExecution[];
};

type WizardStep = 1 | 2 | 3 | 4;

type WizardSource = {
  host: string;
  port: string;
  database: string;
  username: string;
  passwordRef: string;
  tables: string;
};

type WizardDestination = {
  host: string;
  port: string;
  database: string;
  username: string;
  passwordRef: string;
};

type WizardState = {
  step: WizardStep;
  pipelineName: string;
  source: WizardSource;
  destination: WizardDestination;
  applyStatus: string;
  applying: boolean;
};

const NAV_ITEMS: Array<{ key: ViewKey; label: string; eyebrow: string }> = [
  { key: 'overview', label: 'Overview', eyebrow: 'Control plane' },
  { key: 'onboarding', label: 'Onboarding', eyebrow: 'Source → destination' },
  { key: 'jobs', label: 'Job status', eyebrow: 'Operators' },
  { key: 'yaml', label: 'YAML studio', eyebrow: 'Declarative workflows' }
];

const DEFAULT_WIZARD: WizardState = {
  step: 1,
  pipelineName: '',
  source: { host: 'localhost', port: '5432', database: '', username: '', passwordRef: 'POSTGRES_PASSWORD', tables: 'public.users' },
  destination: { host: 'localhost', port: '5432', database: '', username: '', passwordRef: 'DEST_POSTGRES_PASSWORD' },
  applyStatus: '',
  applying: false,
};

function generateWizardYaml(w: WizardState): string {
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

const DEFAULT_AUTHOR = 'web-ui';

function runStatusClass(status: string): string {
  const s = status.toLowerCase();
  if (s === 'succeeded') return 'status-pill--success';
  if (s === 'failed' || s === 'error') return 'status-pill--error';
  if (s === 'running') return 'status-pill--running';
  return 'status-pill--muted';
}

function tableStatusClass(status: string): string {
  const s = status.toLowerCase();
  if (s === 'applied') return 'status-pill--success';
  if (s === 'failed') return 'status-pill--error';
  if (s === 'snapshot') return 'status-pill--running';
  return 'status-pill--muted'; // staged, queued, etc.
}

function formatRowProgress(rowsProcessed: number, rowsTotal: number | null): string {
  if (rowsTotal != null) {
    return `${rowsProcessed.toLocaleString()} / ${rowsTotal.toLocaleString()} rows`;
  }
  return `${rowsProcessed.toLocaleString()} rows`;
}

function formatDuration(startedAt: string, finishedAt: string | null): string {
  if (!finishedAt) return '—';
  const ms = new Date(finishedAt).getTime() - new Date(startedAt).getTime();
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${Math.round(ms / 1000)}s`;
  return `${Math.round(ms / 60_000)}m`;
}

function formatTimestamp(ts: string): string {
  return new Date(ts).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  });
}

export function App() {
  const [activeView, setActiveView] = useState<ViewKey>('overview');
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [pipelinesError, setPipelinesError] = useState<string | null>(null);
  const [pipelinesLoading, setPipelinesLoading] = useState(true);
  const [yaml, setYaml] = useState('');
  const [yamlStatus, setYamlStatus] = useState<string>('Loading canonical example…');
  const [applyStatus, setApplyStatus] = useState<string>('');
  const [refreshToken, setRefreshToken] = useState(0);
  const [expandedPipelines, setExpandedPipelines] = useState<Set<string>>(new Set());
  const [runHistories, setRunHistories] = useState<Record<string, RunHistoryState>>({});
  const [expandedRuns, setExpandedRuns] = useState<Set<string>>(new Set());
  const [tableExecutions, setTableExecutions] = useState<Record<string, TableExecutionState>>({});
  const [wizard, setWizard] = useState<WizardState>(DEFAULT_WIZARD);

  useEffect(() => {
    let cancelled = false;

    async function loadPipelines() {
      setPipelinesLoading(true);
      setPipelinesError(null);
      try {
        const response = await fetch('/api/v1/pipelines');
        if (!response.ok) {
          throw new Error(`Pipeline request failed: ${response.status}`);
        }

        const data = (await response.json()) as PipelinesResponse;
        if (!cancelled) {
          setPipelines(data.pipelines);
        }
      } catch (error) {
        if (!cancelled) {
          setPipelinesError(error instanceof Error ? error.message : 'Failed to load pipelines.');
        }
      } finally {
        if (!cancelled) {
          setPipelinesLoading(false);
        }
      }
    }

    void loadPipelines();

    return () => {
      cancelled = true;
    };
  }, [refreshToken]);

  useEffect(() => {
    let cancelled = false;

    async function loadExampleYaml() {
      try {
        const response = await fetch('/api/v1/examples/postgres-to-warehouse');
        if (!response.ok) {
          throw new Error(`Example YAML request failed: ${response.status}`);
        }

        const exampleYaml = await response.text();
        if (!cancelled) {
          setYaml(exampleYaml);
          setYamlStatus('Canonical example loaded from examples/postgres-to-warehouse.astra.yaml');
        }
      } catch (error) {
        if (!cancelled) {
          setYamlStatus(error instanceof Error ? error.message : 'Failed to load example YAML.');
          setYaml('# Failed to load example YAML\n');
        }
      }
    }

    void loadExampleYaml();

    return () => {
      cancelled = true;
    };
  }, []);

  const pipelineSummary = useMemo(() => {
    if (pipelines.length === 0) {
      return 'No pipelines yet. That is at least honest.';
    }

    const activeCount = pipelines.filter((pipeline) => pipeline.status.toLowerCase() === 'active').length;
    return `${pipelines.length} pipeline${pipelines.length === 1 ? '' : 's'} tracked, ${activeCount} active.`;
  }, [pipelines]);

  async function fetchRunHistory(pipelineName: string) {
    setRunHistories((prev) => ({
      ...prev,
      [pipelineName]: { loading: true, error: null, runs: prev[pipelineName]?.runs ?? [] }
    }));

    try {
      const response = await fetch(`/api/v1/pipelines/${encodeURIComponent(pipelineName)}/run-history`);
      if (!response.ok) {
        throw new Error(`Run history request failed: ${response.status}`);
      }
      const data = (await response.json()) as PipelineRunsResponse;
      setRunHistories((prev) => ({
        ...prev,
        [pipelineName]: { loading: false, error: null, runs: data.runs }
      }));
    } catch (error) {
      setRunHistories((prev) => ({
        ...prev,
        [pipelineName]: {
          loading: false,
          error: error instanceof Error ? error.message : 'Failed to load run history.',
          runs: []
        }
      }));
    }
  }

  async function fetchTableExecutions(runId: string) {
    setTableExecutions((prev) => ({
      ...prev,
      [runId]: { loading: true, error: null, tables: prev[runId]?.tables ?? [] }
    }));
    try {
      const response = await fetch(`/api/v1/pipeline-runs/${encodeURIComponent(runId)}/table-executions`);
      if (!response.ok) {
        throw new Error(`Table executions request failed: ${response.status}`);
      }
      const data = (await response.json()) as TableExecutionsResponse;
      setTableExecutions((prev) => ({
        ...prev,
        [runId]: { loading: false, error: null, tables: data.tables }
      }));
    } catch (error) {
      setTableExecutions((prev) => ({
        ...prev,
        [runId]: {
          loading: false,
          error: error instanceof Error ? error.message : 'Failed to load table executions.',
          tables: []
        }
      }));
    }
  }

  function handleToggleTables(runId: string) {
    setExpandedRuns((prev) => {
      const next = new Set(prev);
      if (next.has(runId)) {
        next.delete(runId);
      } else {
        next.add(runId);
        void fetchTableExecutions(runId);
      }
      return next;
    });
  }

  function handleToggleRuns(pipelineName: string) {
    setExpandedPipelines((prev) => {
      const next = new Set(prev);
      if (next.has(pipelineName)) {
        next.delete(pipelineName);
      } else {
        next.add(pipelineName);
        void fetchRunHistory(pipelineName);
      }
      return next;
    });
  }

  async function handleApplyYaml() {
    setApplyStatus('Applying YAML spec…');

    try {
      const response = await fetch('/api/v1/specs/apply', {
        method: 'POST',
        headers: {
          'content-type': 'application/json'
        },
        body: JSON.stringify({
          yaml,
          created_by: DEFAULT_AUTHOR
        })
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || `Apply failed with status ${response.status}`);
      }

      const payload = (await response.json()) as ApplySpecResponse;
      setApplyStatus(`${payload.message} Saved ${payload.pipeline_name} v${payload.spec_version}.`);
      setActiveView('jobs');
      setRefreshToken((current) => current + 1);
    } catch (error) {
      setApplyStatus(error instanceof Error ? error.message : 'Failed to apply YAML spec.');
    }
  }

  async function handleWizardApply() {
    setWizard((prev) => ({ ...prev, applying: true, applyStatus: 'Applying spec…' }));
    try {
      const specYaml = generateWizardYaml(wizard);
      const response = await fetch('/api/v1/specs/apply', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ yaml: specYaml, created_by: DEFAULT_AUTHOR }),
      });
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || `Apply failed with status ${response.status}`);
      }
      await response.json();
      setWizard(DEFAULT_WIZARD);
      setRefreshToken((current) => current + 1);
      setActiveView('jobs');
    } catch (error) {
      setWizard((prev) => ({
        ...prev,
        applying: false,
        applyStatus: error instanceof Error ? `Error: ${error.message}` : 'Failed to apply spec.',
      }));
    }
  }

  return (
    <div className="shell">
      <aside className="sidebar">
        <div>
          <div className="brand">Astra</div>
          <p className="sidebar-copy">Fast data replication, fewer cursed onboarding flows.</p>
        </div>

        <nav className="nav">
          {NAV_ITEMS.map((item) => (
            <button
              key={item.key}
              type="button"
              className={item.key === activeView ? 'nav-item nav-item--active' : 'nav-item'}
              onClick={() => setActiveView(item.key)}
            >
              <span className="nav-item__eyebrow">{item.eyebrow}</span>
              <span>{item.label}</span>
            </button>
          ))}
        </nav>

        <div className="sidebar-card">
          <span className="sidebar-card__label">Pipeline snapshot</span>
          <strong>{pipelineSummary}</strong>
          <button type="button" className="secondary-button" onClick={() => setRefreshToken((current) => current + 1)}>
            Refresh data
          </button>
        </div>
      </aside>

      <main className="content">
        <header className="hero card">
          <div>
            <p className="eyebrow">Issue #26 foundation</p>
            <h1>Astra control-plane UI</h1>
            <p className="hero-copy">
              This is the React + TypeScript app foundation: enough real shape to build on, without pretending the whole
              product already exists.
            </p>
          </div>
          <div className="hero-status">
            <span className="status-pill">Self-hostable</span>
            <span className="status-pill status-pill--muted">Local-dev friendly</span>
          </div>
        </header>

        {activeView === 'overview' && (
          <section className="panel-grid">
            <article className="card">
              <p className="eyebrow">What landed</p>
              <h2>Foundation, not theatre</h2>
              <ul className="bullet-list">
                <li>Vite + React + TypeScript app scaffold</li>
                <li>Backend-friendly API fetches with sane local proxying</li>
                <li>Job status list wired to the control-plane API</li>
                <li>YAML editor seeded from the canonical example spec</li>
                <li>Apply action hooked to the existing spec endpoint</li>
              </ul>
            </article>

            <article className="card">
              <p className="eyebrow">Why this shape</p>
              <h2>Keep the product honest</h2>
              <p>
                The onboarding, job-status, and YAML surfaces now live inside one app shell so future work can share
                state, styling, and fetch logic instead of spawning more one-off markup goblins.
              </p>
            </article>
          </section>
        )}

        {activeView === 'onboarding' && (
          <section className="card section-stack">
            <div>
              <p className="eyebrow">Source → destination</p>
              <h2>Set up a new pipeline</h2>
              <p className="section-copy">Configure your source and destination. Astra generates the spec — you review and apply it.</p>
            </div>

            <div className="wizard-steps">
              {(['Pipeline', 'Source', 'Destination', 'Review'] as const).map((label, i) => {
                const n = (i + 1) as WizardStep;
                const done = wizard.step > n;
                const active = wizard.step === n;
                return (
                  <div key={label} className={`wizard-step${active ? ' wizard-step--active' : done ? ' wizard-step--done' : ''}`}>
                    <span className="wizard-step__num">{done ? '✓' : n}</span>
                    <span>{label}</span>
                  </div>
                );
              })}
            </div>

            {wizard.step === 1 && (
              <div className="section-stack">
                <div>
                  <h3 className="form-section-heading">Pipeline</h3>
                  <p className="muted">A unique name for this replication pipeline.</p>
                </div>
                <div className="form-group">
                  <label className="field-label" htmlFor="wz-name">Pipeline name</label>
                  <input
                    id="wz-name"
                    className="form-input"
                    type="text"
                    placeholder="e.g. postgres-analytics"
                    value={wizard.pipelineName}
                    onChange={(e) => setWizard((prev) => ({ ...prev, pipelineName: e.target.value }))}
                  />
                </div>
                <div className="form-group">
                  <span className="field-label">Mode</span>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginTop: '0.4rem' }}>
                    <span className="status-pill status-pill--muted">snapshot</span>
                    <span className="muted" style={{ fontSize: '0.85rem' }}>Only snapshot mode is supported in v0.1.</span>
                  </div>
                </div>
                <div className="wizard-actions">
                  <span />
                  <button
                    type="button"
                    disabled={!wizard.pipelineName.trim()}
                    onClick={() => setWizard((prev) => ({ ...prev, step: 2 }))}
                  >
                    Next: Source →
                  </button>
                </div>
              </div>
            )}

            {wizard.step === 2 && (
              <div className="section-stack">
                <div>
                  <h3 className="form-section-heading">Source — Postgres</h3>
                  <p className="muted">Connection details for the database you want to replicate from.</p>
                </div>
                <div className="form-row">
                  <div className="form-group">
                    <label className="field-label" htmlFor="wz-src-host">Host</label>
                    <input id="wz-src-host" className="form-input" type="text" placeholder="localhost"
                      value={wizard.source.host}
                      onChange={(e) => setWizard((prev) => ({ ...prev, source: { ...prev.source, host: e.target.value } }))} />
                  </div>
                  <div className="form-group form-group--narrow">
                    <label className="field-label" htmlFor="wz-src-port">Port</label>
                    <input id="wz-src-port" className="form-input" type="text" placeholder="5432"
                      value={wizard.source.port}
                      onChange={(e) => setWizard((prev) => ({ ...prev, source: { ...prev.source, port: e.target.value } }))} />
                  </div>
                </div>
                <div className="form-group">
                  <label className="field-label" htmlFor="wz-src-db">Database</label>
                  <input id="wz-src-db" className="form-input" type="text" placeholder="app"
                    value={wizard.source.database}
                    onChange={(e) => setWizard((prev) => ({ ...prev, source: { ...prev.source, database: e.target.value } }))} />
                </div>
                <div className="form-row">
                  <div className="form-group">
                    <label className="field-label" htmlFor="wz-src-user">Username</label>
                    <input id="wz-src-user" className="form-input" type="text" placeholder="app_user"
                      value={wizard.source.username}
                      onChange={(e) => setWizard((prev) => ({ ...prev, source: { ...prev.source, username: e.target.value } }))} />
                  </div>
                  <div className="form-group">
                    <label className="field-label" htmlFor="wz-src-pass">Password env var</label>
                    <div className="form-input-prefix-wrap">
                      <span className="form-input-prefix">env:</span>
                      <input id="wz-src-pass" className="form-input form-input--prefixed" type="text" placeholder="POSTGRES_PASSWORD"
                        value={wizard.source.passwordRef}
                        onChange={(e) => setWizard((prev) => ({ ...prev, source: { ...prev.source, passwordRef: e.target.value } }))} />
                    </div>
                  </div>
                </div>
                <div className="form-group">
                  <label className="field-label" htmlFor="wz-src-tables">Tables to replicate</label>
                  <p className="muted" style={{ margin: '0 0 0.4rem', fontSize: '0.85rem' }}>One per line, in <code>schema.table</code> format.</p>
                  <textarea id="wz-src-tables" className="form-input form-input--textarea" placeholder={'public.users\npublic.orders'}
                    value={wizard.source.tables}
                    onChange={(e) => setWizard((prev) => ({ ...prev, source: { ...prev.source, tables: e.target.value } }))} />
                </div>
                <div className="wizard-actions">
                  <button type="button" className="secondary-button" onClick={() => setWizard((prev) => ({ ...prev, step: 1 }))}>
                    ← Back
                  </button>
                  <button
                    type="button"
                    disabled={!wizard.source.host.trim() || !wizard.source.database.trim() || !wizard.source.username.trim() || !wizard.source.tables.trim()}
                    onClick={() => setWizard((prev) => ({ ...prev, step: 3 }))}
                  >
                    Next: Destination →
                  </button>
                </div>
              </div>
            )}

            {wizard.step === 3 && (
              <div className="section-stack">
                <div>
                  <h3 className="form-section-heading">Destination — Postgres</h3>
                  <p className="muted">Connection details for the target database. Data is loaded into the <code>astra_raw</code> schema.</p>
                </div>
                <div className="form-row">
                  <div className="form-group">
                    <label className="field-label" htmlFor="wz-dst-host">Host</label>
                    <input id="wz-dst-host" className="form-input" type="text" placeholder="localhost"
                      value={wizard.destination.host}
                      onChange={(e) => setWizard((prev) => ({ ...prev, destination: { ...prev.destination, host: e.target.value } }))} />
                  </div>
                  <div className="form-group form-group--narrow">
                    <label className="field-label" htmlFor="wz-dst-port">Port</label>
                    <input id="wz-dst-port" className="form-input" type="text" placeholder="5432"
                      value={wizard.destination.port}
                      onChange={(e) => setWizard((prev) => ({ ...prev, destination: { ...prev.destination, port: e.target.value } }))} />
                  </div>
                </div>
                <div className="form-group">
                  <label className="field-label" htmlFor="wz-dst-db">Database</label>
                  <input id="wz-dst-db" className="form-input" type="text" placeholder="warehouse"
                    value={wizard.destination.database}
                    onChange={(e) => setWizard((prev) => ({ ...prev, destination: { ...prev.destination, database: e.target.value } }))} />
                </div>
                <div className="form-row">
                  <div className="form-group">
                    <label className="field-label" htmlFor="wz-dst-user">Username</label>
                    <input id="wz-dst-user" className="form-input" type="text" placeholder="warehouse_user"
                      value={wizard.destination.username}
                      onChange={(e) => setWizard((prev) => ({ ...prev, destination: { ...prev.destination, username: e.target.value } }))} />
                  </div>
                  <div className="form-group">
                    <label className="field-label" htmlFor="wz-dst-pass">Password env var</label>
                    <div className="form-input-prefix-wrap">
                      <span className="form-input-prefix">env:</span>
                      <input id="wz-dst-pass" className="form-input form-input--prefixed" type="text" placeholder="DEST_POSTGRES_PASSWORD"
                        value={wizard.destination.passwordRef}
                        onChange={(e) => setWizard((prev) => ({ ...prev, destination: { ...prev.destination, passwordRef: e.target.value } }))} />
                    </div>
                  </div>
                </div>
                <div className="wizard-actions">
                  <button type="button" className="secondary-button" onClick={() => setWizard((prev) => ({ ...prev, step: 2 }))}>
                    ← Back
                  </button>
                  <button
                    type="button"
                    disabled={!wizard.destination.host.trim() || !wizard.destination.database.trim() || !wizard.destination.username.trim()}
                    onClick={() => setWizard((prev) => ({ ...prev, step: 4 }))}
                  >
                    Review spec →
                  </button>
                </div>
              </div>
            )}

            {wizard.step === 4 && (
              <div className="section-stack">
                <div>
                  <h3 className="form-section-heading">Review spec</h3>
                  <p className="muted">This is the YAML spec Astra will apply. Go back to change any field.</p>
                </div>
                <pre className="yaml-preview">{generateWizardYaml(wizard)}</pre>
                {wizard.applyStatus && (
                  <p className={wizard.applyStatus.startsWith('Error') ? 'error-text' : 'success-text'}>
                    {wizard.applyStatus}
                  </p>
                )}
                <div className="wizard-actions">
                  <button type="button" className="secondary-button"
                    onClick={() => setWizard((prev) => ({ ...prev, step: 3, applyStatus: '' }))}
                    disabled={wizard.applying}
                  >
                    ← Back
                  </button>
                  <div style={{ display: 'flex', gap: '0.75rem' }}>
                    <button type="button" className="secondary-button"
                      onClick={() => setWizard(DEFAULT_WIZARD)}
                      disabled={wizard.applying}
                    >
                      Start over
                    </button>
                    <button type="button"
                      disabled={wizard.applying}
                      onClick={() => void handleWizardApply()}
                    >
                      {wizard.applying ? 'Applying…' : 'Apply spec'}
                    </button>
                  </div>
                </div>
              </div>
            )}
          </section>
        )}

        {activeView === 'jobs' && (
          <section className="card section-stack">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Job status surface</p>
                <h2>Pipeline inventory</h2>
              </div>
              <button type="button" className="secondary-button" onClick={() => setRefreshToken((current) => current + 1)}>
                Reload pipelines
              </button>
            </div>

            {pipelinesLoading ? (
              <p className="muted">Loading pipelines…</p>
            ) : pipelinesError ? (
              <p className="error-text">{pipelinesError}</p>
            ) : pipelines.length === 0 ? (
              <p className="muted">No pipelines yet. Apply a YAML spec and this stops looking abandoned.</p>
            ) : (
              <div className="list">
                {pipelines.map((pipeline) => {
                  const isExpanded = expandedPipelines.has(pipeline.name);
                  const history = runHistories[pipeline.name];

                  return (
                    <article key={`${pipeline.name}-${pipeline.spec_version}`} className="list-row">
                      <div className="list-row__header">
                        <div>
                          <h3>{pipeline.name}</h3>
                          <p className="muted">
                            {pipeline.source_kind} → {pipeline.destination_kind}
                          </p>
                        </div>
                        <div className="list-row__meta">
                          <span className="status-pill">{pipeline.status}</span>
                          <span className="muted">v{pipeline.spec_version}</span>
                          <button
                            type="button"
                            className="secondary-button run-toggle-btn"
                            onClick={() => handleToggleRuns(pipeline.name)}
                            aria-expanded={isExpanded}
                          >
                            {isExpanded ? 'Hide runs' : 'View runs'}
                          </button>
                        </div>
                      </div>

                      {isExpanded && (
                        <div className="run-history">
                          {!history || history.loading ? (
                            <p className="muted run-history__status">Loading run history…</p>
                          ) : history.error ? (
                            <p className="error-text run-history__status">{history.error}</p>
                          ) : history.runs.length === 0 ? (
                            <p className="muted run-history__status">No runs yet for this pipeline.</p>
                          ) : (
                            <div className="run-history__list">
                              <div className="run-history__header-row">
                                <span>Run ID</span>
                                <span>Status</span>
                                <span>Trigger</span>
                                <span>Started</span>
                                <span>Duration</span>
                                <span></span>
                              </div>
                              {history.runs.map((run) => {
                                const isRunExpanded = expandedRuns.has(run.id);
                                const tableState = tableExecutions[run.id];
                                return (
                                  <div key={run.id} className="run-history__item">
                                    <div className="run-history__row">
                                      <span className="run-history__id">{run.id.slice(0, 8)}</span>
                                      <span>
                                        <span className={`status-pill ${runStatusClass(run.status)}`}>{run.status}</span>
                                      </span>
                                      <span className="muted">{run.trigger_mode}</span>
                                      <span className="muted">{formatTimestamp(run.started_at)}</span>
                                      <span className="muted">{formatDuration(run.started_at, run.finished_at)}</span>
                                      <span>
                                        <button
                                          type="button"
                                          className="secondary-button table-toggle-btn"
                                          onClick={() => handleToggleTables(run.id)}
                                          aria-expanded={isRunExpanded}
                                        >
                                          {isRunExpanded ? 'Hide tables' : 'Tables'}
                                        </button>
                                      </span>
                                    </div>

                                    {isRunExpanded && (
                                      <div className="table-executions">
                                        {!tableState || tableState.loading ? (
                                          <p className="muted table-executions__status">Loading table executions…</p>
                                        ) : tableState.error ? (
                                          <p className="error-text table-executions__status">{tableState.error}</p>
                                        ) : tableState.tables.length === 0 ? (
                                          <p className="muted table-executions__status">No table executions recorded for this run.</p>
                                        ) : (
                                          <div className="table-executions__list">
                                            <div className="table-executions__header-row">
                                              <span>Stream</span>
                                              <span>Status</span>
                                              <span>Progress</span>
                                              <span>Error</span>
                                            </div>
                                            {tableState.tables.map((table) => (
                                              <div key={table.id} className="table-executions__row">
                                                <span className="table-executions__name">{table.stream_name}</span>
                                                <span>
                                                  <span className={`status-pill ${tableStatusClass(table.status)}`}>
                                                    {table.status}
                                                  </span>
                                                </span>
                                                <span className="muted">
                                                  {formatRowProgress(table.rows_processed, table.rows_total)}
                                                </span>
                                                <span
                                                  className={table.error_summary ? 'table-executions__error' : 'muted'}
                                                  title={table.error_summary ?? undefined}
                                                >
                                                  {table.error_summary ?? '—'}
                                                </span>
                                              </div>
                                            ))}
                                          </div>
                                        )}
                                      </div>
                                    )}
                                  </div>
                                );
                              })}
                            </div>
                          )}
                        </div>
                      )}
                    </article>
                  );
                })}
              </div>
            )}
          </section>
        )}

        {activeView === 'yaml' && (
          <section className="panel-grid panel-grid--yaml">
            <article className="card section-stack">
              <div>
                <p className="eyebrow">YAML preview surface</p>
                <h2>Spec editor</h2>
                <p className="section-copy">
                  Seeded from the canonical example so the UI stays attached to the real Astra contract instead of making
                  things up.
                </p>
              </div>

              <label className="field-label" htmlFor="yaml-editor">
                Pipeline spec
              </label>
              <textarea id="yaml-editor" className="yaml-editor" value={yaml} onChange={(event) => setYaml(event.target.value)} />
              <div className="action-row">
                <button type="button" onClick={handleApplyYaml}>
                  Apply spec
                </button>
                <button
                  type="button"
                  className="secondary-button"
                  onClick={() => {
                    setApplyStatus('Reloading canonical example…');
                    void fetch('/api/v1/examples/postgres-to-warehouse')
                      .then(async (response) => {
                        if (!response.ok) {
                          throw new Error(`Example YAML request failed: ${response.status}`);
                        }

                        const exampleYaml = await response.text();
                        setYaml(exampleYaml);
                        setApplyStatus('Canonical example restored.');
                      })
                      .catch((error: unknown) => {
                        setApplyStatus(error instanceof Error ? error.message : 'Failed to reload example YAML.');
                      });
                  }}
                >
                  Reset example
                </button>
              </div>
              <p className="muted">{yamlStatus}</p>
              {applyStatus ? <p className="success-text">{applyStatus}</p> : null}
            </article>

            <article className="card section-stack">
              <div>
                <p className="eyebrow">Preview</p>
                <h2>What the operator is about to apply</h2>
              </div>
              <pre className="yaml-preview">{yaml}</pre>
            </article>
          </section>
        )}
      </main>
    </div>
  );
}
