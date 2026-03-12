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

type ApplySpecResponse = {
  pipeline_name: string;
  spec_version: number;
  content_hash: string;
  message: string;
};

const NAV_ITEMS: Array<{ key: ViewKey; label: string; eyebrow: string }> = [
  { key: 'overview', label: 'Overview', eyebrow: 'Control plane' },
  { key: 'onboarding', label: 'Onboarding', eyebrow: 'Source → destination' },
  { key: 'jobs', label: 'Job status', eyebrow: 'Operators' },
  { key: 'yaml', label: 'YAML studio', eyebrow: 'Declarative workflows' }
];

const ONBOARDING_STEPS = [
  {
    title: 'Connect a source',
    detail: 'Postgres is the first serious citizen. More connectors can stop pretending later.'
  },
  {
    title: 'Pick a destination',
    detail: 'Model the destination in YAML now so the UI never drifts off into fantasy product land.'
  },
  {
    title: 'Review the generated spec',
    detail: 'Operators can validate what Astra plans to do before a pipeline gets near production.'
  },
  {
    title: 'Apply and observe',
    detail: 'Persist the spec, start the run, and surface status without making people spelunk through logs.'
  }
];

const DEFAULT_AUTHOR = 'web-ui';

export function App() {
  const [activeView, setActiveView] = useState<ViewKey>('overview');
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [pipelinesError, setPipelinesError] = useState<string | null>(null);
  const [pipelinesLoading, setPipelinesLoading] = useState(true);
  const [yaml, setYaml] = useState('');
  const [yamlStatus, setYamlStatus] = useState<string>('Loading canonical example…');
  const [applyStatus, setApplyStatus] = useState<string>('');
  const [refreshToken, setRefreshToken] = useState(0);

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
              <p className="eyebrow">Onboarding surface</p>
              <h2>Stub the flow, keep it useful</h2>
              <p className="section-copy">
                This preserves the original onboarding intent while setting up the app structure for real forms and API
                mutations later.
              </p>
            </div>

            <div className="step-grid">
              {ONBOARDING_STEPS.map((step, index) => (
                <article key={step.title} className="step-card">
                  <span className="step-index">0{index + 1}</span>
                  <h3>{step.title}</h3>
                  <p>{step.detail}</p>
                </article>
              ))}
            </div>
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
                {pipelines.map((pipeline) => (
                  <article key={`${pipeline.name}-${pipeline.spec_version}`} className="list-row">
                    <div>
                      <h3>{pipeline.name}</h3>
                      <p className="muted">
                        {pipeline.source_kind} → {pipeline.destination_kind}
                      </p>
                    </div>
                    <div className="list-row__meta">
                      <span className="status-pill">{pipeline.status}</span>
                      <span className="muted">v{pipeline.spec_version}</span>
                    </div>
                  </article>
                ))}
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
