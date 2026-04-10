import type {
  ApplySpecResponse,
  MeResponse,
  PipelineRunsResponse,
  PipelinesResponse,
  PipelineSummaryResponse,
  SavedConnectionRequest,
  SavedConnectionResponse,
  SavedConnectionsResponse,
  TableExecutionsResponse,
} from '@/types';

// ── Auth helpers ─────────────────────────────────────────────────────────────

/**
 * Registered by AuthContext on mount. Called when a token refresh fails so the
 * context can clear state and force the login page.
 */
let _onUnauthorized: (() => void) | null = null;
export function registerUnauthorizedHandler(fn: () => void) {
  _onUnauthorized = fn;
}

/**
 * Wraps `fetch` with automatic token refresh on 401.
 * Cookies are sent automatically (same-origin); no extra headers needed.
 * On a second 401 after refresh the caller receives the failed response and
 * the unauthorized handler is invoked to clear auth state.
 */
export async function fetchWithAuth(input: string, init?: RequestInit): Promise<Response> {
  const res = await fetch(input, init);
  if (res.status !== 401) return res;

  // Attempt refresh.
  const refreshRes = await fetch('/api/v1/auth/refresh', { method: 'POST' });
  if (!refreshRes.ok) {
    _onUnauthorized?.();
    return res;
  }

  // Retry original request once.
  const retried = await fetch(input, init);
  if (retried.status === 401) _onUnauthorized?.();
  return retried;
}

// ── Auth API ─────────────────────────────────────────────────────────────────

export async function loginUser(email: string, password: string): Promise<void> {
  const res = await fetch('/api/v1/auth/login', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Login failed: ${res.status}`);
  }
}

export async function logoutUser(): Promise<void> {
  await fetch('/api/v1/auth/logout', { method: 'POST' });
}

export async function fetchMe(): Promise<MeResponse> {
  const res = await fetchWithAuth('/api/v1/auth/me');
  if (!res.ok) throw new Error(`Unauthenticated`);
  return res.json() as Promise<MeResponse>;
}

// ── Pipeline API ─────────────────────────────────────────────────────────────

export async function fetchPipelines(): Promise<PipelinesResponse> {
  const res = await fetchWithAuth('/api/v1/pipelines');
  if (!res.ok) throw new Error(`Pipeline request failed: ${res.status}`);
  return res.json() as Promise<PipelinesResponse>;
}

export async function fetchExampleYaml(): Promise<string> {
  const res = await fetch('/api/v1/examples/postgres-to-warehouse');
  if (!res.ok) throw new Error(`Example YAML request failed: ${res.status}`);
  return res.text();
}

export async function fetchRunHistory(pipelineName: string): Promise<PipelineRunsResponse> {
  const res = await fetchWithAuth(
    `/api/v1/pipelines/${encodeURIComponent(pipelineName)}/run-history`,
  );
  if (!res.ok) throw new Error(`Run history request failed: ${res.status}`);
  return res.json() as Promise<PipelineRunsResponse>;
}

export async function fetchTableExecutions(runId: string): Promise<TableExecutionsResponse> {
  const res = await fetchWithAuth(
    `/api/v1/pipeline-runs/${encodeURIComponent(runId)}/table-executions`,
  );
  if (!res.ok) throw new Error(`Table executions request failed: ${res.status}`);
  return res.json() as Promise<TableExecutionsResponse>;
}

export async function applySpec(yaml: string, createdBy: string): Promise<ApplySpecResponse> {
  const res = await fetchWithAuth('/api/v1/specs/apply', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ yaml, created_by: createdBy }),
  });
  if (!res.ok) {
    const errorText = await res.text();
    throw new Error(errorText || `Apply failed with status ${res.status}`);
  }
  return res.json() as Promise<ApplySpecResponse>;
}

export async function deletePipeline(pipelineName: string): Promise<void> {
  const res = await fetchWithAuth(
    `/api/v1/pipelines/${encodeURIComponent(pipelineName)}`,
    { method: 'DELETE' },
  );
  if (!res.ok) {
    const errorText = await res.text();
    throw new Error(errorText || `Delete failed with status ${res.status}`);
  }
}

export async function disablePipeline(pipelineName: string): Promise<PipelineSummaryResponse> {
  const res = await fetchWithAuth(
    `/api/v1/pipelines/${encodeURIComponent(pipelineName)}/disable`,
    { method: 'POST' },
  );
  if (!res.ok) {
    const errorText = await res.text();
    throw new Error(errorText || `Disable failed with status ${res.status}`);
  }
  return res.json() as Promise<PipelineSummaryResponse>;
}

export async function enablePipeline(pipelineName: string): Promise<PipelineSummaryResponse> {
  const res = await fetchWithAuth(
    `/api/v1/pipelines/${encodeURIComponent(pipelineName)}/enable`,
    { method: 'POST' },
  );
  if (!res.ok) {
    const errorText = await res.text();
    throw new Error(errorText || `Enable failed with status ${res.status}`);
  }
  return res.json() as Promise<PipelineSummaryResponse>;
}

export async function triggerPipeline(pipelineName: string): Promise<{ run_id: string }> {
  const res = await fetchWithAuth(
    `/api/v1/pipelines/${encodeURIComponent(pipelineName)}/trigger`,
    { method: 'POST' },
  );
  if (!res.ok) {
    const errorText = await res.text();
    throw new Error(errorText || `Trigger failed with status ${res.status}`);
  }
  return res.json() as Promise<{ run_id: string }>;
}

// ── Connections API ───────────────────────────────────────────────────────────

export async function fetchConnections(): Promise<SavedConnectionResponse[]> {
  const res = await fetch('/api/v1/connections');
  if (!res.ok) throw new Error(`Connections request failed: ${res.status}`);
  const data = (await res.json()) as SavedConnectionsResponse;
  return data.connections;
}

export async function createConnection(
  req: SavedConnectionRequest,
): Promise<SavedConnectionResponse> {
  const res = await fetch('/api/v1/connections', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(req),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Create connection failed: ${res.status}`);
  }
  return res.json() as Promise<SavedConnectionResponse>;
}

export async function updateConnection(
  name: string,
  req: SavedConnectionRequest,
): Promise<SavedConnectionResponse> {
  const res = await fetch(`/api/v1/connections/${encodeURIComponent(name)}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(req),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Update connection failed: ${res.status}`);
  }
  return res.json() as Promise<SavedConnectionResponse>;
}

export async function deleteConnection(name: string): Promise<void> {
  const res = await fetch(`/api/v1/connections/${encodeURIComponent(name)}`, {
    method: 'DELETE',
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Delete connection failed: ${res.status}`);
  }
}
