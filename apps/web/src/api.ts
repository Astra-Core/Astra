import type {
  ApplySpecResponse,
  PipelineRunsResponse,
  PipelinesResponse,
  PipelineSummaryResponse,
  TableExecutionsResponse,
} from '@/types';

export async function fetchPipelines(): Promise<PipelinesResponse> {
  const res = await fetch('/api/v1/pipelines');
  if (!res.ok) throw new Error(`Pipeline request failed: ${res.status}`);
  return res.json() as Promise<PipelinesResponse>;
}

export async function fetchExampleYaml(): Promise<string> {
  const res = await fetch('/api/v1/examples/postgres-to-warehouse');
  if (!res.ok) throw new Error(`Example YAML request failed: ${res.status}`);
  return res.text();
}

export async function fetchRunHistory(pipelineName: string): Promise<PipelineRunsResponse> {
  const res = await fetch(`/api/v1/pipelines/${encodeURIComponent(pipelineName)}/run-history`);
  if (!res.ok) throw new Error(`Run history request failed: ${res.status}`);
  return res.json() as Promise<PipelineRunsResponse>;
}

export async function fetchTableExecutions(runId: string): Promise<TableExecutionsResponse> {
  const res = await fetch(`/api/v1/pipeline-runs/${encodeURIComponent(runId)}/table-executions`);
  if (!res.ok) throw new Error(`Table executions request failed: ${res.status}`);
  return res.json() as Promise<TableExecutionsResponse>;
}

export async function applySpec(yaml: string, createdBy: string): Promise<ApplySpecResponse> {
  const res = await fetch('/api/v1/specs/apply', {
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
  const res = await fetch(`/api/v1/pipelines/${encodeURIComponent(pipelineName)}`, {
    method: 'DELETE',
  });
  if (!res.ok) {
    const errorText = await res.text();
    throw new Error(errorText || `Delete failed with status ${res.status}`);
  }
}

export async function disablePipeline(pipelineName: string): Promise<PipelineSummaryResponse> {
  const res = await fetch(`/api/v1/pipelines/${encodeURIComponent(pipelineName)}/disable`, {
    method: 'POST',
  });
  if (!res.ok) {
    const errorText = await res.text();
    throw new Error(errorText || `Disable failed with status ${res.status}`);
  }
  return res.json() as Promise<PipelineSummaryResponse>;
}

export async function enablePipeline(pipelineName: string): Promise<PipelineSummaryResponse> {
  const res = await fetch(`/api/v1/pipelines/${encodeURIComponent(pipelineName)}/enable`, {
    method: 'POST',
  });
  if (!res.ok) {
    const errorText = await res.text();
    throw new Error(errorText || `Enable failed with status ${res.status}`);
  }
  return res.json() as Promise<PipelineSummaryResponse>;
}
