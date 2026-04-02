export type ViewKey = 'overview' | 'onboarding' | 'jobs' | 'yaml';

export type Pipeline = {
  name: string;
  source_kind: string;
  destination_kind: string;
  status: string;
  spec_version: number;
};

export type PipelineSummaryResponse = Pipeline;

export type PipelinesResponse = {
  pipelines: Pipeline[];
};

export type PipelineRun = {
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

export type PipelineRunsResponse = {
  runs: PipelineRun[];
};

export type RunHistoryState = {
  loading: boolean;
  error: string | null;
  runs: PipelineRun[];
};

export type ApplySpecResponse = {
  pipeline_name: string;
  spec_version: number;
  content_hash: string;
  message: string;
};

export type TableExecution = {
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

export type TableExecutionsResponse = {
  tables: TableExecution[];
};

export type TableExecutionState = {
  loading: boolean;
  error: string | null;
  tables: TableExecution[];
};

export type WizardStep = 1 | 2 | 3 | 4;

export type WizardSource = {
  host: string;
  port: string;
  database: string;
  username: string;
  passwordRef: string;
  tables: string;
};

export type WizardDestination = {
  host: string;
  port: string;
  database: string;
  username: string;
  passwordRef: string;
};

export type WizardState = {
  step: WizardStep;
  pipelineName: string;
  source: WizardSource;
  destination: WizardDestination;
  applyStatus: string;
  applying: boolean;
};
