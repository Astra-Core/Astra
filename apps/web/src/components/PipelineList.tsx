import { useEffect, useState } from 'react';
import type { Pipeline, RunHistoryState, TableExecutionState } from '@/types';
import { fetchPipelines, fetchRunHistory, fetchTableExecutions, deletePipeline, disablePipeline, enablePipeline } from '@/api';
import { formatDuration, formatTimestamp, formatRowProgress, runStatusVariant, tableStatusVariant } from '@/utils';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

type Props = { refreshToken: number };

export function PipelineList({ refreshToken }: Props) {
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [localRefresh, setLocalRefresh] = useState(0);
  const [expandedPipelines, setExpandedPipelines] = useState<Set<string>>(new Set());
  const [runHistories, setRunHistories] = useState<Record<string, RunHistoryState>>({});
  const [expandedRuns, setExpandedRuns] = useState<Set<string>>(new Set());
  const [tableExecutions, setTableExecutions] = useState<Record<string, TableExecutionState>>({});
  const [actionLoading, setActionLoading] = useState<Record<string, boolean>>({});

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchPipelines()
      .then((data) => {
        if (!cancelled) {
          setPipelines(data.pipelines);
          setLoading(false);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to load pipelines.');
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [refreshToken, localRefresh]);

  function handleToggleRuns(pipelineName: string) {
    setExpandedPipelines((prev) => {
      const next = new Set(prev);
      if (next.has(pipelineName)) {
        next.delete(pipelineName);
      } else {
        next.add(pipelineName);
        setRunHistories((h) => ({
          ...h,
          [pipelineName]: { loading: true, error: null, runs: h[pipelineName]?.runs ?? [] },
        }));
        fetchRunHistory(pipelineName)
          .then((data) =>
            setRunHistories((h) => ({ ...h, [pipelineName]: { loading: false, error: null, runs: data.runs } }))
          )
          .catch((err: unknown) =>
            setRunHistories((h) => ({
              ...h,
              [pipelineName]: {
                loading: false,
                error: err instanceof Error ? err.message : 'Failed to load run history.',
                runs: [],
              },
            }))
          );
      }
      return next;
    });
  }

  function handleToggleTables(runId: string) {
    setExpandedRuns((prev) => {
      const next = new Set(prev);
      if (next.has(runId)) {
        next.delete(runId);
      } else {
        next.add(runId);
        setTableExecutions((t) => ({
          ...t,
          [runId]: { loading: true, error: null, tables: t[runId]?.tables ?? [] },
        }));
        fetchTableExecutions(runId)
          .then((data) =>
            setTableExecutions((t) => ({ ...t, [runId]: { loading: false, error: null, tables: data.tables } }))
          )
          .catch((err: unknown) =>
            setTableExecutions((t) => ({
              ...t,
              [runId]: {
                loading: false,
                error: err instanceof Error ? err.message : 'Failed to load table executions.',
                tables: [],
              },
            }))
          );
      }
      return next;
    });
  }

  function handleToggleStatus(pipeline: Pipeline) {
    const key = `status-${pipeline.name}`;
    setActionLoading((a) => ({ ...a, [key]: true }));
    const action = pipeline.status === 'disabled' ? enablePipeline : disablePipeline;
    action(pipeline.name)
      .then((updated) => {
        setPipelines((prev) =>
          prev.map((p) => (p.name === updated.name ? { ...p, status: updated.status } : p))
        );
      })
      .catch(() => {
        // Refresh list on error to get accurate state
        setLocalRefresh((n) => n + 1);
      })
      .finally(() => {
        setActionLoading((a) => ({ ...a, [key]: false }));
      });
  }

  function handleDelete(pipelineName: string) {
    if (!window.confirm(`Delete pipeline "${pipelineName}"? This cannot be undone.`)) return;
    const key = `delete-${pipelineName}`;
    setActionLoading((a) => ({ ...a, [key]: true }));
    deletePipeline(pipelineName)
      .then(() => {
        setPipelines((prev) => prev.filter((p) => p.name !== pipelineName));
        setExpandedPipelines((prev) => {
          const next = new Set(prev);
          next.delete(pipelineName);
          return next;
        });
      })
      .catch(() => {
        setLocalRefresh((n) => n + 1);
      })
      .finally(() => {
        setActionLoading((a) => ({ ...a, [key]: false }));
      });
  }

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between space-y-0">
        <CardTitle>Pipeline inventory</CardTitle>
        <Button variant="outline" size="sm" onClick={() => setLocalRefresh((n) => n + 1)}>
          Reload
        </Button>
      </CardHeader>
      <CardContent className="space-y-0 p-0">
        {loading ? (
          <p className="px-5 pb-5 text-sm text-muted-foreground">Loading pipelines…</p>
        ) : error ? (
          <p className="px-5 pb-5 text-sm text-destructive">{error}</p>
        ) : pipelines.length === 0 ? (
          <p className="px-5 pb-5 text-sm text-muted-foreground">
            No pipelines yet. Apply a YAML spec and this stops looking abandoned.
          </p>
        ) : (
          <div className="divide-y divide-border">
            {pipelines.map((pipeline) => {
              const isExpanded = expandedPipelines.has(pipeline.name);
              const history = runHistories[pipeline.name];
              const isDisabled = pipeline.status === 'disabled';
              const statusLoading = actionLoading[`status-${pipeline.name}`] ?? false;
              const deleteLoading = actionLoading[`delete-${pipeline.name}`] ?? false;

              return (
                <div key={`${pipeline.name}-${pipeline.spec_version}`}>
                  <div className="flex items-center justify-between gap-4 px-5 py-4">
                    <div>
                      <p className="text-sm font-medium">{pipeline.name}</p>
                      <p className="text-xs text-muted-foreground mt-0.5">
                        {pipeline.source_kind} → {pipeline.destination_kind}
                      </p>
                    </div>
                    <div className="flex items-center gap-3 shrink-0">
                      <Badge variant="muted">{pipeline.status}</Badge>
                      <span className="text-xs text-muted-foreground">v{pipeline.spec_version}</span>
                      <Button
                        variant="outline"
                        size="sm"
                        aria-expanded={isExpanded}
                        aria-controls={`runs-${pipeline.name}-${pipeline.spec_version}`}
                        onClick={() => handleToggleRuns(pipeline.name)}
                      >
                        {isExpanded ? 'Hide runs' : 'View runs'}
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        disabled={statusLoading}
                        onClick={() => handleToggleStatus(pipeline)}
                      >
                        {isDisabled ? 'Enable' : 'Disable'}
                      </Button>
                      <Button
                        variant="destructive"
                        size="sm"
                        disabled={deleteLoading}
                        onClick={() => handleDelete(pipeline.name)}
                      >
                        Delete
                      </Button>
                    </div>
                  </div>

                  {isExpanded && (
                    <div
                      id={`runs-${pipeline.name}-${pipeline.spec_version}`}
                      className="border-t border-border bg-muted/20 px-5 py-4 space-y-3"
                    >
                      {!history || history.loading ? (
                        <p className="text-xs text-muted-foreground">Loading run history…</p>
                      ) : history.error ? (
                        <p className="text-xs text-destructive">{history.error}</p>
                      ) : history.runs.length === 0 ? (
                        <p className="text-xs text-muted-foreground">No runs yet for this pipeline.</p>
                      ) : (
                        <div>
                          <div className="grid grid-cols-[6rem_1fr_1fr_1fr_1fr_auto] gap-2 px-2 pb-2 text-xs uppercase tracking-wider text-muted-foreground border-b border-border">
                            <span>Run ID</span>
                            <span>Status</span>
                            <span>Trigger</span>
                            <span>Started</span>
                            <span>Duration</span>
                            <span />
                          </div>
                          {history.runs.map((run) => {
                            const isRunExpanded = expandedRuns.has(run.id);
                            const tableState = tableExecutions[run.id];
                            return (
                              <div key={run.id}>
                                <div className="grid grid-cols-[6rem_1fr_1fr_1fr_1fr_auto] gap-2 items-center px-2 py-2.5 text-sm border-b border-border/50 last:border-0">
                                  <span className="font-mono text-xs text-muted-foreground">{run.id.slice(0, 8)}</span>
                                  <span>
                                    <Badge variant={runStatusVariant(run.status)}>{run.status}</Badge>
                                  </span>
                                  <span className="text-xs text-muted-foreground">{run.trigger_mode}</span>
                                  <span className="text-xs text-muted-foreground">
                                    {formatTimestamp(run.started_at)}
                                  </span>
                                  <span className="text-xs text-muted-foreground">
                                    {formatDuration(run.started_at, run.finished_at)}
                                  </span>
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    aria-expanded={isRunExpanded}
                                    aria-controls={`tables-${run.id}`}
                                    onClick={() => handleToggleTables(run.id)}
                                  >
                                    {isRunExpanded ? 'Hide tables' : 'Tables'}
                                  </Button>
                                </div>

                                {isRunExpanded && (
                                  <div id={`tables-${run.id}`} className="mx-2 mb-2 rounded-md border border-border/50 bg-background/40">
                                    {!tableState || tableState.loading ? (
                                      <p className="p-3 text-xs text-muted-foreground">Loading table executions…</p>
                                    ) : tableState.error ? (
                                      <p className="p-3 text-xs text-destructive">{tableState.error}</p>
                                    ) : tableState.tables.length === 0 ? (
                                      <p className="p-3 text-xs text-muted-foreground">
                                        No table executions recorded for this run.
                                      </p>
                                    ) : (
                                      <div className="divide-y divide-border/50">
                                        <div className="grid grid-cols-[2fr_auto_9rem_2fr] gap-2 px-3 py-2 text-xs uppercase tracking-wider text-muted-foreground">
                                          <span>Stream</span>
                                          <span>Status</span>
                                          <span>Progress</span>
                                          <span>Error</span>
                                        </div>
                                        {tableState.tables.map((table) => (
                                          <div
                                            key={table.id}
                                            className="grid grid-cols-[2fr_auto_9rem_2fr] gap-2 items-center px-3 py-2 text-xs"
                                          >
                                            <span className="font-mono text-muted-foreground">
                                              {table.stream_name}
                                            </span>
                                            <span>
                                              <Badge variant={tableStatusVariant(table.status)}>{table.status}</Badge>
                                            </span>
                                            <span className="text-muted-foreground">
                                              {formatRowProgress(table.rows_processed, table.rows_total)}
                                            </span>
                                            <span
                                              className={
                                                table.error_summary
                                                  ? 'text-destructive truncate cursor-default'
                                                  : 'text-muted-foreground'
                                              }
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
                </div>
              );
            })}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
