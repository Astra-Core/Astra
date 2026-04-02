use crate::{
    error::NotFoundError,
    repositories::{
        AppliedPipelineRecord, CreatePipelineRunRecord, PipelineRecord, PipelineRepository,
        PipelineRunRecord, RecordStagedArtifactRecord, StagedArtifactRecord, TableExecutionRecord,
        UpsertTableExecutionRecord,
    },
};
use anyhow::Context;
use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{types::Json, Client, NoTls, Row};
use uuid::Uuid;

#[derive(Clone)]
pub struct PostgresPipelineRepository {
    client: Arc<Mutex<Client>>,
}

impl PostgresPipelineRepository {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls)
            .await
            .with_context(|| {
                format!("failed to connect to Postgres using ASTRA_DATABASE_URL: {database_url}")
            })?;

        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(?error, "postgres connection task exited");
            }
        });

        let repository = Self {
            client: Arc::new(Mutex::new(client)),
        };
        repository.ensure_schema().await?;
        Ok(repository)
    }

    async fn ensure_schema(&self) -> anyhow::Result<()> {
        self.client
            .lock()
            .await
            .batch_execute(
                r#"
                CREATE TABLE IF NOT EXISTS pipelines (
                    id UUID PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    source_kind TEXT NOT NULL,
                    destination_kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    active_spec_id UUID,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS pipeline_specs (
                    id UUID PRIMARY KEY,
                    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
                    version INTEGER NOT NULL,
                    spec_version TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    spec_yaml TEXT NOT NULL,
                    spec_json JSONB NOT NULL,
                    created_by TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (pipeline_id, version),
                    UNIQUE (pipeline_id, content_hash)
                );

                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id UUID PRIMARY KEY,
                    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
                    trigger_mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    worker_id TEXT,
                    started_at TIMESTAMPTZ NOT NULL,
                    finished_at TIMESTAMPTZ,
                    stats_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_started_at
                    ON pipeline_runs (pipeline_id, started_at DESC);

                CREATE TABLE IF NOT EXISTS staged_artifacts (
                    id UUID PRIMARY KEY,
                    pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
                    stream_name TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    sequence BIGINT NOT NULL,
                    bucket TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    bytes_written BIGINT NOT NULL,
                    row_count BIGINT NOT NULL,
                    content_type TEXT NOT NULL,
                    content_encoding TEXT NOT NULL,
                    schema_fingerprint TEXT,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (pipeline_run_id, object_key),
                    UNIQUE (pipeline_run_id, stream_name, partition_key, sequence)
                );

                CREATE INDEX IF NOT EXISTS idx_staged_artifacts_run_stream_sequence
                    ON staged_artifacts (pipeline_run_id, stream_name, partition_key, sequence);

                CREATE TABLE IF NOT EXISTS table_executions (
                    id UUID PRIMARY KEY,
                    pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
                    stream_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    rows_processed BIGINT NOT NULL DEFAULT 0,
                    rows_total BIGINT,
                    error_summary TEXT,
                    checkpoint_next_sequence BIGINT,
                    checkpoint_rows_staged BIGINT,
                    checkpoint_last_chunk_key TEXT,
                    checkpoint_completed BOOLEAN NOT NULL DEFAULT FALSE,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(pipeline_run_id, stream_name)
                );

                ALTER TABLE table_executions
                    ADD COLUMN IF NOT EXISTS checkpoint_next_sequence BIGINT,
                    ADD COLUMN IF NOT EXISTS checkpoint_rows_staged BIGINT,
                    ADD COLUMN IF NOT EXISTS checkpoint_last_chunk_key TEXT,
                    ADD COLUMN IF NOT EXISTS checkpoint_completed BOOLEAN NOT NULL DEFAULT FALSE;

                CREATE INDEX IF NOT EXISTS idx_table_executions_run
                    ON table_executions (pipeline_run_id);
                "#,
            )
            .await
            .context("failed to bootstrap pipeline persistence schema")?;
        Ok(())
    }
}

#[async_trait]
impl PipelineRepository for PostgresPipelineRepository {
    async fn get_pipeline_yaml(&self, pipeline_name: &str) -> anyhow::Result<Option<String>> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                r#"
            SELECT ps.spec_yaml
            FROM pipelines p
            JOIN pipeline_specs ps ON ps.id = p.active_spec_id
            WHERE p.name = $1
            "#,
                &[&pipeline_name],
            )
            .await
            .context("failed to get pipeline yaml")?;
        Ok(row.map(|r| r.get("spec_yaml")))
    }

    async fn update_pipeline_run_status(
        &self,
        run_id: Uuid,
        status: String,
        stats_json: serde_json::Value,
    ) -> anyhow::Result<PipelineRunRecord> {
        let client = self.client.lock().await;
        let row = client.query_one(
            r#"
            UPDATE pipeline_runs pr
            SET status = $1,
                stats_json = $2,
                finished_at = CASE WHEN $1 IN ('succeeded','failed','error','cancelled') THEN NOW() ELSE pr.finished_at END,
                updated_at = NOW()
            FROM pipelines p
            WHERE pr.id = $3 AND pr.pipeline_id = p.id
            RETURNING pr.id, p.name AS pipeline_name, pr.trigger_mode, pr.status, pr.worker_id,
                       pr.started_at, pr.finished_at, pr.created_at, pr.updated_at, pr.stats_json
            "#,
            &[&status, &tokio_postgres::types::Json(&stats_json), &run_id],
        ).await.context("failed to update run status")?;
        Ok(map_pipeline_run_row(row))
    }
    async fn list_pipelines(&self) -> anyhow::Result<Vec<PipelineRecord>> {
        let rows = self
            .client
            .lock()
            .await
            .query(
                r#"
                SELECT p.name, p.source_kind, p.destination_kind, p.status, COALESCE(ps.version, 0) AS spec_version
                FROM pipelines p
                LEFT JOIN pipeline_specs ps ON ps.id = p.active_spec_id
                ORDER BY p.name ASC
                "#,
                &[],
            )
            .await
            .context("failed to list pipelines from Postgres")?;

        Ok(rows
            .into_iter()
            .map(|row| PipelineRecord {
                name: row.get("name"),
                source_kind: row.get("source_kind"),
                destination_kind: row.get("destination_kind"),
                status: row.get("status"),
                spec_version: row.get("spec_version"),
            })
            .collect())
    }

    async fn apply_spec(
        &self,
        spec: astra_yaml::AstraSpec,
        raw_yaml: String,
        created_by: Option<String>,
    ) -> anyhow::Result<AppliedPipelineRecord> {
        let pipeline_name = spec.pipeline.name.clone();
        let source_kind = spec.source.kind.clone();
        let destination_kind = spec.destination.kind.clone();
        let status = "active".to_string();
        let spec_version_label = spec.version.clone();
        let spec_model: Value =
            serde_json::to_value(&spec).context("failed to serialize normalized spec JSON")?;
        let content_hash = hash_content(&raw_yaml);

        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .context("failed to start Postgres transaction")?;

        let existing = transaction
            .query_opt(
                r#"
                SELECT p.id, COALESCE(ps.version, 0) AS active_version, ps.content_hash
                FROM pipelines p
                LEFT JOIN pipeline_specs ps ON ps.id = p.active_spec_id
                WHERE p.name = $1
                "#,
                &[&pipeline_name],
            )
            .await
            .context("failed to load existing pipeline row")?;

        let (pipeline_id, next_version) = if let Some(row) = existing {
            let pipeline_id: Uuid = row.get("id");
            let active_version: i32 = row.get("active_version");
            let active_hash: Option<String> = row.get("content_hash");

            if active_hash.as_deref() == Some(content_hash.as_str()) {
                transaction
                    .commit()
                    .await
                    .context("failed to commit no-op Postgres apply")?;
                return Ok(AppliedPipelineRecord {
                    pipeline: PipelineRecord {
                        name: pipeline_name,
                        source_kind,
                        destination_kind,
                        status,
                        spec_version: active_version,
                    },
                    content_hash,
                });
            }

            (pipeline_id, active_version + 1)
        } else {
            let pipeline_id = Uuid::new_v4();
            transaction
                .execute(
                    r#"
                    INSERT INTO pipelines (id, name, source_kind, destination_kind, status)
                    VALUES ($1, $2, $3, $4, $5)
                    "#,
                    &[
                        &pipeline_id,
                        &pipeline_name,
                        &source_kind,
                        &destination_kind,
                        &status,
                    ],
                )
                .await
                .context("failed to insert pipeline row")?;
            (pipeline_id, 1)
        };

        let spec_id = Uuid::new_v4();
        transaction
            .execute(
                r#"
                INSERT INTO pipeline_specs (id, pipeline_id, version, spec_version, content_hash, spec_yaml, spec_json, created_by)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
                &[
                    &spec_id,
                    &pipeline_id,
                    &next_version,
                    &spec_version_label,
                    &content_hash,
                    &raw_yaml,
                    &Json(&spec_model),
                    &created_by,
                ],
            )
            .await
            .context("failed to insert pipeline spec row")?;

        transaction
            .execute(
                r#"
                UPDATE pipelines
                SET source_kind = $2,
                    destination_kind = $3,
                    status = $4,
                    active_spec_id = $5,
                    updated_at = NOW()
                WHERE id = $1
                "#,
                &[
                    &pipeline_id,
                    &source_kind,
                    &destination_kind,
                    &status,
                    &spec_id,
                ],
            )
            .await
            .context("failed to update pipeline row")?;

        transaction
            .commit()
            .await
            .context("failed to commit Postgres apply")?;

        Ok(AppliedPipelineRecord {
            pipeline: PipelineRecord {
                name: pipeline_name,
                source_kind,
                destination_kind,
                status,
                spec_version: next_version,
            },
            content_hash,
        })
    }

    async fn create_pipeline_run(
        &self,
        run: CreatePipelineRunRecord,
    ) -> anyhow::Result<PipelineRunRecord> {
        let row = self
            .client
            .lock()
            .await
            .query_one(
                r#"
                INSERT INTO pipeline_runs (id, pipeline_id, trigger_mode, status, worker_id, started_at)
                SELECT $1, p.id, $2, $3, $4, $5
                FROM pipelines p
                WHERE p.name = $6
                RETURNING id, $6::text AS pipeline_name, trigger_mode, status, worker_id, started_at, finished_at, created_at, updated_at
                "#,
                &[
                    &Uuid::new_v4(),
                    &run.trigger_mode,
                    &run.status,
                    &run.worker_id,
                    &run.started_at,
                    &run.pipeline_name,
                ],
            )
            .await
            .with_context(|| format!("failed to create pipeline run for '{}'", run.pipeline_name))?;

        Ok(map_pipeline_run_row(row))
    }

    async fn list_pipeline_runs(
        &self,
        pipeline_name: &str,
    ) -> anyhow::Result<Vec<PipelineRunRecord>> {
        let rows = self
            .client
            .lock()
            .await
            .query(
                r#"
                SELECT pr.id, p.name AS pipeline_name, pr.trigger_mode, pr.status, pr.worker_id,
                       pr.started_at, pr.finished_at, pr.created_at, pr.updated_at
                FROM pipeline_runs pr
                INNER JOIN pipelines p ON p.id = pr.pipeline_id
                WHERE p.name = $1
                ORDER BY pr.started_at DESC, pr.created_at DESC
                "#,
                &[&pipeline_name],
            )
            .await
            .with_context(|| format!("failed to list runs for pipeline '{}'", pipeline_name))?;

        Ok(rows.into_iter().map(map_pipeline_run_row).collect())
    }

    async fn get_latest_run(
        &self,
        pipeline_name: &str,
    ) -> anyhow::Result<Option<PipelineRunRecord>> {
        let row = self
            .client
            .lock()
            .await
            .query_opt(
                r#"
                SELECT pr.id, p.name AS pipeline_name, pr.trigger_mode, pr.status, pr.worker_id,
                       pr.started_at, pr.finished_at, pr.created_at, pr.updated_at
                FROM pipeline_runs pr
                INNER JOIN pipelines p ON p.id = pr.pipeline_id
                WHERE p.name = $1
                ORDER BY pr.started_at DESC, pr.created_at DESC
                LIMIT 1
                "#,
                &[&pipeline_name],
            )
            .await
            .with_context(|| {
                format!("failed to get latest run for pipeline '{}'", pipeline_name)
            })?;

        Ok(row.map(map_pipeline_run_row))
    }

    async fn get_run_history(
        &self,
        pipeline_name: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<PipelineRunRecord>> {
        let rows = self
            .client
            .lock()
            .await
            .query(
                r#"
                SELECT pr.id, p.name AS pipeline_name, pr.trigger_mode, pr.status, pr.worker_id,
                       pr.started_at, pr.finished_at, pr.created_at, pr.updated_at
                FROM pipeline_runs pr
                INNER JOIN pipelines p ON p.id = pr.pipeline_id
                WHERE p.name = $1
                ORDER BY pr.started_at DESC, pr.created_at DESC
                LIMIT $2
                "#,
                &[&pipeline_name, &(limit as i64)],
            )
            .await
            .with_context(|| {
                format!("failed to get run history for pipeline '{}'", pipeline_name)
            })?;

        Ok(rows.into_iter().map(map_pipeline_run_row).collect())
    }

    async fn record_staged_artifact(
        &self,
        artifact: RecordStagedArtifactRecord,
    ) -> anyhow::Result<StagedArtifactRecord> {
        let row = self
            .client
            .lock()
            .await
            .query_one(
                r#"
                INSERT INTO staged_artifacts (
                    id, pipeline_run_id, stream_name, partition_key, sequence, bucket, object_key,
                    bytes_written, row_count, content_type, content_encoding, schema_fingerprint, metadata_json
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                RETURNING id, pipeline_run_id, stream_name, partition_key, sequence, bucket, object_key,
                          bytes_written, row_count, content_type, content_encoding, schema_fingerprint,
                          metadata_json, created_at
                "#,
                &[
                    &Uuid::new_v4(),
                    &artifact.pipeline_run_id,
                    &artifact.stream_name,
                    &artifact.partition_key,
                    &artifact.sequence,
                    &artifact.bucket,
                    &artifact.object_key,
                    &artifact.bytes_written,
                    &artifact.row_count,
                    &artifact.content_type,
                    &artifact.content_encoding,
                    &artifact.schema_fingerprint,
                    &Json(&artifact.metadata_json),
                ],
            )
            .await
            .with_context(|| format!("failed to record staged artifact for run '{}'", artifact.pipeline_run_id))?;

        Ok(map_staged_artifact_row(row))
    }

    async fn list_staged_artifacts(
        &self,
        pipeline_run_id: Uuid,
    ) -> anyhow::Result<Vec<StagedArtifactRecord>> {
        let rows = self
            .client
            .lock()
            .await
            .query(
                r#"
                SELECT id, pipeline_run_id, stream_name, partition_key, sequence, bucket, object_key,
                       bytes_written, row_count, content_type, content_encoding, schema_fingerprint,
                       metadata_json, created_at
                FROM staged_artifacts
                WHERE pipeline_run_id = $1
                ORDER BY stream_name ASC, partition_key ASC, sequence ASC
                "#,
                &[&pipeline_run_id],
            )
            .await
            .with_context(|| format!("failed to list staged artifacts for run '{}'", pipeline_run_id))?;

        Ok(rows.into_iter().map(map_staged_artifact_row).collect())
    }

    async fn list_table_executions(
        &self,
        pipeline_run_id: Uuid,
    ) -> anyhow::Result<Vec<TableExecutionRecord>> {
        let rows = self
            .client
            .lock()
            .await
            .query(
                r#"
                SELECT id, pipeline_run_id, stream_name, status, rows_processed, rows_total,
                       error_summary, checkpoint_next_sequence, checkpoint_rows_staged,
                       checkpoint_last_chunk_key, checkpoint_completed,
                       started_at, finished_at, updated_at
                FROM table_executions
                WHERE pipeline_run_id = $1
                ORDER BY stream_name ASC
                "#,
                &[&pipeline_run_id],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to list table executions for run '{}'",
                    pipeline_run_id
                )
            })?;

        Ok(rows.into_iter().map(map_table_execution_row).collect())
    }

    async fn upsert_table_execution(
        &self,
        record: UpsertTableExecutionRecord,
    ) -> anyhow::Result<TableExecutionRecord> {
        let client = self.client.lock().await;
        let row = client.query_one(
            r#"
            INSERT INTO table_executions (
                id, pipeline_run_id, stream_name, status, rows_processed, rows_total, error_summary,
                checkpoint_next_sequence, checkpoint_rows_staged, checkpoint_last_chunk_key, checkpoint_completed
            )
            VALUES (
                gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8, $9, COALESCE($10, FALSE)
            )
            ON CONFLICT (pipeline_run_id, stream_name)
            DO UPDATE SET
                status = EXCLUDED.status,
                rows_processed = EXCLUDED.rows_processed,
                rows_total = EXCLUDED.rows_total,
                error_summary = EXCLUDED.error_summary,
                checkpoint_next_sequence = EXCLUDED.checkpoint_next_sequence,
                checkpoint_rows_staged = EXCLUDED.checkpoint_rows_staged,
                checkpoint_last_chunk_key = EXCLUDED.checkpoint_last_chunk_key,
                checkpoint_completed = EXCLUDED.checkpoint_completed,
                finished_at = CASE WHEN EXCLUDED.status IN ('staged', 'applied', 'failed', 'snapshot_complete') THEN NOW() ELSE table_executions.finished_at END,
                updated_at = NOW()
            RETURNING id, pipeline_run_id, stream_name, status, rows_processed, rows_total,
                      error_summary, checkpoint_next_sequence, checkpoint_rows_staged,
                      checkpoint_last_chunk_key, checkpoint_completed,
                      started_at, finished_at, updated_at
            "#,
            &[
                &record.pipeline_run_id,
                &record.stream_name,
                &record.status,
                &record.rows_processed,
                &record.rows_total,
                &record.error_summary,
                &record.checkpoint_next_sequence,
                &record.checkpoint_rows_staged,
                &record.checkpoint_last_chunk_key,
                &record.checkpoint_completed,
            ],
        )
        .await
        .with_context(|| format!("failed to upsert table execution for run '{}' table '{}'", record.pipeline_run_id, record.stream_name))?;

        Ok(map_table_execution_row(row))
    }

    async fn delete_pipeline(&self, pipeline_name: &str) -> anyhow::Result<()> {
        let rows_affected = self
            .client
            .lock()
            .await
            .execute("DELETE FROM pipelines WHERE name = $1", &[&pipeline_name])
            .await
            .with_context(|| format!("failed to delete pipeline '{}'", pipeline_name))?;

        if rows_affected == 0 {
            return Err(anyhow::Error::new(NotFoundError(pipeline_name.to_string())));
        }
        Ok(())
    }

    async fn update_pipeline_status(
        &self,
        pipeline_name: &str,
        status: &str,
    ) -> anyhow::Result<PipelineRecord> {
        let row = self
            .client
            .lock()
            .await
            .query_opt(
                r#"
                UPDATE pipelines
                SET status = $1, updated_at = NOW()
                WHERE name = $2
                RETURNING name, source_kind, destination_kind, status,
                          (SELECT version FROM pipeline_specs WHERE id = active_spec_id) AS spec_version
                "#,
                &[&status, &pipeline_name],
            )
            .await
            .with_context(|| format!("failed to update status for pipeline '{}'", pipeline_name))?;

        match row {
            Some(r) => Ok(PipelineRecord {
                name: r.get("name"),
                source_kind: r.get("source_kind"),
                destination_kind: r.get("destination_kind"),
                status: r.get("status"),
                spec_version: r.get::<_, Option<i32>>("spec_version").unwrap_or(0),
            }),
            None => Err(anyhow::Error::new(NotFoundError(pipeline_name.to_string()))),
        }
    }
}

fn map_table_execution_row(row: Row) -> TableExecutionRecord {
    TableExecutionRecord {
        id: row.get("id"),
        pipeline_run_id: row.get("pipeline_run_id"),
        stream_name: row.get("stream_name"),
        status: row.get("status"),
        rows_processed: row.get("rows_processed"),
        rows_total: row.get("rows_total"),
        error_summary: row.get("error_summary"),
        checkpoint_next_sequence: row.get("checkpoint_next_sequence"),
        checkpoint_rows_staged: row.get("checkpoint_rows_staged"),
        checkpoint_last_chunk_key: row.get("checkpoint_last_chunk_key"),
        checkpoint_completed: row.get("checkpoint_completed"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
        updated_at: row.get("updated_at"),
    }
}

fn map_pipeline_run_row(row: Row) -> PipelineRunRecord {
    let stats_json: Option<serde_json::Value> = row
        .try_get::<_, Option<tokio_postgres::types::Json<serde_json::Value>>>("stats_json")
        .ok()
        .flatten()
        .map(|j| j.0);
    PipelineRunRecord {
        id: row.get("id"),
        pipeline_name: row.get("pipeline_name"),
        trigger_mode: row.get("trigger_mode"),
        status: row.get("status"),
        worker_id: row.get("worker_id"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        stats_json,
    }
}

fn map_staged_artifact_row(row: Row) -> StagedArtifactRecord {
    let metadata_json: Json<Value> = row.get("metadata_json");
    StagedArtifactRecord {
        id: row.get("id"),
        pipeline_run_id: row.get("pipeline_run_id"),
        stream_name: row.get("stream_name"),
        partition_key: row.get("partition_key"),
        sequence: row.get("sequence"),
        bucket: row.get("bucket"),
        object_key: row.get("object_key"),
        bytes_written: row.get("bytes_written"),
        row_count: row.get("row_count"),
        content_type: row.get("content_type"),
        content_encoding: row.get("content_encoding"),
        schema_fingerprint: row.get("schema_fingerprint"),
        metadata_json: metadata_json.0,
        created_at: row.get("created_at"),
    }
}

fn hash_content(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}
