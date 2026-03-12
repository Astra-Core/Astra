use crate::repositories::{AppliedPipelineRecord, PipelineRecord, PipelineRepository};
use anyhow::Context;
use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{types::Json, Client, NoTls};
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
                "#,
            )
            .await
            .context("failed to bootstrap pipeline persistence schema")?;
        Ok(())
    }
}

#[async_trait]
impl PipelineRepository for PostgresPipelineRepository {
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
}

fn hash_content(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}
