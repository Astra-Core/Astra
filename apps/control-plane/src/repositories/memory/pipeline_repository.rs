use crate::repositories::{
    AppliedPipelineRecord, CreatePipelineRunRecord, PipelineRecord, PipelineRepository,
    PipelineRunRecord, RecordStagedArtifactRecord, StagedArtifactRecord,
};
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Default, Clone)]
pub struct InMemoryPipelineRepository {
    inner: Arc<RwLock<HashMap<String, StoredPipeline>>>,
    runs: Arc<RwLock<HashMap<Uuid, PipelineRunRecord>>>,
    artifacts: Arc<RwLock<HashMap<Uuid, Vec<StagedArtifactRecord>>>>,
}

#[derive(Clone)]
struct StoredPipeline {
    record: PipelineRecord,
    latest_hash: String,
    #[allow(dead_code)]
    pipeline_id: Uuid,
    #[allow(dead_code)]
    source_id: Uuid,
    #[allow(dead_code)]
    destination_id: Uuid,
    #[allow(dead_code)]
    active_spec_id: Uuid,
    #[allow(dead_code)]
    raw_yaml: String,
    #[allow(dead_code)]
    created_by: Option<String>,
    #[allow(dead_code)]
    updated_at: chrono::DateTime<Utc>,
}

#[async_trait]
impl PipelineRepository for InMemoryPipelineRepository {
    async fn list_pipelines(&self) -> anyhow::Result<Vec<PipelineRecord>> {
        let guard = self.inner.read().await;
        let mut items: Vec<_> = guard.values().map(|x| x.record.clone()).collect();
        items.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(items)
    }

    async fn apply_spec(
        &self,
        spec: astra_yaml::AstraSpec,
        raw_yaml: String,
        created_by: Option<String>,
    ) -> anyhow::Result<AppliedPipelineRecord> {
        let mut guard = self.inner.write().await;
        let name = spec.pipeline.name.clone();
        let content_hash = hash_content(&raw_yaml);

        let next_version = match guard.get(&name) {
            Some(existing) if existing.latest_hash == content_hash => existing.record.spec_version,
            Some(existing) => existing.record.spec_version + 1,
            None => 1,
        };

        let record = PipelineRecord {
            name: name.clone(),
            source_kind: spec.source.kind.clone(),
            destination_kind: spec.destination.kind.clone(),
            status: "active".to_string(),
            spec_version: next_version,
        };

        let stored = StoredPipeline {
            record: record.clone(),
            latest_hash: content_hash.clone(),
            pipeline_id: Uuid::new_v4(),
            source_id: Uuid::new_v4(),
            destination_id: Uuid::new_v4(),
            active_spec_id: Uuid::new_v4(),
            raw_yaml,
            created_by,
            updated_at: Utc::now(),
        };
        guard.insert(name, stored);

        Ok(AppliedPipelineRecord {
            pipeline: record,
            content_hash,
        })
    }

    async fn create_pipeline_run(
        &self,
        run: CreatePipelineRunRecord,
    ) -> anyhow::Result<PipelineRunRecord> {
        let pipelines = self.inner.read().await;
        if !pipelines.contains_key(&run.pipeline_name) {
            return Err(anyhow!("pipeline '{}' does not exist", run.pipeline_name));
        }
        drop(pipelines);

        let now = Utc::now();
        let record = PipelineRunRecord {
            id: Uuid::new_v4(),
            pipeline_name: run.pipeline_name,
            trigger_mode: run.trigger_mode,
            status: run.status,
            worker_id: run.worker_id,
            started_at: run.started_at,
            finished_at: None,
            created_at: now,
            updated_at: now,
        };

        self.runs.write().await.insert(record.id, record.clone());
        Ok(record)
    }

    async fn list_pipeline_runs(
        &self,
        pipeline_name: &str,
    ) -> anyhow::Result<Vec<PipelineRunRecord>> {
        let mut runs: Vec<_> = self
            .runs
            .read()
            .await
            .values()
            .filter(|run| run.pipeline_name == pipeline_name)
            .cloned()
            .collect();
        runs.sort_by(|a, b| {
            b.started_at
                .cmp(&a.started_at)
                .then_with(|| b.created_at.cmp(&a.created_at))
        });
        Ok(runs)
    }

    async fn record_staged_artifact(
        &self,
        artifact: RecordStagedArtifactRecord,
    ) -> anyhow::Result<StagedArtifactRecord> {
        let runs = self.runs.read().await;
        if !runs.contains_key(&artifact.pipeline_run_id) {
            return Err(anyhow!(
                "pipeline run '{}' does not exist",
                artifact.pipeline_run_id
            ));
        }
        drop(runs);

        let record = StagedArtifactRecord {
            id: Uuid::new_v4(),
            pipeline_run_id: artifact.pipeline_run_id,
            stream_name: artifact.stream_name,
            partition_key: artifact.partition_key,
            sequence: artifact.sequence,
            bucket: artifact.bucket,
            object_key: artifact.object_key,
            bytes_written: artifact.bytes_written,
            row_count: artifact.row_count,
            content_type: artifact.content_type,
            content_encoding: artifact.content_encoding,
            schema_fingerprint: artifact.schema_fingerprint,
            metadata_json: artifact.metadata_json,
            created_at: Utc::now(),
        };

        let mut artifacts = self.artifacts.write().await;
        artifacts
            .entry(record.pipeline_run_id)
            .or_default()
            .push(record.clone());
        Ok(record)
    }

    async fn list_staged_artifacts(
        &self,
        pipeline_run_id: Uuid,
    ) -> anyhow::Result<Vec<StagedArtifactRecord>> {
        let mut artifacts = self
            .artifacts
            .read()
            .await
            .get(&pipeline_run_id)
            .cloned()
            .unwrap_or_default();
        artifacts.sort_by(|a, b| {
            a.stream_name
                .cmp(&b.stream_name)
                .then_with(|| a.partition_key.cmp(&b.partition_key))
                .then_with(|| a.sequence.cmp(&b.sequence))
        });
        Ok(artifacts)
    }
}

fn hash_content(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}
