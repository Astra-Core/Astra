use crate::repositories::{AppliedPipelineRecord, PipelineRecord, PipelineRepository};
use async_trait::async_trait;
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Default, Clone)]
pub struct InMemoryPipelineRepository {
    inner: Arc<RwLock<HashMap<String, StoredPipeline>>>,
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
}

fn hash_content(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}
