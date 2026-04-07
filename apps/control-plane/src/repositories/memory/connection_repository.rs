use crate::repositories::connection_repository::{
    ConnectionRepository, CreateSavedConnectionInput, SavedConnectionRecord,
};
use anyhow::Context;
use async_trait::async_trait;
use chrono::Utc;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Default, Clone)]
pub struct InMemoryConnectionRepository {
    inner: Arc<RwLock<HashMap<String, SavedConnectionRecord>>>,
}

#[async_trait]
impl ConnectionRepository for InMemoryConnectionRepository {
    async fn list_connections(&self) -> anyhow::Result<Vec<SavedConnectionRecord>> {
        let guard = self.inner.read().await;
        let mut records: Vec<_> = guard.values().cloned().collect();
        records.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(records)
    }

    async fn get_by_name(&self, name: &str) -> anyhow::Result<Option<SavedConnectionRecord>> {
        let guard = self.inner.read().await;
        Ok(guard.get(name).cloned())
    }

    async fn create(
        &self,
        input: CreateSavedConnectionInput,
    ) -> anyhow::Result<SavedConnectionRecord> {
        let mut guard = self.inner.write().await;
        if guard.contains_key(&input.name) {
            anyhow::bail!("connection '{}' already exists", input.name);
        }
        let now = Utc::now();
        let record = SavedConnectionRecord {
            id: Uuid::new_v4(),
            name: input.name.clone(),
            kind: input.kind,
            config_json: input.config_json,
            secret_ref: input.secret_ref,
            created_by: input.created_by,
            created_at: now,
            updated_at: now,
        };
        guard.insert(input.name, record.clone());
        Ok(record)
    }

    async fn update(
        &self,
        name: &str,
        input: CreateSavedConnectionInput,
    ) -> anyhow::Result<SavedConnectionRecord> {
        let mut guard = self.inner.write().await;
        let existing = guard
            .get_mut(name)
            .with_context(|| format!("connection '{name}' not found"))?;
        existing.kind = input.kind;
        existing.config_json = input.config_json;
        existing.secret_ref = input.secret_ref;
        existing.updated_at = Utc::now();
        Ok(existing.clone())
    }

    async fn delete(&self, name: &str) -> anyhow::Result<()> {
        let mut guard = self.inner.write().await;
        if guard.remove(name).is_none() {
            anyhow::bail!("connection '{name}' not found");
        }
        Ok(())
    }
}
