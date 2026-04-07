use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct SavedConnectionRecord {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub config_json: Value,
    pub secret_ref: Option<String>,
    pub created_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CreateSavedConnectionInput {
    pub name: String,
    pub kind: String,
    pub config_json: Value,
    pub secret_ref: Option<String>,
    pub created_by: Option<String>,
}

#[async_trait]
pub trait ConnectionRepository: Send + Sync {
    async fn list_connections(&self) -> anyhow::Result<Vec<SavedConnectionRecord>>;
    async fn get_by_name(&self, name: &str) -> anyhow::Result<Option<SavedConnectionRecord>>;
    async fn create(
        &self,
        input: CreateSavedConnectionInput,
    ) -> anyhow::Result<SavedConnectionRecord>;
    async fn update(
        &self,
        name: &str,
        input: CreateSavedConnectionInput,
    ) -> anyhow::Result<SavedConnectionRecord>;
    async fn delete(&self, name: &str) -> anyhow::Result<()>;
}
