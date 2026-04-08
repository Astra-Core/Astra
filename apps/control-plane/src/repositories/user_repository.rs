use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct UserRecord {
    pub id: Uuid,
    pub email: String,
    pub password_hash: Option<String>,
    pub auth_source: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RefreshTokenRecord {
    pub id: Uuid,
    pub user_id: Uuid,
    pub token_hash: String,
    pub expires_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn find_by_email(&self, email: &str) -> anyhow::Result<Option<UserRecord>>;
    async fn find_by_id(&self, id: Uuid) -> anyhow::Result<Option<UserRecord>>;
    async fn create_user(
        &self,
        email: &str,
        password_hash: Option<&str>,
        auth_source: &str,
    ) -> anyhow::Result<UserRecord>;
    async fn list_users(&self) -> anyhow::Result<Vec<UserRecord>>;
    async fn delete_user(&self, id: Uuid) -> anyhow::Result<()>;
    async fn store_refresh_token(
        &self,
        user_id: Uuid,
        token_hash: &str,
        expires_at: DateTime<Utc>,
    ) -> anyhow::Result<()>;
    async fn find_refresh_token(
        &self,
        token_hash: &str,
    ) -> anyhow::Result<Option<RefreshTokenRecord>>;
    async fn revoke_refresh_token(&self, token_hash: &str) -> anyhow::Result<()>;
}
