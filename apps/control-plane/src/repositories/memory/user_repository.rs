use crate::repositories::user_repository::{RefreshTokenRecord, UserRecord, UserRepository};
use anyhow::Context;
use async_trait::async_trait;
use chrono::Utc;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Default, Clone)]
pub struct InMemoryUserRepository {
    users: Arc<RwLock<HashMap<Uuid, UserRecord>>>,
    tokens: Arc<RwLock<HashMap<String, RefreshTokenRecord>>>,
}

#[async_trait]
impl UserRepository for InMemoryUserRepository {
    async fn find_by_email(&self, email: &str) -> anyhow::Result<Option<UserRecord>> {
        let guard = self.users.read().await;
        Ok(guard.values().find(|u| u.email == email).cloned())
    }

    async fn find_by_id(&self, id: Uuid) -> anyhow::Result<Option<UserRecord>> {
        let guard = self.users.read().await;
        Ok(guard.get(&id).cloned())
    }

    async fn create_user(
        &self,
        email: &str,
        password_hash: Option<&str>,
        auth_source: &str,
    ) -> anyhow::Result<UserRecord> {
        let mut guard = self.users.write().await;
        if guard.values().any(|u| u.email == email) {
            anyhow::bail!("user with email '{}' already exists", email);
        }
        let now = Utc::now();
        let record = UserRecord {
            id: Uuid::new_v4(),
            email: email.to_string(),
            password_hash: password_hash.map(str::to_string),
            auth_source: auth_source.to_string(),
            created_at: now,
            updated_at: now,
        };
        guard.insert(record.id, record.clone());
        Ok(record)
    }

    async fn list_users(&self) -> anyhow::Result<Vec<UserRecord>> {
        let guard = self.users.read().await;
        let mut records: Vec<_> = guard.values().cloned().collect();
        records.sort_by_key(|u| u.created_at);
        Ok(records)
    }

    async fn delete_user(&self, id: Uuid) -> anyhow::Result<()> {
        let mut guard = self.users.write().await;
        if guard.remove(&id).is_none() {
            anyhow::bail!("user {id} not found");
        }
        Ok(())
    }

    async fn store_refresh_token(
        &self,
        user_id: Uuid,
        token_hash: &str,
        expires_at: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        let mut guard = self.tokens.write().await;
        let now = Utc::now();
        let record = RefreshTokenRecord {
            id: Uuid::new_v4(),
            user_id,
            token_hash: token_hash.to_string(),
            expires_at,
            revoked_at: None,
            created_at: now,
        };
        guard.insert(token_hash.to_string(), record);
        Ok(())
    }

    async fn find_refresh_token(
        &self,
        token_hash: &str,
    ) -> anyhow::Result<Option<RefreshTokenRecord>> {
        let guard = self.tokens.read().await;
        Ok(guard.get(token_hash).cloned())
    }

    async fn revoke_refresh_token(&self, token_hash: &str) -> anyhow::Result<()> {
        let mut guard = self.tokens.write().await;
        let record = guard
            .get_mut(token_hash)
            .with_context(|| "refresh token not found".to_string())?;
        record.revoked_at = Some(Utc::now());
        Ok(())
    }
}
