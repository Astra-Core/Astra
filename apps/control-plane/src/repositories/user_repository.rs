use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct LdapGroupMapping {
    pub id: Uuid,
    pub ldap_group: String,
    pub astra_role: String,
    pub created_at: DateTime<Utc>,
}

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
    /// Insert an LDAP user if one does not already exist; return the existing
    /// record if found. LDAP users have `password_hash = NULL`.
    async fn upsert_ldap_user(&self, email: &str) -> anyhow::Result<UserRecord>;
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
    /// Return the Astra roles mapped to the given LDAP groups.
    async fn get_ldap_group_mappings(&self, ldap_groups: &[String]) -> anyhow::Result<Vec<String>>;

    // ── LDAP group mapping admin ─────────────────────────────────────────────

    /// List all LDAP group → Astra role mappings.
    async fn list_ldap_group_mappings(&self) -> anyhow::Result<Vec<LdapGroupMapping>>;

    /// Add a new mapping. Returns an error if the `(ldap_group, astra_role)` pair already exists.
    async fn add_ldap_group_mapping(
        &self,
        ldap_group: &str,
        astra_role: &str,
    ) -> anyhow::Result<LdapGroupMapping>;

    /// Remove a mapping by its ID.
    async fn delete_ldap_group_mapping(&self, id: Uuid) -> anyhow::Result<()>;
}
