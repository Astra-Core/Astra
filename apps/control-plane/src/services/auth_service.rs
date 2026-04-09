use crate::{
    auth::{
        decode_access_token, encode_access_token, generate_refresh_token, hash_password,
        verify_password, Claims, ACCESS_TOKEN_TTL_SECS, REFRESH_TOKEN_TTL_DAYS,
    },
    authz::AstraEnforcer,
    ldap::LdapClient,
    repositories::{UserRecord, UserRepository},
};
use anyhow::bail;
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

pub struct AuthService {
    user_repo: Arc<dyn UserRepository>,
    jwt_secret: Vec<u8>,
    /// Present when `ASTRA_LDAP_URL` is configured.
    ldap_client: Option<Arc<LdapClient>>,
    enforcer: Arc<AstraEnforcer>,
}

impl AuthService {
    pub fn new(
        user_repo: Arc<dyn UserRepository>,
        jwt_secret: Vec<u8>,
        ldap_client: Option<Arc<LdapClient>>,
        enforcer: Arc<AstraEnforcer>,
    ) -> Self {
        Self {
            user_repo,
            jwt_secret,
            ldap_client,
            enforcer,
        }
    }

    /// Authenticate a local user.
    ///
    /// Returns `(access_token, refresh_token)` on success. The raw refresh
    /// token is only ever returned here; only its SHA-256 hash is persisted.
    pub async fn login_local(
        &self,
        email: &str,
        password: &str,
    ) -> anyhow::Result<(String, String)> {
        let user = self
            .user_repo
            .find_by_email(email)
            .await?
            .ok_or_else(|| anyhow::anyhow!("invalid email or password"))?;

        if user.auth_source != "local" {
            bail!(
                "user '{}' is not a local user; use LDAP authentication",
                email
            );
        }

        let hash = user
            .password_hash
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("invalid email or password"))?;

        if !verify_password(password, hash)? {
            bail!("invalid email or password");
        }

        let (access_token, refresh_token) = self.issue_tokens(&user).await?;
        Ok((access_token, refresh_token))
    }

    /// Validate a refresh token and issue a new access token.
    pub async fn refresh(&self, refresh_token: &str) -> anyhow::Result<String> {
        let token_hash = sha256_hex(refresh_token);
        let record = self
            .user_repo
            .find_refresh_token(&token_hash)
            .await?
            .ok_or_else(|| anyhow::anyhow!("invalid or expired refresh token"))?;

        if record.revoked_at.is_some() {
            bail!("refresh token has been revoked");
        }
        if record.expires_at < Utc::now() {
            bail!("refresh token has expired");
        }

        let user = self
            .user_repo
            .find_by_id(record.user_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("user not found"))?;

        let now = Utc::now();
        let exp = (now + chrono::Duration::seconds(ACCESS_TOKEN_TTL_SECS)).timestamp();
        let claims = Claims {
            sub: user.id,
            email: user.email,
            exp,
            iat: now.timestamp(),
        };
        encode_access_token(&claims, &self.jwt_secret)
    }

    /// Revoke a refresh token (logout).
    pub async fn logout(&self, refresh_token: &str) -> anyhow::Result<()> {
        let token_hash = sha256_hex(refresh_token);
        self.user_repo.revoke_refresh_token(&token_hash).await
    }

    /// Create a new local user with a hashed password.
    pub async fn create_user(&self, email: &str, password: &str) -> anyhow::Result<UserRecord> {
        let hashed = hash_password(password)?;
        self.user_repo
            .create_user(email, Some(&hashed), "local")
            .await
    }

    pub async fn list_users(&self) -> anyhow::Result<Vec<UserRecord>> {
        self.user_repo.list_users().await
    }

    pub async fn delete_user(&self, id: Uuid) -> anyhow::Result<()> {
        self.user_repo.delete_user(id).await
    }

    /// Authenticate via LDAP, sync group→role mappings, and issue tokens.
    ///
    /// Requires `ASTRA_LDAP_URL` to be configured; returns an error otherwise.
    ///
    /// Steps:
    /// 1. Authenticate `email`/`password` against the directory.
    /// 2. Resolve LDAP groups → Astra roles via `ldap_group_mappings`.
    /// 3. Upsert the user in the `users` table (`auth_source = 'ldap'`).
    /// 4. Re-assign casbin roles so they stay in sync with the directory.
    /// 5. Issue access + refresh tokens.
    pub async fn login_ldap(
        &self,
        email: &str,
        password: &str,
    ) -> anyhow::Result<(String, String)> {
        let ldap = self
            .ldap_client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("LDAP is not configured"))?;

        // 1. Authenticate against the directory.
        let ldap_user = ldap.authenticate(email, password).await?;

        // 2. Resolve group → role mappings.
        let roles = self
            .user_repo
            .get_ldap_group_mappings(&ldap_user.groups)
            .await?;

        // 3. Upsert user record.
        let user = self.user_repo.upsert_ldap_user(email).await?;

        // 4. Sync casbin roles (applied fresh on every login).
        for role in &roles {
            self.enforcer
                .add_role_for_user(&user.id.to_string(), role)
                .await?;
        }

        // 5. Issue tokens.
        let (access_token, refresh_token) = self.issue_tokens(&user).await?;
        Ok((access_token, refresh_token))
    }

    /// Decode and validate a JWT access token. Used by auth middleware (issue #148).
    pub fn decode_token(&self, token: &str) -> anyhow::Result<Claims> {
        decode_access_token(token, &self.jwt_secret)
    }

    // ── private helpers ──────────────────────────────────────────────────────

    async fn issue_tokens(&self, user: &UserRecord) -> anyhow::Result<(String, String)> {
        let now = Utc::now();
        let exp = (now + chrono::Duration::seconds(ACCESS_TOKEN_TTL_SECS)).timestamp();
        let claims = Claims {
            sub: user.id,
            email: user.email.clone(),
            exp,
            iat: now.timestamp(),
        };
        let access_token = encode_access_token(&claims, &self.jwt_secret)?;

        let refresh_token = generate_refresh_token();
        let token_hash = sha256_hex(&refresh_token);
        let expires_at = now + chrono::Duration::days(REFRESH_TOKEN_TTL_DAYS);
        self.user_repo
            .store_refresh_token(user.id, &token_hash, expires_at)
            .await?;

        Ok((access_token, refresh_token))
    }
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authz::AstraEnforcer;
    use crate::repositories::memory::InMemoryUserRepository;

    async fn make_service() -> AuthService {
        let repo = Arc::new(InMemoryUserRepository::default());
        let enforcer = Arc::new(AstraEnforcer::new_memory().await.unwrap());
        AuthService::new(repo, b"test-secret".to_vec(), None, enforcer)
    }

    #[tokio::test]
    async fn create_and_login_user() {
        let svc = make_service().await;
        svc.create_user("alice@example.com", "s3cr3t")
            .await
            .unwrap();
        let (access, refresh) = svc
            .login_local("alice@example.com", "s3cr3t")
            .await
            .unwrap();
        assert!(!access.is_empty());
        assert!(!refresh.is_empty());
    }

    #[tokio::test]
    async fn wrong_password_is_rejected() {
        let svc = make_service().await;
        svc.create_user("bob@example.com", "correct").await.unwrap();
        assert!(svc.login_local("bob@example.com", "wrong").await.is_err());
    }

    #[tokio::test]
    async fn refresh_issues_new_access_token() {
        let svc = make_service().await;
        svc.create_user("carol@example.com", "pw").await.unwrap();
        let (_, refresh) = svc.login_local("carol@example.com", "pw").await.unwrap();
        let new_access = svc.refresh(&refresh).await.unwrap();
        assert!(!new_access.is_empty());
    }

    #[tokio::test]
    async fn logout_revokes_refresh_token() {
        let svc = make_service().await;
        svc.create_user("dave@example.com", "pw").await.unwrap();
        let (_, refresh) = svc.login_local("dave@example.com", "pw").await.unwrap();
        svc.logout(&refresh).await.unwrap();
        assert!(svc.refresh(&refresh).await.is_err());
    }

    #[tokio::test]
    async fn unknown_email_is_rejected() {
        let svc = make_service().await;
        assert!(svc.login_local("nobody@example.com", "pw").await.is_err());
    }
}
