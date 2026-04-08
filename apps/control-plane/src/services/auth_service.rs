// AuthService methods are consumed by HTTP handlers (issue #149).
// Allow dead_code until that issue lands.
#![allow(dead_code)]

use crate::{
    auth::{
        encode_access_token, generate_refresh_token, hash_password, verify_password, Claims,
        ACCESS_TOKEN_TTL_SECS, REFRESH_TOKEN_TTL_DAYS,
    },
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
}

impl AuthService {
    pub fn new(user_repo: Arc<dyn UserRepository>, jwt_secret: Vec<u8>) -> Self {
        Self {
            user_repo,
            jwt_secret,
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
    use crate::repositories::memory::InMemoryUserRepository;

    fn make_service() -> AuthService {
        let repo = Arc::new(InMemoryUserRepository::default());
        AuthService::new(repo, b"test-secret".to_vec())
    }

    #[tokio::test]
    async fn create_and_login_user() {
        let svc = make_service();
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
        let svc = make_service();
        svc.create_user("bob@example.com", "correct").await.unwrap();
        assert!(svc.login_local("bob@example.com", "wrong").await.is_err());
    }

    #[tokio::test]
    async fn refresh_issues_new_access_token() {
        let svc = make_service();
        svc.create_user("carol@example.com", "pw").await.unwrap();
        let (_, refresh) = svc.login_local("carol@example.com", "pw").await.unwrap();
        let new_access = svc.refresh(&refresh).await.unwrap();
        assert!(!new_access.is_empty());
    }

    #[tokio::test]
    async fn logout_revokes_refresh_token() {
        let svc = make_service();
        svc.create_user("dave@example.com", "pw").await.unwrap();
        let (_, refresh) = svc.login_local("dave@example.com", "pw").await.unwrap();
        svc.logout(&refresh).await.unwrap();
        assert!(svc.refresh(&refresh).await.is_err());
    }

    #[tokio::test]
    async fn unknown_email_is_rejected() {
        let svc = make_service();
        assert!(svc.login_local("nobody@example.com", "pw").await.is_err());
    }
}
