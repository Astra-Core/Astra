use anyhow::{anyhow, Context};
use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Access token TTL: 15 minutes.
pub const ACCESS_TOKEN_TTL_SECS: i64 = 15 * 60;
/// Refresh token TTL: 7 days.
pub const REFRESH_TOKEN_TTL_DAYS: i64 = 7;

/// Claims embedded in an Astra access token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// User ID.
    pub sub: Uuid,
    pub email: String,
    /// Expiry (Unix timestamp).
    pub exp: i64,
    /// Issued-at (Unix timestamp).
    pub iat: i64,
}

impl Claims {
    /// Synthetic admin claims injected when auth is disabled (dev / in-memory mode).
    pub fn dev_admin() -> Self {
        Self {
            sub: Uuid::nil(),
            email: "dev@astra.local".to_string(),
            exp: i64::MAX,
            iat: 0,
        }
    }
}

/// Sign a new access token with HS256.
pub fn encode_access_token(claims: &Claims, secret: &[u8]) -> anyhow::Result<String> {
    encode(
        &Header::default(),
        claims,
        &EncodingKey::from_secret(secret),
    )
    .context("failed to encode access token")
}

/// Decode and verify an access token. Returns an error when the token is
/// expired, malformed, or signed with a different secret.
pub fn decode_access_token(token: &str, secret: &[u8]) -> anyhow::Result<Claims> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret),
        &Validation::default(),
    )
    .context("failed to decode access token")?;
    Ok(data.claims)
}

/// Hash a password with Argon2id using a random salt.
pub fn hash_password(password: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| anyhow!("failed to hash password: {e}"))
}

/// Return `true` when `password` matches the stored Argon2 `hash`.
pub fn verify_password(password: &str, hash: &str) -> anyhow::Result<bool> {
    let parsed = PasswordHash::new(hash).map_err(|e| anyhow!("invalid password hash: {e}"))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

/// Generate a cryptographically random 32-byte refresh token encoded as hex.
pub fn generate_refresh_token() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    bytes.iter().fold(String::with_capacity(64), |mut s, b| {
        use std::fmt::Write;
        write!(s, "{b:02x}").unwrap();
        s
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn round_trips_access_token() {
        let secret = b"test-secret";
        let now = Utc::now();
        let claims = Claims {
            sub: Uuid::new_v4(),
            email: "alice@example.com".to_string(),
            exp: (now + chrono::Duration::seconds(ACCESS_TOKEN_TTL_SECS)).timestamp(),
            iat: now.timestamp(),
        };
        let token = encode_access_token(&claims, secret).unwrap();
        let decoded = decode_access_token(&token, secret).unwrap();
        assert_eq!(decoded.email, claims.email);
        assert_eq!(decoded.sub, claims.sub);
    }

    #[test]
    fn rejects_token_with_wrong_secret() {
        let now = Utc::now();
        let claims = Claims {
            sub: Uuid::new_v4(),
            email: "bob@example.com".to_string(),
            exp: (now + chrono::Duration::seconds(ACCESS_TOKEN_TTL_SECS)).timestamp(),
            iat: now.timestamp(),
        };
        let token = encode_access_token(&claims, b"correct-secret").unwrap();
        assert!(decode_access_token(&token, b"wrong-secret").is_err());
    }

    #[test]
    fn password_hash_and_verify() {
        let hash = hash_password("hunter2").unwrap();
        assert!(verify_password("hunter2", &hash).unwrap());
        assert!(!verify_password("wrong", &hash).unwrap());
    }

    #[test]
    fn refresh_token_is_64_hex_chars() {
        let token = generate_refresh_token();
        assert_eq!(token.len(), 64);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
