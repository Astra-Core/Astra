#[derive(Debug, Clone)]
pub struct ConfigModule {
    pub bind_addr: String,
    pub database_url: Option<String>,
    /// JWT signing secret (`ASTRA_JWT_SECRET`). When absent, auth is disabled.
    pub jwt_secret: Option<String>,
    /// True only when both `database_url` and `jwt_secret` are set.
    pub auth_enabled: bool,
    /// Email for the first-run admin user seed (`ASTRA_ADMIN_EMAIL`).
    pub admin_email: Option<String>,
    /// Plain-text password for the first-run admin user seed (`ASTRA_ADMIN_PASSWORD`).
    pub admin_password: Option<String>,
}

impl ConfigModule {
    pub fn from_env() -> Self {
        let database_url = std::env::var("ASTRA_DATABASE_URL")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let jwt_secret = std::env::var("ASTRA_JWT_SECRET")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let auth_enabled = database_url.is_some() && jwt_secret.is_some();
        Self {
            bind_addr: std::env::var("ASTRA_CONTROL_PLANE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8080".to_string()),
            database_url,
            jwt_secret,
            auth_enabled,
            admin_email: std::env::var("ASTRA_ADMIN_EMAIL")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            admin_password: std::env::var("ASTRA_ADMIN_PASSWORD")
                .ok()
                .filter(|v| !v.trim().is_empty()),
        }
    }

    pub const fn status(&self) -> &'static str {
        "configured"
    }

    pub fn database_backend_label(&self) -> &'static str {
        if self.database_url.is_some() {
            "postgres-or-fallback"
        } else {
            "memory"
        }
    }
}
