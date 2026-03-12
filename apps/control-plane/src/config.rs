#[derive(Debug, Clone)]
pub struct ConfigModule {
    pub bind_addr: String,
    pub database_url: Option<String>,
}

impl ConfigModule {
    pub fn from_env() -> Self {
        Self {
            bind_addr: std::env::var("ASTRA_CONTROL_PLANE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8080".to_string()),
            database_url: std::env::var("ASTRA_DATABASE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty()),
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
