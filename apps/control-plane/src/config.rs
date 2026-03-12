#[derive(Debug, Clone)]
pub struct ConfigModule {
    pub bind_addr: String,
}

impl ConfigModule {
    pub fn from_env() -> Self {
        Self {
            bind_addr: std::env::var("ASTRA_CONTROL_PLANE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8080".to_string()),
        }
    }

    pub const fn status(&self) -> &'static str {
        "stubbed"
    }
}
