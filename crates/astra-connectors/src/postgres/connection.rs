use super::types::PostgresConnectionConfig;
use crate::config_parser::resolve_password_ref;
use anyhow::Context;
use astra_metadata::AstraError;
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, NoTls};

/// Result of a Postgres connection test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionTestResult {
    /// `"ok"` or `"error"`.
    pub status: String,
    /// Round-trip latency for `SELECT 1`; present when `status == "ok"`.
    pub latency_ms: Option<u64>,
    /// Human-readable error description; present when `status == "error"`.
    pub message: Option<String>,
    /// Tables that were not found in the database (subset of the tables checked).
    #[serde(default)]
    pub missing_tables: Vec<String>,
}

impl ConnectionTestResult {
    fn success(latency_ms: u64, missing_tables: Vec<String>) -> Self {
        if missing_tables.is_empty() {
            Self {
                status: "ok".to_string(),
                latency_ms: Some(latency_ms),
                message: None,
                missing_tables: vec![],
            }
        } else {
            Self {
                status: "error".to_string(),
                latency_ms: Some(latency_ms),
                message: Some(format!("tables not found: {}", missing_tables.join(", "))),
                missing_tables,
            }
        }
    }

    fn failure(message: impl Into<String>) -> Self {
        Self {
            status: "error".to_string(),
            latency_ms: None,
            message: Some(message.into()),
            missing_tables: vec![],
        }
    }
}

/// Test connectivity to a Postgres instance.
///
/// Connects, runs `SELECT 1` to measure latency, and — when `tables` is
/// non-empty — verifies each table exists in `information_schema.tables`.
pub async fn test_postgres_connection(
    config: &PostgresConnectionConfig,
    tables: &[String],
) -> ConnectionTestResult {
    let start = std::time::Instant::now();
    let client = match connect(config).await {
        Ok(c) => c,
        Err(e) => return ConnectionTestResult::failure(format!("{:#}", e)),
    };

    if let Err(e) = client.query_one("SELECT 1", &[]).await {
        return ConnectionTestResult::failure(format!("ping query failed: {e}"));
    }
    let latency_ms = start.elapsed().as_millis() as u64;

    if tables.is_empty() {
        return ConnectionTestResult::success(latency_ms, vec![]);
    }

    let mut missing = Vec::new();
    for table in tables {
        let (schema, name) = match super::split_table_name(table) {
            Ok(pair) => pair,
            Err(_) => {
                missing.push(table.clone());
                continue;
            }
        };
        let rows = match client
            .query(
                "SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name = $2",
                &[&schema, &name],
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return ConnectionTestResult::failure(format!("table check failed: {e}")),
        };
        if rows.is_empty() {
            missing.push(table.clone());
        }
    }

    ConnectionTestResult::success(latency_ms, missing)
}

/// Open an authenticated connection to the Postgres server described by `config`.
pub(super) async fn connect(config: &PostgresConnectionConfig) -> anyhow::Result<Client> {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config.host(&config.host);
    pg_config.port(config.port);
    pg_config.dbname(&config.database);
    pg_config.user(&config.username);
    if let Some(application_name) = &config.application_name {
        pg_config.application_name(application_name);
    }

    if let Some(ref password_ref) = config.password_ref {
        if let Some(password) = resolve_password_ref(password_ref)? {
            pg_config.password(password);
        }
    }

    let (client, connection) = pg_config.connect(NoTls).await.map_err(|e| {
        AstraError::connection_failed_retryable(
            format!(
                "failed to connect to Postgres source at {}:{}",
                config.host, config.port
            ),
            e,
        )
    })?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!(?error, "postgres source connection task exited");
        }
    });

    Ok(client)
}

/// Open a destination connection, resolving the password reference when present.
///
/// Exposed separately from `connect` so the destination loader can reuse the same
/// authenticated connection logic without going through `PostgresConnectionConfig`.
pub(crate) async fn connect_destination(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password_ref: Option<&str>,
    application_name: Option<&str>,
) -> anyhow::Result<Client> {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config.host(host);
    pg_config.port(port);
    pg_config.dbname(database);
    pg_config.user(username);
    if let Some(app_name) = application_name {
        pg_config.application_name(app_name);
    }
    if let Some(password_ref) = password_ref {
        if let Some(password) = resolve_password_ref(password_ref)? {
            pg_config.password(password);
        }
    }

    let (client, connection) = pg_config
        .connect(NoTls)
        .await
        .context("failed to connect to Postgres destination")?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!(?error, "postgres destination connection task exited");
        }
    });

    Ok(client)
}
