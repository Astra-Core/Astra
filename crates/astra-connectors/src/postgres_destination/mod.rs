mod loader;
mod types;

pub use types::{PostgresDestinationConfig, RawLoadChunkResult, RawLoadReport};

use crate::config_parser::{optional_string, require_string, require_u16};
use crate::postgres::types::PostgresConnectionConfig;
use crate::postgres::{test_postgres_connection, ConnectionTestResult};
use anyhow::Context;

/// Destination connector for PostgreSQL — loads staged JSONL.gz chunks into raw tables.
#[derive(Debug, Clone)]
pub struct PostgresDestinationLoader {
    config: PostgresDestinationConfig,
}

impl PostgresDestinationLoader {
    /// Build a `PostgresDestinationLoader` from a validated [`astra_yaml::AstraSpec`].
    pub fn from_spec(spec: &astra_yaml::AstraSpec) -> anyhow::Result<Self> {
        if spec.destination.kind != "postgres" {
            anyhow::bail!(
                "expected destination.kind=postgres, got {}",
                spec.destination.kind
            );
        }
        let values = spec
            .destination
            .connection
            .as_ref()
            .context("destination.connection is required for postgres destination loading")?;
        let ctx = "destination.connection";
        Ok(Self {
            config: PostgresDestinationConfig {
                connection: PostgresConnectionConfig {
                    host: require_string(values, "host", ctx)?,
                    port: require_u16(values, "port", ctx)?,
                    database: require_string(values, "database", ctx)?,
                    username: require_string(values, "username", ctx)?,
                    password_ref: optional_string(values, "passwordRef", ctx)?,
                    ssl_mode: optional_string(values, "sslMode", ctx)?,
                    application_name: optional_string(values, "applicationName", ctx)?,
                },
                schema: optional_string(values, "schema", "destination")?,
                table_prefix: optional_string(values, "tablePrefix", "destination")?,
            },
        })
    }

    pub fn config(&self) -> &PostgresDestinationConfig {
        &self.config
    }

    /// Test connectivity to this Postgres destination.
    pub async fn test_connection(&self) -> ConnectionTestResult {
        test_postgres_connection(&self.config.connection, &[]).await
    }

    /// Load a list of staged chunks into the destination Postgres instance.
    pub async fn load_local_stage_chunks(
        &self,
        chunks: Vec<(astra_runtime::StageChunk, Vec<u8>)>,
    ) -> anyhow::Result<RawLoadReport> {
        let conn = &self.config.connection;
        let request = types::LoadChunkRequest {
            schema: self
                .config
                .schema
                .clone()
                .unwrap_or_else(|| "astra_raw".to_string()),
            table_prefix: self
                .config
                .table_prefix
                .clone()
                .unwrap_or_else(|| "raw_".to_string()),
            chunks,
        };

        loader::load_chunks(
            &conn.host,
            conn.port,
            &conn.database,
            &conn.username,
            conn.password_ref.as_deref(),
            conn.application_name.as_deref(),
            request,
        )
        .await
    }
}
