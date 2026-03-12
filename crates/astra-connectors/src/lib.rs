pub mod postgres;

pub use postgres::{
    ColumnSchema, DiscoverReport, PostgresCdcSettings, PostgresConnectionConfig,
    PostgresDiscoverOptions, PostgresSnapshotPlan, PostgresSource, PostgresSourceConfig,
    SnapshotExecutionOptions, SnapshotExecutionReport, SnapshotTableChunk, SourceCatalog,
    SourceTable,
};

pub const CRATE_NAME: &str = "astra-connectors";

pub fn status() -> &'static str {
    "postgres source skeleton available"
}
