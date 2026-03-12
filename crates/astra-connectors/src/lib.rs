pub mod postgres;
pub mod postgres_destination;

pub use postgres::{
    ColumnSchema, DiscoverReport, PostgresCdcSettings, PostgresConnectionConfig,
    PostgresDiscoverOptions, PostgresSnapshotPlan, PostgresSource, PostgresSourceConfig,
    SnapshotExecutionOptions, SnapshotExecutionReport, SnapshotTableChunk, SnapshotTableProgress,
    SourceCatalog, SourceTable,
};
pub use postgres_destination::{
    PostgresDestinationConfig, PostgresDestinationLoader, RawLoadChunkResult, RawLoadReport,
};

pub const CRATE_NAME: &str = "astra-connectors";

pub fn status() -> &'static str {
    "postgres source skeleton available"
}
