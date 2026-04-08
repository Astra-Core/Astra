use super::types::{ColumnSchema, SourceTable};
use astra_metadata::AstraError;
use tokio_postgres::Client;

/// Inspect the schema of each table in `table_names` and return their column
/// and primary-key metadata.
pub(super) async fn discover_tables(
    client: &Client,
    table_names: &[String],
) -> anyhow::Result<Vec<SourceTable>> {
    let mut tables = Vec::new();

    for table_name in table_names {
        let (schema, table) = super::split_table_name(table_name)?;

        let columns = client
            .query(
                r#"
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
                "#,
                &[&schema, &table],
            )
            .await
            .map_err(|e| {
                AstraError::query_failed_permanent(
                    format!("failed to inspect columns for {table_name}"),
                    e,
                )
            })?;

        if columns.is_empty() {
            return Err(AstraError::NotFound(format!(
                "table {table_name} was not found in Postgres"
            ))
            .into());
        }

        let primary_key_rows = client
            .query(
                r#"
                SELECT a.attname AS column_name
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
                WHERE i.indisprimary = true AND n.nspname = $1 AND c.relname = $2
                ORDER BY array_position(i.indkey, a.attnum)
                "#,
                &[&schema, &table],
            )
            .await
            .map_err(|e| {
                AstraError::query_failed_permanent(
                    format!("failed to inspect primary key for {table_name}"),
                    e,
                )
            })?;

        tables.push(SourceTable {
            schema: schema.to_string(),
            name: table.to_string(),
            fully_qualified_name: format!("{schema}.{table}"),
            columns: columns
                .into_iter()
                .map(|row| ColumnSchema {
                    name: row.get("column_name"),
                    data_type: row.get("data_type"),
                    is_nullable: matches!(row.get::<_, String>("is_nullable").as_str(), "YES"),
                })
                .collect(),
            primary_key: primary_key_rows
                .into_iter()
                .map(|row| row.get("column_name"))
                .collect(),
        });
    }

    Ok(tables)
}
