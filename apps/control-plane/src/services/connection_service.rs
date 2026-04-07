use crate::repositories::{
    ConnectionRepository, CreateSavedConnectionInput, SavedConnectionRecord,
};
use astra_yaml::AstraSpec;
use std::sync::Arc;

pub struct ConnectionService {
    repo: Arc<dyn ConnectionRepository>,
}

impl ConnectionService {
    pub fn new(repo: Arc<dyn ConnectionRepository>) -> Self {
        Self { repo }
    }

    /// Resolves `connectionRef` in source and destination.
    ///
    /// For each side that has a `connection_ref` set, the named connection is
    /// looked up, its `config_json` fields are merged into the inline
    /// `connection` map, and `connection_ref` is cleared so that downstream
    /// validation sees a fully-specified connection block.
    pub async fn resolve_spec(&self, mut spec: AstraSpec) -> anyhow::Result<AstraSpec> {
        if let Some(ref name) = spec.source.connection_ref.clone() {
            let record = self.repo.get_by_name(name).await?.ok_or_else(|| {
                anyhow::anyhow!(
                    "connectionRef '{}' not found in saved connections (source)",
                    name
                )
            })?;

            merge_connection_fields(&mut spec.source.connection, &record);
            spec.source.connection_ref = None;
        }

        if let Some(ref name) = spec.destination.connection_ref.clone() {
            let record = self.repo.get_by_name(name).await?.ok_or_else(|| {
                anyhow::anyhow!(
                    "connectionRef '{}' not found in saved connections (destination)",
                    name
                )
            })?;

            let conn = spec
                .destination
                .connection
                .get_or_insert_with(Default::default);
            merge_connection_fields(conn, &record);
            spec.destination.connection_ref = None;
        }

        Ok(spec)
    }

    pub async fn list_connections(&self) -> anyhow::Result<Vec<SavedConnectionRecord>> {
        self.repo.list_connections().await
    }

    pub async fn get_connection(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<SavedConnectionRecord>> {
        self.repo.get_by_name(name).await
    }

    pub async fn create_connection(
        &self,
        input: CreateSavedConnectionInput,
    ) -> anyhow::Result<SavedConnectionRecord> {
        self.repo.create(input).await
    }

    pub async fn update_connection(
        &self,
        name: &str,
        input: CreateSavedConnectionInput,
    ) -> anyhow::Result<SavedConnectionRecord> {
        self.repo.update(name, input).await
    }

    pub async fn delete_connection(&self, name: &str) -> anyhow::Result<()> {
        self.repo.delete(name).await
    }
}

/// Merges fields from a saved connection record into a YAML connection map.
///
/// All fields from `config_json` are inserted (existing inline fields take
/// precedence — they are not overwritten).  When the connection has a
/// `secret_ref`, it is injected as `password: env:<SECRET_REF>` if no
/// `password` key is already present.
fn merge_connection_fields(
    connection: &mut std::collections::BTreeMap<String, serde_yaml::Value>,
    record: &SavedConnectionRecord,
) {
    if let Some(obj) = record.config_json.as_object() {
        for (key, val) in obj {
            // Do not overwrite fields the user has set inline.
            if connection.contains_key(key) {
                continue;
            }
            let yaml_val = json_to_yaml(val);
            connection.insert(key.clone(), yaml_val);
        }
    }

    if let Some(ref secret_ref) = record.secret_ref {
        connection
            .entry("password".to_string())
            .or_insert_with(|| serde_yaml::Value::String(format!("env:{}", secret_ref)));
    }
}

fn json_to_yaml(val: &serde_json::Value) -> serde_yaml::Value {
    match val {
        serde_json::Value::Null => serde_yaml::Value::Null,
        serde_json::Value::Bool(b) => serde_yaml::Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_yaml::Value::Number(serde_yaml::Number::from(i))
            } else if let Some(f) = n.as_f64() {
                serde_yaml::Value::Number(serde_yaml::Number::from(f))
            } else {
                serde_yaml::Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => serde_yaml::Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            serde_yaml::Value::Sequence(arr.iter().map(json_to_yaml).collect())
        }
        serde_json::Value::Object(map) => {
            let mapping = map
                .iter()
                .map(|(k, v)| (serde_yaml::Value::String(k.clone()), json_to_yaml(v)))
                .collect();
            serde_yaml::Value::Mapping(mapping)
        }
    }
}
