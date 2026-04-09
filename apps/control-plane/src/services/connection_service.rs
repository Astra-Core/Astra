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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repositories::InMemoryConnectionRepository;

    fn make_service() -> ConnectionService {
        ConnectionService::new(Arc::new(InMemoryConnectionRepository::default()))
    }

    async fn seeded_service(name: &str) -> ConnectionService {
        let svc = make_service();
        svc.create_connection(CreateSavedConnectionInput {
            name: name.to_string(),
            kind: "postgres".to_string(),
            config_json: serde_json::json!({
                "host": "db.internal",
                "port": 5432,
                "database": "mydb",
                "username": "replicator"
            }),
            secret_ref: Some("POSTGRES_PASSWORD".to_string()),
            created_by: None,
        })
        .await
        .unwrap();
        svc
    }

    fn minimal_spec(source_ref: Option<&str>, dest_ref: Option<&str>) -> astra_yaml::AstraSpec {
        let yaml = format!(
            r#"
version: v1alpha1
pipeline:
  name: test-pipeline
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  {source_ref_line}
  {source_conn_line}
  capture:
    tables:
      - public.users
destination:
  kind: postgres
  {dest_ref_line}
  {dest_conn_line}
  staging:
    kind: local
    bucket: staging
  write:
    mode: append
runtime: {{}}
"#,
            source_ref_line = source_ref
                .map(|r| format!("connectionRef: {r}"))
                .unwrap_or_default(),
            source_conn_line = if source_ref.is_none() {
                "connection:\n    host: src-host\n    port: 5432\n    database: srcdb\n    username: src_user"
            } else {
                ""
            },
            dest_ref_line = dest_ref
                .map(|r| format!("connectionRef: {r}"))
                .unwrap_or_default(),
            dest_conn_line = if dest_ref.is_none() {
                "connection:\n    host: dst-host\n    port: 5432\n    database: dstdb\n    username: dst_user"
            } else {
                ""
            },
        );
        astra_yaml::AstraSpec::parse_yaml(&yaml).expect("spec parses")
    }

    #[tokio::test]
    async fn resolve_spec_merges_source_connection_ref() {
        let svc = seeded_service("my-db").await;
        let spec = minimal_spec(Some("my-db"), None);
        let resolved = svc.resolve_spec(spec).await.unwrap();
        assert!(resolved.source.connection_ref.is_none());
        assert_eq!(
            resolved.source.connection.get("host").unwrap(),
            &serde_yaml::Value::String("db.internal".to_string())
        );
        assert_eq!(
            resolved.source.connection.get("database").unwrap(),
            &serde_yaml::Value::String("mydb".to_string())
        );
        // secret_ref injected as password
        assert_eq!(
            resolved.source.connection.get("password").unwrap(),
            &serde_yaml::Value::String("env:POSTGRES_PASSWORD".to_string())
        );
    }

    #[tokio::test]
    async fn resolve_spec_merges_destination_connection_ref() {
        let svc = seeded_service("dst-db").await;
        let spec = minimal_spec(None, Some("dst-db"));
        let resolved = svc.resolve_spec(spec).await.unwrap();
        assert!(resolved.destination.connection_ref.is_none());
        let conn = resolved.destination.connection.unwrap();
        assert_eq!(
            conn.get("host").unwrap(),
            &serde_yaml::Value::String("db.internal".to_string())
        );
        assert_eq!(
            conn.get("password").unwrap(),
            &serde_yaml::Value::String("env:POSTGRES_PASSWORD".to_string())
        );
    }

    #[tokio::test]
    async fn resolve_spec_errors_on_missing_source_ref() {
        let svc = make_service();
        let spec = minimal_spec(Some("nonexistent"), None);
        let err = svc.resolve_spec(spec).await.unwrap_err();
        assert!(
            err.to_string().contains("nonexistent"),
            "error should name the missing ref"
        );
    }

    #[tokio::test]
    async fn resolve_spec_errors_on_missing_destination_ref() {
        let svc = make_service();
        let spec = minimal_spec(None, Some("nonexistent"));
        let err = svc.resolve_spec(spec).await.unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
    }

    #[tokio::test]
    async fn resolve_spec_passthrough_when_no_refs() {
        let svc = make_service();
        let spec = minimal_spec(None, None);
        let resolved = svc.resolve_spec(spec).await.unwrap();
        // inline fields unchanged
        assert_eq!(
            resolved.source.connection.get("host").unwrap(),
            &serde_yaml::Value::String("src-host".to_string())
        );
    }

    #[tokio::test]
    async fn inline_fields_take_precedence_over_saved_connection() {
        let svc = seeded_service("my-db").await;
        // spec with a connectionRef but also an inline `host` that should win
        let yaml = r#"
version: v1alpha1
pipeline:
  name: override-test
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connectionRef: my-db
  connection:
    host: override-host
  capture:
    tables:
      - public.users
destination:
  kind: postgres
  connection:
    host: dst-host
    port: 5432
    database: dstdb
    username: dst_user
  staging:
    kind: local
    bucket: staging
  write:
    mode: append
runtime: {}
"#;
        // NOTE: spec with both connectionRef and non-empty connection is an
        // AmbiguousConnection validation error, but resolve_spec itself should
        // not error — it merges first, validate() catches the conflict later.
        // We test the merge priority here before validation runs.
        let spec = astra_yaml::AstraSpec::parse_yaml(yaml).unwrap();
        let resolved = svc.resolve_spec(spec).await.unwrap();
        // The inline `host` should be preserved (not overwritten by saved "db.internal")
        assert_eq!(
            resolved.source.connection.get("host").unwrap(),
            &serde_yaml::Value::String("override-host".to_string())
        );
        // Fields absent inline are filled from the saved record
        assert_eq!(
            resolved.source.connection.get("database").unwrap(),
            &serde_yaml::Value::String("mydb".to_string())
        );
    }

    #[tokio::test]
    async fn crud_create_list_get_update_delete() {
        let svc = make_service();
        let input = CreateSavedConnectionInput {
            name: "test-conn".to_string(),
            kind: "postgres".to_string(),
            config_json: serde_json::json!({"host": "h", "port": 5432, "database": "d", "username": "u"}),
            secret_ref: None,
            created_by: Some("alice".to_string()),
        };
        let created = svc.create_connection(input).await.unwrap();
        assert_eq!(created.name, "test-conn");

        let listed = svc.list_connections().await.unwrap();
        assert_eq!(listed.len(), 1);

        let fetched = svc.get_connection("test-conn").await.unwrap().unwrap();
        assert_eq!(fetched.id, created.id);

        let updated = svc
            .update_connection(
                "test-conn",
                CreateSavedConnectionInput {
                    name: "test-conn".to_string(),
                    kind: "postgres".to_string(),
                    config_json: serde_json::json!({"host": "h2", "port": 5433, "database": "d2", "username": "u2"}),
                    secret_ref: None,
                    created_by: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            updated.config_json.get("host").unwrap().as_str().unwrap(),
            "h2"
        );

        svc.delete_connection("test-conn").await.unwrap();
        let after_delete = svc.get_connection("test-conn").await.unwrap();
        assert!(after_delete.is_none());
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
