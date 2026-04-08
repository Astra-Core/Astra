/// Shared helpers for parsing `BTreeMap<String, serde_yaml::Value>` connection blocks
/// that appear in Astra YAML specs.  Both the Postgres source and destination parsers
/// use these so that error messages, coercion rules, and `passwordRef` resolution live
/// in exactly one place.
use anyhow::{anyhow, bail};
use std::collections::BTreeMap;

/// Extract a required, non-empty string field from a YAML map.
///
/// `context` is used in the error message, e.g. `"source.connection"`.
pub fn require_string(
    values: &BTreeMap<String, serde_yaml::Value>,
    key: &str,
    context: &str,
) -> anyhow::Result<String> {
    match values.get(key) {
        Some(serde_yaml::Value::String(value)) if !value.trim().is_empty() => Ok(value.clone()),
        Some(_) => bail!("{context}.{key} must be a non-empty string"),
        None => bail!("{context}.{key} is required"),
    }
}

/// Extract an optional string field from a YAML map.
///
/// Returns `Ok(None)` when the key is absent, null, or an empty/whitespace-only string.
pub fn optional_string(
    values: &BTreeMap<String, serde_yaml::Value>,
    key: &str,
    context: &str,
) -> anyhow::Result<Option<String>> {
    match values.get(key) {
        None | Some(serde_yaml::Value::Null) => Ok(None),
        Some(serde_yaml::Value::String(value)) if value.trim().is_empty() => Ok(None),
        Some(serde_yaml::Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => bail!("{context}.{key} must be a string"),
    }
}

/// Extract a required `u16` port field from a YAML map.
pub fn require_u16(
    values: &BTreeMap<String, serde_yaml::Value>,
    key: &str,
    context: &str,
) -> anyhow::Result<u16> {
    match values.get(key) {
        Some(serde_yaml::Value::Number(value)) => value
            .as_u64()
            .and_then(|n| u16::try_from(n).ok())
            .ok_or_else(|| anyhow!("{context}.{key} must be a valid port")),
        Some(serde_yaml::Value::String(value)) => value
            .parse::<u16>()
            .map_err(|_| anyhow!("{context}.{key} must be a valid port")),
        Some(_) => bail!("{context}.{key} must be a valid port"),
        None => bail!("{context}.{key} is required"),
    }
}

/// Resolve a `passwordRef` to an actual password string.
///
/// Currently only the `env:NAME` scheme is supported.
pub fn resolve_password_ref(password_ref: &str) -> anyhow::Result<Option<String>> {
    if let Some(env_name) = password_ref.strip_prefix("env:") {
        return std::env::var(env_name)
            .ok()
            .filter(|value| !value.is_empty())
            .map(Some)
            .ok_or_else(|| anyhow!("environment variable {env_name} is not set for passwordRef"));
    }
    bail!("passwordRef currently supports env:NAME for local Postgres testing")
}
