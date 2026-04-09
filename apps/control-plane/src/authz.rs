use anyhow::Context;
use casbin::{CoreApi, DefaultModel, Enforcer, MemoryAdapter, MgmtApi, RbacApi};
use sqlx_adapter::SqlxAdapter;
use std::sync::Arc;
use tokio::sync::RwLock;

const MODEL_CONF: &str = "\
[request_definition]\n\
r = sub, obj, act\n\
\n\
[policy_definition]\n\
p = sub, obj, act\n\
\n\
[role_definition]\n\
g = _, _\n\
\n\
[policy_effect]\n\
e = some(where (p.eft == allow))\n\
\n\
[matchers]\n\
m = g(r.sub, p.sub) && r.obj == p.obj && r.act == p.act\n\
";

/// Default (role, object, action) permissions seeded on first run.
const DEFAULT_POLICIES: &[(&str, &str, &str)] = &[
    ("admin", "pipelines", "read"),
    ("admin", "pipelines", "write"),
    ("admin", "pipelines", "delete"),
    ("admin", "connections", "read"),
    ("admin", "connections", "write"),
    ("admin", "connections", "delete"),
    ("admin", "users", "read"),
    ("admin", "users", "write"),
    ("operator", "pipelines", "read"),
    ("operator", "pipelines", "write"),
    ("operator", "connections", "read"),
    ("operator", "connections", "write"),
    ("viewer", "pipelines", "read"),
    ("viewer", "connections", "read"),
];

/// Role self-assignments that declare the named roles in the policy store.
const DEFAULT_ROLES: &[&str] = &["admin", "operator", "viewer"];

/// Thread-safe casbin enforcer wrapper.
///
/// Wraps an [`Enforcer`] in `Arc<RwLock<_>>` so that read operations (enforce)
/// take a shared lock while mutations (add_role_for_user) take an exclusive lock.
/// Methods are consumed by authorization middleware (issue #148).
#[derive(Clone)]
#[allow(dead_code)]
pub struct AstraEnforcer(Arc<RwLock<Enforcer>>);

#[allow(dead_code)]
impl AstraEnforcer {
    /// Create an enforcer backed by Postgres via `sqlx-adapter`.
    ///
    /// The `casbin_rule` table is created automatically on first startup.
    /// Default policies are seeded when the table is empty.
    pub async fn new(db_url: &str) -> anyhow::Result<Self> {
        let model = DefaultModel::from_str(MODEL_CONF)
            .await
            .context("failed to parse casbin model")?;
        let adapter = SqlxAdapter::new(db_url, 8)
            .await
            .context("failed to connect casbin sqlx-adapter")?;
        let mut enforcer = Enforcer::new(model, adapter)
            .await
            .context("failed to create casbin enforcer")?;

        if enforcer.get_all_policy().is_empty() {
            Self::seed_policies(&mut enforcer).await?;
        }

        Ok(Self(Arc::new(RwLock::new(enforcer))))
    }

    /// Create an enforcer backed by an in-memory adapter (dev / in-memory mode).
    ///
    /// Default policies are always seeded.
    pub async fn new_memory() -> anyhow::Result<Self> {
        let model = DefaultModel::from_str(MODEL_CONF)
            .await
            .context("failed to parse casbin model")?;
        let adapter = MemoryAdapter::default();
        let mut enforcer = Enforcer::new(model, adapter)
            .await
            .context("failed to create in-memory casbin enforcer")?;

        Self::seed_policies(&mut enforcer).await?;

        Ok(Self(Arc::new(RwLock::new(enforcer))))
    }

    /// Returns `true` if `user_id` is allowed to perform `act` on `obj`.
    pub async fn enforce(&self, user_id: &str, obj: &str, act: &str) -> anyhow::Result<bool> {
        let guard = self.0.read().await;
        guard
            .enforce((user_id, obj, act))
            .context("casbin enforce error")
    }

    /// Assign `role` to `user_id` (e.g. `"admin"`, `"operator"`, `"viewer"`).
    pub async fn add_role_for_user(&self, user_id: &str, role: &str) -> anyhow::Result<()> {
        let mut guard = self.0.write().await;
        guard
            .add_role_for_user(user_id, role, None)
            .await
            .context("failed to add role for user")?;
        Ok(())
    }

    /// Returns all roles currently assigned to `user_id`.
    pub async fn get_roles_for_user(&self, user_id: &str) -> Vec<String> {
        let guard = self.0.read().await;
        guard.get_roles_for_user(user_id, None)
    }

    /// Replace all roles for `user_id` with the given `roles` list atomically.
    pub async fn set_roles_for_user(
        &self,
        user_id: &str,
        roles: Vec<String>,
    ) -> anyhow::Result<()> {
        let mut guard = self.0.write().await;
        guard
            .delete_roles_for_user(user_id, None)
            .await
            .context("failed to clear roles for user")?;
        for role in &roles {
            guard
                .add_role_for_user(user_id, role, None)
                .await
                .context("failed to add role for user")?;
        }
        Ok(())
    }

    async fn seed_policies(enforcer: &mut Enforcer) -> anyhow::Result<()> {
        let policies: Vec<Vec<String>> = DEFAULT_POLICIES
            .iter()
            .map(|(sub, obj, act)| vec![sub.to_string(), obj.to_string(), act.to_string()])
            .collect();

        enforcer
            .add_policies(policies)
            .await
            .context("failed to seed default policies")?;

        for role in DEFAULT_ROLES {
            enforcer
                .add_role_for_user(role, role, None)
                .await
                .context("failed to seed role hierarchy")?;
        }

        Ok(())
    }
}
