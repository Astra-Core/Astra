/// LDAP connection and search configuration.
///
/// Present only when `ASTRA_LDAP_URL` is set.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LdapConfig {
    /// LDAP server URL, e.g. `ldaps://ldap.corp.example.com:636`.
    pub url: String,
    /// DN of the service account used for the initial bind.
    pub bind_dn: String,
    /// Password for the service account.
    pub bind_password: String,
    /// Base DN for user searches, e.g. `ou=people,dc=corp,dc=example,dc=com`.
    pub base_dn: String,
    /// Base DN for group searches, e.g. `ou=groups,dc=corp,dc=example,dc=com`.
    pub group_base_dn: String,
    /// LDAP filter used to find a user by email. `{}` is replaced with the submitted email.
    /// Default: `(mail={})`.
    pub user_filter: String,
    /// Attribute on the user entry that lists group memberships.
    /// Default: `memberOf`.
    pub group_attr: String,
}

#[derive(Debug, Clone)]
pub struct ConfigModule {
    pub bind_addr: String,
    pub database_url: Option<String>,
    /// JWT signing secret (`ASTRA_JWT_SECRET`). When absent, auth is disabled.
    pub jwt_secret: Option<String>,
    /// True only when both `database_url` and `jwt_secret` are set.
    pub auth_enabled: bool,
    /// Email for the first-run admin user seed (`ASTRA_ADMIN_EMAIL`).
    pub admin_email: Option<String>,
    /// Plain-text password for the first-run admin user seed (`ASTRA_ADMIN_PASSWORD`).
    pub admin_password: Option<String>,
    /// LDAP configuration. Present only when `ASTRA_LDAP_URL` is set.
    pub ldap: Option<LdapConfig>,
}

impl ConfigModule {
    pub fn from_env() -> Self {
        let database_url = std::env::var("ASTRA_DATABASE_URL")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let jwt_secret = std::env::var("ASTRA_JWT_SECRET")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let auth_enabled = database_url.is_some() && jwt_secret.is_some();

        let ldap = std::env::var("ASTRA_LDAP_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(|url| LdapConfig {
                url,
                bind_dn: std::env::var("ASTRA_LDAP_BIND_DN").unwrap_or_default(),
                bind_password: std::env::var("ASTRA_LDAP_BIND_PASSWORD").unwrap_or_default(),
                base_dn: std::env::var("ASTRA_LDAP_BASE_DN").unwrap_or_default(),
                group_base_dn: std::env::var("ASTRA_LDAP_GROUP_BASE_DN").unwrap_or_default(),
                user_filter: std::env::var("ASTRA_LDAP_USER_FILTER")
                    .unwrap_or_else(|_| "(mail={})".to_string()),
                group_attr: std::env::var("ASTRA_LDAP_GROUP_ATTR")
                    .unwrap_or_else(|_| "memberOf".to_string()),
            });

        Self {
            bind_addr: std::env::var("ASTRA_CONTROL_PLANE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8080".to_string()),
            database_url,
            jwt_secret,
            auth_enabled,
            admin_email: std::env::var("ASTRA_ADMIN_EMAIL")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            admin_password: std::env::var("ASTRA_ADMIN_PASSWORD")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            ldap,
        }
    }

    pub const fn status(&self) -> &'static str {
        "configured"
    }

    pub fn database_backend_label(&self) -> &'static str {
        if self.database_url.is_some() {
            "postgres-or-fallback"
        } else {
            "memory"
        }
    }
}
