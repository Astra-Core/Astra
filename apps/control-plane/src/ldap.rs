use crate::config::LdapConfig;
use anyhow::Context;
use ldap3::{LdapConnAsync, Scope, SearchEntry};

/// A user entry returned by a successful LDAP authentication.
#[allow(dead_code)]
pub struct LdapUser {
    /// Distinguished name of the user entry.
    pub dn: String,
    /// Email address (as submitted by the caller).
    pub email: String,
    /// Values of the configured group-membership attribute (e.g. `memberOf`).
    pub groups: Vec<String>,
}

/// Thin async wrapper around an LDAP directory.
///
/// Each call to [`authenticate`](LdapClient::authenticate) opens a fresh
/// connection, binds with the service account, searches for the user, then
/// re-binds as the user to verify the password before collecting group
/// memberships.
pub struct LdapClient {
    config: LdapConfig,
}

impl LdapClient {
    pub fn new(config: LdapConfig) -> Self {
        Self { config }
    }

    /// Authenticate `email` / `password` against the directory.
    ///
    /// Steps:
    /// 1. Bind with the configured service account.
    /// 2. Search for the user DN via `ASTRA_LDAP_USER_FILTER`.
    /// 3. Re-bind as the user (proves identity; fails on wrong password).
    /// 4. Read group memberships from `ASTRA_LDAP_GROUP_ATTR`.
    pub async fn authenticate(&self, email: &str, password: &str) -> anyhow::Result<LdapUser> {
        let (conn, mut ldap) = LdapConnAsync::new(&self.config.url)
            .await
            .context("failed to connect to LDAP server")?;
        ldap3::drive!(conn);

        // 1. Service-account bind.
        ldap.simple_bind(&self.config.bind_dn, &self.config.bind_password)
            .await
            .context("LDAP service-account bind failed")?
            .success()
            .context("LDAP service-account bind rejected")?;

        // 2. Search for the user.
        let filter = self.config.user_filter.replace("{}", email);
        let attrs = vec!["dn", self.config.group_attr.as_str()];
        let (entries, _res) = ldap
            .search(&self.config.base_dn, Scope::Subtree, &filter, attrs)
            .await
            .context("LDAP user search failed")?
            .success()
            .context("LDAP user search returned an error")?;

        let raw = entries
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("LDAP: no user found for '{}'", email))?;
        let entry = SearchEntry::construct(raw);
        let dn = entry.dn.clone();

        // 3. Re-bind as the user to verify the supplied password.
        ldap.simple_bind(&dn, password)
            .await
            .context("LDAP user bind failed")?
            .success()
            .context("LDAP authentication failed: invalid credentials")?;

        // 4. Collect group memberships.
        let groups = entry
            .attrs
            .get(&self.config.group_attr)
            .cloned()
            .unwrap_or_default();

        ldap.unbind().await.ok();

        Ok(LdapUser {
            dn,
            email: email.to_string(),
            groups,
        })
    }
}
