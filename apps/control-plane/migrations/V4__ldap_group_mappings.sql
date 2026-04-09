CREATE TABLE IF NOT EXISTS ldap_group_mappings (
    ldap_group  TEXT NOT NULL,   -- e.g. "cn=data-engineers,ou=groups,dc=corp,dc=example,dc=com"
    astra_role  TEXT NOT NULL,   -- e.g. "operator"
    PRIMARY KEY (ldap_group, astra_role)
);
