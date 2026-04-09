-- Add a stable UUID primary key and timestamp to ldap_group_mappings.
-- Existing rows get generated UUIDs via gen_random_uuid().

ALTER TABLE ldap_group_mappings
    ADD COLUMN id         UUID        NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Swap primary key from (ldap_group, astra_role) to id.
ALTER TABLE ldap_group_mappings DROP CONSTRAINT ldap_group_mappings_pkey;
ALTER TABLE ldap_group_mappings ADD PRIMARY KEY (id);
ALTER TABLE ldap_group_mappings ADD UNIQUE (ldap_group, astra_role);
