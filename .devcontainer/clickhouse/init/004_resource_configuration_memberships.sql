-- Resource-config device membership state.
-- Inactive rows are tombstones; consumers select the newest row for each
-- resource-config/device identity pair.
CREATE TABLE IF NOT EXISTS metranova.resource_configuration_memberships
(
    resource_config_id String,
    device_identity String,
    active Bool,
    updated_at DateTime64(9) DEFAULT now64(9)
) ENGINE = MergeTree()
ORDER BY (resource_config_id, device_identity, updated_at);
