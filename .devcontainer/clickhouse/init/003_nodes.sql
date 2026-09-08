-- Replayed on every `compose up` by the clickhouse-init sidecar, in filename order.
-- All statements must be idempotent.

-- -------------------------------------------------------------------------
-- Nodes
-- One row is the current record for a node; updates insert a new row for
-- the same node_id and the latest is picked by `updated_at` at query time.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metranova.nodes
(
    node_id String,

    host String,
    port UInt16,
    community String,

    name String,
    make String,
    model String,

    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree()
ORDER BY (node_id);

-- Seed the dev SNMP simulator as a node so resource configurations with no
-- node_selectors have something to collect from. Guarded so the replay stays
-- idempotent.
INSERT INTO metranova.nodes (node_id, host, port, community, name, make, model)
SELECT 'snmp-simulator', 'snmp-simulator', 161, 'public', 'snmp-simulator', 'generic', 'simulator'
WHERE (SELECT count() FROM metranova.nodes WHERE node_id = 'snmp-simulator') = 0;
