-- Replayed on every `compose up` by the clickhouse-init sidecar, in filename order.
-- All statements must be idempotent.

-- -------------------------------------------------------------------------
-- Collector resource configurations
-- One row is one immutable snapshot of a ResourceConfiguration; updates append
-- a new row with a bumped ref rather than mutating in place.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metranova.resource_configuration
(
    id String,                    -- Stable identifier (e.g., 'example_telegraf')
    ref String,                   -- Immutable snapshot ('example_telegraf__v1')

    name String,
    slug String,
    resource_type String,         -- definition slug this config collects for
    collector_plugin String,      -- e.g., 'telegraf_vscode'
    interval UInt32 DEFAULT 60,
    timeout UInt32 DEFAULT 15,

    field_mappings Array(Tuple(
        field_name String,
        oid String,
        is_tag Bool,
        secondary_index_table Nullable(String),
        secondary_index_use Nullable(Bool)
    )),
    node_selectors Array(Tuple(
        type String,
        value String
    )),

    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ref);
