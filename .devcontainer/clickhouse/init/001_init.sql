-- Runs automatically on first container start via docker-entrypoint-initdb.d
-- Executed as the admin user (set via CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1)

CREATE DATABASE IF NOT EXISTS metranova;


CREATE TABLE IF NOT EXISTS metranova.definition 
(
id String,                -- Stable external identifier (e.g., 'def_interface_01')
    ref String,               -- Immutable snapshot version (e.g., 'def_interface_01__v1')
    
    name String,              -- e.g., 'Interface Traffic'
    slug String,              -- e.g., 'interface-traffic'
    type Enum8('data' = 1, 'metadata' = 2),
    
    -- Table Construction Schema
    fields Array(Tuple(
        field_name String, 
        field_type String, 
        nullable Bool
    )),
    identifier Array(String),-- e.g., ['interface_ref', 'timestamp']
    ttl String,               -- e.g., '365 DAY'
    
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ref);

CREATE TABLE IF NOT EXISTS metranova.transformer (
    id String,                -- Stable identifier (e.g., 'trans_interface_std')
    ref String,               -- Immutable snapshot (e.g., 'trans_interface_std__v1')
    definition_ref String,    -- Downstream join to the specific Definition snapshot ('def_int_01__v1')
    
    name String,              
    slug String,              
    description String,       
    
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ref);

CREATE TABLE IF NOT EXISTS metranova.transformer_column (
    transformer_ref String,   -- Links strictly to 'trans_int_std__v1'
    target_column String,     
    
    match_field String,       
    match_value String,       
    
    extract_field String,     
    
    rules Array(Tuple(
        action String, 
        arguments Map(String, String) 
    ))
) ENGINE = MergeTree()
ORDER BY (transformer_ref, target_column, match_field, match_value);



-- -------------------------------------------------------------------------
-- Raw SNMP metrics landing table
-- Written to by the pipeline consumer from the snmp.metrics Kafka topic.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metranova.snmp_metrics
(
    timestamp   DateTime64(3, 'UTC'),
    host        LowCardinality(String),   -- device hostname / IP
    oid         String,                   -- e.g. 1.3.6.1.2.1.1.3.0
    metric_name LowCardinality(String),   -- human-friendly name from Telegraf
    value       Float64,
    tags        Map(String, String),      -- extra k/v from Telegraf
    raw_value   String                    -- original string value before coercion
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (host, metric_name, timestamp)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- -------------------------------------------------------------------------
-- Materialized view: 1-minute rollup for fast dashboard queries
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metranova.snmp_metrics_1m
(
    timestamp   DateTime('UTC'),
    host        LowCardinality(String),
    metric_name LowCardinality(String),
    avg_value   Float64,
    min_value   Float64,
    max_value   Float64,
    sample_count UInt32
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (host, metric_name, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS metranova.snmp_metrics_1m_mv
TO metranova.snmp_metrics_1m
AS
SELECT
    toStartOfMinute(timestamp) AS timestamp,
    host,
    metric_name,
    avg(value)   AS avg_value,
    min(value)   AS min_value,
    max(value)   AS max_value,
    count()      AS sample_count
FROM metranova.snmp_metrics
GROUP BY timestamp, host, metric_name;

-- -------------------------------------------------------------------------
-- Pipeline audit log — track consumer offsets, errors, etc.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metranova.pipeline_events
(
    event_time  DateTime64(3, 'UTC') DEFAULT now64(),
    level       LowCardinality(String),  -- INFO, WARN, ERROR
    component   LowCardinality(String),  -- consumer, api, etc.
    message     String,
    metadata    Map(String, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (component, level, event_time)
TTL event_time + INTERVAL 30 DAY;
