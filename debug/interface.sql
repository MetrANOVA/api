-- ClickHouse dump: definition slug 'interface'
-- Import with: docker exec -i clickhouse clickhouse-client --multiquery < interface.sql

-- definition (1 row(s))
INSERT INTO `metranova`.`definition` FORMAT JSONEachRow
{"id":"def_interface","ref":"def_interface__v1","name":"interface","slug":"interface","type":"data","meta_fields":[{"field_name":"node","field_type":"String","nullable":false,"table":""},{"field_name":"intf","field_type":"String","nullable":false,"table":""},{"field_name":"alternate_intf","field_type":"String","nullable":true,"table":""},{"field_name":"interface_id","field_type":"Int32","nullable":true,"table":""},{"field_name":"description","field_type":"String","nullable":true,"table":""},{"field_name":"network","field_type":"String","nullable":true,"table":""},{"field_name":"max_bandwidth","field_type":"Int64","nullable":true,"table":""},{"field_name":"parent_interface","field_type":"String","nullable":true,"table":""},{"field_name":"node_ref","field_type":"String","nullable":true,"table":"meta_device"},{"field_name":"tag_ref","field_type":"String","nullable":true,"table":"meta_tag"},{"field_name":"entity_ref","field_type":"String","nullable":true,"table":"meta_entity"},{"field_name":"kvp_ref","field_type":"String","nullable":true,"table":"meta_kvp"},{"field_name":"circuit_ref","field_type":"String","nullable":true,"table":"meta_circuit"},{"field_name":"address_ref","field_type":"String","nullable":true,"table":"meta_address"},{"field_name":"pop_ref","field_type":"String","nullable":true,"table":"meta_pop"},{"field_name":"service_ref","field_type":"String","nullable":true,"table":"meta_service"},{"field_name":"type","field_type":"String","nullable":true,"table":""}],"data_fields":[{"field_name":"node","field_type":"String","nullable":false},{"field_name":"intf","field_type":"String","nullable":false},{"field_name":"input","field_type":"Float32","nullable":false},{"field_name":"output","field_type":"Float32","nullable":true},{"field_name":"inerror","field_type":"Float32","nullable":true},{"field_name":"outerror","field_type":"Float32","nullable":true},{"field_name":"inUcast","field_type":"Float32","nullable":true},{"field_name":"outUcast","field_type":"Float32","nullable":true}],"identifier":["node","intf"],"ttl":"INTERVAL 365 DAY","engine_type":"CoalescingMergeTree","is_replicated":true,"updated_at":"2026-06-09 21:54:49"}
;

-- transformer (1 row(s))
INSERT INTO `metranova`.`transformer` FORMAT JSONEachRow
{"id":"all_interfaces","ref":"all_interfaces__v1","definition_ref":"def_interface","name":"all_interfaces","slug":"all_interfaces","description":"Interface Traffic transformers","match_field":"protocol","updated_at":"2026-06-09 22:08:21"}
;

-- DDL for data_interface
CREATE TABLE IF NOT EXISTS metranova.data_interface
(
    `collector_id` LowCardinality(String),
    `policy_level` LowCardinality(String),
    `policy_scope` Array(LowCardinality(String)),
    `policy_originator` LowCardinality(String),
    `insert_time` DateTime DEFAULT now(),
    `node` String,
    `intf` String,
    `input` Float32,
    `output` Float32,
    `inerror` Float32,
    `outerror` Float32,
    `inUcast` Float32,
    `outUcast` Float32,
    `ext` JSON
)
ENGINE = CoalescingMergeTree()
PARTITION BY toYYYYMM(insert_time)
PRIMARY KEY (collector_id, insert_time, node, intf)
ORDER BY (collector_id, insert_time, node, intf)
TTL insert_time + toIntervalDay(365)
SETTINGS index_granularity = 8192;

-- DDL for meta_interface
CREATE TABLE IF NOT EXISTS metranova.meta_interface
(
    `id` String,
    `ref` String,
    `hash` String,
    `insert_time` DateTime DEFAULT now(),
    `ext` JSON(sap_name Nullable(String), vrtr_id LowCardinality(Nullable(String)), vrtr_interface_encap Nullable(UInt32), vrtr_interface_global_index Nullable(UInt32), vrtr_interface_index Nullable(String), vrtr_name LowCardinality(Nullable(String))),
    `tag` Array(LowCardinality(String)),
    `type` LowCardinality(String),
    `description` Nullable(String),
    `device_id` String,
    `device_ref` Nullable(String),
    `edge` Bool,
    `flow_index` Nullable(UInt32),
    `ipv4` Nullable(IPv4),
    `ipv6` Nullable(IPv6),
    `name` String,
    `speed` Nullable(UInt64),
    `circuit_id` Array(String),
    `circuit_ref` Array(Nullable(String)),
    `peer_as_id` Nullable(UInt32),
    `peer_as_ref` Nullable(String),
    `peer_interface_ipv4` Nullable(IPv4),
    `peer_interface_ipv6` Nullable(IPv6),
    `lag_member_interface_id` Array(LowCardinality(String)),
    `lag_member_interface_ref` Array(Nullable(String)),
    `port_interface_id` LowCardinality(Nullable(String)),
    `port_interface_ref` Nullable(String),
    `remote_interface_id` LowCardinality(Nullable(String)),
    `remote_interface_ref` Nullable(String),
    `remote_organization_id` LowCardinality(Nullable(String)),
    `remote_organization_ref` Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (ref, id, insert_time)
SETTINGS index_granularity = 8192;
