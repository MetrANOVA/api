import asyncio
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from admin_api.collector.model import (
    FieldConfig,
    ResourceConfiguration,
    ResourceConfigurationRequest,
    ResourceConfigurationUpdate,
    Selector,
)
from admin_api.collector.service import CollectorService, _from_row, _to_row

INTERFACE_DEFINITION = {
    "id": "def_interface",
    "ref": "def_interface__v1",
    "slug": "interface",
    "meta_fields": [
        {"field_name": "node", "field_type": "String", "nullable": False},
        {"field_name": "intf", "field_type": "String", "nullable": False},
    ],
    "data_fields": [
        {"field_name": "input", "field_type": "UInt64", "nullable": True},
        {"field_name": "output", "field_type": "UInt64", "nullable": True},
    ],
}


class DummyClient:
    """Records writes and replays a scripted queue of query results."""

    def __init__(self, query_results=None):
        self.query_calls = []
        self.insert_calls = []
        self.command_calls = []
        self.query_results = list(query_results or [])

    async def query(self, query, parameters=None):
        self.query_calls.append((query, parameters))
        rows = self.query_results.pop(0) if self.query_results else []
        return SimpleNamespace(
            row_count=len(rows),
            named_results=lambda: iter(rows),
        )

    async def insert(self, **kwargs):
        self.insert_calls.append(kwargs)

    async def command(self, command, parameters=None):
        self.command_calls.append((command, parameters))


class DummyStorage:
    def __init__(self, query_results=None, definition=INTERFACE_DEFINITION):
        self.database = "metranova"
        self.client = DummyClient(query_results)
        self.ensure_calls = 0
        self._definition = definition

    async def ensure_resource_configuration_table(self):
        self.ensure_calls += 1

    def _qualified_table_name(self, table_name: str) -> str:
        return f"`{self.database}`.`{table_name}`"

    async def find_resource_type_by_slug(self, slug: str):
        return self._definition


class DummyPlugin:
    """Stands in for a collector plugin so tests never write a config file."""

    plugin_id = "telegraf_vscode"

    def __init__(self, error=None):
        self.error = error
        self.rendered = []

    async def render_config(self, config):
        if self.error:
            raise self.error
        self.rendered.append(config)
        return "[rendered]"


def _service(storage, plugin=None) -> CollectorService:
    """A service whose plugin registry is a stub, not the entry-point plugins."""
    service = CollectorService(storage)
    plugin = plugin or DummyPlugin()
    service.plugins = {plugin.plugin_id: plugin}
    return service


def _config(**overrides) -> ResourceConfiguration:
    defaults = dict(
        id="example_telegraf",
        ref="example_telegraf__v1",
        name="example telegraf",
        resource_type="interface",
        collector_plugin="telegraf_vscode",
        interval=10,
        timeout=15,
        field_mappings={
            "intf": FieldConfig(oid=".1.3.6.1.2.1.2.2.1.2", is_tag=True),
            "input": FieldConfig(oid=".1.3.6.1.2.1.2.2.1.10"),
            "output": FieldConfig(
                oid=".1.3.6.1.2.1.2.2.1.16",
                secondary_index_table="ifXTable",
                secondary_index_use=True,
            ),
        },
        node_selectors=[Selector(type="role", value="rtr")],
    )
    defaults.update(overrides)
    return ResourceConfiguration(**defaults)


def _request(**overrides) -> ResourceConfigurationRequest:
    defaults = dict(
        name="example telegraf",
        resource_type="interface",
        collector_plugin="telegraf_vscode",
    )
    defaults.update(overrides)
    return ResourceConfigurationRequest(**defaults)


def _stored_row(config: ResourceConfiguration) -> dict:
    """A row as ClickHouse would hand it back, including updated_at."""
    row = _to_row(config)
    row["updated_at"] = "2026-08-11 00:00:00"
    return row


def test_to_row_from_row_round_trip_preserves_mappings_and_nulls():
    original = _config()

    restored = _from_row(_stored_row(original))

    assert restored == original
    assert restored.field_mappings["input"].secondary_index_table is None
    assert restored.field_mappings["input"].secondary_index_use is None
    assert restored.field_mappings["output"].secondary_index_table == ("ifXTable")
    assert restored.field_mappings["output"].secondary_index_use is True
    assert restored.field_mappings["intf"].is_tag is True


def test_from_row_accepts_positional_tuples():
    row = _stored_row(_config())
    positional = tuple(row.values())

    restored = _from_row(positional)

    assert restored == _config()


def test_to_row_orders_field_mappings_deterministically():
    row = _to_row(_config())

    assert [entry[0] for entry in row["field_mappings"]] == ["input", "intf", "output"]


def test_create_assigns_v1_ref_and_inserts():
    storage = DummyStorage(query_results=[[]])  # no existing row with that slug
    service = CollectorService(storage)

    result = asyncio.run(
        service.create_resource_configuration(
            _request(
                field_mappings={"input": FieldConfig(oid=".1.3.6.1.2.1.2.2.1.10")},
                node_selectors=[Selector(type="role", value="rtr")],
                interval=10,
            )
        )
    )

    assert result.id == "example_telegraf"
    assert result.ref == "example_telegraf__v1"
    assert result.interval == 10
    assert storage.ensure_calls == 1

    assert len(storage.client.insert_calls) == 1
    insert = storage.client.insert_calls[0]
    assert insert["table"] == "resource_configuration"
    assert insert["database"] == "metranova"
    row = _to_row(result)
    assert row["slug"] == "example_telegraf"
    assert insert["column_names"] == list(row.keys())
    assert insert["data"] == [list(row.values())]


def test_create_rejects_duplicate_slug():
    storage = DummyStorage(query_results=[[{"id": "example_telegraf"}]])
    service = CollectorService(storage)

    with pytest.raises(ValueError, match="already exists"):
        asyncio.run(service.create_resource_configuration(_request()))

    assert storage.client.insert_calls == []


def test_create_rejects_field_not_declared_by_resource_type():
    storage = DummyStorage(query_results=[[]])
    service = CollectorService(storage)

    with pytest.raises(ValueError, match="oper_status, rx_bytes"):
        asyncio.run(
            service.create_resource_configuration(
                _request(
                    field_mappings={
                        "input": FieldConfig(oid=".1.3.6.1.2.1.2.2.1.10"),
                        "rx_bytes": FieldConfig(oid=".1.3.6.1.2.1.2.2.1.10"),
                        "oper_status": FieldConfig(oid=".1.3.6.1.2.1.2.2.1.8"),
                    }
                )
            )
        )

    assert storage.client.insert_calls == []


def test_create_rejects_unknown_resource_type():
    storage = DummyStorage(query_results=[[]], definition=None)
    service = CollectorService(storage)

    with pytest.raises(LookupError, match="No resource type with slug 'nonexistent'"):
        asyncio.run(
            service.create_resource_configuration(_request(resource_type="nonexistent"))
        )


def test_update_appends_v2_snapshot_without_mutating():
    current = _config()
    storage = DummyStorage(query_results=[[_stored_row(current)]])
    service = _service(storage)

    result = asyncio.run(
        service.update_resource_configuration(
            "example_telegraf", ResourceConfigurationUpdate(interval=30)
        )
    )

    assert result.ref == "example_telegraf__v2"
    assert result.id == "example_telegraf"
    assert result.interval == 30
    # untouched fields carry over
    assert result.collector_plugin == "telegraf_vscode"
    assert result.timeout == 15
    assert result.field_mappings == current.field_mappings
    # append-only: an insert, never an ALTER
    assert len(storage.client.insert_calls) == 1


def test_update_rejects_invalid_field_mappings():
    storage = DummyStorage(query_results=[[_stored_row(_config())]])
    service = _service(storage)

    with pytest.raises(ValueError, match="bogus"):
        asyncio.run(
            service.update_resource_configuration(
                "example_telegraf",
                ResourceConfigurationUpdate(
                    field_mappings={"bogus": FieldConfig(oid=".1.2.3")}
                ),
            )
        )

    assert storage.client.insert_calls == []


def test_update_requires_at_least_one_field():
    storage = DummyStorage(query_results=[[_stored_row(_config())]])
    service = _service(storage)

    with pytest.raises(ValueError, match="^No fields provided to update$"):
        asyncio.run(
            service.update_resource_configuration(
                "example_telegraf", ResourceConfigurationUpdate()
            )
        )


def test_request_model_rejects_blank_name():
    # Previously a missing/blank name surfaced as a bare KeyError in the router.
    with pytest.raises(ValidationError):
        ResourceConfigurationRequest(
            name="",
            resource_type="interface",
            collector_plugin="telegraf_vscode",
        )

    with pytest.raises(ValidationError):
        ResourceConfigurationRequest(resource_type="interface")


def test_update_treats_explicit_nulls_as_unchanged():
    storage = DummyStorage(query_results=[[_stored_row(_config())]])
    service = _service(storage)

    with pytest.raises(ValueError, match="^No fields provided to update$"):
        asyncio.run(
            service.update_resource_configuration(
                "example_telegraf",
                ResourceConfigurationUpdate(name=None, interval=None),
            )
        )

    assert storage.client.insert_calls == []


def test_update_reports_missing_configuration():
    storage = DummyStorage(query_results=[[]])
    service = _service(storage)

    with pytest.raises(LookupError, match="not found"):
        asyncio.run(
            service.update_resource_configuration(
                "nope", ResourceConfigurationUpdate(interval=30)
            )
        )


def test_get_collector_configurations_returns_latest_per_id():
    rows = [_stored_row(_config()), _stored_row(_config(id="other", ref="other__v1"))]
    storage = DummyStorage(query_results=[rows])
    service = CollectorService(storage)

    configs = asyncio.run(service.get_collector_configurations())

    assert [c.id for c in configs] == ["example_telegraf", "other"]
    query, _ = storage.client.query_calls[0]
    assert "LIMIT 1 BY id" in query
    # Ordered by ref version, not updated_at: DateTime is second-granular, so
    # two versions written in the same second would tie and pick arbitrarily.
    assert "extract(ref" in query
    assert "updated_at DESC" not in query


def test_get_collector_configurations_filters_by_plugin():
    storage = DummyStorage(query_results=[[]])
    service = CollectorService(storage)

    asyncio.run(
        service.get_collector_configurations(collector_plugin="telegraf_vscode")
    )

    query, parameters = storage.client.query_calls[0]
    assert "collector_plugin = {plugin:String}" in query
    assert parameters == {"plugin": "telegraf_vscode"}


def test_get_resource_configuration_by_id_returns_model():
    storage = DummyStorage(query_results=[[_stored_row(_config())]])
    service = CollectorService(storage)

    config = asyncio.run(service.get_resource_configuration_by_id("example_telegraf"))

    assert config == _config()


def test_get_resource_configuration_by_id_reports_missing():
    storage = DummyStorage(query_results=[[]])
    service = CollectorService(storage)

    with pytest.raises(LookupError, match="not found"):
        asyncio.run(service.get_resource_configuration_by_id("nope"))


def test_update_renders_the_new_snapshot():
    storage = DummyStorage(query_results=[[_stored_row(_config())]])
    plugin = DummyPlugin()
    service = _service(storage, plugin)

    result = asyncio.run(
        service.update_resource_configuration(
            "example_telegraf",
            ResourceConfigurationUpdate(interval=30),
            collector_plugin="telegraf_vscode",
        )
    )

    assert result.ref == "example_telegraf__v2"
    # the render sees the merged v2, not the stored v1
    assert [c.ref for c in plugin.rendered] == ["example_telegraf__v2"]
    assert plugin.rendered[0].interval == 30


def test_update_keeps_the_snapshot_when_rendering_fails():
    storage = DummyStorage(query_results=[[_stored_row(_config())]])
    service = _service(storage, DummyPlugin(error=OSError("read-only file system")))

    result = asyncio.run(
        service.update_resource_configuration(
            "example_telegraf", ResourceConfigurationUpdate(interval=30)
        )
    )

    # The insert is append-only and cannot be rolled back, so the version stands
    # and the render failure is logged rather than raised.
    assert len(storage.client.insert_calls) == 1
    assert result.ref == "example_telegraf__v2"


def test_update_refuses_configuration_owned_by_another_plugin():
    storage = DummyStorage(query_results=[[_stored_row(_config())]])
    service = _service(storage)

    with pytest.raises(LookupError, match="not found for plugin 'other_plugin'"):
        asyncio.run(
            service.update_resource_configuration(
                "example_telegraf",
                ResourceConfigurationUpdate(interval=30),
                collector_plugin="other_plugin",
            )
        )

    assert storage.client.insert_calls == []


def test_delete_removes_every_snapshot_of_the_id():
    storage = DummyStorage(query_results=[[_stored_row(_config())]])
    service = CollectorService(storage)

    result = asyncio.run(service.delete_resource_configuration("example_telegraf"))

    assert result["id"] == "example_telegraf"
    assert len(storage.client.command_calls) == 1
    command, parameters = storage.client.command_calls[0]
    # No ref predicate: every version of the id goes, not just the latest.
    assert "DELETE WHERE id = {id:String}" in command
    assert parameters == {"id": "example_telegraf"}


def test_delete_reports_missing_configuration():
    storage = DummyStorage(query_results=[[]])
    service = CollectorService(storage)

    with pytest.raises(LookupError, match="not found"):
        asyncio.run(service.delete_resource_configuration("nope"))

    assert storage.client.command_calls == []


def test_generate_configuration_rejects_unsafe_resource_type():
    storage = DummyStorage()
    service = CollectorService(storage)

    try:
        asyncio.run(
            service.generate_configuration(_config(resource_type="../../etc/passwd"))
        )
    except ValueError as exc:
        assert "Unsafe resource type name" in str(exc)
    else:
        raise AssertionError("expected ValueError for unsafe resource_type")
