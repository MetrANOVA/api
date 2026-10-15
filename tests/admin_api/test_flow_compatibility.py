import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from admin_api.metadata.service import MetadataService, resolve_python_type

spec = importlib.util.spec_from_file_location(
    "bootstrap_flow_metadata",
    Path(__file__).resolve().parents[2] / "bin" / "bootstrap-flow-metadata.py",
)
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)
BASE_FIELDS = bootstrap.BASE_FIELDS
COMPATIBILITY_FIELDS = bootstrap.COMPATIBILITY_FIELDS
DEVICE_FIELDS = bootstrap.DEVICE_FIELDS
INTERFACE_FIELDS = bootstrap.INTERFACE_FIELDS
bootstrap_flow_metadata = bootstrap.bootstrap_flow_metadata


class Client:
    def __init__(self, tables):
        self.tables = tables
        self.commands = []
        self.inserts = []

    async def query(self, query, parameters):
        if "SELECT engine" in query:
            rows = [("MergeTree",)]
        else:
            rows = list(self.tables.get(parameters["table"], {}).items())
        return SimpleNamespace(result_rows=rows)

    async def command(self, query):
        self.commands.append(query)
        for table in ("meta_device", "meta_interface"):
            if table in query:
                for name, typ in COMPATIBILITY_FIELDS:
                    if f"`{name}`" in query:
                        self.tables[table][name] = typ.split(" DEFAULT ")[0]

    async def insert(self, **kwargs):
        self.inserts.append(kwargs)


class Storage:
    database = "demo"
    metadata_engine = "MergeTree"

    def __init__(self):
        self.client = Client({
            "meta_device": {n: t.split(" DEFAULT ")[0] for n, t in BASE_FIELDS + DEVICE_FIELDS},
            "meta_interface": {n: t.split(" DEFAULT ")[0] for n, t in BASE_FIELDS + INTERFACE_FIELDS},
        })
        self.existing = {}

    async def _get_on_cluster_clause(self):
        return " ON CLUSTER 'demo-cluster'"

    def _qualified_table_name(self, table):
        return f"`demo`.`{table}`"

    async def find_resource_type_by_slug(self, slug):
        return self.existing.get(slug)


def test_adoption_is_additive_cluster_aware_and_idempotent():
    storage = Storage()
    asyncio.run(bootstrap_flow_metadata(storage))
    assert len(storage.client.commands) == 10
    assert all(" ON CLUSTER 'demo-cluster'" in q for q in storage.client.commands)
    assert len(storage.client.inserts) == 2
    for insert in storage.client.inserts:
        row = dict(zip(insert["column_names"], insert["data"][0]))
        storage.existing[row["slug"]] = {
            **row, "meta_fields": [
                dict(zip(("field_name", "field_type", "nullable", "table"), f))
                for f in row["meta_fields"]
            ],
        }
        assert row["identifier"] == ["id"]
    asyncio.run(bootstrap_flow_metadata(storage))
    assert len(storage.client.commands) == 10
    assert len(storage.client.inserts) == 2


def test_dry_run_does_not_write():
    storage = Storage()
    asyncio.run(bootstrap_flow_metadata(storage, dry_run=True))
    assert storage.client.commands == []
    assert storage.client.inserts == []


def test_preflight_rejects_incompatible_table_before_any_ddl():
    storage = Storage()
    storage.client.tables["meta_interface"]["flow_index"] = "String"
    with pytest.raises(ValueError, match="flow_index"):
        asyncio.run(bootstrap_flow_metadata(storage))
    assert storage.client.commands == []


def test_preflight_refuses_snmp_definition_conflict():
    storage = Storage()
    storage.existing["interface"] = {"identifier": ["node", "intf"], "data_fields": []}
    with pytest.raises(ValueError, match="conflicts"):
        asyncio.run(bootstrap_flow_metadata(storage))
    assert storage.client.commands == []


@pytest.mark.parametrize("typ,expected", [
    ("LowCardinality(Nullable(String))", str),
    ("Nullable(UInt32)", int), ("Nullable(Float64)", float),
    ("Array(Nullable(String))", list), ("Nullable(IPv4)", str),
    ("Tuple(String, UInt32)", list),
])
def test_type_wrappers(typ, expected):
    assert resolve_python_type(typ) is expected


@pytest.mark.parametrize("field_type,value", [
    ("Nullable(UInt32)", 42),
    ("LowCardinality(Nullable(UInt32))", 42),
    ("Nullable(LowCardinality(String))", "router"),
    ("LowCardinality(String)", "router"),
    ("Nullable(Float64)", 1.5),
    ("Nullable(IPv4)", "192.0.2.1"),
    ("Tuple(String, UInt32)", ["router", 42]),
])
def test_wrapped_types_validate_values_and_reject_wrong_types(field_type, value):
    service = MetadataService(SimpleNamespace(client=None))
    definition = {
        "identifier": ["id"],
        "meta_fields": [
            {"field_name": "value", "field_type": field_type, "nullable": True},
        ],
    }
    asyncio.run(service.validate_metadata_record(definition, {"id": "x", "value": value}))
    asyncio.run(service.validate_metadata_record(definition, {"id": "x", "value": None}))
    wrong_value = 42 if isinstance(value, str) else str(value)
    with pytest.raises(ValueError, match="not of type"):
        asyncio.run(service.validate_metadata_record(
            definition, {"id": "x", "value": wrong_value},
        ))


@pytest.mark.parametrize("mid,encoded", [
    ("router::xe-0/0/1", "router%3A%3Axe-0%2F0%2F1"),
    ("router::eth0", "router%3A%3Aeth0"),
])
def test_interface_ids_with_slashes_are_routable(monkeypatch, mid, encoded):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from admin_api.metadata.router import router

    async def get_type(self, slug):
        return {"identifier": ["id"], "meta_fields": []}

    async def update(self, definition, record, version):
        copied_record = record.copy()
        copied_record["ref"] = f"{record['id']}__v{version}"

    async def history(self, slug, mid):
        return [{"id": mid}]

    monkeypatch.setattr(MetadataService, "get_metadata_type", get_type)
    monkeypatch.setattr(MetadataService, "update_metadata_record", update)
    monkeypatch.setattr(MetadataService, "get_metadata_record_history", history)
    app = FastAPI()
    app.state.se = SimpleNamespace(client=None)
    app.include_router(router, prefix="/metadata")
    client = TestClient(app)
    response = client.put(
        f"/metadata/interface/{encoded}/1",
        json={"id": mid, "flow_index": 42},
    )
    assert response.status_code == 200
    assert response.json() == {"id": mid, "ref": f"{mid}__v1"}
    assert client.get(f"/metadata/interface/{encoded}").json() == [{"id": mid}]
