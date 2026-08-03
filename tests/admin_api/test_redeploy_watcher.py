import asyncio
from types import SimpleNamespace

from admin_api.redeploy_watcher import DefinitionTransformerRedeployWatcher


class FakeClient:
    def __init__(self, storage):
        self.storage = storage
        self.queries: list[str] = []

    async def query(self, query: str):
        self.queries.append(query)
        q = query.lower()
        for table, value in self.storage.fingerprints.items():
            if f"`{table}`" in q:
                return SimpleNamespace(result_rows=[value])
        raise AssertionError(f"Unexpected query: {query}")


class FakeStorage:
    def __init__(self):
        self.database = "metranova"
        self.client = FakeClient(self)
        self.restart_called = 0
        # (count, hash) per table
        self.fingerprints = {
            "definition": (1, 111),
            "transformer": (1, 222),
            "transformer_column": (2, 333),
        }

    def _qualified_table_name(self, table_name: str) -> str:
        return f"`{self.database}`.`{table_name}`"

    async def restart_pipelines(self):
        self.restart_called += 1


def test_initial_poll_does_not_restart_by_default():
    storage = FakeStorage()
    watcher = DefinitionTransformerRedeployWatcher(storage=storage)
    assert asyncio.run(watcher.poll_once()) is False
    assert storage.restart_called == 0


def test_initial_poll_restarts_when_trigger_on_startup_true_and_non_empty():
    storage = FakeStorage()
    watcher = DefinitionTransformerRedeployWatcher(
        storage=storage, trigger_on_startup=True
    )
    assert asyncio.run(watcher.poll_once()) is True
    assert storage.restart_called == 1


def test_no_change_does_not_restart():
    storage = FakeStorage()
    watcher = DefinitionTransformerRedeployWatcher(storage=storage)
    asyncio.run(watcher.poll_once())
    assert asyncio.run(watcher.poll_once()) is False
    assert storage.restart_called == 0


def test_definition_change_triggers_restart():
    storage = FakeStorage()
    watcher = DefinitionTransformerRedeployWatcher(storage=storage)
    asyncio.run(watcher.poll_once())
    storage.fingerprints["definition"] = (1, 999)
    assert asyncio.run(watcher.poll_once()) is True
    assert storage.restart_called == 1


def test_transformer_change_triggers_restart():
    storage = FakeStorage()
    watcher = DefinitionTransformerRedeployWatcher(storage=storage)
    asyncio.run(watcher.poll_once())
    storage.fingerprints["transformer"] = (2, 888)
    assert asyncio.run(watcher.poll_once()) is True
    assert storage.restart_called == 1


def test_transformer_column_change_triggers_restart():
    storage = FakeStorage()
    watcher = DefinitionTransformerRedeployWatcher(storage=storage)
    asyncio.run(watcher.poll_once())
    # New column added -> count and hash change
    storage.fingerprints["transformer_column"] = (3, 777)
    assert asyncio.run(watcher.poll_once()) is True
    assert storage.restart_called == 1


def test_transformer_column_fingerprint_query_is_null_safe():
    storage = FakeStorage()
    watcher = DefinitionTransformerRedeployWatcher(storage=storage)

    asyncio.run(
        watcher._fingerprint(
            "transformer_column",
            (
                "id",
                "transformer_ref",
                "target_column",
                "match_value",
                "vendor_match_field",
                "vendor_match_value",
                "operation",
                "config",
                "default_value",
                "`order`",
            ),
        )
    )

    query = storage.client.queries[-1]
    assert "ifNull(toString(match_value), '<NULL>')" in query
    assert "ifNull(toString(vendor_match_field), '<NULL>')" in query
    assert "ifNull(toString(vendor_match_value), '<NULL>')" in query
    assert "ifNull(toString(default_value), '<NULL>')" in query
