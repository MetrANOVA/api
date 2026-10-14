import pytest
from fastapi.testclient import TestClient

from admin_api.app import app
from admin_api.paging import ListParams, limit_clause, search_clause

INTERFACE_COLUMNS = [
    ("id", "String"),
    ("ref", "String"),
    ("hash", "String"),
    ("updated_at", "DateTime"),
    ("tag", "Array(LowCardinality(String))"),
    ("policy_level", "LowCardinality(String)"),
    ("node", "String"),
    ("interface_id", "Int32"),
    ("ext", "JSON"),
]


class QueryResult:
    def __init__(self, named_rows=None, result_rows=None):
        self._named_rows = named_rows or []
        self.result_rows = result_rows or []
        self.row_count = len(self._named_rows) or len(self.result_rows)

    def named_results(self):
        return iter(self._named_rows)


class FakeClient:
    def __init__(self, rows, total):
        self.rows = rows
        self.total = total
        self.queries = []

    async def query(self, query, parameters=None):
        self.queries.append((query, dict(parameters or {})))
        if "system.columns" in query:
            return QueryResult(result_rows=INTERFACE_COLUMNS)
        if query.lstrip().startswith("SELECT count()"):
            return QueryResult(result_rows=[[self.total]])
        return QueryResult(named_rows=self.rows)

    @property
    def data_queries(self):
        return [
            (query, parameters)
            for query, parameters in self.queries
            if "system.columns" not in query
            and not query.lstrip().startswith("SELECT count()")
        ]

    @property
    def count_queries(self):
        return [
            (query, parameters)
            for query, parameters in self.queries
            if query.lstrip().startswith("SELECT count()")
        ]


class FakeStorage:
    def __init__(self, rows, total):
        self.database = "metranova"
        self.client = FakeClient(rows, total)

    def close(self):
        pass

    def _qualified_table_name(self, table):
        return f"`{self.database}`.`{table}`"

    def _quoted_identifier(self, name):
        return f"`{name}`"


@pytest.fixture
def api(monkeypatch):
    def start(rows, total=None):
        storage = FakeStorage(rows, len(rows) if total is None else total)

        async def fake_create(cls):
            return storage

        monkeypatch.setattr(
            "admin_api.context.Clickhouse.create", classmethod(fake_create)
        )
        client = TestClient(app)
        client.__enter__()
        clients.append(client)
        return client, storage.client

    clients = []
    yield start
    for client in clients:
        client.__exit__(None, None, None)


# --- helpers ---


def test_limit_clause_is_empty_without_paging():
    parameters = {}

    assert limit_clause(ListParams(), parameters) == ""
    assert parameters == {}


def test_limit_clause_binds_limit_and_offset():
    parameters = {}

    clause = limit_clause(ListParams(limit=25, offset=50), parameters)

    assert clause == " LIMIT {limit:UInt32} OFFSET {offset:UInt32}"
    assert parameters == {"limit": 25, "offset": 50}


def test_search_clause_binds_the_term_instead_of_inlining_it():
    parameters = {}

    clause = search_clause(["`node`", "`intf`"], parameters, "x' OR 1=1")

    assert "x'" not in clause
    assert clause.count("{q:String}") == 2
    assert parameters == {"q": "x' OR 1=1"}


def test_search_clause_matches_nothing_without_columns():
    assert search_clause([], {}, "anything") == "0"


# --- GET /metadata/{slug} ---


def test_metadata_records_unpaged_returns_everything_with_total(api):
    client, ch = api([{"id": "a"}, {"id": "b"}, {"id": "c"}])

    response = client.get("/metadata/interface")

    assert response.status_code == 200
    assert len(response.json()) == 3
    assert response.headers["X-Total-Count"] == "3"
    assert ch.count_queries == []
    query, parameters = ch.data_queries[0]
    assert "LIMIT" not in query
    assert "ORDER BY `id` ASC, id ASC" in query
    # The row_number() helper column stays out of the response.
    assert "SELECT * EXCEPT (_rn)" in query
    assert parameters == {"db": "metranova", "table": "meta_interface"}


def test_metadata_records_paged_reports_total_of_all_matches(api):
    client, ch = api([{"id": "a"}, {"id": "b"}], total=137)

    response = client.get("/metadata/interface", params={"limit": 2, "offset": 4})

    assert response.status_code == 200
    assert len(response.json()) == 2
    assert response.headers["X-Total-Count"] == "137"
    query, parameters = ch.data_queries[0]
    assert query.rstrip().endswith("LIMIT {limit:UInt32} OFFSET {offset:UInt32}")
    assert parameters["limit"] == 2
    assert parameters["offset"] == 4
    count_query, count_parameters = ch.count_queries[0]
    assert "LIMIT" not in count_query
    assert "limit" not in count_parameters


def test_metadata_records_search_covers_string_columns_only(api):
    client, ch = api([{"id": "a"}], total=1)

    response = client.get("/metadata/interface", params={"q": "core", "limit": 10})

    assert response.status_code == 200
    query, parameters = ch.data_queries[0]
    assert parameters["q"] == "core"
    for column in ("`id`", "`ref`", "`node`", "`policy_level`"):
        assert f"positionCaseInsensitiveUTF8({column}, {{q:String}})" in query
    for column in ("`hash`", "`interface_id`", "`tag`", "`ext`", "`updated_at`"):
        assert f"positionCaseInsensitiveUTF8({column}," not in query
    count_query, count_parameters = ch.count_queries[0]
    assert "positionCaseInsensitiveUTF8" in count_query
    assert count_parameters["q"] == "core"


def test_metadata_records_sorts_by_requested_column(api):
    client, ch = api([{"id": "a"}])

    response = client.get(
        "/metadata/interface", params={"sort": "interface_id", "order": "desc"}
    )

    assert response.status_code == 200
    assert "ORDER BY `interface_id` DESC, id ASC" in ch.data_queries[0][0]


@pytest.mark.parametrize("column", ["missing", "tag", "ext", "id; DROP TABLE x"])
def test_metadata_records_rejects_unknown_or_unsortable_column(api, column):
    client, ch = api([{"id": "a"}])

    response = client.get("/metadata/interface", params={"sort": column})

    assert response.status_code == 400
    assert response.json()["detail"] == f"Cannot sort by '{column}'."
    assert ch.data_queries == []


@pytest.mark.parametrize(
    "params",
    [{"limit": 0}, {"limit": 501}, {"offset": -1}, {"order": "sideways"}],
)
def test_list_parameters_are_validated(api, params):
    client, _ = api([])

    assert client.get("/metadata/interface", params=params).status_code == 422
    assert client.get("/transformers/", params=params).status_code == 422


# --- GET /transformers/ ---


def test_transformers_unpaged_returns_everything_with_total(api):
    client, ch = api([{"id": "a"}, {"id": "b"}])

    response = client.get("/transformers/")

    assert response.status_code == 200
    assert len(response.json()) == 2
    assert response.headers["X-Total-Count"] == "2"
    assert ch.count_queries == []
    query, parameters = ch.data_queries[0]
    assert query.endswith("ORDER BY name ASC, ref ASC")
    assert parameters == {}


def test_transformers_paged_counts_with_the_same_filters(api):
    client, ch = api([{"id": "a"}], total=31)

    response = client.get(
        "/transformers/",
        params={
            "definition_ref": "def_cpu__%",
            "q": "gnmi",
            "sort": "updated_at",
            "order": "desc",
            "limit": 1,
            "offset": 3,
        },
    )

    assert response.status_code == 200
    assert response.headers["X-Total-Count"] == "31"
    query, parameters = ch.data_queries[0]
    assert "definition_ref LIKE {definition_ref:String} AND (" in query
    assert "ORDER BY updated_at DESC, ref ASC LIMIT {limit:UInt32}" in query
    assert parameters == {
        "definition_ref": "def_cpu__%",
        "q": "gnmi",
        "limit": 1,
        "offset": 3,
    }
    count_query, count_parameters = ch.count_queries[0]
    assert "definition_ref LIKE {definition_ref:String} AND (" in count_query
    assert count_parameters == {"definition_ref": "def_cpu__%", "q": "gnmi"}


def test_transformers_rejects_unknown_sort_column(api):
    client, ch = api([{"id": "a"}])

    response = client.get("/transformers/", params={"sort": "name; DROP TABLE x"})

    assert response.status_code == 400
    assert ch.queries == []


# --- CORS ---


def test_total_count_header_is_exposed_to_browsers(api):
    client, _ = api([{"id": "a"}])

    response = client.get(
        "/transformers/", headers={"Origin": "https://ui.grnoc.iu.edu"}
    )

    assert response.headers["access-control-expose-headers"] == "X-Total-Count"
