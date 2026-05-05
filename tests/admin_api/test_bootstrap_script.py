from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_bootstrap_module():
    spec = importlib.util.spec_from_file_location(
        "bootstrap_script", "bin/bootstrap.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_collect_definitions_recursively(tmp_path):
    module = _load_bootstrap_module()

    types_file = tmp_path / "types.yaml"
    types_file.write_text(
        """
types:
  interface:
    data_fields:
      - field_name: timestamp
        field_type: DateTime64
        nullable: false
    meta_fields:
      - field_name: if_name
        field_type: String
        nullable: false
    identifier:
      - if_name
""".strip() + "\n",
        encoding="utf-8",
    )

    nested = tmp_path / "nested"
    nested.mkdir()
    transformers_file = nested / "transformers.yaml"
    transformers_file.write_text(
        """
transformers:
  all_interfaces:
    definition_id: def_interface
    description: Interface Traffic transformers
    match_field: protocol
    columns:
      inUcast_field:
        target_column: inUcast
        operation: field
        config:
          source: SNMP_IF-MIB::ifHCInUcastPkts
""".strip() + "\n",
        encoding="utf-8",
    )

    files = module.discover_yaml_files(tmp_path)
    assert files == sorted([types_file, transformers_file])

    type_defs, transformer_defs = module.collect_definitions(files)

    assert len(type_defs) == 1
    assert type_defs[0]["name"] == "interface"

    assert len(transformer_defs) == 1
    assert transformer_defs[0]["name"] == "all_interfaces"
    assert transformer_defs[0]["definition_ref"] == "def_interface__v1"
    assert transformer_defs[0]["columns"][0]["id"] == "inUcast_field"


def test_main_posts_wrapped_batch_payloads(tmp_path, monkeypatch, capsys):
    module = _load_bootstrap_module()

    bootstrap_file = tmp_path / "bootstrap.yaml"
    bootstrap_file.write_text(
        """
types:
  interface:
    data_fields:
      - field_name: timestamp
        field_type: DateTime64
        nullable: false
    meta_fields:
      - field_name: if_name
        field_type: String
        nullable: false
    identifier:
      - if_name
transformers:
  all_interfaces:
    definition_id: def_interface
    description: Interface Traffic transformers
    match_field: protocol
    columns:
      inUcast_field:
        target_column: inUcast
        operation: field
        config:
          source: SNMP_IF-MIB::ifHCInUcastPkts
""".strip() + "\n",
        encoding="utf-8",
    )

    calls = []

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload
            self.text = ""

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, json):
            calls.append((url, json))
            return FakeResponse({"created": [], "updated": [], "failed": []})

    monkeypatch.setattr(module.httpx, "Client", FakeClient)

    exit_code = module.main(
        [
            "--from",
            str(bootstrap_file),
            "--api-url",
            "http://localhost:8000/",
        ]
    )

    assert exit_code == 0
    assert len(calls) == 2

    assert calls[0][0] == "http://localhost:8000/type/batch"
    assert "definitions" in calls[0][1]

    assert calls[1][0] == "http://localhost:8000/transformers/batch"
    assert "transformers" in calls[1][1]

    output = capsys.readouterr().out
    assert "Types batch response:" in output
    assert "Transformers batch response:" in output


def test_main_returns_error_for_missing_required_top_level_key(tmp_path, capsys):
    module = _load_bootstrap_module()

    invalid_file = tmp_path / "invalid.yaml"
    invalid_file.write_text(
        """
metadata:
  foo: bar
""".strip() + "\n",
        encoding="utf-8",
    )

    exit_code = module.main(
        [
            "--from",
            str(invalid_file),
            "--api-url",
            "http://localhost:8000",
        ]
    )

    assert exit_code == 1
    stderr = capsys.readouterr().err
    assert "types' and/or 'transformers" in stderr
