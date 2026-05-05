#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import httpx
import yaml

YAML_SUFFIXES = {".yaml", ".yml"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap resource types and transformers via Admin API batch endpoints."
    )
    parser.add_argument(
        "--from",
        dest="source",
        required=True,
        help="YAML file or directory containing YAML files.",
    )
    parser.add_argument(
        "--api-url",
        required=True,
        help="Base URL where the Admin API is running, e.g. http://localhost:8000",
    )
    return parser.parse_args(argv)


def discover_yaml_files(source: Path) -> list[Path]:
    if not source.exists():
        raise ValueError(f"Input path does not exist: {source}")

    if source.is_file():
        if source.suffix.lower() not in YAML_SUFFIXES:
            raise ValueError(f"Input file must end with .yaml or .yml: {source}")
        return [source]

    if not source.is_dir():
        raise ValueError(f"Input path must be a file or directory: {source}")

    files = sorted(
        path
        for path in source.rglob("*")
        if path.is_file() and path.suffix.lower() in YAML_SUFFIXES
    )
    if not files:
        raise ValueError(f"No YAML files found under directory: {source}")

    return files


def load_bootstrap_yaml(file_path: Path) -> dict[str, Any]:
    try:
        raw = yaml.safe_load(file_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in {file_path}: {exc}") from exc

    if raw is None:
        raise ValueError(f"YAML file is empty: {file_path}")

    if not isinstance(raw, dict):
        raise ValueError(f"Top level must be a mapping in {file_path}")

    if "types" not in raw and "transformers" not in raw:
        raise ValueError(
            f"File must include top-level 'types' and/or 'transformers': {file_path}"
        )

    return raw


def _normalize_types(raw_types: Any, file_path: Path) -> list[dict[str, Any]]:
    if raw_types is None:
        return []

    if isinstance(raw_types, list):
        normalized: list[dict[str, Any]] = []
        for idx, item in enumerate(raw_types):
            if not isinstance(item, dict):
                raise ValueError(f"types[{idx}] must be an object in {file_path}")
            if "name" not in item:
                raise ValueError(f"types[{idx}] must include 'name' in {file_path}")
            normalized.append(dict(item))
        return normalized

    if isinstance(raw_types, dict):
        normalized = []
        for type_name, spec in raw_types.items():
            if not isinstance(spec, dict):
                raise ValueError(f"types.{type_name} must be an object in {file_path}")
            payload = dict(spec)
            payload.setdefault("name", str(type_name))
            normalized.append(payload)
        return normalized

    raise ValueError(f"Top-level 'types' must be an object or list in {file_path}")


def _normalize_columns(
    raw_columns: Any, file_path: Path, transformer_name: str
) -> list[dict[str, Any]]:
    if raw_columns is None:
        return []

    if isinstance(raw_columns, list):
        normalized: list[dict[str, Any]] = []
        for idx, item in enumerate(raw_columns):
            if not isinstance(item, dict):
                raise ValueError(
                    f"columns[{idx}] for transformer '{transformer_name}' must be an object in {file_path}"
                )
            if "id" not in item:
                raise ValueError(
                    f"columns[{idx}] for transformer '{transformer_name}' must include 'id' in {file_path}"
                )
            normalized.append(dict(item))
        return normalized

    if isinstance(raw_columns, dict):
        normalized = []
        for column_id, spec in raw_columns.items():
            if not isinstance(spec, dict):
                raise ValueError(
                    f"columns.{column_id} for transformer '{transformer_name}' must be an object in {file_path}"
                )
            payload = dict(spec)
            payload.setdefault("id", str(column_id))
            normalized.append(payload)
        return normalized

    raise ValueError(
        f"'columns' for transformer '{transformer_name}' must be an object or list in {file_path}"
    )


def _normalize_transformer_item(
    item: dict[str, Any], file_path: Path, name_fallback: str | None = None
) -> dict[str, Any]:
    payload = dict(item)

    if "name" not in payload:
        if name_fallback is None:
            raise ValueError(f"Transformer entry must include 'name' in {file_path}")
        payload["name"] = name_fallback

    if "definition_ref" not in payload and "definition_id" in payload:
        definition_id = str(payload.pop("definition_id"))
        if "__v" in definition_id:
            payload["definition_ref"] = definition_id
        else:
            payload["definition_ref"] = f"{definition_id}__v1"

    payload["columns"] = _normalize_columns(
        payload.get("columns"), file_path, str(payload["name"])
    )

    return payload


def _normalize_transformers(
    raw_transformers: Any, file_path: Path
) -> list[dict[str, Any]]:
    if raw_transformers is None:
        return []

    if isinstance(raw_transformers, list):
        normalized: list[dict[str, Any]] = []
        for idx, item in enumerate(raw_transformers):
            if not isinstance(item, dict):
                raise ValueError(
                    f"transformers[{idx}] must be an object in {file_path}"
                )
            normalized.append(_normalize_transformer_item(item, file_path))
        return normalized

    if isinstance(raw_transformers, dict):
        normalized = []
        for transformer_name, spec in raw_transformers.items():
            if not isinstance(spec, dict):
                raise ValueError(
                    f"transformers.{transformer_name} must be an object in {file_path}"
                )
            normalized.append(
                _normalize_transformer_item(
                    spec,
                    file_path,
                    name_fallback=str(transformer_name),
                )
            )
        return normalized

    raise ValueError(
        f"Top-level 'transformers' must be an object or list in {file_path}"
    )


def collect_definitions(
    files: list[Path],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    all_types: list[dict[str, Any]] = []
    all_transformers: list[dict[str, Any]] = []
    errors: list[str] = []

    for file_path in files:
        try:
            data = load_bootstrap_yaml(file_path)
            all_types.extend(_normalize_types(data.get("types"), file_path))
            all_transformers.extend(
                _normalize_transformers(data.get("transformers"), file_path)
            )
        except ValueError as exc:
            errors.append(str(exc))

    if errors:
        details = "\n".join(f"- {msg}" for msg in errors)
        raise ValueError(f"Bootstrap validation failed:\n{details}")

    if not all_types and not all_transformers:
        raise ValueError("No type or transformer definitions found.")

    return all_types, all_transformers


def post_batch_payload(
    client: httpx.Client,
    api_url: str,
    endpoint: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    url = f"{api_url.rstrip('/')}{endpoint}"
    print(payload)
    response = client.post(url, json=payload)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        body = response.text.strip()
        raise RuntimeError(f"Request to {url} failed: {exc}. Body: {body}") from exc

    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError(f"Response from {url} is not JSON") from exc


def run_bootstrap(source: Path, api_url: str) -> tuple[dict[str, Any], int]:
    files = discover_yaml_files(source)
    type_definitions, transformer_definitions = collect_definitions(files)

    print(f"Discovered {len(files)} YAML file(s).")
    print(f"Collected {len(type_definitions)} type definition(s).")
    print(f"Collected {len(transformer_definitions)} transformer definition(s).")

    results: dict[str, Any] = {}
    with httpx.Client(timeout=30.0) as client:
        if type_definitions:
            results["types"] = post_batch_payload(
                client,
                api_url,
                "/type/batch",
                {"definitions": type_definitions},
            )

        if transformer_definitions:
            results["transformers"] = post_batch_payload(
                client,
                api_url,
                "/transformers/batch",
                {"transformers": transformer_definitions},
            )

    exit_code = 0
    for response in results.values():
        if isinstance(response, dict) and response.get("failed"):
            exit_code = 1
            break

    return results, exit_code


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        source = Path(args.source).expanduser().resolve()
        results, exit_code = run_bootstrap(source, args.api_url)

        for key, value in results.items():
            print(f"\n{key.title()} batch response:")
            print(json.dumps(value, indent=2, sort_keys=True))

        return exit_code
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
