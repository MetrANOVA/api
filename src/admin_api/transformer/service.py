import logging
import json

from metranova.storage.clickhouse import Clickhouse
from fastapi import Depends
from typing import Optional
from ..context import get_clickhouse
from metranova.transformer.operations import operations, validate_config

logger = logging.getLogger(__name__)


class TransformerService:
    def __init__(
        self,
        storage: Clickhouse = Depends(get_clickhouse),
    ):
        self.storage = storage

    async def create_transformer(
        self, name: str, definition_ref: str, description: str, match_field: str
    ) -> tuple[bool, dict]:
        await self.storage.ensure_transformer_table()

        slug = name.lower().replace(" ", "_")

        table_name = self.storage._qualified_table_name("transformer")
        try:
            # Identify existing records with matching id or slug
            result = await self.storage.client.query(
                f"SELECT id FROM {table_name}"
                + " WHERE id = {s:String} OR slug = {s:String}",
                parameters={"s": slug},
            )
            if result.row_count != 0:
                raise Exception("A record with that id or slug already exists")

            # Lookup the referenced definition
            result = await self.storage.client.query(
                f"SELECT id from {self.storage._qualified_table_name('definition')}"
                + " WHERE ref = {ref:String}",
                parameters={"ref": definition_ref},
            )
            if result.row_count == 0:
                raise Exception("No definition with that ref found")

            data = {
                "id": slug,
                "ref": f"{slug}__v1",
                "definition_ref": definition_ref,
                "name": name,
                "slug": slug,
                "description": description,
                "match_field": match_field,
            }
            await self.storage.client.insert(
                table="transformer",
                database=self.storage.database,
                column_names=list(data.keys()),
                data=[list(data.values())],
            )
            return True, data
        except Exception as e:
            logger.exception("Error creating transformer")
            return False, {"message": f"Error creating transformer: {e}"}

    async def get_all_transformers(
        self, definition_ref: str | None = None
    ) -> list[dict]:
        await self.storage.ensure_transformer_table()

        table_name = self.storage._qualified_table_name("transformer")
        if definition_ref is not None:
            result = await self.storage.client.query(
                f"SELECT id, ref, definition_ref, name, slug, description, match_field, updated_at FROM {table_name}"
                + " WHERE definition_ref LIKE {definition_ref:String} ORDER BY name",
                parameters={"definition_ref": f"{definition_ref}__%"},
            )
        else:
            result = await self.storage.client.query(
                f"SELECT id, ref, definition_ref, name, slug, description, match_field, updated_at FROM {table_name}"
                + " ORDER BY name",
                parameters={},
            )

        if hasattr(result, "named_results"):
            return list(result.named_results())

        columns = [
            "id",
            "ref",
            "definition_ref",
            "name",
            "slug",
            "description",
            "match_field",
            "updated_at",
        ]
        return [
            dict(zip(columns, row))
            for row in (getattr(result, "result_rows", None) or [])
        ]

    async def get_transformer_by_id(self, transformer_id: str) -> tuple[bool, dict]:
        await self.storage.ensure_transformer_table()

        table_name = self.storage._qualified_table_name("transformer")
        try:
            result = await self.storage.client.query(
                f"SELECT id, ref, definition_ref, name, slug, description, match_field, updated_at FROM {table_name}"
                + " WHERE id = {id:String} ORDER BY updated_at DESC LIMIT 1",
                parameters={"id": transformer_id},
            )

            if result.row_count == 0:
                return False, {
                    "message": f"Transformer with id '{transformer_id}' not found"
                }

            if hasattr(result, "named_results"):
                rows = list(result.named_results())
                if rows:
                    return True, rows[0]

            raw_rows = getattr(result, "result_rows", None) or []
            if raw_rows:
                values = raw_rows[0]
                columns = [
                    "id",
                    "ref",
                    "definition_ref",
                    "name",
                    "slug",
                    "description",
                    "match_field",
                    "updated_at",
                ]
                return True, dict(zip(columns, values))

            return False, {"message": "Error fetching transformer: empty result"}
        except Exception as e:
            logger.exception("Error fetching transformer")
            return False, {"message": f"Error fetching transformer: {e}"}

    async def update_transformer(
        self,
        transformer_id: str,
        name: str | None,
        description: str | None,
        match_field: str | None,
    ) -> tuple[bool, dict]:
        await self.storage.ensure_transformer_table()

        try:
            # Confirm the transformer exists first and keep the current payload
            found, current = await self.get_transformer_by_id(transformer_id)
            if not found:
                return False, current

            if name is None and description is None and match_field is None:
                return False, {"message": "No fields provided to update"}

            ref_parts = current["ref"].rsplit("__v", 1)
            if len(ref_parts) == 2 and ref_parts[1].isdigit():
                new_ref = f"{ref_parts[0]}__v{int(ref_parts[1]) + 1}"
            else:
                new_ref = f"{current['ref']}__v2"

            data = {
                "id": transformer_id,
                "ref": new_ref,
                "definition_ref": current["definition_ref"],
                "name": name if name is not None else current["name"],
                "slug": (
                    name.lower().replace(" ", "_")
                    if name is not None
                    else current["slug"]
                ),
                "description": (
                    description if description is not None else current["description"]
                ),
                "match_field": (
                    match_field if match_field is not None else current["match_field"]
                ),
            }

            await self.storage.client.insert(
                table="transformer",
                database=self.storage.database,
                column_names=list(data.keys()),
                data=[list(data.values())],
            )

            return True, data
        except Exception as e:
            logger.exception("Error updating transformer")
            return False, {"message": f"Error updating transformer: {e}"}

    async def create_transformer_column(
        self,
        id: str,
        transformer_ref: str,
        target_column: str,
        match_value: str | None,
        vendor_match_field: str | None,
        vendor_match_value: str | None,
        operation: str,
        config: dict,
        default_value: str | None,
        order: int = 1,
    ) -> tuple[bool, dict]:
        await self.storage.ensure_transformer_column_table()

        try:
            if operation not in operations:
                raise Exception("Unknown operation: " + operation)

            # Validate config against operation schema
            is_valid, error_message = validate_config(operation, config)
            if not is_valid:
                raise Exception(
                    f"Invalid config for operation '{operation}': {error_message}"
                )

            data = {
                "id": id,
                "transformer_ref": transformer_ref,
                "target_column": target_column,
                "match_value": match_value,
                "vendor_match_field": vendor_match_field,
                "vendor_match_value": vendor_match_value,
                "operation": operation,
                "config": json.dumps(config),
                "default_value": default_value,
                "order": order,
            }
            await self.storage.client.insert(
                table="transformer_column",
                database=self.storage.database,
                column_names=list(data.keys()),
                data=[list(data.values())],
            )
            return True, data

        except Exception as e:
            logger.exception("Error creating transformer column")
            return False, {"message": f"Error creating transformer column: {e}"}

    async def get_transformer_columns(self, transformer_ref: str) -> list[dict]:
        await self.storage.ensure_transformer_column_table()

        table_name = self.storage._qualified_table_name("transformer_column")
        result = await self.storage.client.query(
            f"SELECT id, transformer_ref, target_column, match_value, vendor_match_field, vendor_match_value, operation, config, default_value, `order` FROM {table_name}"
            + " WHERE transformer_ref = {transformer_ref:String} ORDER BY `order`, target_column",
            parameters={"transformer_ref": transformer_ref},
        )

        if hasattr(result, "named_results"):
            return list(result.named_results())

        columns = [
            "id",
            "transformer_ref",
            "target_column",
            "match_value",
            "vendor_match_field",
            "vendor_match_value",
            "operation",
            "config",
            "default_value",
            "order",
        ]
        return [
            dict(zip(columns, row))
            for row in (getattr(result, "result_rows", None) or [])
        ]

    async def get_transformer_column_by_id(
        self, transformer_ref: str, column_id: str
    ) -> tuple[bool, dict]:
        await self.storage.ensure_transformer_column_table()

        table_name = self.storage._qualified_table_name("transformer_column")
        result = await self.storage.client.query(
            f"SELECT id, transformer_ref, target_column, match_value, vendor_match_field, vendor_match_value, operation, config, default_value, `order` FROM {table_name}"
            + " WHERE transformer_ref = {transformer_ref:String} AND id = {id:String} LIMIT 1",
            parameters={"transformer_ref": transformer_ref, "id": column_id},
        )

        if result.row_count == 0:
            return False, {
                "message": f"Transformer column with id '{column_id}' not found"
            }

        if hasattr(result, "named_results"):
            rows = list(result.named_results())
            if rows:
                return True, rows[0]

        rows = getattr(result, "result_rows", None) or []
        if rows:
            values = rows[0]
            columns = [
                "id",
                "transformer_ref",
                "target_column",
                "match_value",
                "vendor_match_field",
                "vendor_match_value",
                "operation",
                "config",
                "default_value",
                "order",
            ]
            return True, dict(zip(columns, values))

        return False, {"message": "Error fetching transformer column: empty result"}

    async def update_transformer_column(
        self,
        transformer_ref: str,
        column_id: str,
        target_column: str | None,
        match_value: str | None,
        vendor_match_field: str | None,
        vendor_match_value: str | None,
        operation: str | None,
        config: dict | None,
        default_value: str | None,
        order: int | None,
    ) -> tuple[bool, dict]:
        await self.storage.ensure_transformer_column_table()

        found, current = await self.get_transformer_column_by_id(
            transformer_ref=transformer_ref,
            column_id=column_id,
        )
        if not found:
            return False, current

        if operation is not None and operation not in operations:
            return False, {"message": "Unknown operation: " + operation}

        # Validate config if provided
        if config is not None:
            # Use the new operation if provided, otherwise use the existing one
            operation_to_validate = operation or current.get("operation")
            is_valid, error_message = validate_config(operation_to_validate, config)
            if not is_valid:
                return False, {
                    "message": f"Invalid config for operation '{operation_to_validate}': {error_message}"
                }

        assignments: list[str] = []
        parameters: dict = {
            "transformer_ref": transformer_ref,
            "id": column_id,
        }

        if target_column is not None:
            assignments.append("target_column = {target_column:String}")
            parameters["target_column"] = target_column

        if match_value is not None:
            assignments.append("match_value = {match_value:String}")
            parameters["match_value"] = match_value

        if vendor_match_field is not None:
            assignments.append("vendor_match_field = {vendor_match_field:String}")
            parameters["vendor_match_field"] = vendor_match_field

        if vendor_match_value is not None:
            assignments.append("vendor_match_value = {vendor_match_value:String}")
            parameters["vendor_match_value"] = vendor_match_value

        if operation is not None:
            assignments.append("operation = {operation:String}")
            parameters["operation"] = operation

        if config is not None:
            assignments.append("config = {config:String}")
            parameters["config"] = json.dumps(config)

        if default_value is not None:
            assignments.append("default_value = {default_value:String}")
            parameters["default_value"] = default_value

        if order is not None:
            assignments.append("`order` = {order:UInt16}")
            parameters["order"] = order

        if not assignments:
            return False, {"message": "No fields provided to update"}

        table_name = self.storage._qualified_table_name("transformer_column")
        await self.storage.client.command(
            f"ALTER TABLE {table_name} UPDATE {', '.join(assignments)}"
            + " WHERE transformer_ref = {transformer_ref:String} AND id = {id:String}",
            parameters=parameters,
        )

        updated = dict(current)
        if target_column is not None:
            updated["target_column"] = target_column
        if match_value is not None:
            updated["match_value"] = match_value
        if vendor_match_field is not None:
            updated["vendor_match_field"] = vendor_match_field
        if vendor_match_value is not None:
            updated["vendor_match_value"] = vendor_match_value
        if operation is not None:
            updated["operation"] = operation
        if config is not None:
            updated["config"] = json.dumps(config)
        if default_value is not None:
            updated["default_value"] = default_value
        if order is not None:
            updated["order"] = order

        return True, updated

    async def delete_transformer_column(
        self, transformer_ref: str, column_id: str
    ) -> tuple[bool, dict]:
        await self.storage.ensure_transformer_column_table()

        found, column = await self.get_transformer_column_by_id(
            transformer_ref=transformer_ref,
            column_id=column_id,
        )
        if not found:
            return False, column

        table_name = self.storage._qualified_table_name("transformer_column")
        await self.storage.client.command(
            f"ALTER TABLE {table_name} DELETE"
            + " WHERE transformer_ref = {transformer_ref:String} AND id = {id:String}",
            parameters={"transformer_ref": transformer_ref, "id": column_id},
        )

        return True, {
            "message": f"Transformer column '{column_id}' deleted",
            "id": column_id,
        }
