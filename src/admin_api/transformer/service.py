import logging

from metranova.storage.clickhouse import Clickhouse
from fastapi import Depends
from ..context import get_clickhouse

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

        table_name = self.storage._qualified_table_name("transformer")
        try:
            # Confirm the transformer exists first
            result = await self.storage.client.query(
                f"SELECT id, name, slug, description, match_field FROM {table_name}"
                + " WHERE id = {id:String} LIMIT 1",
                parameters={"id": transformer_id},
            )
            if result.row_count == 0:
                return False, {
                    "message": f"Transformer with id '{transformer_id}' not found"
                }

            assignments: list[str] = []
            parameters: dict = {"id": transformer_id}

            if name is not None:
                new_slug = name.lower().replace(" ", "_")
                assignments.append("name = {name:String}")
                assignments.append("slug = {slug:String}")
                parameters["name"] = name
                parameters["slug"] = new_slug

            if description is not None:
                assignments.append("description = {description:String}")
                parameters["description"] = description

            if match_field is not None:
                assignments.append("match_field = {match_field:String}")
                parameters["match_field"] = match_field

            if not assignments:
                return False, {"message": "No fields provided to update"}

            await self.storage.client.command(
                f"ALTER TABLE {table_name} UPDATE {', '.join(assignments)}"
                + " WHERE id = {id:String}",
                parameters=parameters,
            )

            # Re-fetch the updated record to return it
            _, data = await self.get_transformer_by_id(transformer_id)
            return True, data
        except Exception as e:
            logger.exception("Error updating transformer")
            return False, {"message": f"Error updating transformer: {e}"}
