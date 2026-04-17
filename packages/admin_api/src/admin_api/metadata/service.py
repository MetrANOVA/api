import logging
import re

from datetime import date, datetime
from metranova.storage.clickhouse import Clickhouse
from pydantic import BaseModel, model_validator, create_model

logger = logging.getLogger(__name__)


def slugify(value: str) -> str:
    """Convert a string to a slug format."""
    return value.lower().replace(" ", "_")


CH_TYPE_MAP: dict[str, type] = {
    "String": str,
    "UUID": str,
    "Bool": bool,
    "UInt8": int,
    "UInt16": int,
    "UInt32": int,
    "UInt64": int,
    "Int8": int,
    "Int16": int,
    "Int32": int,
    "Int64": int,
    "Float32": float,
    "Float64": float,
    "Date": date,
    "DateTime": datetime,
    "DateTime64": datetime,
}


def resolve_python_type(ch_type: str) -> type:
    """Strip parameterization (e.g. DateTime64(3, 'UTC')) and resolve."""
    base = re.split(r"[\(\s]", ch_type)[0]
    return CH_TYPE_MAP.get(base, str)  # fall back to str for unknown types


class MetadataField(BaseModel):
    name: str
    type: str
    nullable: bool
    table: str | None = None

    @model_validator(mode="after")
    def validate_reference(self) -> "MetadataField":
        if self.type == "reference" and self.table is None:
            raise ValueError("table is required when type is reference")
        if self.type != "reference" and self.table is not None:
            raise ValueError("table should only be provided when type is reference")
        return self


RESERVED_COLUMNS = {
    "id", "ref", "hash", "created_at", "updated_at",
    "tag", "policy_level", "policy_scope", "policy_originator", "ext",
}


class MetadataService:
    """Service class for handling metadata operations."""

    def __init__(self, storage: Clickhouse):
        self.storage = storage
        self.client = storage.client

    async def create_metadata_type(
        self, name: str, identifier: list[str], fields: list[MetadataField]
    ):
        """Create a new metadata type

        This creates both a new database table and a new an entry in
        metranova.definition.
        """
        slug = slugify(name)
        existing = await self.storage.find_resource_type_by_slug(slug)
        if existing is not None:
            raise ValueError(f"Metadata type with slug '{slug}' already exists.")

        _primary_keys = [self.storage._quoted_identifier(key) for key in identifier]
        _table = f"meta_{slug}"
        _cols = []
        for f in fields:
            _name = self.storage._quoted_identifier(f.name)
            _type = self.storage._validated_column_type(f.type)
            if f.nullable:
                _cols.append(f"{_name} {_type}")
            else:
                _cols.append(f"{_name} {_type} NOT NULL")

        query = f"""
        CREATE TABLE {self.storage._qualified_table_name(_table)} (
            id String NOT NULL,
            ref String NOT NULL,
            hash String NOT NULL,
            created_at DateTime DEFAULT now() NOT NULL,
            updated_at DateTime DEFAULT now() NOT NULL,
            tag Array(LowCardinality(String)), 
            policy_level LowCardinality(String) NOT NULL,
            policy_scope Array(LowCardinality(String)) NOT NULL,
            policy_originator LowCardinality(String) NOT NULL,
            {",\n".join(_cols)},
            ext JSON
        )
        ENGINE = {self.storage._validated_engine_name(self.storage.metadata_engine)}()
        ORDER BY (id, {', '.join(_primary_keys)})
        PRIMARY KEY (id, {', '.join(_primary_keys)})
        PARTITION BY created_at;
        """

        try:
            await self.client.command(query)
        except Exception as e:
            logger.exception(f"Failed to create metadata table for type '{slug}': {e}")
            raise Exception(f"Failed to create metadata table for type '{slug}': {e}")

        try:
            await self.client.insert(
                database=self.storage.database,
                table="definition",
                data=[
                    [
                        f"def_{slug}",
                        f"def_{slug}__v1",
                        name,
                        slug,
                        "metadata",
                        [(f.name, f.type, f.nullable) for f in fields],
                        identifier,
                        "ttl",
                    ]
                ],
                column_names=[
                    "id",
                    "ref",
                    "name",
                    "slug",
                    "type",
                    "fields",
                    "identifier",
                    "ttl",
                ],
            )
        except Exception as e:
            logger.exception(f"Error creating metadata type definition '{slug}': {e}")
            raise Exception(f"Error creating metadata type definition '{slug}': {e}")

    async def delete_metadata_type(self, slug: str):
        await self.client.command(
            f"DROP TABLE IF EXISTS {self.storage._qualified_table_name('meta_'+slug)}"
        )
        await self.client.command(
            "DELETE FROM definition WHERE slug = %s AND type = 'metadata'",
            parameters=[slug],
        )

    async def update_metadata_type(self, slug: str, fields: list[MetadataField]):
        type_def = await self.get_metadata_type(slug)
        if type_def is None:
            raise LookupError(f"Metadata type '{slug}' not found.")

        identifier_set = set(type_def["identifier"])
        protected = RESERVED_COLUMNS | identifier_set

        for f in fields:
            if f.name in RESERVED_COLUMNS:
                raise ValueError(f"Field '{f.name}' is a reserved column and cannot be modified.")
            if f.name in identifier_set:
                raise ValueError(f"Field '{f.name}' is an identifier field and cannot be modified.")

        existing_fields = {
            f["field_name"]: f
            for f in type_def["fields"]
            if f["field_name"] not in protected
        }
        new_fields = {f.name: f for f in fields}

        to_add = {name: f for name, f in new_fields.items() if name not in existing_fields}
        to_remove = {name for name in existing_fields if name not in new_fields}

        table = self.storage._qualified_table_name(f"meta_{slug}")

        for name, f in to_add.items():
            col = self.storage._quoted_identifier(f.name)
            typ = self.storage._validated_column_type(f.type)
            null_clause = "" if f.nullable else " NOT NULL"
            await self.client.command(f"ALTER TABLE {table} ADD COLUMN {col} {typ}{null_clause}")

        for name in to_remove:
            col = self.storage._quoted_identifier(name)
            await self.client.command(f"ALTER TABLE {table} DROP COLUMN {col}")

        identifier_field_tuples = [
            (f["field_name"], f["field_type"], f["nullable"])
            for f in type_def["fields"]
            if f["field_name"] in identifier_set
        ]
        updated_fields = identifier_field_tuples + [(f.name, f.type, f.nullable) for f in fields]

        existing_ref = (await self.client.query(
            "SELECT ref FROM definition WHERE slug = {slug:String} AND type = 'metadata' ORDER BY updated_at DESC LIMIT 1",
            parameters={"slug": slug},
        )).first_row[0]
        next_version = int(existing_ref.split("__v")[-1]) + 1

        await self.client.insert(
            database=self.storage.database,
            table="definition",
            data=[[
                f"def_{slug}",
                f"def_{slug}__v{next_version}",
                type_def["name"],
                slug,
                "metadata",
                updated_fields,
                type_def["identifier"],
                type_def["ttl"],
            ]],
            column_names=["id", "ref", "name", "slug", "type", "fields", "identifier", "ttl"],
        )

    async def get_metadata_type(self, slug):
        result = await self.client.query(
            "SELECT name, slug, type, fields, identifier, ttl, updated_at FROM definition WHERE type = 'metadata' and slug = {slug:String}",
            parameters={"slug": slug},
        )
        if result.row_count == 0:
            return None
        return list(result.named_results())[0]

    async def validate_metadata_record(self, definition: str, record: dict[str, any]):
        # definition = await self.get_metadata_type(slug)

        for field in definition["fields"]:
            field_name = field.get("field_name")
            field_type = field.get("field_type")
            nullable = field.get("nullable")

            v = record.get(field_name, None)
            if not nullable and v is None:
                raise ValueError(f"Field '{field_name}' is required.")

            python_type = resolve_python_type(field_type)
            if v is not None and type(v) != python_type:
                raise ValueError(
                    f"Field' {field_name}' is not of type '{python_type.__name__}'."
                )
        return definition

    async def get_metadata_types(self):
        result = await self.client.query(
            "SELECT name, slug, type, fields, identifier, ttl, updated_at FROM definition WHERE type = 'metadata'"
        )
        return list(result.named_results())

    async def create_metadata_record(
        self, definition: dict[str, any], record: dict[str, any]
    ):
        table = f"meta_{definition["slug"]}"

        # We perform an _id lookup for the user based on their provided metadata
        record["id"] = "::".join([record[i] for i in definition["identifier"]])

        # Determine using type_def["fields"] sorted by name. This is the hash of all user
        # defined fields.
        record["hash"] = "examplehash"
        record["ext"] = {}

        # Determine the ref for this new record
        records = await self.get_metadata_record_history(
            definition["slug"], record["id"]
        )
        version = 1
        if len(records) > 0:
            version = int(records[0]["ref"].split("__v")[-1]) + 1
        record["ref"] = f"{record['id']}__v{version}"

        time = datetime.now()
        record["created_at"] = time
        record["updated_at"] = time

        cols = list(record.keys())
        rows = list(record.values())
        print(f"cols {cols}")
        print(f"row {rows}")

        return await self.client.insert(
            database=self.storage.database,
            table=table,
            column_names=list(record.keys()),
            data=[list(record.values())],
        )

    async def update_metadata_record(
        self, definition: dict[str, any], record: dict[str, any], version: str
    ):
        table = f"meta_{definition['slug']}"

        record["hash"] = "examplehash"
        record["ext"] = {}
        record["ref"] = f"{record['id']}__v{version}"

        existing = await self.client.query(
            f"""
            SELECT * FROM {self.storage._qualified_table_name(table)} WHERE (id, updated_at) IN (
                SELECT id, max(updated_at) FROM {self.storage._qualified_table_name(table)} WHERE ref=%s GROUP BY (id, created_at)
            ) ORDER BY created_at DESC
            """,
            parameters=[record["ref"]],
        )
        existing_record = next(existing.named_results(), None)
        if existing_record is None:
            raise ValueError(
                f"Record '{record['ref']}' not found in metadata type '{definition['slug']}'."
            )

        record["created_at"] = existing_record["created_at"]
        record["updated_at"] = datetime.now()

        return await self.client.insert(
            database=self.storage.database,
            table=table,
            column_names=list(record.keys()),
            data=[list(record.values())],
        )

    async def get_metadata_record_history(self, slug: str, _id: str) -> list[dict]:
        result = await self.client.query(
            f"""
            SELECT * FROM {self.storage._qualified_table_name("meta_"+slug)} WHERE (id, updated_at) IN (
                SELECT id, max(updated_at) FROM {self.storage._qualified_table_name("meta_"+slug)} WHERE id=%s GROUP BY (id, created_at)
            ) ORDER BY created_at DESC
            """,
            parameters=[_id],
        )
        return list(result.named_results())

    async def get_metadata_records(self, slug: str) -> list[dict]:
        result = await self.client.query(
            """
            SELECT t.* FROM {db:Identifier}.{table:Identifier} t
            INNER JOIN (
                SELECT id, max(created_at) AS max_created_at
                FROM {db:Identifier}.{table:Identifier}
                GROUP BY id
            ) latest ON t.id = latest.id AND t.created_at = latest.max_created_at
            """,
            parameters={"db": "metranova", "table": f"meta_{slug}"},
        )
        return list(result.named_results())
