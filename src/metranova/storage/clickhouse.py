import clickhouse_connect
import json
import logging
import os
import re

from .base import StorageEngine, CollectionField, CollectionType, ConsumerType

logger = logging.getLogger(__name__)


class Clickhouse(StorageEngine):
    _IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    _TYPE_PATTERN = re.compile(r"^[A-Za-z0-9_,()\s]+$")

    def __init__(self):
        super().__init__()

        # ClickHouse configuration from environment
        self.host = os.getenv("CLICKHOUSE_HOST", "localhost")
        # self.port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        port = os.getenv("CLICKHOUSE_PORT", "8123")
        try:
            self.port = int(port)
        except ValueError:
            logger.warning(
                f"Invalid CLICKHOUSE_PORT value '{port}', defaulting to 8123"
            )
            self.port = 8123
        self.database = os.getenv("CLICKHOUSE_DB", "default")
        self.username = os.getenv("CLICKHOUSE_USERNAME", "default")
        self.password = os.getenv("CLICKHOUSE_PASSWORD", "")
        self.cluster_name = os.getenv("CLICKHOUSE_CLUSTER_NAME", None)

        # self.is_connected = False
        self.client = None

    @classmethod
    async def create(cls) -> "Clickhouse":
        instance = cls()

        await instance.connect()
        return instance

    async def connect(self):
        # Check to see if we need to care about TLS verification
        secure = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
        verify_env = os.getenv("CLICKHOUSE_VERIFY")
        verify = secure if verify_env is None else verify_env.lower() == "true"
        # Initialize ClickHouse connection
        try:
            self.client = await clickhouse_connect.create_async_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                secure=secure,
                verify=verify,
            )
            # Test connection
            await self.client.ping()
            logger.info(
                f"Connected to ClickHouse at {self.host}:{self.port}, database: {self.database}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    async def create_database(self):
        """Create the target database if it doesn't exist"""
        if self.database is None:
            logger.warning("No database name specified, skipping database creation")
            return
        try:
            create_db_query = f"CREATE DATABASE IF NOT EXISTS {self.database}"
            if self.cluster_name:
                create_db_query += f" ON CLUSTER '{self.cluster_name}'"
            logger.debug(f"Creating database with query: {create_db_query}")
            await self.client.command(create_db_query)
            logger.info(f"Database {self.database} is ready")
            # client.close()
        except Exception as e:
            logger.error(f"Failed to create database {self.database}: {e}")
            raise

    def close(self):
        if self.client:
            self.client.close()
            logger.info("Connection to Clickhouse closed")

    async def is_connected(self):
        if self.client and await self.client.ping():
            return True
        else:
            return False

    def _qualified_table_name(self, table_name: str) -> str:
        return f"`{self.database}`.`{table_name}`"

    def _quoted_identifier(self, name: str) -> str:
        if not self._IDENTIFIER_PATTERN.fullmatch(name):
            raise ValueError(f"Invalid identifier: {name}")
        return f"`{name}`"

    def _validated_column_type(self, field_type: str) -> str:
        value = field_type.strip()
        if not value:
            raise ValueError("Column type cannot be empty")
        if any(token in value for token in (";", "--", "/*", "*/", "\\")):
            raise ValueError(f"Invalid column type: {field_type}")
        if not self._TYPE_PATTERN.fullmatch(value):
            raise ValueError(f"Invalid column type: {field_type}")
        if value.count("(") != value.count(")"):
            raise ValueError(f"Invalid column type: {field_type}")
        return value

    async def create_resource_type(
        self,
        name: str,
        slug: str,
        collection_type: CollectionType,
        consumer_type: ConsumerType,
        consumer_config: dict,
        fields: list[CollectionField],
        primary_key: list[str],
        partition_by: str,
        ttl: str,
        engine_type="CoalescingMergeTree",
        is_replicated=True,
    ) -> tuple[bool, str]:
        if not await self.is_connected():
            return False, "couldn't connect to Clickhouse"

        try:
            await self._ensure_definition_table()
        except Exception as e:
            logger.exception(e)
            return False, "couldn't ensure type definition table exists"

        # Check if resource type with same slug already exists
        existing = await self.find_resource_type_by_slug(slug)
        if existing:
            logger.warning(f"Resource type with slug '{slug}' already exists")
            return False, f"Resource type with slug '{slug}' already exists"

        definition_id = f"def_{slug}"
        ref = f"{definition_id}__v1"

        # Validate primary key is listed as field
        primary_fields = [f for f in fields if f.field_name in primary_key]
        if len(primary_fields) != len(primary_key):
            logger.error("Primary Key and Fields mismatch")
            return False, "mismatch between primary keys and fields"

        fields_tuple = [(f.field_name, f.field_type, f.nullable) for f in fields]

        # Create the data/meta table BEFORE writing the definition row.
        # ClickHouse DDL cannot be rolled back, so we establish the table first —
        # if it fails, no definition row is written. If the definition insert later
        # fails we are left with an orphaned table (recoverable), rather than a
        # definition row pointing at a non-existent table (not recoverable).
        try:
            if collection_type == CollectionType.DATA:
                await self.create_data_table(
                    slug, primary_key, partition_by, ttl, engine_type, fields_tuple
                )
            elif collection_type == CollectionType.METADATA:
                await self.create_meta_table(
                    slug, fields_tuple, engine_type, partition_by, primary_key
                )
        except Exception as e:
            logger.exception(f"Error while creating table for '{slug}': {e}")
            return False, "Error during table creation"

        row = [
            definition_id,
            ref,
            name,
            slug,
            collection_type,
            consumer_type,
            json.dumps(consumer_config),
            fields_tuple,
            primary_key,
            partition_by,
            ttl,
            engine_type,
            is_replicated,
        ]

        try:
            await self.client.insert(
                database=self.database,
                table="definition",
                data=[row],
                column_names=[
                    "id",
                    "ref",
                    "name",
                    "slug",
                    "type",
                    "consumer_type",
                    "consumer_config",
                    "fields",
                    "primary_key",
                    "partition_by",
                    "ttl",
                    "engine_type",
                    "is_replicated",
                ],
            )
        except Exception as e:
            logger.exception(
                f"Error during type definition insertion for '{slug}': {e}"
            )
            return False, "Error during type definition insertion"

        return True, f"Type {name} has been successfully created"

    async def find_all_resource_types(self):
        if not await self.is_connected():
            return None

        try:
            result = await self.client.query(
                f"""
                SELECT
                    id,
                    ref,
                    name,
                    slug,
                    type,
                    consumer_type,
                    consumer_config,
                    fields,
                    primary_key,
                    partition_by,
                    ttl,
                    engine_type,
                    is_replicated,
                    updated_at
                FROM {self.database}.definition
                """
            )
            column_names = [
                "id",
                "ref",
                "name",
                "slug",
                "type",
                "consumer_type",
                "consumer_config",
                "fields",
                "primary_key",
                "partition_by",
                "ttl",
                "engine_type",
                "is_replicated",
                "updated_at",
            ]
            return [dict(zip(column_names, row)) for row in result.result_rows]
        except Exception as e:
            logger.exception(f"Error retrieving all resource types: {e}")
            return None

    async def find_resource_type_by_slug(self, slug: str):
        """Find a resource type by slug. Returns the row if found, None otherwise."""
        if not await self.is_connected():
            return None

        try:
            result = await self.client.query(
                f"SELECT * FROM {self.database}.definition WHERE slug = %s ORDER BY updated_at DESC LIMIT 1",
                parameters=[slug],
            )
            if result.result_rows and len(result.result_rows) > 0:
                return next(iter(result.named_results()))
            return None
        except Exception as e:
            logger.error(f"Error checking for existing slug '{slug}': {e}")
            return None

    def _definition_to_dict(self, definition):
        if isinstance(definition, dict):
            return definition

        keys = [
            "id",
            "ref",
            "name",
            "slug",
            "type",
            "consumer_type",
            "consumer_config",
            "fields",
            "primary_key",
            "partition_by",
            "ttl",
            "engine_type",
            "is_replicated",
            "updated_at",
        ]
        return dict(zip(keys, definition))

    def _bump_ref_version(self, ref: str, definition_id: str) -> str:
        match = re.search(r"__v(\d+)$", ref)
        if match:
            return f"{definition_id}__v{int(match.group(1)) + 1}"
        return f"{definition_id}__v2"

    async def _add_columns_to_table(
        self,
        table_name: str,
        fields: list[tuple[str, str, bool]],
    ):
        for field_name, field_type, nullable in fields:
            safe_field_name = self._quoted_identifier(field_name)
            safe_field_type = self._validated_column_type(field_type)
            query = (
                f"ALTER TABLE {self._qualified_table_name(table_name)} "
                f"ADD COLUMN IF NOT EXISTS {safe_field_name} {safe_field_type}"
            )
            if not nullable:
                query += " NOT NULL"
            await self.client.command(query)

    async def find_resource_type_schema_by_slug(self, slug: str):
        if not await self.is_connected():
            return None

        definition = await self.find_resource_type_by_slug(slug)
        if definition is None:
            return None

        resource_type = (
            definition["type"] if isinstance(definition, dict) else definition[4]
        )
        if str(resource_type) == CollectionType.DATA:
            table_name = f"data_{slug}"
        elif str(resource_type) == CollectionType.METADATA:
            table_name = f"meta_{slug}"
        else:
            logger.error(f"Unknown resource type '{resource_type}' for slug '{slug}'")
            return None

        try:
            result = await self.client.query(
                f"DESCRIBE TABLE {self._qualified_table_name(table_name)}"
            )
            column_names = [
                "name",
                "type",
                "default_type",
                "default_expression",
                "comment",
                "codec_expression",
                "ttl_expression",
            ]

            columns = []
            for row in result.result_rows:
                row_dict = {}
                for index, key in enumerate(column_names):
                    row_dict[key] = row[index] if index < len(row) else None
                columns.append(row_dict)

            return {
                "slug": slug,
                "type": str(resource_type),
                "table": f"metranova.{table_name}",
                "columns": columns,
            }
        except Exception as e:
            logger.error(f"Error retrieving schema for slug '{slug}': {e}")
            return None

    async def create_data_table(
        self,
        slug: str,
        primary_key: list[str],
        partition_by: str,
        ttl: str,
        engine: str,
        fields: list[tuple[str, str, bool]],
    ):
        field_columns = []
        for f in fields:
            safe_field_name = self._quoted_identifier(f[0])
            safe_field_type = self._validated_column_type(f[1])
            col = f"{safe_field_name} {safe_field_type}"
            if f[2] is False:
                col += " NOT NULL"
            field_columns.append(col)

        safe_primary_keys = [self._quoted_identifier(key) for key in primary_key]

        table_name = f"data_{slug}"
        query = f"""
        CREATE TABLE {self._qualified_table_name(table_name)} 
        (
            collector_id LowCardinality(String) NOT NULL,
            policy_level LowCardinality(String) NOT NULL,
            policy_scope Array(LowCardinality(String)) NOT NULL,
            policy_originator LowCardinality(String) NOT NULL,
            insert_time DateTime DEFAULT now(),
            {",\n".join(field_columns)},
            ext JSON
        )
        ENGINE = {engine}
        ORDER BY (collector_id, {', '.join(safe_primary_keys)})
        PRIMARY KEY (collector_id, {', '.join(safe_primary_keys)})
        PARTITION BY {partition_by}
        TTL insert_time + INTERVAL {ttl};
        """

        logger.info(query)
        try:
            await self.client.command(query)
        except Exception as e:
            logger.exception(e)
            raise

    async def create_meta_table(
        self,
        slug: str,
        fields: list[tuple[str, str, bool]],
        engine: str,
        partition_by: str,
        primary_key: list[str],
    ):
        field_columns = []
        for f in fields:
            safe_field_name = self._quoted_identifier(f[0])
            safe_field_type = self._validated_column_type(f[1])
            col = f"{safe_field_name} {safe_field_type}"
            if f[2] is False:
                col += " NOT NULL"
            field_columns.append(col)

        safe_primary_keys = [self._quoted_identifier(key) for key in primary_key]

        table_name = f"meta_{slug}"
        query = f"""
        CREATE TABLE {self._qualified_table_name(table_name)} (
            id String NOT NULL,
            ref String NOT NULL,
            hash String NOT NULL,
            insert_time DateTime DEFAULT now() NOT NULL,
            tag Array(LowCardinality(String)), 
            policy_level LowCardinality(String) NOT NULL,
            policy_scope Array(LowCardinality(String)) NOT NULL,
            policy_originator LowCardinality(String) NOT NULL,
            {",\n".join(field_columns)},
            ext JSON
        )
        ENGINE = {engine}
        ORDER BY (id, {', '.join(safe_primary_keys)})
        PRIMARY KEY (id, {', '.join(safe_primary_keys)})
        PARTITION BY {partition_by};
        """

        logger.info(query)
        try:
            await self.client.command(query)
        except Exception as e:
            logger.exception(e)
            raise

    async def update_resource_type(
        self,
        slug: str,
        fields: list[CollectionField] | None = None,
        consumer_config_updates: dict | None = None,
        ext_updates: dict | None = None,
    ) -> tuple[bool, str]:
        if not await self.is_connected():
            return False, "Couldn't connect to Clickhouse"

        current = await self.find_resource_type_by_slug(slug)
        if current is None:
            return False, f"Resource type with slug '{slug}' not found"

        current_def = self._definition_to_dict(current)
        new_fields = fields or []
        config_updates = consumer_config_updates or {}
        ext_updates = ext_updates or {}

        if not new_fields and not config_updates and not ext_updates:
            return False, "No additive updates provided"

        existing_fields = current_def.get("fields") or []
        # Handle both dict and tuple field representations from ClickHouse
        existing_field_names = set()
        normalized_fields = []
        for field in existing_fields:
            if isinstance(field, dict):
                field_name = field.get("field_name")
                field_type = field.get("field_type")
                nullable = field.get("nullable", True)
                existing_field_names.add(field_name)
                normalized_fields.append((field_name, field_type, nullable))
            else:
                existing_field_names.add(field[0])
                normalized_fields.append(field)

        fields_to_add = []
        for field in new_fields:
            if field.field_name in existing_field_names:
                return False, f"Field '{field.field_name}' already exists"
            fields_to_add.append((field.field_name, field.field_type, field.nullable))

        table_type = str(current_def["type"])
        if table_type == CollectionType.DATA:
            table_name = f"data_{slug}"
        elif table_type == CollectionType.METADATA:
            table_name = f"meta_{slug}"
        else:
            return False, f"Unknown type '{table_type}' for slug '{slug}'"

        try:
            if fields_to_add:
                await self._add_columns_to_table(table_name, fields_to_add)
        except Exception as e:
            logger.exception(f"Error altering table metranova.{table_name}: {e}")
            return False, "Error updating table schema"

        merged_fields = [*normalized_fields, *fields_to_add]

        try:
            current_config = current_def.get("consumer_config") or {}
            if isinstance(current_config, str):
                current_config = json.loads(current_config)
        except Exception:
            current_config = {}

        if not isinstance(current_config, dict):
            current_config = {}

        merged_config = {**current_config, **config_updates}
        if ext_updates:
            existing_ext = merged_config.get("ext") or {}
            if not isinstance(existing_ext, dict):
                existing_ext = {}
            merged_config["ext"] = {**existing_ext, **ext_updates}

        new_ref = self._bump_ref_version(current_def["ref"], current_def["id"])
        row = [
            current_def["id"],
            new_ref,
            current_def["name"],
            current_def["slug"],
            current_def["type"],
            current_def["consumer_type"],
            json.dumps(merged_config),
            merged_fields,
            current_def["primary_key"],
            current_def["partition_by"],
            current_def["ttl"],
            current_def["engine_type"],
            current_def["is_replicated"],
        ]

        try:
            await self.client.insert(
                database="metranova",
                table="definition",
                data=[row],
                column_names=[
                    "id",
                    "ref",
                    "name",
                    "slug",
                    "type",
                    "consumer_type",
                    "consumer_config",
                    "fields",
                    "primary_key",
                    "partition_by",
                    "ttl",
                    "engine_type",
                    "is_replicated",
                ],
            )
            return True, f"Resource type '{slug}' updated to {new_ref}"
        except Exception as e:
            logger.exception(f"Error writing updated definition for slug '{slug}': {e}")
            return False, "Error persisting updated definition"

    async def _ensure_definition_table(self) -> None:
        if not await self.is_connected():
            await self.connect()

        result = await self.client.query("EXISTS TABLE metranova.definition")
        exists = int(result.result_rows[0][0]) == 1

        if exists:
            return

        await self.client.command(
            """
            CREATE TABLE IF NOT EXISTS metranova.definition
            (
                id String,
                ref String,
                name String,
                slug String,
                type Enum8('data' = 1, 'metadata' = 2),
                consumer_type String,
                consumer_config String,
                fields Array(Tuple(
                    field_name String,
                    field_type String,
                    nullable Bool
                )),
                primary_key Array(String),
                partition_by String,
                ttl String,
                engine_type String DEFAULT 'CoalescingMergeTree',
                is_replicated Bool DEFAULT true,
                updated_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY ref
        """
        )
