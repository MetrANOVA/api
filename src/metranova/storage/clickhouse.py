import clickhouse_connect
import inspect
import json
import logging
import os
import re

from pydantic import BaseModel, model_validator

from .base import StorageEngine, CollectionField, MetaCollectionField, CollectionType, ConsumerType
from clickhouse_connect.driver.query import QueryResult


logger = logging.getLogger(__name__)


class MetadataField(BaseModel):
    name: str
    type: str
    nullable: bool
    table: str | None = None

    @model_validator(mode='after')
    def validate_reference(self) -> 'MetadataField':
        if self.type == 'reference' and self.table is None:
            raise ValueError('table is required when type is reference')
        if self.type != 'reference' and self.table is not None:
            raise ValueError('table should only be provided when type is reference')
        return self

class Clickhouse(StorageEngine):
    _IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    _TYPE_PATTERN = re.compile(r"^[A-Za-z0-9_,()\s]+$")

    def __init__(self):
        super().__init__()

        self.metadata_engine = "MergeTree"
        self.data_engine = "CoalescingMergeTree"

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

            clusters: QueryResult = await self.client.query("select * from system.clusters")
            if clusters.row_count > 1:
                logger.info("ClickHouse cluster configuration detected. Using cluster-aware database engines.")
                self.metadata_engine = "ReplacingMergeTree"
                self.data_engine = "ReplicatingCoalescingMergeTree"
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    async def create_database(self):
        """Create the target database if it doesn't exist"""
        if not await self.is_connected():
            logger.error("Not connected to database")
            await self.connect()
        
        if self.database is None:
            logger.warning("No database name specified, skipping database creation")
            return
        try:
            on_cluster_clause = await self._get_on_cluster_clause()
            create_db_query = (
                f"CREATE DATABASE IF NOT EXISTS {self.database}{on_cluster_clause}"
            )
            logger.debug(f"Creating database with query: {create_db_query}")
            await self.client.command(create_db_query)
            logger.info(f"Database {self.database} is ready")
            # client.close()
        except Exception as e:
            logger.error(f"Failed to create database {self.database}: {e}")
            raise

    async def close(self):
        if self.client is not None:
            close_result = self.client.close()
            if inspect.isawaitable(close_result):
                await close_result
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

    def _validated_engine_name(self, engine: str) -> str:
        if not self._IDENTIFIER_PATTERN.fullmatch(engine):
            raise ValueError(f"Invalid engine name: {engine}")
        return engine

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

    def _canonicalize_column_type(self, field_type: str, valid_types: list[str]) -> str:
        value = self._validated_column_type(field_type)
        valid_lookup = {valid_type.lower(): valid_type for valid_type in valid_types}
        type_token_pattern = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

        def replace_token(match: re.Match[str]) -> str:
            token = match.group(0)
            canonical = valid_lookup.get(token.lower())
            if canonical is None:
                raise RuntimeError(f"{field_type} is not a valid ClickHouse column type")
            return canonical

        return type_token_pattern.sub(replace_token, value)

    async def create_resource_type(
        self,
        name: str,
        slug: str | None = None,
        data_fields: list[CollectionField] | None = None,
        meta_fields: list[MetaCollectionField] | None = None,
        identifier: list[str] | None = None,
        ttl: str = "",
        engine_type: str = "CoalescingMergeTree",
    ) -> tuple[bool, str]:
        if not await self.is_connected():
            return False, "couldn't connect to Clickhouse"

        data_fields = data_fields or []
        meta_fields = meta_fields or []
        identifier = identifier or []

        try:
            await self._ensure_definition_table()
        except Exception as e:
            logger.exception(e)
            return False, "couldn't ensure type definition table exists"
        if not slug:
            slug = name.lower().replace(' ', '-')

        existing = await self.find_resource_type_by_slug(slug)
        if existing:
            logger.warning(f"Resource type with slug '{slug}' already exists")
            return False, f"Resource type with slug '{slug}' already exists"

        definition_id = f"def_{slug}"

        # Validate and canonicalize data field types against live CH types
        ch_types = await self._get_ch_types()
        for f in data_fields:
            f.field_type = self._canonicalize_column_type(f.field_type, ch_types)

        # Validate meta field types — "reference" is a logical type, accepted as-is
        for f in meta_fields:
            if f.field_type.lower() != "reference":
                f.field_type = self._canonicalize_column_type(f.field_type, ch_types)
            else:
                f.field_type = "String"

        # Verify all identifier fields are present in data fields
        meta_field_names = {f.field_name for f in meta_fields}
        missing = [key for key in identifier if key not in meta_field_names]
        if missing:
            logger.error(f"Identifier fields missing from data fields: {missing}")
            return False, f"identifier fields not found in data fields: {missing}"

        data_fields_tuple = [(f.field_name, f.field_type, f.nullable) for f in data_fields]
        meta_fields_tuple = [
            (f.field_name, f.field_type, f.nullable, f.table or "")
            for f in meta_fields
        ]

        cluster_info = await self.get_cluster_info()
        is_replicated = cluster_info["mode"] == "clustered"

        # Create both tables BEFORE writing definition rows.
        # ClickHouse DDL cannot be rolled back — tables first so that a failed
        # definition insert leaves orphaned tables (recoverable) rather than
        # definition rows pointing at non-existent tables (not recoverable).
        try:
            await self.create_data_table(slug, identifier, ttl, engine_type, data_fields_tuple)
        except Exception as e:
            logger.exception(f"Error creating data table for '{slug}': {e}")
            return False, "Error during data table creation"

        try:
            # Reference-type fields are logical; skip them in the physical DDL
            physical_meta_fields = [
                (f.field_name, f.field_type, f.nullable)
                for f in meta_fields
                if f.field_type.lower() != "reference"
            ]
            await self.create_meta_table(slug, physical_meta_fields, engine_type, identifier)
        except Exception as e:
            logger.exception(f"Error creating meta table for '{slug}': {e}")
            return False, "Error during meta table creation"

        column_names = [
            "id", "ref", "name", "slug", 
            "meta_fields", "data_fields", "identifier", "ttl", "engine_type", "is_replicated",
        ]

        data_row = [
            definition_id,
            f"{definition_id}__v1",
            name,
            slug,
            meta_fields_tuple,
            data_fields_tuple,
            identifier,
            ttl,
            engine_type,
            is_replicated,
        ]
        try:
            await self.client.insert(
                database=self.database,
                table="definition",
                data=[data_row],
                column_names=column_names,
            )
        except Exception as e:
            logger.exception(f"Error during type definition insertion for '{slug}': {e}")
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
                    meta_fields,
                    data_fields,
                    identifier,
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
                "meta_fields",
                "data_fields",
                "identifier",
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
            if hasattr(result, "named_results"):
                named_rows = list(result.named_results())
                if named_rows:
                    return named_rows[0]

            rows = getattr(result, "result_rows", None)
            if rows and len(rows) > 0:
                return rows[0]

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
            "meta_fields",
            "data_fields",
            "identifier",
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

        table_name = f"data_{slug}"

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
                "type": str(CollectionType.DATA),
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
        on_cluster_clause = await self._get_on_cluster_clause()
        query = f"""
        CREATE TABLE {self._qualified_table_name(table_name)}{on_cluster_clause}
        (
            collector_id LowCardinality(String) NOT NULL,
            policy_level LowCardinality(String) NOT NULL,
            policy_scope Array(LowCardinality(String)) NOT NULL,
            policy_originator LowCardinality(String) NOT NULL,
            insert_time DateTime DEFAULT now(),
            {",\n".join(field_columns)},
            ext JSON
        )
        ENGINE = {self._validated_engine_name(self.data_engine)}()
        ORDER BY (collector_id, {', '.join(safe_primary_keys)})
        PRIMARY KEY (collector_id, {', '.join(safe_primary_keys)})
        PARTITION BY toYYYYMM(insert_time)
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
        fields: list[MetadataField],
        engine: str,
        primary_key: list[str],
    ):
        field_columns = []
        for f in fields:
            safe_field_name = self._quoted_identifier(f.name)
            safe_field_type = self._validated_column_type(f.type)
            col = f"{safe_field_name} {safe_field_type}"
            if f.nullable is False:
                col += " NOT NULL"
            field_columns.append(col)

        safe_primary_keys = [self._quoted_identifier(key) for key in primary_key]

        table_name = f"meta_{slug}"
        on_cluster_clause = await self._get_on_cluster_clause()
        query = f"""
        CREATE TABLE {self._qualified_table_name(table_name)}{on_cluster_clause} (
            id String NOT NULL,
            ref String NOT NULL,
            hash String NOT NULL,
            created_at DateTime DEFAULT now() NOT NULL,
            updated_at DateTime DEFAULT now() NOT NULL,
            tag Array(LowCardinality(String)), 
            policy_level LowCardinality(String) NOT NULL,
            policy_scope Array(LowCardinality(String)) NOT NULL,
            policy_originator LowCardinality(String) NOT NULL,
            {",\n".join(field_columns)},
            ext JSON
        )
        ENGINE = {self._validated_engine_name(self.metadata_engine)}()
        ORDER BY (id, {', '.join(safe_primary_keys)})
        PRIMARY KEY (id, {', '.join(safe_primary_keys)})
        PARTITION BY toYYYYMM(insert_time);
        """

        logger.info(query)
        try:
            return await self.client.command(query)
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

        existing_fields = current_def.get("data_fields") or []
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

        ch_types = await self._get_ch_types()
        fields_to_add = []
        for field in new_fields:
            if field.field_name in existing_field_names:
                return False, f"Field '{field.field_name}' already exists"
            canonical_type = self._canonicalize_column_type(field.field_type, ch_types)
            fields_to_add.append((field.field_name, canonical_type, field.nullable))

        table_name = f"data_{slug}"

        try:
            if fields_to_add:
                await self._add_columns_to_table(table_name, fields_to_add)
        except Exception as e:
            logger.exception(f"Error altering table metranova.{table_name}: {e}")
            return False, "Error updating table schema"

        merged_fields = [*normalized_fields, *fields_to_add]

        new_ref = self._bump_ref_version(current_def["ref"], current_def["id"])
        row = [
            current_def["id"],
            new_ref,
            current_def["name"],
            current_def["slug"],
            current_def.get("meta_fields") or [],
            merged_fields,
            current_def.get("identifier") or [],
            current_def["ttl"],
            current_def["engine_type"],
            current_def["is_replicated"],
        ]

        try:
            await self.client.insert(
                database=self.database,
                table="definition",
                data=[row],
                column_names=[
                    "id", "ref", "name", "slug", "meta_fields",
                    "data_fields", "identifier", "ttl", "engine_type", "is_replicated",
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

        on_cluster_clause = await self._get_on_cluster_clause()

        await self.client.command(
            f"""
            CREATE TABLE IF NOT EXISTS metranova.definition{on_cluster_clause}
            (
                id String,
                ref String,
                name String,
                slug String,
                meta_fields Array(Tuple(
                    field_name String,
                    field_type String,
                    nullable Bool,
                    table String
                )),
                data_fields Array(Tuple(
                    field_name String,
                    field_type String,
                    nullable Bool,
                )),
                identifier Array(String),
                ttl String,
                engine_type String DEFAULT 'CoalescingMergeTree',
                is_replicated Bool DEFAULT true,
                updated_at DateTime DEFAULT now()
            )
            ENGINE = {self._validated_engine_name(self.data_engine)}()
            ORDER BY ref
        """
        )

    async def _get_ch_types(self):
        names = await self.client.query('SELECT name FROM system.data_type_families')
        if hasattr(names, "named_results"):
            results = list(names.named_results())
            types = [row["name"] for row in results if "name" in row]
        elif hasattr(names, "result_rows"):
            types = [row[0] for row in names.result_rows if row]
        elif hasattr(names, "result_columns"):
            types = list(names.result_columns[0]) if names.result_columns else []
        else:
            types = []
        logger.info(types)
        return types

    async def _get_on_cluster_clause(self) -> str:
        try:
            cluster_info = await self.get_cluster_info()
        except Exception as exc:
            logger.warning(f"Unable to detect cluster mode, defaulting to standalone: {exc}")
            return ""

        if cluster_info.get("mode") != "clustered":
            return ""

        cluster_name = cluster_info.get("cluster_name")
        if not cluster_name:
            return ""

        safe_cluster_name = str(cluster_name).replace("'", "\\'")
        return f" ON CLUSTER '{safe_cluster_name}'"

    async def get_cluster_info(self):
        result = await self.client.query("""
            SELECT
                cluster,
                shard_num,
                replica_num,
                host_name,
                host_address,
                port
            FROM system.clusters
            WHERE is_local = 0
            ORDER BY cluster, shard_num, replica_num
        """)

        rows = result.result_rows
        if not rows:
            return {"mode": "standalone", "clusters": []}

        valid_rows = []
        for row in rows:
            if not row:
                continue
            cluster_name = row.get("cluster") if isinstance(row, dict) else row[0]
            if isinstance(cluster_name, str) and cluster_name:
                valid_rows.append(row)

        if not valid_rows:
            return {"mode": "standalone", "clusters": []}

        first_row = valid_rows[0]
        cluster_name = (
            first_row.get("cluster") if isinstance(first_row, dict) else first_row[0]
        )

        return {
            "mode": "clustered",
            "cluster_name": cluster_name,
            "clusters": valid_rows,
        }
