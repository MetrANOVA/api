import clickhouse_connect
import json
import logging
import os

from .base import StorageEngine

logger = logging.getLogger(__name__)


class Clickhouse(StorageEngine):
    def __init__(self):
        super().__init__()

        # ClickHouse configuration from environment
        self.host = os.getenv("CLICKHOUSE_HOST", "localhost")
        # self.port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.port = 8123
        self.database = os.getenv("CLICKHOUSE_DB", "default")
        self.username = os.getenv("CLICKHOUSE_USERNAME", "default")
        self.password = os.getenv("CLICKHOUSE_PASSWORD", "")
        self.cluster_name = os.getenv("CLICKHOUSE_CLUSTER_NAME", None)

        # Create database
        skip_db_creation = os.getenv("CLICKHOUSE_SKIP_DB_CREATE", "false").lower() in [
            "1",
            "true",
            "yes",
        ]
        if not skip_db_creation:
            # setup database
            self.create_database()

        # self.is_connected = False
        self.client = None

    @classmethod
    async def create(cls) -> "Clickhouse":
        instance = cls()
        await instance.connect()
        await instance._ensure_definition_table()
        return instance

    async def connect(self):
        # Initialize ClickHouse connection
        try:
            self.client = await clickhouse_connect.create_async_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true",
                verify=False,
            )
            # Test connection
            await self.client.ping()
            logger.info(
                f"Connected to ClickHouse at {self.host}:{self.port}, database: {self.database}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def create_database(self):
        """Create the target database if it doesn't exist"""
        if self.database is None:
            logger.warning("No database name specified, skipping database creation")
            return
        try:
            client = clickhouse_connect.create_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true",
                verify=False,
            )
            create_db_query = f"CREATE DATABASE IF NOT EXISTS {self.database}"
            if self.cluster_name:
                create_db_query += f" ON CLUSTER '{self.cluster_name}'"
            logger.debug(f"Creating database with query: {create_db_query}")
            client.command(create_db_query)
            logger.info(f"Database {self.database} is ready")
            client.close()
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

    async def create_resource_type(
        self,
        name,
        slug,
        type,
        consumer_type,
        consumer_config,
        fields,
        primary_key,
        partition_by,
        ttl,
        engine_type="CoalescingMergeTree",
        is_replicated=True,
    ) -> bool:
        if not await self.is_connected():
            await self.connect()
        if not await self.is_connected():
            raise ("Couldn't connect to Clickhouse")

        id = f"def_{slug}"
        ref = f"{id}__v1"

        fields_tuple = [(f.field_name, f.field_type, f.nullable) for f in fields]

        row = [
            id,
            ref,
            name,
            slug,
            type,
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
        except Exception as e:
            logger.error(f"Error during type definition insertion: {e}")
            return False

        return True

    async def find_all_resource_types(self):
        pass

    async def find_resource_type_by_name(self, name):
        pass

    async def update_resource_type(self, name):
        pass

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
