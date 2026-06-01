import asyncio
import logging
import os

from metranova.storage.clickhouse import Clickhouse

logger = logging.getLogger("admin_api.redeploy_watcher")


# Tables to watch and the fields whose values contribute to the fingerprint.
# A change to any value in any of these fields triggers a pipeline restart.
WATCHED_TABLES: dict[str, tuple[str, ...]] = {
    "definition": ("id", "ref", "updated_at"),
    "transformer": ("id", "ref", "updated_at"),
    "transformer_column": (
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
}


class DefinitionTransformerRedeployWatcher:
    def __init__(
        self,
        storage: Clickhouse,
        poll_interval_seconds: float = 10.0,
        trigger_on_startup: bool = False,
    ):
        self.storage = storage
        self.poll_interval_seconds = poll_interval_seconds
        self.trigger_on_startup = trigger_on_startup
        self._last_state: dict[str, tuple[int, int]] | None = None

    async def _fingerprint(
        self, table: str, fields: tuple[str, ...]
    ) -> tuple[int, int]:
        """Return (row_count, content_hash) for a table.

        Returns (0, 0) if the table is missing or the query fails.
        """
        qualified = self.storage._qualified_table_name(table)
        try:
            # Null-safe serialization prevents cityHash64 from returning NULL.
            row_payload = (
                "concat("
                + ", '|', ".join(f"ifNull(toString({f}), '<NULL>')" for f in fields)
                + ")"
            )
            result = await self.storage.client.query(
                f"SELECT count() AS cnt, "
                f"sum(cityHash64({row_payload})) AS h "
                f"FROM {qualified}"
            )
            rows = getattr(result, "result_rows", None) or []
            if not rows:
                return (0, 0)
            row = rows[0]
            if isinstance(row, dict):
                return (int(row.get("cnt", 0) or 0), int(row.get("h", 0) or 0))
            return (int(row[0] or 0), int(row[1] or 0))
        except Exception as exc:
            logger.warning("Failed to fingerprint '%s': %s", table, exc)
            return (0, 0)

    async def _capture_state(self) -> dict[str, tuple[int, int]]:
        return {
            table: await self._fingerprint(table, fields)
            for table, fields in WATCHED_TABLES.items()
        }

    async def poll_once(self) -> bool:
        current = await self._capture_state()

        if self._last_state is None:
            self._last_state = current
            if self.trigger_on_startup and any(c > 0 for c, _ in current.values()):
                logger.info("Initial state captured; triggering pipeline restart")
                await self.storage.restart_pipelines()
                return True
            logger.info("Initial state captured: %s", current)
            return False

        if current == self._last_state:
            logger.debug("No changes detected")
            return False

        changed = [t for t in current if current[t] != self._last_state[t]]
        self._last_state = current
        logger.info(
            "Detected changes in %s; triggering pipeline restart", ", ".join(changed)
        )
        await self.storage.restart_pipelines()
        return True

    async def run_forever(self) -> None:
        while True:
            try:
                await self.poll_once()
            except Exception:
                logger.exception("Redeploy watcher poll failed")
            await asyncio.sleep(self.poll_interval_seconds)


async def run_watcher() -> None:
    import admin_api.logs as logs

    logs.configure(format="text")
    logs.set_level("admin_api", os.getenv("LOG_LEVEL", "INFO"))

    poll_interval_raw = os.getenv("REDEPLOY_WATCHER_POLL_INTERVAL", "10")
    try:
        poll_interval = float(poll_interval_raw)
    except ValueError:
        logger.warning(
            "Invalid REDEPLOY_WATCHER_POLL_INTERVAL '%s', defaulting to 10",
            poll_interval_raw,
        )
        poll_interval = 10.0

    trigger_on_startup = (
        os.getenv("REDEPLOY_WATCHER_TRIGGER_ON_STARTUP", "false").lower() == "true"
    )

    logger.info(
        "Starting redeploy watcher (poll_interval=%ss, trigger_on_startup=%s)",
        poll_interval,
        trigger_on_startup,
    )

    storage = await Clickhouse.create()
    watcher = DefinitionTransformerRedeployWatcher(
        storage=storage,
        poll_interval_seconds=poll_interval,
        trigger_on_startup=trigger_on_startup,
    )

    try:
        await watcher.run_forever()
    finally:
        await storage.close()


def main() -> None:
    try:
        asyncio.run(run_watcher())
    except KeyboardInterrupt:
        logger.info("Redeploy watcher stopped")


if __name__ == "__main__":
    main()
