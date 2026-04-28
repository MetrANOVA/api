import os

import uvicorn


def main() -> None:
    host = os.getenv("ADMIN_API_HOST", "0.0.0.0")
    port = int(os.getenv("ADMIN_API_PORT", "8000"))
    root_path = os.getenv("ADMIN_API_ROOT_PATH", "")
    log_level = os.getenv("LOG_LEVEL", "info").lower()
    reload = os.getenv("ADMIN_API_RELOAD", "false").lower() == "true"

    uvicorn.run(
        "admin_api.app:app",
        host=host,
        port=port,
        root_path=root_path,
        log_level=log_level,
        reload=reload,
    )
