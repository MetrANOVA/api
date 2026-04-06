import logging
import logging.config


log_format: str = "json"

logging_config: dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "()": "pythonjsonlogger.json.JsonFormatter",
            "format": "%(asctime)s %(levelname)s %(name)s %(lineno)d %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
        "text": {
            "format": "%(asctime)s %(levelname)s %(name)s %(lineno)d %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
    },
    "handlers": {
        "text": {
            "formatter": "text",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        "json": {
            "formatter": "json",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "": {
            "handlers": [log_format],
            "level": "INFO",
            "propagate": False,
        },
    },
}


def configure(format: str = "json"):
    if format not in ["json", "text"]:
        raise ValueError(f"Invalid log format: {format}")

    global log_format
    log_format = format
    logging.config.dictConfig(logging_config)


def set_level(module: str, level: str):
    if level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        raise ValueError(f"Invalid log level: {level}")

    logging_config["loggers"][module] = {
        "handlers": [log_format],
        "level": level,
        "propagate": False,
    }

    configure(format=log_format)
