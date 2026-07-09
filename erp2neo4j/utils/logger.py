"""Structured logger for erp2neo4j."""
import logging
import os
from rich.logging import RichHandler
from rich.console import Console

console = Console()

def get_logger(name: str) -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file = os.getenv("LOG_FILE", "logs/erp2neo4j.log")

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Rich console handler
    rich_handler = RichHandler(console=console, rich_tracebacks=True, markup=True)
    rich_handler.setLevel(level)
    logger.addHandler(rich_handler)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))
    logger.addHandler(file_handler)

    return logger
