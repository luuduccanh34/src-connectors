import sys
from typing import Any

__all__ = ["SQLServerConnector", "SparkConnector", "TrinoConnector"]


def __getattr__(name: str) -> Any:
    if name == "SQLServerConnector":
        from src_connectors.src_sqlserver.connector import SQLServerConnector

        return SQLServerConnector

    if name == "SparkConnector":
        from src_connectors.src_spark.connector import SparkConnector

        return SparkConnector

    if name == "TrinoConnector":
        from src_connectors.src_trino.connector import TrinoConnector

        return TrinoConnector

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
