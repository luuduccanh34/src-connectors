import sys
from typing import Any

__all__ = ["SQLServerConnector", "SparkConnector"]


def __getattr__(name: str) -> Any:
    if name == "SQLServerConnector":
        from src_connectors.src_sqlserver.connector import SQLServerConnector

        return SQLServerConnector

    if name == "SparkConnector":
        from src_connectors.src_spark.connector import SparkConnector

        return SparkConnector

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
