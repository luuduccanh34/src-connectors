from typing import Dict, Any
from variables.helper import BaseConfig


class SQLServerVariables(BaseConfig):
    """
    Configuration mapping for SQL Server environment variables.

    This class provides centralized access to SQL Server connection parameters,
    including host, port, database name, and authentication credentials.
    All settings are loaded from environment variables using the BaseConfig helper.
    """

    VARIABLES = [
        "SQLSERVER_HOST",
        "SQLSERVER_PORT",
        "SQLSERVER_DATABASE",
        "SQLSERVER_USERNAME",
        "SQLSERVER_PASSWORD",
        "SQLSERVER_DRIVER",
    ]

    @classmethod
    def config(cls) -> Dict[str, Any]:
        """
        Loads and returns all SQL Server-related environment variables.

        Returns:
            Dict[str, Any]: A dictionary containing the configured SQL Server parameters.
        """
        return cls.load()
