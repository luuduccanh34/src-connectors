from typing import Dict, Any
from variables.helper import BaseConfig

class TrinoVariables(BaseConfig):
    """
    Configuration mapping for Trino environment variables.

    This class provides centralized access to Trino connection parameters,
    including host, port, user, and password.
    """

    VARIABLES = [
        "TRINO_HOST",
        "TRINO_PORT",
        "TRINO_USERNAME",
        "TRINO_PASSWORD",
    ]

    @classmethod
    def config(cls) -> Dict[str, Any]:
        """
        Loads and returns all Trino-related environment variables.

        Returns:
            Dict[str, Any]: A dictionary containing the configured Trino parameters.
        """
        return cls.load()
