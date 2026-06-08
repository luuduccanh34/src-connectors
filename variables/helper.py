"""
helper.py

This module provides configuration helpers to load environment variables
"""

import os
import json
import logging
from dotenv import load_dotenv
from pathlib import Path
from typing import Type, Dict, Any, List

logger = logging.getLogger(__name__)

# Load local .env file
baseFolder = Path(__file__).parent.parent.absolute()
load_dotenv(os.path.join(str(baseFolder), ".env"))


class BaseConfig:
    """
    A base configuration class for loading environment variables.

    Attributes:
        VARIABLES (list): List of environment variable names to load.
    """

    VARIABLES = []

    @classmethod
    def load(cls, mode: str = 'basic') -> Dict[str, Any]:
        """
        Load the specified environment variables.

        Args:
            mode: The loading mode. Currently only 'basic' is supported.

        Returns:
            Dictionary containing the variable names and their corresponding values.

        Raises:
            ValueError: If an unsupported mode is provided.
        """
        if (mode == 'basic'):
            config = {var: cls.getVariable(var) for var in cls.VARIABLES}
        else:
            raise ValueError(
                "Invalid 'mode' value. It must be one of the following: 'basic'."
            )
        return config

    @staticmethod
    def getVariable(name: str, defaultValue: Any = None, deserializeJson: bool = False) -> Any:
        """
        Retrieve the value of an environment variable from local env only.

        Args:
            name: The name of the variable.
            defaultValue: Default value if not found.
            deserializeJson: If True, attempts to parse as JSON.

        Returns:
            The variable value (parsed if JSON).
        """
        value = os.getenv(name)

        if value is None:
            return defaultValue

        # Strip whitespace for consistency
        value = value.strip()

        if value == "":
            return defaultValue

        if deserializeJson:
            try:
                return json.loads(value)
            except json.JSONDecodeError as e:
                logger.error("Failed to parse JSON from env var %s: %s", name, str(e))
                raise ValueError(f"Env var {name} contains invalid JSON.")

        return value

class ConfigLoader:
    """
    Utility class for loading environment variables from one or multiple configs.
    """

    @staticmethod
    def loadSingle(configCls: Type[BaseConfig]) -> Dict[str, Any]:
        """
        Load variables from a single config class.

        Args:
            configCls: The configuration class to load.

        Returns:
            A dictionary of environment variables defined in the config.
        """
        return configCls.load()

    @staticmethod
    def loadMultiple(configClasses: List[Type[BaseConfig]]) -> Dict[str, Any]:
        """
        Load and merge variables from multiple config classes.

        Args:
            configClasses: A list of configuration classes.

        Returns:
            A merged dictionary containing all environment variables.
        """
        mergedConfig = {}
        for configCls in configClasses:
            logger.info("Loading config from class: %s", configCls.__name__)
            mergedConfig.update(configCls.load())

        return mergedConfig
