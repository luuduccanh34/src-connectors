from typing import Dict, Any, List
from pydantic import BaseModel, Field, ConfigDict

from src_connectors.src_spark.configs.base import SparkBaseComponent
from variables.spark import SparkVariables

# Load environment-based defaults from centralized variables
_jdbc_defaults = SparkVariables.get_jdbc_config()


class SparkJdbcConfig(BaseModel, SparkBaseComponent):
    """
    Configuration model for Spark JDBC connections.

    This class manages JDBC connectivity parameters, including driver details,
    connection URIs, and authentication credentials. It provides both descriptive
    metadata and functional options for Spark data source operations.
    """
    model_config = ConfigDict(extra='allow')

    jdbc_url: str = Field(
        default=_jdbc_defaults.get("SPARK_JDBC_URL") or "",
        description="The JDBC connection URL (e.g., 'jdbc:postgresql://localhost:5432/db')."
    )
    jdbc_driver: str = Field(
        default=_jdbc_defaults.get("SPARK_JDBC_DRIVER") or "",
        description="The fully qualified class name of the JDBC driver (e.g., 'org.postgresql.Driver')."
    )
    jdbc_username: str = Field(
        default=_jdbc_defaults.get("SPARK_JDBC_USERNAME") or "",
        description="The database username for the JDBC connection."
    )
    jdbc_password: str = Field(
        default=_jdbc_defaults.get("SPARK_JDBC_PASSWORD") or "",
        description="The database password for the JDBC connection."
    )

    def get_spark_config(self) -> Dict[str, Any]:
        """
        Returns a descriptive overview of the JDBC configuration.

        Security Note: Sensitive credentials (password) are masked in the output.
        Note: JDBC parameters are typically utilized in .read.format("jdbc").options(...)
        rather than being part of the global SparkConf.

        Returns:
            Dict[str, Any]: A dictionary containing JDBC connection metadata.
        """
        return {
            "spark_jdbc_url": self.jdbc_url,
            "spark_jdbc_driver": self.jdbc_driver,
            "spark_jdbc_user": self.jdbc_username,
            "spark_jdbc_password_provided": bool(self.jdbc_password)
        }

    def get_jdbc_options(self) -> Dict[str, str]:
        """
        Generates connection options for Spark's JDBC data source.

        Suitable for use with spark.read.format("jdbc").options(**config.get_jdbc_options()).load().

        Returns:
            Dict[str, str]: A dictionary of key-value pairs representing JDBC options.
        """
        options = {
            "url": self.jdbc_url,
            "driver": self.jdbc_driver,
            "user": self.jdbc_username,
            "password": self.jdbc_password,
        }
        # Filter out empty options and ensure all values are strings for Spark compatibility
        return {k: str(v) for k, v in options.items() if v}

    def get_required_spark_packages(self) -> List[str]:
        """
        Returns a list of required Spark packages for JDBC.

        Returns:
            List[str]: Usually an empty list as JDBC drivers are managed via JARs.
        """
        return []
