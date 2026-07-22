from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from src_connectors.src_spark.configs.base.base import SparkBaseComponent
from variables.spark import SparkVariables

_jdbc_defaults = SparkVariables.get_jdbc_config()


class SparkJdbcConfig(BaseModel, SparkBaseComponent):
    """
    Configuration model for Spark JDBC connections.
    Serves as the base class for database-specific configs (Oracle, Postgres, etc.).
    """
    model_config = ConfigDict(extra='allow')

    jdbc_url: str = Field(
        default=_jdbc_defaults.get("SPARK_JDBC_URL") or "",
        description="The JDBC connection URL (e.g., 'jdbc:postgresql://localhost:5432/db')."
    )
    jdbc_driver: str = Field(
        default=_jdbc_defaults.get("SPARK_JDBC_DRIVER") or "",
        description="The fully qualified class name of the JDBC driver."
    )
    jdbc_username: str = Field(
        default=_jdbc_defaults.get("SPARK_JDBC_USERNAME") or "",
        description="The database username for the JDBC connection."
    )
    jdbc_password: str = Field(
        default=_jdbc_defaults.get("SPARK_JDBC_PASSWORD") or "",
        description="The database password for the JDBC connection."
    )

    # Dependencies riêng cho JDBC
    spark_jars: Optional[str] = Field(
        default=_jdbc_defaults.get("SPARK_JDBC_LOCAL_JARS") or "",
        description="Local jar file paths specifically for JDBC drivers."
    )
    spark_jars_packages: Optional[str] = Field(
        default=_jdbc_defaults.get("SPARK_JDBC_JARS_PACKAGES") or "",
        description="Maven package coordinates specifically for JDBC drivers."
    )

    def get_spark_config(self) -> Dict[str, Any]:
        """
        Returns runtime Spark configs if needed.
        Note: Empty dict by default as JDBC options are handled via get_jdbc_options().
        """
        return {}

    def get_jdbc_options(self) -> Dict[str, str]:
        """
        Generates connection options for Spark's JDBC data source.
        Used via spark.read.format("jdbc").options(**cfg.get_jdbc_options()).load()
        """
        options = {
            "url": self.jdbc_url,
            "driver": self.jdbc_driver,
            "user": self.jdbc_username,
            "password": self.jdbc_password,
        }
        return {k: str(v) for k, v in options.items() if v}

    def get_required_spark_packages(self) -> List[str]:
        """Returns Maven packages for JDBC if specified."""
        if not self.spark_jars_packages:
            return []
        return [pkg.strip() for pkg in self.spark_jars_packages.split(",") if pkg.strip()]

    def get_required_local_jars(self) -> List[str]:
        """Returns local JAR file paths for JDBC if specified."""
        if not self.spark_jars:
            return []
        return [jar.strip() for jar in self.spark_jars.split(",") if jar.strip()]
