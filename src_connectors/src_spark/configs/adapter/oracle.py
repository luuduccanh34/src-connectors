from typing import Dict, Any, List
from pydantic import Field
from src_connectors.src_spark.configs.adapter.jdbc import SparkJdbcConfig


class SparkOracleConfig(SparkJdbcConfig):
    """
    Configuration model specifically designed for Oracle Database connections.

    Extends SparkJdbcConfig to provide intelligent defaults for Oracle JDBC driver,
    automated Maven package dependency management, and Oracle-specific performance tuning.
    """

    # Override default driver class name for Oracle.
    # Users can still explicitly pass a custom driver class if necessary.
    jdbc_driver: str = Field(
        default="oracle.jdbc.driver.OracleDriver",
        description="The fully qualified class name of the Oracle JDBC driver."
    )

    # Oracle JDBC Driver version (ojdbc8)
    oracle_driver_version: str = Field(
        default="19.3.0.0",
        description="The target version of the com.oracle.database.jdbc:ojdbc8 Maven package."
    )

    # Performance tuning parameters
    fetch_size: int = Field(
        default=10000,
        description="The number of rows fetched per round trip to the database for optimized read performance."
    )

    def get_jdbc_options(self) -> Dict[str, str]:
        """
        Generates connection options tailored for Spark's Oracle JDBC data source operations.

        Applies fetch size optimizations and resolves common NLS timezone region issues.

        Returns:
            Dict[str, str]: A dictionary of stringified key-value pairs representing JDBC options.
        """
        # Retrieve standard options (url, driver, user, password) from the parent class
        options = super().get_jdbc_options()

        options["fetchsize"] = str(self.fetch_size)

        # Bypass the 'ORA-01882: Timezone region not found' error commonly encountered in Spark-Oracle JDBC links
        options["oracle.jdbc.timezoneAsRegion"] = "false"

        return options

    def get_required_spark_packages(self) -> List[str]:
        """
        Returns required Maven package coordinates for the Oracle JDBC driver.

        Prioritizes user-configured package overrides if provided; otherwise,
        falls back to constructing the default ojdbc8 Maven dependency.

        Returns:
            List[str]: A list containing Maven package coordinates.
        """
        if self.spark_jars_packages:
            return [pkg.strip() for pkg in self.spark_jars_packages.split(",") if pkg.strip()]

        return [f"com.oracle.database.jdbc:ojdbc8:{self.oracle_driver_version}"]
