from typing import Dict, Any, List, Optional
from pydantic import Field, ConfigDict
from src_connectors.src_spark.configs.adapter.jdbc import SparkJdbcConfig


class SparkOracleConfig(SparkJdbcConfig):
    """
    Configuration model specifically designed for Oracle Database connections.

    Extends SparkJdbcConfig to provide intelligent defaults for Oracle JDBC driver,
    automated Maven package dependency management, parallel JDBC partitioning, and
    Oracle-specific performance tuning.
    """

    # Enable extra fields for arbitrary option overrides
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    # Override default driver class name for Oracle.
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

    # Parallel JDBC Partitioning Parameters
    partition_column: Optional[str] = Field(
        default=None,
        description="Column name (numeric/date/timestamp) used to split reading into parallel tasks."
    )
    lower_bound: Optional[int] = Field(
        default=None,
        description="Minimum value of partition_column for stride calculation."
    )
    upper_bound: Optional[int] = Field(
        default=None,
        description="Maximum value of partition_column for stride calculation."
    )
    num_partitions: Optional[int] = Field(
        default=None,
        description="The maximum number of partitions that can be used for parallel reading."
    )

    def get_jdbc_options(self) -> Dict[str, str]:
        """
        Generates connection options tailored for Spark's Oracle JDBC data source operations.

        Applies fetch size optimizations, maps parallel JDBC partitioning keys to PySpark standard
        camelCase properties, and resolves common NLS timezone region issues.

        Returns:
            Dict[str, str]: A dictionary of stringified key-value pairs representing JDBC options.
        """
        # Retrieve standard options (url, driver, user, password) from parent
        options = super().get_jdbc_options()

        options["fetchsize"] = str(self.fetch_size)

        # Map snake_case partitioning parameters to Spark JDBC camelCase requirements
        if self.partition_column and self.num_partitions is not None:
            options["partitionColumn"] = self.partition_column
            options["numPartitions"] = str(self.num_partitions)

            if self.lower_bound is not None:
                options["lowerBound"] = str(self.lower_bound)
            if self.upper_bound is not None:
                options["upperBound"] = str(self.upper_bound)

        # Bypass the 'ORA-01882: Timezone region not found' error commonly encountered in Spark-Oracle JDBC links
        options["oracle.jdbc.timezoneAsRegion"] = "false"

        # Capture any dynamic extra fields passed to the model
        extra_fields = getattr(self, "__pydantic_extra__", {}) or {}
        for k, v in extra_fields.items():
            if v is not None and k not in options:
                options[k] = str(v)

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
