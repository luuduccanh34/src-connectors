from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field, ConfigDict

from src_connectors.src_spark.configs.base.base import SparkBaseComponent
from variables.spark import SparkVariables

# Load environment-based defaults
_spark_defaults = SparkVariables.get_spark_base_config()


class SparkCoreConfig(BaseModel, SparkBaseComponent):
    """
    Configuration model for core Apache Spark engine settings.

    This class manages Spark core parameters including resource allocation (memory, cores),
    cluster connectivity, runtime dependencies, and arbitrary Spark property overrides.
    Defaults are initialized from environment variables.
    """

    # Enable arbitrary extra fields so users can pass any spark_* properties dynamically
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    # Cluster & Application Settings
    spark_master: str = Field(
        default=_spark_defaults.get("SPARK_MASTER") or "local[*]",
        description="The Spark master URL to connect to."
    )
    spark_driver_host: str = Field(
        default=_spark_defaults.get("SPARK_DRIVER_HOST") or "127.0.0.1",
        description="Hostname or IP address for the driver to listen on."
    )
    spark_driver_bind_address: str = Field(
        default=_spark_defaults.get("SPARK_DRIVER_BIND_ADDRESS") or "127.0.0.1",
        description="Address to bind to for the driver."
    )
    spark_app_name: str = Field(
        default=_spark_defaults.get("SPARK_APP_NAME") or "SparkApp",
        description="The name of the Spark application."
    )

    # Resource Management
    spark_executor_memory: str = Field(
        default=_spark_defaults.get("SPARK_EXECUTOR_MEMORY") or "2g",
        description="Amount of memory to use per executor process."
    )
    spark_executor_cores: int = Field(
        default=int(_spark_defaults.get("SPARK_EXECUTOR_CORES") or 2),
        description="The number of cores to use on each executor."
    )
    spark_driver_memory: str = Field(
        default=_spark_defaults.get("SPARK_DRIVER_MEMORY") or "4g",
        description="Amount of memory to use for the driver process."
    )
    spark_driver_cores: int = Field(
        default=int(_spark_defaults.get("SPARK_DRIVER_CORES") or 2),
        description="Number of cores to use for the driver process."
    )

    # Dependencies & Runtime
    spark_jars: Optional[str] = Field(
        default=_spark_defaults.get("SPARK_LOCAL_JARS") or "",
        description="Comma-separated local jar paths."
    )
    spark_jars_packages: Optional[str] = Field(
        default=_spark_defaults.get("SPARK_JARS_PACKAGES") or "",
        description="Comma-separated Maven coordinates of jars to include."
    )
    spark_jars_ivy: str = Field(
        default=_spark_defaults.get("SPARK_JARS_IVY") or "/tmp/.ivy2",
        description="Path to local Ivy repository for dependency resolution."
    )

    # Environment & Versioning
    spark_version: str = Field(
        default=_spark_defaults.get("SPARK_VERSION") or "3.0.0",
        description="Target Spark version."
    )
    spark_minor_version: str = Field(
        default=_spark_defaults.get("SPARK_MINOR_VERSION") or "3.0",
        description="Target Spark minor version."
    )
    spark_local_dir: str = Field(
        default=_spark_defaults.get("SPARK_LOCAL_DIR") or "/tmp/spark",
        description="Directory for Spark 'scratch' space (map outputs, RDD spills)."
    )

    def get_spark_config(self) -> Dict[str, Any]:
        """
        Generates a dictionary of Core Spark configuration properties.
        Automatically includes standard properties and any dynamically passed extra spark_* fields.

        Note: JARs and Maven packages are intentionally excluded here to be handled
        by SparkConnector's dependency resolution step.
        """
        # Baseline Core Configs
        config = {
            "spark.master": self.spark_master,
            "spark.app.name": self.spark_app_name,
            "spark.executor.memory": str(self.spark_executor_memory),
            "spark.executor.cores": str(self.spark_executor_cores),
            "spark.driver.memory": str(self.spark_driver_memory),
            "spark.driver.cores": str(self.spark_driver_cores),
            "spark.jars.ivy": self.spark_jars_ivy,
            "spark.local.dir": self.spark_local_dir,
        }

        # Handle Driver Host logic
        is_local = self.spark_master.startswith("local")
        if is_local:
            config["spark.driver.host"] = self.spark_driver_host
            config["spark.driver.bindAddress"] = self.spark_driver_bind_address
        else:
            if self.spark_driver_host != "127.0.0.1":
                config["spark.driver.host"] = self.spark_driver_host
            if self.spark_driver_bind_address != "127.0.0.1":
                config["spark.driver.bindAddress"] = self.spark_driver_bind_address

        # Dynamic extra fields handling (e.g. spark_sql_shuffle_partitions -> spark.sql.shuffle.partitions)
        known_explicit_fields = {
            "spark_master", "spark_driver_host", "spark_driver_bind_address",
            "spark_app_name", "spark_executor_memory", "spark_executor_cores",
            "spark_driver_memory", "spark_driver_cores", "spark_jars",
            "spark_jars_packages", "spark_jars_ivy", "spark_version",
            "spark_minor_version", "spark_local_dir"
        }

        # Dump extra parameters passed at instantiation time
        extra_fields = getattr(self, "__pydantic_extra__", {}) or {}
        for k, v in extra_fields.items():
            if k in known_explicit_fields or v is None:
                continue

            # Convert key format: spark_sql_shuffle_partitions -> spark.sql.shuffle.partitions
            if k.startswith("spark_"):
                key = k.replace("_", ".")
            elif k.startswith("spark."):
                key = k
            else:
                key = f"spark.{k}"

            # Format boolean as string 'true'/'false'
            if isinstance(v, bool):
                config[key] = str(v).lower()
            else:
                config[key] = str(v)

        return config

    def get_required_spark_packages(self) -> List[str]:
        """
        Returns a cleaned list of required Maven package coordinates for Core.
        """
        if not self.spark_jars_packages:
            return []

        return [pkg.strip() for pkg in self.spark_jars_packages.split(",") if pkg.strip()]

    def get_required_local_jars(self) -> List[str]:
        """
        Returns a cleaned list of local JAR file paths for Core.
        """
        if not self.spark_jars:
            return []

        return [jar.strip() for jar in self.spark_jars.split(",") if jar.strip()]
