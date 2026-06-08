from typing import Any, Optional, Dict, List, Union
from pyspark.sql import SparkSession
import structlog

from src_connectors.src_base.connector import BaseConnector
from src_connectors.src_spark.configs.core_config import SparkCoreConfig
from src_connectors.src_spark.configs.iceberg import SparkIcebergConfig
from src_connectors.src_spark.configs.jdbc import SparkJdbcConfig
from src_connectors.src_spark.configs.base import SparkBaseComponent

logger = structlog.get_logger(__name__)

class SparkConnector(BaseConnector):
    """
    Professional Apache Spark connector that manages SparkSession lifecycle and configurations.

    This connector supports a modular approach to configuration, starting with a core
    engine setup and allowing for pluggable components like Iceberg and JDBC.
    """

    def __init__(
        self,
        config: Optional[Union[SparkCoreConfig, Dict[str, Any]]] = None,
        **kwargs: Any
    ):
        """
        Initializes the SparkConnector.

        Args:
            config: A SparkCoreConfig instance or a dictionary of raw Spark properties.
            **kwargs: Additional Spark properties or component configurations (iceberg, jdbc).
        """
        self._spark: Optional[SparkSession] = None
        self._components: Dict[str, SparkBaseComponent] = {}
        self._extra_overwrites: Dict[str, Any] = {}

        # 1. Handle Core Configuration baseline
        if isinstance(config, SparkCoreConfig):
            self.core_config = config
        elif isinstance(config, dict):
            # If a dict is passed, we use defaults but record the values to overwrite later
            self.core_config = SparkCoreConfig()
            self._extra_overwrites.update(config)
        else:
            self.core_config = SparkCoreConfig()

        # 2. Handle kwargs for component initialization or raw property overwrites
        if kwargs:
            # Check for special component keys
            if "iceberg" in kwargs:
                self.configure_iceberg(kwargs.get("iceberg"))
            if "jdbc" in kwargs:
                self.configure_jdbc(kwargs.get("jdbc"))

            # Everything else is treated as a raw Spark property overwrite
            for k, v in kwargs.items():
                if k not in ["iceberg", "jdbc"]:
                    # Support both dot notation (spark.master) and underscore (spark_master)
                    key = k.replace("_", ".") if not k.startswith("spark.") else k
                    self._extra_overwrites[key] = v

    def configure_iceberg(self, config: Optional[Union[SparkIcebergConfig, Dict[str, Any]]] = None) -> "SparkConnector":
        """
        Enables and configures Apache Iceberg for this Spark session.

        Args:
            config: SparkIcebergConfig instance or dict of field overrides.
                    If None, defaults to environment settings.
        """
        if isinstance(config, dict):
            # Create config from dict mapping fields
            self._components["iceberg"] = SparkIcebergConfig(**config)
        else:
            self._components["iceberg"] = config or SparkIcebergConfig()
        return self

    def configure_jdbc(self, config: Optional[Union[SparkJdbcConfig, Dict[str, Any]]] = None) -> "SparkConnector":
        """
        Enables and configures JDBC data sources for this Spark session.

        Args:
            config: SparkJdbcConfig instance or dict of field overrides.
                    If None, defaults to environment settings.
        """
        if isinstance(config, dict):
            self._components["jdbc"] = SparkJdbcConfig(**config)
        else:
            self._components["jdbc"] = config or SparkJdbcConfig()
        return self

    def connect(self, **overwrites: Any) -> SparkSession:
        """
        Initializes and returns the SparkSession with all aggregated configurations.

        Args:
            **overwrites: Highest priority Spark configuration keys (e.g., 'spark.master').

        Returns:
            SparkSession: The established Spark session.
        """
        if self._spark:
            return self._spark

        builder = SparkSession.builder

        # Phase 1: Engine (Core) Configuration baseline
        final_config = self.core_config.get_spark_config()
        all_required_packages = self.core_config.get_required_spark_packages()

        # Phase 2: Layer component-specific configurations
        for name, component in self._components.items():
            final_config.update(component.get_spark_config())
            all_required_packages.extend(component.get_required_spark_packages())

        # Phase 3: Init-time overwrites (from constructor dictionary or kwargs)
        final_config.update(self._extra_overwrites)

        # Phase 4: Connect-time overwrites (Highest priority)
        if overwrites:
            # Map underscores to dots for convenience if needed
            formatted_overwrites = {
                (k.replace("_", ".") if "." not in k else k): v
                for k, v in overwrites.items()
            }
            final_config.update(formatted_overwrites)

        # Ensure spark.local.dir exists and is absolute to avoid DiskBlockManager errors
        local_dir = final_config.get("spark.local.dir")
        if local_dir:
            from pathlib import Path
            local_path = Path(local_dir).expanduser().resolve()
            try:
                local_path.mkdir(parents=True, exist_ok=True)
                final_config["spark.local.dir"] = str(local_path)
            except Exception as e:
                logger.warning("Could not create spark.local.dir, falling back to system default",
                               path=str(local_path), error=str(e))
                final_config.pop("spark.local.dir", None)

        # Handle Maven Packages (Deduplicate and clean)
        if all_required_packages:
            unique_packages = sorted(list(set(pkg.strip() for pkg in all_required_packages if pkg and pkg.strip())))
            # If user explicitly provided packages in overwrites, prioritize them
            if "spark.jars.packages" not in final_config:
                final_config["spark.jars.packages"] = ",".join(unique_packages)

        # Apply final settings to the SparkSession builder
        for key, value in final_config.items():
            builder = builder.config(key, str(value))
            logger.debug("Applying Spark config", key=key, value="***" if any(s in key.lower() for s in ["password", "secret", "credential"]) else value)

        self._spark = builder.getOrCreate()
        logger.info("SparkSession established", app_name=final_config.get("spark.app.name", self.core_config.spark_app_name))
        return self._spark

    def close(self) -> None:
        """Stops the active SparkSession and releases resources."""
        if self._spark:
            self._spark.stop()
            self._spark = None
            logger.info("SparkSession stopped successfully")

    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        stream: bool = False,
        batch_size: int = 1000,
        output_type: str = "dataframe",
        **kwargs: Any
    ) -> Any:
        """
        Executes a SQL query within the Spark session.

        Args:
            query: The SQL query string.
            params: Parameters for the query (not natively supported by spark.sql, usually handled via string formatting).
            stream: Whether to stream the result (returns an iterator of DataFrames/batches).
            batch_size: Size of batches if streaming.
            output_type: The format of the output ('dataframe', 'list', 'array').
            **kwargs: Additional parameters.

        Returns:
            The query result in the specified format or a Generator if stream=True.

        Raises:
            ValueError: If the output_type is not supported.
        """
        supported_types = ["dataframe", "list", "array"]
        if output_type not in supported_types:
            raise ValueError(
                f"Output type '{output_type}' is not supported. "
                f"Supported types: {', '.join(supported_types)}."
            )

        spark = self.connect()
        df = spark.sql(query)

        if stream:
            return self._stream_results(df, batch_size, output_type)

        if output_type == "dataframe":
            return df

        # Collect to driver for list and array (use with caution for large data)
        rows = df.collect()
        return self._format_rows(rows, df.columns, output_type)

    def _stream_results(self, df: Any, batch_size: int, output_type: str) -> Any:
        """Helper to stream Spark DataFrame results in batches."""
        columns = df.columns
        rows_accumulator = []

        # toLocalIterator consumes the data partition by partition without collecting all at once
        for row in df.toLocalIterator():
            rows_accumulator.append(row)
            if len(rows_accumulator) >= batch_size:
                yield self._format_rows(rows_accumulator, columns, output_type)
                rows_accumulator = []

        if rows_accumulator:
            yield self._format_rows(rows_accumulator, columns, output_type)

    def _format_rows(self, rows: List[Any], columns: List[str], output_type: str) -> Any:
        """Formats a list of Spark Rows into the requested output format."""
        if output_type == "dataframe":
            import pandas as pd
            return pd.DataFrame([list(row) for row in rows], columns=columns)

        if output_type == "array":
            import numpy as np
            return np.array([list(row) for row in rows])

        # Default: list of dictionaries
        return [dict(zip(columns, row)) for row in rows]
