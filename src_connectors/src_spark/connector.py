from pathlib import Path
from typing import Any, Optional, Dict, List, Union, Generator
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import structlog

from src_connectors.src_base.connector import BaseConnector
from src_connectors.src_spark.configs.core.core_config import SparkCoreConfig
from src_connectors.src_spark.configs.adapter.iceberg import SparkIcebergConfig
from src_connectors.src_spark.configs.adapter.jdbc import SparkJdbcConfig
from src_connectors.src_spark.configs.adapter.oracle import SparkOracleConfig
from src_connectors.src_spark.configs.base.base import SparkBaseComponent

# Import centralized exceptions and utilities
from src_connectors.exceptions import DependencyConflictError, SparkConnectorError
from src_connectors.utils import resolve_via_ivy_and_check_conflicts

logger = structlog.get_logger(__name__)


class SparkConnector(BaseConnector):
    """
    Professional Apache Spark connector that manages SparkSession lifecycle,
    pluggable adapter configurations (Oracle, Iceberg, JDBC), and cross-source dependency resolution.

    This class serves as the central manager for building and executing queries against
    a unified Spark Session.
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
            **kwargs: Additional Spark properties or adapter configurations (oracle, iceberg, jdbc).
        """
        self._spark: Optional[SparkSession] = None
        self._components: Dict[str, SparkBaseComponent] = {}
        self._extra_overwrites: Dict[str, Any] = {}

        # 1. Handle Core Configuration baseline
        if isinstance(config, SparkCoreConfig):
            self.core_config = config
        elif isinstance(config, dict):
            self.core_config = SparkCoreConfig()
            self._extra_overwrites.update(config)
        else:
            self.core_config = SparkCoreConfig()

        # 2. Handle kwargs for adapter initialization or raw property overwrites
        if kwargs:
            if "oracle" in kwargs:
                self.configure_oracle(kwargs.get("oracle"))
            if "iceberg" in kwargs:
                self.configure_iceberg(kwargs.get("iceberg"))
            if "jdbc" in kwargs:
                self.configure_jdbc(kwargs.get("jdbc"))

            # Treat remaining kwargs as raw Spark property overwrites
            for k, v in kwargs.items():
                if k not in ["oracle", "iceberg", "jdbc"]:
                    key = k.replace("_", ".") if not k.startswith("spark.") else k
                    self._extra_overwrites[key] = v

    def configure_oracle(
        self,
        config: Optional[Union[SparkOracleConfig, Dict[str, Any]]] = None
    ) -> "SparkConnector":
        """
        Enables and configures Oracle database adapter for this Spark session.

        Args:
            config: SparkOracleConfig instance or dict of field overrides.

        Returns:
            SparkConnector: Self instance for method chaining.
        """
        if isinstance(config, dict):
            self._components["oracle"] = SparkOracleConfig(**config)
        else:
            self._components["oracle"] = config or SparkOracleConfig()
        return self

    def configure_iceberg(
        self,
        config: Optional[Union[SparkIcebergConfig, Dict[str, Any]]] = None
    ) -> "SparkConnector":
        """
        Enables and configures Apache Iceberg for this Spark session.

        Args:
            config: SparkIcebergConfig instance or dict of field overrides.

        Returns:
            SparkConnector: Self instance for method chaining.
        """
        if isinstance(config, dict):
            self._components["iceberg"] = SparkIcebergConfig(**config)
        else:
            self._components["iceberg"] = config or SparkIcebergConfig()
        return self

    def configure_jdbc(
        self,
        config: Optional[Union[SparkJdbcConfig, Dict[str, Any]]] = None
    ) -> "SparkConnector":
        """
        Enables and configures generic JDBC data sources for this Spark session.

        Args:
            config: SparkJdbcConfig instance or dict of field overrides.

        Returns:
            SparkConnector: Self instance for method chaining.
        """
        if isinstance(config, dict):
            self._components["jdbc"] = SparkJdbcConfig(**config)
        else:
            self._components["jdbc"] = config or SparkJdbcConfig()
        return self

    @property
    def oracle_config(self) -> Optional[SparkOracleConfig]:
        """Helper property to access registered Oracle configuration instance."""
        comp = self._components.get("oracle")
        return comp if isinstance(comp, SparkOracleConfig) else None

    @property
    def iceberg_config(self) -> Optional[SparkIcebergConfig]:
        """Helper property to access registered Iceberg configuration instance."""
        comp = self._components.get("iceberg")
        return comp if isinstance(comp, SparkIcebergConfig) else None

    @property
    def jdbc_config(self) -> Optional[SparkJdbcConfig]:
        """Helper property to access registered generic JDBC configuration instance."""
        comp = self._components.get("jdbc")
        return comp if isinstance(comp, SparkJdbcConfig) else None

    def connect(self, **overwrites: Any) -> SparkSession:
        """
        Initializes and returns the SparkSession with aggregated, cross-validated configurations.

        Args:
            **overwrites: Highest priority Spark configuration keys (e.g., 'spark.master').

        Returns:
            SparkSession: The established active Spark session.

        Raises:
            DependencyConflictError: If version mismatches exist between local JARs and Maven packages.
        """
        if self._spark:
            return self._spark

        builder = SparkSession.builder

        # Phase 1: Engine (Core) Configuration baseline
        final_config = self.core_config.get_spark_config()
        all_required_packages = self.core_config.get_required_spark_packages()
        all_local_jars = self.core_config.get_required_local_jars()

        # Phase 2: Aggregate component-specific configurations and dependencies
        for name, component in self._components.items():
            final_config.update(component.get_spark_config())
            all_required_packages.extend(component.get_required_spark_packages())
            all_local_jars.extend(component.get_required_local_jars())

        # Phase 3: Init-time overwrites
        final_config.update(self._extra_overwrites)

        # Phase 4: Connect-time overwrites (Highest priority)
        if overwrites:
            formatted_overwrites = {
                (k.replace("_", ".") if "." not in k else k): v
                for k, v in overwrites.items()
            }
            final_config.update(formatted_overwrites)

        # Ensure spark.local.dir exists and is absolute to avoid DiskBlockManager errors
        local_dir = final_config.get("spark.local.dir")
        if local_dir:
            local_path = Path(local_dir).expanduser().resolve()
            try:
                local_path.mkdir(parents=True, exist_ok=True)
                final_config["spark.local.dir"] = str(local_path)
            except Exception as e:
                logger.warning(
                    "Could not create spark.local.dir, falling back to default",
                    path=str(local_path),
                    error=str(e)
                )
                final_config.pop("spark.local.dir", None)

        # Phase 5: Resolve and Cross-Check Dependencies (Fail-Fast on Version Conflicts)
        try:
            final_maven_pkgs, final_local_jars = resolve_via_ivy_and_check_conflicts(
                maven_packages=all_required_packages,
                local_jar_paths=all_local_jars,
                ivy_cache_dir=final_config.get("spark.jars.ivy", "/tmp/.ivy2")
            )

            if "spark.jars.packages" not in final_config and final_maven_pkgs:
                final_config["spark.jars.packages"] = ",".join(final_maven_pkgs)

            if "spark.jars" not in final_config and final_local_jars:
                final_config["spark.jars"] = ",".join(final_local_jars)

        except DependencyConflictError as e:
            logger.error("SparkSession startup aborted due to dependency conflict", error=str(e))
            raise e

        # Phase 6: Apply settings to the SparkSession builder
        for key, value in final_config.items():
            builder = builder.config(key, str(value))
            logger.debug(
                "Applying Spark config",
                key=key,
                value="***" if any(s in key.lower() for s in ["password", "secret", "credential"]) else value
            )

        self._spark = builder.getOrCreate()
        logger.info(
            "SparkSession established successfully",
            app_name=final_config.get("spark.app.name", self.core_config.spark_app_name)
        )
        return self._spark

    def register_jdbc_view(
        self,
        view_name: str,
        config: SparkJdbcConfig,
        dbtable_or_query: str
    ) -> None:
        """
        Registers a JDBC table or inline query as a Spark Temporary View.
        Enables seamless execution of pure Spark SQL queries across multiple database sources.

        Args:
            view_name: Name of the temporary view to register in the Spark Catalog.
            config: SparkJdbcConfig (or SparkOracleConfig) instance containing connection details.
            dbtable_or_query: Table name (e.g., 'SCHEMA.TABLE') or inline subquery e.g. '(SELECT * FROM TABLE) sub'.
        """
        spark = self.connect()
        options = config.get_jdbc_options()
        options["dbtable"] = dbtable_or_query

        spark.read.format("jdbc").options(**options).load().createOrReplaceTempView(view_name)
        logger.info("Registered JDBC Temp View", view_name=view_name, source=dbtable_or_query)

    def close(self) -> None:
        """Stops the active SparkSession and releases allocated resources."""
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
        Executes a Spark SQL query within the active Spark session.

        Args:
            query: The SQL query string.
            params: Unused in spark.sql, kept for BaseConnector interface compatibility.
            stream: Whether to stream the result (returns a generator of batched outputs).
            batch_size: Size of batches if streaming is enabled.
            output_type: The output format ('dataframe', 'list', 'array').
            **kwargs: Additional execution options.

        Returns:
            DataFrame, List, Array, or Generator depending on parameters.

        Raises:
            ValueError: If an unsupported output_type is specified.
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

        # Collect to driver for list and array (use with caution for large datasets)
        rows = df.collect()
        return self._format_rows(rows, df.columns, output_type)

    def _stream_results(
        self,
        df: Any,
        batch_size: int,
        output_type: str
    ) -> Generator[Any, None, None]:
        """Helper generator to stream Spark DataFrame results partition by partition."""
        columns = df.columns
        rows_accumulator = []

        # toLocalIterator consumes data partition by partition without bringing all into Driver memory at once
        for row in df.toLocalIterator():
            rows_accumulator.append(row)
            if len(rows_accumulator) >= batch_size:
                yield self._format_rows(rows_accumulator, columns, output_type)
                rows_accumulator = []

        if rows_accumulator:
            yield self._format_rows(rows_accumulator, columns, output_type)

    def _format_rows(self, rows: List[Any], columns: List[str], output_type: str) -> Any:
        """Formats a list of PySpark Rows into the requested output format."""
        if output_type == "dataframe":
            import pandas as pd
            return pd.DataFrame([list(row) for row in rows], columns=columns)

        if output_type == "array":
            import numpy as np
            return np.array([list(row) for row in rows])

        # Default: list of dictionaries
        return [dict(zip(columns, row)) for row in rows]

    def write_jdbc_table(
            self,
            df: DataFrame,
            config: SparkJdbcConfig,
            target_table: str,
            mode: str = "append",
            batch_size: int = 5000
    ) -> None:
        """
        Writes a PySpark DataFrame into a target JDBC table using the provided configuration.

        Args:
            df: The transformed PySpark DataFrame to persist.
            config: SparkJdbcConfig or SparkOracleConfig instance with connection details.
            target_table: Destination table name in format 'SCHEMA.TABLE_NAME'.
            mode: Save mode - 'append', 'overwrite', 'ignore', or 'errorifexists'. Defaults to 'append'.
            batch_size: Number of records inserted per round-trip to the target DB.
        """
        options = config.get_jdbc_options()
        options["dbtable"] = target_table
        options["batchsize"] = str(batch_size)

        logger.info(
            "Writing DataFrame to JDBC target",
            target_table=target_table,
            jdbc_url=config.jdbc_url,
            mode=mode,
            batch_size=batch_size
        )

        df.write.format("jdbc").options(**options).mode(mode).save()

        logger.info("Successfully written DataFrame to JDBC target", target_table=target_table)
