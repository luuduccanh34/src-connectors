from typing import Dict, Any, List
from variables.helper import BaseConfig


class SparkVariables(BaseConfig):
    """
    Configuration mapping for Apache Spark environment variables.

    This class provides centralized access to Spark engine settings,
    Iceberg catalog integration, and JDBC connectivity configurations.
    All settings are loaded from environment variables using the BaseConfig helper.
    """

    # Core Spark Engine settings
    BASE_VARIABLES: List[str] = [
        "SPARK_MASTER",
        "SPARK_DRIVER_HOST",
        "SPARK_DRIVER_BIND_ADDRESS",
        "SPARK_APP_NAME",
        "SPARK_EXECUTOR_MEMORY",
        "SPARK_EXECUTOR_CORES",
        "SPARK_DRIVER_MEMORY",
        "SPARK_DRIVER_CORES",
        "SPARK_LOCAL_DIR",
        "SPARK_JARS_PACKAGES",
        "SPARK_JARS_IVY",
        "SPARK_LOCAL_JARS",
        "SPARK_VERSION",
        "SPARK_MINOR_VERSION",
    ]

    # Iceberg Catalog Configuration
    ICEBERG_VARIABLES: List[str] = [
        "SPARK_ICEBERG_CATALOG_URI",
        "SPARK_ICEBERG_CATALOG_TYPE",
        "SPARK_ICEBERG_OAUTH2_SERVER_URI",
        "SPARK_ICEBERG_CREDENTIAL",
        "SPARK_ICEBERG_SCOPE",
        "SPARK_ICEBERG_WAREHOUSE",
        "SPARK_ICEBERG_VERSION",
        "SPARK_ICEBERG_LOCAL_JARS",
        "SPARK_ICEBERG_JARS_PACKAGES"
    ]

    # JDBC Datasource Configuration
    JDBC_VARIABLES: List[str] = [
        "SPARK_JDBC_URL",
        "SPARK_JDBC_DRIVER",
        "SPARK_JDBC_USERNAME",
        "SPARK_JDBC_PASSWORD",
        "SPARK_JDBC_LOCAL_JARS",
        "SPARK_JDBC_JARS_PACKAGES"
    ]

    # Minio and S3 Configuration (used for Iceberg storage)
    MINIO_VARIABLES: List[str] = [
        "SPARK_MINIO_ACCESS_KEY_ID",
        "SPARK_MINIO_SECRET_ACCESS_KEY",
        "SPARK_MINIO_ENDPOINT_URL",
    ]

    # Combined list for BaseConfig loader
    VARIABLES: List[str] = BASE_VARIABLES + ICEBERG_VARIABLES + JDBC_VARIABLES + MINIO_VARIABLES

    @classmethod
    def config(cls) -> Dict[str, Any]:
        """
        Loads and returns all Spark-related environment variables as a dictionary.

        Returns:
            Dict[str, Any]: Dictionary containing all loaded Spark settings.
        """
        return cls.load()

    @classmethod
    def get_spark_base_config(cls) -> Dict[str, Any]:
        """
        Extracts core Spark engine configurations.

        Returns:
            Dict[str, Any]: Dictionary of core Spark settings (Master, Memory, etc.).
        """
        all_config = cls.config()
        return {var: all_config.get(var) for var in cls.BASE_VARIABLES}

    @classmethod
    def get_iceberg_config(cls) -> Dict[str, Any]:
        """
        Extracts Iceberg-specific Spark configurations.

        Returns:
            Dict[str, Any]: Dictionary of Iceberg catalog and storage settings.
        """
        all_config = cls.config()
        return {var: all_config.get(var) for var in cls.ICEBERG_VARIABLES}

    @classmethod
    def get_jdbc_config(cls) -> Dict[str, Any]:
        """
        Extracts JDBC-specific Spark configurations.

        Returns:
            Dict[str, Any]: Dictionary of JDBC connection settings.
        """
        all_config = cls.config()
        return {var: all_config.get(var) for var in cls.JDBC_VARIABLES}

    @classmethod
    def get_minio_config(cls) -> Dict[str, Any]:
        """
        Extracts MinIO/S3-specific configurations for Spark.

        Returns:
            Dict[str, Any]: Dictionary of MinIO access keys and endpoint settings.
        """
        all_config = cls.config()
        return {var: all_config.get(var) for var in cls.MINIO_VARIABLES}
