from typing import Dict, Any, Optional, List, Union
from pydantic import BaseModel, Field, ConfigDict

from src_connectors.src_spark.configs.base.base import SparkBaseComponent
from variables.spark import SparkVariables

# Load environment-based defaults
_iceberg_defaults = SparkVariables.get_iceberg_config()
_minio_defaults = SparkVariables.get_minio_config()
_spark_defaults = SparkVariables.get_spark_base_config()


class SparkIcebergConfig(BaseModel, SparkBaseComponent):
    """
    Configuration model for Apache Iceberg integration with Spark.

    Manages Iceberg catalog settings, connection URIs, authentication,
    warehouse locations, and adapter-specific dependencies.
    """
    model_config = ConfigDict(extra='allow')

    iceberg_catalog_uri: str = Field(
        default=_iceberg_defaults.get("SPARK_ICEBERG_CATALOG_URI") or "",
        description="The URI for the Iceberg catalog (REST/Hive/Glue endpoint)."
    )
    iceberg_catalog_type: str = Field(
        default=_iceberg_defaults.get("SPARK_ICEBERG_CATALOG_TYPE") or "hadoop",
        description="The type of Iceberg catalog (e.g., 'rest', 'hive', 'hadoop')."
    )
    iceberg_oauth2_server_uri: str = Field(
        default=_iceberg_defaults.get("SPARK_ICEBERG_OAUTH2_SERVER_URI") or "",
        description="OAuth2 server URI for REST catalog authentication."
    )
    iceberg_credential: str = Field(
        default=_iceberg_defaults.get("SPARK_ICEBERG_CREDENTIAL") or "",
        description="Credentials for Iceberg catalog (e.g., client_id:client_secret)."
    )
    iceberg_scope: str = Field(
        default=_iceberg_defaults.get("SPARK_ICEBERG_SCOPE") or "",
        description="OAuth2 scope for catalog authentication."
    )
    iceberg_warehouse: Union[str, List[str]] = Field(
        default=_iceberg_defaults.get("SPARK_ICEBERG_WAREHOUSE") or "",
        description="The warehouse location/name used for the Spark SQL catalog prefix."
    )
    iceberg_version: str = Field(
        default=_iceberg_defaults.get("SPARK_ICEBERG_VERSION") or "1.4.2",
        description="The Version of Iceberg runtime packages to load."
    )

    # Adapter-specific Dependencies (Đã tách riêng Env vars cho Iceberg)
    spark_jars: Optional[str] = Field(
        default=_iceberg_defaults.get("SPARK_ICEBERG_LOCAL_JARS") or "",
        description="Local jar file paths specifically for Iceberg."
    )
    spark_jars_packages: Optional[str] = Field(
        default=_iceberg_defaults.get("SPARK_ICEBERG_JARS_PACKAGES") or "",
        description="Maven package coordinates specifically for Iceberg overrides."
    )

    # Storage Settings (MinIO / S3)
    spark_minio_access_key: str = Field(
        default=_minio_defaults.get("SPARK_MINIO_ACCESS_KEY_ID") or "",
        description="Access key for MinIO or S3 storage."
    )
    spark_minio_secret_key: str = Field(
        default=_minio_defaults.get("SPARK_MINIO_SECRET_ACCESS_KEY") or "",
        description="Secret key for MinIO or S3 storage."
    )
    spark_minio_endpoint: str = Field(
        default=_minio_defaults.get("SPARK_MINIO_ENDPOINT_URL") or "",
        description="Endpoint URL for MinIO or S3 storage (e.g., http://localhost:9000)."
    )

    def get_spark_config(self) -> Dict[str, Any]:
        """
        Generates Spark configuration for the Iceberg catalog and S3/MinIO storage.
        Note: JARs and Maven packages are intentionally excluded here to be handled
        by SparkConnector's dependency resolution step.
        """
        config = {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        }

        warehouses = [self.iceberg_warehouse] if isinstance(self.iceberg_warehouse, str) else self.iceberg_warehouse

        for wh in warehouses:
            prefix = f"spark.sql.catalog.{wh}"
            config[prefix] = "org.apache.iceberg.spark.SparkCatalog"

            if self.iceberg_catalog_type != "rest":
                config[f"{prefix}.type"] = self.iceberg_catalog_type

            if self.iceberg_catalog_uri:
                config[f"{prefix}.uri"] = self.iceberg_catalog_uri
                config[f"{prefix}.classloader.isolation.enabled"] = "false"

                if any(s in wh.lower() or s in self.iceberg_catalog_uri.lower() for s in ["s3", "minio"]):
                    config[f"{prefix}.io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"

            if wh:
                config[f"{prefix}.warehouse"] = wh

            if self.iceberg_catalog_type == "rest":
                self._apply_rest_catalog_config(config, prefix)

        # Apply Hadoop S3A storage configurations if credentials are provided
        if self.spark_minio_access_key and self.spark_minio_secret_key:
            config.update(self._get_s3_hadoop_config())

        return config

    def _apply_rest_catalog_config(self, config: Dict[str, Any], prefix: str) -> None:
        """Applies REST catalog specific authentication and refresh settings."""
        config[f"{prefix}.catalog-impl"] = "org.apache.iceberg.rest.RESTCatalog"
        if self.iceberg_oauth2_server_uri:
            config[f"{prefix}.oauth2-server-uri"] = self.iceberg_oauth2_server_uri
        if self.iceberg_credential:
            config[f"{prefix}.credential"] = self.iceberg_credential
        if self.iceberg_scope:
            config[f"{prefix}.scope"] = self.iceberg_scope

        config[f"{prefix}.rest.auth.type"] = "oauth2"
        config[f"{prefix}.token-refresh-enabled"] = "true"
        config[f"{prefix}.token-refresh-min-validity-time"] = "60s"

    def _get_s3_hadoop_config(self) -> Dict[str, str]:
        """Generates Hadoop S3A configurations for MinIO/S3 storage."""
        hadoop_conf = {
            "spark.hadoop.fs.s3a.access.key": self.spark_minio_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.spark_minio_secret_key,
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false" if "http://" in self.spark_minio_endpoint.lower() else "true"
        }

        if self.spark_minio_endpoint:
            hadoop_conf["spark.hadoop.fs.s3a.endpoint"] = self.spark_minio_endpoint

        hadoop_conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        hadoop_conf["spark.hadoop.fs.s3.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"

        return hadoop_conf

    def get_required_spark_packages(self) -> List[str]:
        """
        Returns required Maven package coordinates for Iceberg.
        Uses user-specified packages if provided, otherwise falls back to default Iceberg bundles.
        """
        if self.spark_jars_packages:
            return [pkg.strip() for pkg in self.spark_jars_packages.split(",") if pkg.strip()]

        # Default Iceberg Maven bundles based on spark minor version and iceberg version
        spark_minor_version = _spark_defaults.get("SPARK_MINOR_VERSION", "3.0")
        return [
            f"org.apache.iceberg:iceberg-spark-runtime-{spark_minor_version}_2.12:{self.iceberg_version}",
            f"org.apache.iceberg:iceberg-azure-bundle:{self.iceberg_version}",
            f"org.apache.iceberg:iceberg-aws-bundle:{self.iceberg_version}",
            f"org.apache.iceberg:iceberg-gcp-bundle:{self.iceberg_version}"
        ]

    def get_required_local_jars(self) -> List[str]:
        """
        Returns local JAR file paths specifically configured for Iceberg.
        """
        if not self.spark_jars:
            return []

        return [jar.strip() for jar in self.spark_jars.split(",") if jar.strip()]
