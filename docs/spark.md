# Apache Spark Connector

The `SparkConnector` is a professional-grade implementation for managing the Apache Spark lifecycle. It features a modular configuration system supporting **Iceberg** catalogs, **JDBC** data sources, and cloud storage integrations like **MinIO/S3**.

## Architecture

The connector is built on a layered configuration engine:
1. **Core Config**: Engine settings (master, memory, cores).
2. **Components**: Pluggable modules for specific features (Iceberg, JDBC).
3. **Overwrites**: Dynamic overrides during initialization or connection.

## Configuration

### Core Settings

Primary environment variables for the Spark engine:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `SPARK_MASTER` | Spark master URL | `local[*]` |
| `SPARK_APP_NAME` | Name of the application | `SparkApp` |
| `SPARK_EXECUTOR_MEMORY` | Memory per executor | `2g` |
| `SPARK_DRIVER_MEMORY` | Memory for the driver | `4g` |
| `SPARK_LOCAL_DIR` | Local scratch directory | `/tmp/spark` |

### Iceberg Settings

| Variable | Description | Default |
| :--- | :--- | :--- |
| `SPARK_ICEBERG_WAREHOUSE` | Catalog identifier and location | - |
| `SPARK_ICEBERG_CATALOG_TYPE` | Catalog type (`rest`, `hive`, `hadoop`) | `hadoop` |
| `SPARK_ICEBERG_CATALOG_URI` | Catalog endpoint URI | - |
| `SPARK_ICEBERG_VERSION` | Iceberg runtime version | `1.4.2` |

## Usage

### Initialization

Initialize with core defaults or custom engine settings:

```python
from src_connectors import SparkConnector

# Default (env variables)
connector = SparkConnector()

# Custom engine settings
connector = SparkConnector(spark_master="local[2]", spark_driver_memory="8g")
```

### Configuring Components

Enable features like Iceberg or JDBC before connecting:

```python
# Configure Iceberg using defaults or dict overrides
connector.configure_iceberg({
    "iceberg_warehouse": "my_catalog",
    "iceberg_catalog_type": "rest",
    "iceberg_catalog_uri": "https://catalog.endpoint"
})

# Configure JDBC
connector.configure_jdbc({
    "jdbc_url": "jdbc:sqlserver://host:1433;database=db",
    "jdbc_username": "user",
    "jdbc_password": "password"
})
```

### Connecting

Establish the `SparkSession`. You can provide final, high-priority overrides here.

```python
spark = connector.connect(spark_app_name="DataProductionJob")
```

### Executing Queries

Standard interface for Spark SQL queries.

```python
# Fetch as PySpark DataFrame
df = connector.execute_query("SELECT * FROM my_catalog.db.table")

# Fetch as list of dictionaries
data = connector.execute_query("SELECT * FROM table", output_type="list")
```

### Streaming Batches

Efficiently process massive datasets by streaming batches through a generator.

```python
# Returns an iterator of batches
batches = connector.execute_query(
    "SELECT * FROM large_table",
    stream=True,
    batch_size=10000,
    output_type="list"
)

for batch in batches:
    # Process batch (list of dicts)
    process_parallel(batch)
```

## Storage Integration (MinIO/S3)

If you configure MinIO/S3 credentials, the connector automatically sets up the Hadoop S3A file system:

```env
SPARK_MINIO_ACCESS_KEY_ID=admin
SPARK_MINIO_SECRET_ACCESS_KEY=password
SPARK_MINIO_ENDPOINT_URL=http://minio:9000
```
The framework handles SSL detection and S3A protocol optimization automatically.
