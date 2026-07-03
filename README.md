<p align="center">
  <img src="https://img.icons8.com/dt/200/database-connectivity.png" alt="src-connectors Logo" width="160" />
</p>

<h1 align="center">src-connectors</h1>

<p align="center">
  <strong>The ultimate library for high-speed data connectivity, querying, and reading.</strong>
</p>

<p align="center">
  <a href="#"><img src="https://img.shields.io/badge/version-0.1.0-blue.svg" alt="Version"></a>
  <a href="#"><img src="https://img.shields.io/badge/python-3.11+-green.svg" alt="Python Versions"></a>
  <a href="#"><img src="https://img.shields.io/badge/license-MIT-orange.svg" alt="License"></a>
</p>

---

## 📖 What is src-connectors?

**src-connectors** is a professional-grade Python library designed to simplify the complexity of connecting to, querying, and reading data from multiple sources. It provides a standardized, high-performance interface for data engineers and analysts to pull data into their applications without worrying about underlying driver intricacies or memory management.

Whether you are fetching a small sample for exploration or streaming billions of rows for a production pipeline, **src-connectors** ensures your data access is reliable, secure, and fast.

**Current Support:**
- **SQL Server**: Robust connectivity via `pyodbc` with support for high-speed batched reads.
- **Apache Spark**: Modular engine setup supporting **Iceberg** catalogs (Hadoop/REST/S3) and **JDBC** data sources.
- **Trino**: Fast interactive querying capabilities supporting extraction to DataFrames and lists.

---

## 🚀 Getting Started

### Installation

Install `src-connectors` utilizing `pip` or `Poetry`. Choose your extras based on the platforms you need to query:

```bash
# Using pip for a specific extra
pip install "src-connectors[spark,sqlserver]"

# Using Poetry (Recommended)
poetry add "src-connectors[all]"
```

### Quick Start

1. **Querying SQL Server**: Fetch data directly into a DataFrame.
```python
from src_connectors import SQLServerConnector

connector = SQLServerConnector()
# Reading data into a pandas DataFrame
df = connector.execute_query("SELECT TOP 10 * FROM orders", output_type="dataframe")
```

2. **Reading from Iceberg (Spark Engine)**: Standardized data access for big data.
```python
from src_connectors import SparkConnector

# Initialize and configure the Iceberg catalog
connector = SparkConnector(spark_master="local[*]")
connector.configure_iceberg({"iceberg_warehouse": "prod_catalog"})
# --- Step 4: Execute Query using Spark SQL ---
# Execute a query and fetch results
df = connector.execute_query("SELECT * FROM prod_catalog.db.table", output_type="dataframe")
```

---

## 📚 Documentation

For in-depth architectural overviews, detailed configuration settings, and complex usage examples, please refer to the unified documentation in the `docs/` folder:

- [Index Overview](docs/index.md)
- [SQL Server Connector Guide](docs/sqlserver.md)
- [Apache Spark (Iceberg & JDBC) Guide](docs/spark.md)

---

## 🏗 Architecture & Design

The library is built on a **Modular Connector Pattern**. Every component focuses on a specific data source while sharing a common execution interface. This decoupling allows engineers to inject custom configurations without breaking the core read/query logic.

<p align="center">
  <strong>Execution Logic:</strong><br>
  <code>Connector Initialization</code> → <code>Component Configuration</code> → <code>Unified Connection</code> → <code>Optimized Query Execution</code>
</p>

---

## ✨ Key Features

- **Querying Consistency**: One standard `execute_query()` method across all connectors, supporting SQL and Spark SQL.
- **Optimized Data Reading**: Native support for returning DataFrames (Pandas/Spark), Lists of Dictionaries, or NumPy Arrays.
- **Memory-Safe Batching**: Integrated `stream=True` functionality for reading large datasets through Python Generators to prevent memory overflow.
- **Enterprise Configuration**: Layered settings management allowing for Environment defaults with per-query overwrites.
- **Security-First**: Automatic protection and masking of credentials in all logs and metadata exports.
- **Cloud-Ready Big Data**: Specialized support for Iceberg REST Catalogs, OAuth2, and MinIO/S3 compatible storage.

---

## 🛠 Supported Data Sources

| Connector | Source | Driver/Engine | Role |
| :--- | :--- | :--- | :--- |
| `SQLServerConnector` | SQL Server | `pyodbc` | Query & Read |
| `SparkConnector` | Spark / Iceberg / JDBC | `pyspark` | Query & Big Data Read |
| `TrinoConnector` | Trino | `trino` | Query & Read |
| `OracleConnector` | Oracle DB | `oracledb` | *Coming Soon* |
| `PostgresConnector` | PostgreSQL | `psycopg3` | *Planned* |

---

## 🔮 Roadmap

We are expanding **src-connectors** to become the default data access layer for all modern infrastructures:

- **Federated Query Engines**: Adding Trino and Presto support for cross-catalog querying.
- **Streaming Sinks**: Support for writing queried data into Kafka or RabbitMQ.
- **Advanced Authentication**: Native integration with AWS Secrets Manager, Azure Key Vault, and HashiCorp Vault.
- **Observability**: Built-in OpenTelemetry hooks to track query performance and latency.

---

## 🤝 Contributing & License

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](#).

This project is licensed under the terms of the **MIT** license.
