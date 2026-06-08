# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-08

### Added
- **Unified Connector API**: Established the `BaseConnector` interface to standardize `connect()`, `close()`, and `execute_query()` across all data sources.
- **Spark Connector**: Professional-grade implementation with a modular configuration system for **Apache Iceberg** and **JDBC**.
- **Iceberg REST Support**: Full integration with REST catalogs, including OAuth2 authentication, token refresh, and S3A/MinIO storage compatibility.
- **SQL Server Connector**: Enhanced `pyodbc` implementation with specialized fixes for macOS connection pooling issues and robust connection string building.
- **High-Performance Streaming**: Introduced a native `stream=True` mode in `execute_query` utilizing Python Generators and Spark's `toLocalIterator` for memory-safe data processing.
- **Layered Configuration Engine**: Built a 4-layer priority system (Env Defaults -> Initialization Kwargs -> Component Config -> Connect Overwrites) powered by Pydantic V2.
- **Documentation Suite**: Created a unified documentation structure in `docs/` covering index overview, SQL Server, and Spark/Iceberg deep dives.
- **Developer Examples**: Added a comprehensive set of examples in `examples/src_spark/` demonstrating reading, streaming, and dynamic configuration overwrites.

### Changed
- **Framework Identity**: Rebranded the library and repository to `src-connectors` with updated metadata in `pyproject.toml`.
- **Modernized README**: Completely redesigned the `README.md` to professional enterprise standards, including a visual architecture flow and roadmap.
