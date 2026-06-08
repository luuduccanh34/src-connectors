# src-connectors Documentation

Welcome to the documentation for **src-connectors**, a unified high-performance framework for data connectivity, querying, and reading.

## Architecture Overview

The library provides a consistent interface for different data platforms, allowing engineers to transition between sources like SQL Server and Spark SQL seamlessly. 

Every connector implements the `BaseConnector` interface:
- `connect()`: Establishes the connection.
- `close()`: Cleans up resources.
- `execute_query()`: Standard method for data retrieval with support for múltiples output types and streaming.

## Core Concepts

### 1. Unified Execute API
Query data from any source using the same method call. Specify your desired `output_type` (`dataframe`, `list`, or `array`) and let the framework handle the conversion and memory management.

### 2. Layered Configuration
Leverage Pydantic-powered configuration models. The library follows a strict priority flow:
`Connect Overwrites` > `Component Config` > `Initialization Kwargs` > `Environment Variables (.env)`

### 3. Memory-Safe Streaming
Never crash your application due to large result sets. Use the `stream=True` parameter to fetch data in batches using Python generators.

## Detailed Guides

- [SQL Server Connector](sqlserver.md)
- [Apache Spark Connector](spark.md)
