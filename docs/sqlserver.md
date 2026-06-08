# SQL Server Connector

The `SQLServerConnector` is a robust implementation for interacting with Microsoft SQL Server using the `pyodbc` driver. It supports various output formats and memory-efficient streaming.

## Configuration

The connector can be initialized with a dictionary of settings or it will automatically load them from environment variables via `SQLServerVariables`.

### Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `SQLSERVER_HOST` | Hostname or IP address of the server | - |
| `SQLSERVER_PORT` | Port number (usually 1433) | `1433` |
| `SQLSERVER_DATABASE` | Name of the database | - |
| `SQLSERVER_USERNAME` | Database username | - |
| `SQLSERVER_PASSWORD` | Database password | - |
| `SQLSERVER_DRIVER` | ODBC Driver name | `ODBC Driver 18 for SQL Server` |

## Usage

### Basic Initialization

```python
from src_connectors import SQLServerConnector

# Using environment variables
connector = SQLServerConnector()

# Using a dictionary
config = {
    "SQLSERVER_HOST": "localhost",
    "SQLSERVER_DATABASE": "master",
    # ... other settings
}
connector = SQLServerConnector(config=config)
```

### Executing Queries

The `execute_query` method is the primary interface for data retrieval.

```python
# Fetch as a list of dictionaries (default)
results = connector.execute_query("SELECT * FROM users")

# Fetch as a Pandas DataFrame
df = connector.execute_query("SELECT * FROM users", output_type="dataframe")

# Fetch as a NumPy array (list of lists)
array = connector.execute_query("SELECT * FROM users", output_type="array")
```

### Memory-Safe Streaming

For large datasets, use the `stream=True` parameter to return a generator that yields batches of data.

```python
batches = connector.execute_query(
    "SELECT * FROM large_table",
    stream=True,
    batch_size=5000,
    output_type="list"
)

for batch in batches:
    for row in batch:
        process(row)
```

### Connection Management

Always remember to close the connection when finished:

```python
connector.close()
```
