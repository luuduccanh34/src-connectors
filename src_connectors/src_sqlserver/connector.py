import platform
import pyodbc
import pandas as pd
from typing import Any, Optional, List, Union, Generator, Dict, Callable
from src_connectors.src_base.connector import BaseConnector

# Disable connection pooling on macOS to avoid memory corruption / segmentation fault from unixODBC
if platform.system() == "Darwin":
    # Ref: https://github.com/mkleehammer/pyodbc/issues/325
    pyodbc.pooling = False


class SQLServerConnector(BaseConnector):
    """
    A robust SQL Server database connector using pyodbc.

    This class provides a high-level API to connect to SQL Server, execute queries,
    and retrieve results in various formats (list of dicts, list of lists, or Pandas DataFrame).
    It also supports streaming large result sets in batches to manage memory efficiently.
    """

    def __init__(self, config: Optional[Union[Dict[str, Any], Any]] = None):
        """
        Initialize the SQL Server connector.

        Args:
            config: Configuration dictionary or object. If a list is provided, the first element is used.
                   If None, settings are loaded from environment variables.
        """
        # Support both single config and list of configs for compatibility
        actual_config = config
        if isinstance(config, list) and len(config) > 0:
            actual_config = config[0]

        if actual_config is None:
            from variables.sqlserver import SQLServerVariables
            actual_config = SQLServerVariables.config()

        self.config = actual_config
        self.connection_string = self._build_connection_string(actual_config)
        self.connection: Optional[pyodbc.Connection] = None

    def _build_connection_string(self, config: Any) -> str:
        """
        Builds an ODBC connection string from various configuration formats.
        """
        if isinstance(config, dict):
            driver = str(config.get("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")).strip("'\"")
            host = str(config.get("SQLSERVER_HOST", "")).strip("'\"")
            port = str(config.get("SQLSERVER_PORT", "1433")).strip("'\"")
            database = str(config.get("SQLSERVER_DATABASE", "")).strip("'\"")
            username = str(config.get("SQLSERVER_USERNAME", "")).strip("'\"")
            password = str(config.get("SQLSERVER_PASSWORD", "")).strip("'\"")

            driver_str = f"DRIVER={driver};" if not (driver.startswith("{") and driver.endswith("}")) else f"DRIVER={driver};"

            return (
                f"{driver_str}"
                f"SERVER={host},{port};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=yes;"
            )

        if hasattr(config, "get_connection_string"):
            return config.get_connection_string()

        return getattr(config, "connection_string", str(config))

    def connect(self) -> pyodbc.Connection:
        """
        Establish a connection to the SQL Server database.

        Returns:
            pyodbc.Connection: The active database connection.
        """
        if not self.connection:
            self.connection = pyodbc.connect(self.connection_string)
        return self.connection

    def close(self) -> None:
        """Close the active database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        stream: bool = False,
        batch_size: int = 1000,
        output_type: str = "list",
        **kwargs: Any
    ) -> Union[List[Any], pd.DataFrame, Generator[Union[List[Any], pd.DataFrame], None, None]]:
        """
        Execute a SQL query and return results in the requested format.

        Args:
            query: SQL query string.
            params: Parameters for query binding.
            stream: Whether to stream results using a generator.
            batch_size: Number of rows per batch when streaming.
            output_type: Output format ('list', 'array', or 'dataframe').
            **kwargs: Additional keyword arguments.

        Returns:
            Query results in the specified format or a generator yielding batches.

        Raises:
            ValueError: If the output_type is unsupported.
        """
        supported_types = ["list", "array", "dataframe"]
        if output_type not in supported_types:
            raise ValueError(
                f"Output type '{output_type}' is not supported. "
                f"Supported types: {', '.join(supported_types)}."
            )

        conn = self.connect()
        cursor = conn.cursor()

        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if not cursor.description:
                conn.commit()
                cursor.close()
                return pd.DataFrame() if output_type == "dataframe" else []

            columns = [column[0] for column in cursor.description]

            def _format_batch(rows: List[pyodbc.Row]) -> Union[List[Any], pd.DataFrame]:
                if output_type == "dataframe":
                    return pd.DataFrame([list(row) for row in rows], columns=columns)
                if output_type == "array":
                    return [list(row) for row in rows]
                return [dict(zip(columns, row)) for row in rows]

            if stream:
                return self._stream_results(cursor, batch_size, _format_batch)

            rows = cursor.fetchall()
            result = _format_batch(rows)
            cursor.close()
            return result

        except Exception:
            cursor.close()
            raise

    def _stream_results(self, cursor: pyodbc.Cursor, batch_size: int, formatter: Callable) -> Generator:
        """Helper generator to stream results in batches."""
        try:
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield formatter(rows)
        finally:
            cursor.close()

