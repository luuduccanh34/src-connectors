import pandas as pd
from typing import Any, Optional, List, Union, Generator, Dict, Callable
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from src_connectors.src_base.connector import BaseConnector

class TrinoConnector(BaseConnector):
    """
    A robust Trino database connector.

    This class provides a high-level API to connect to Trino, execute queries,
    and retrieve results in various formats (list of dicts, list of lists, or Pandas DataFrame).
    It also supports streaming large result sets in batches to manage memory efficiently.
    """

    def __init__(self, config: Optional[Union[Dict[str, Any], Any]] = None):
        """
        Initialize the Trino connector.

        Args:
            config: Configuration dictionary or object. If a list is provided, the first element is used.
                   If None, settings are loaded from environment variables.
        """
        actual_config = config
        if isinstance(config, list) and len(config) > 0:
            actual_config = config[0]

        if actual_config is None:
            from variables.trino import TrinoVariables
            actual_config = TrinoVariables.config()

        self.config = actual_config
        self._parsed_params = self._parse_config(actual_config)
        self.connection = None

    def _parse_config(self, config: Any) -> Dict[str, Any]:
        """
        Parses configuration dictionary to Trino connection parameters.
        """
        if isinstance(config, dict):
            host = str(config.get("TRINO_HOST", "")).strip("'\"")
            port = int(config.get("TRINO_PORT", 8443))
            user = str(config.get("TRINO_USERNAME", "")).strip("'\"")
            password = str(config.get("TRINO_PASSWORD", "")).strip("'\"")

            return {
                "host": host,
                "port": port,
                "user": user,
                "password": password
            }

        # Fallback if config is some object
        return {
            "host": getattr(config, "host", ""),
            "port": getattr(config, "port", 8443),
            "user": getattr(config, "user", ""),
            "password": getattr(config, "password", "")
        }

    def connect(self) -> Any:
        """
        Establish a connection to the Trino database.

        Returns:
            The active database connection.
        """
        if not self.connection:
            params = self._parsed_params
            self.connection = connect(
                host=params["host"],
                port=params["port"],
                user=params["user"],
                auth=BasicAuthentication(params["user"], params["password"]),
                http_scheme="https",
                verify=False,
            )
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
                # trino doesn't support conn.commit(), just return empty
                cursor.close()
                return pd.DataFrame() if output_type == "dataframe" else []

            columns = [column[0] for column in cursor.description]

            def _format_batch(rows: List[List[Any]]) -> Union[List[Any], pd.DataFrame]:
                if output_type == "dataframe":
                    return pd.DataFrame(rows, columns=columns)
                if output_type == "array":
                    return list(rows)
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

    def _stream_results(self, cursor: Any, batch_size: int, formatter: Callable) -> Generator:
        """Helper generator to stream results in batches."""
        try:
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield formatter(rows)
        finally:
            cursor.close()
