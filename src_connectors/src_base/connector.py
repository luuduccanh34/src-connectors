from abc import ABC, abstractmethod
from typing import Any, Optional

class BaseConnector(ABC):
    """
    Abstract base class for database connectors.
    """

    @abstractmethod
    def connect(self) -> Any:
        """Establish a connection to the database."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""
        pass

    @abstractmethod
    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        stream: bool = False,
        batch_size: int = 1000,
        output_type: str = "list",
        **kwargs: Any
    ) -> Any:
        """Execute a query and return the results."""
        pass

