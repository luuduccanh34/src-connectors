from src_connectors.exceptions.base import ConnectorBaseError


class SparkConnectorError(ConnectorBaseError):
    """Base exception for Spark connector operations."""
    pass


class DependencyConflictError(SparkConnectorError):
    """Raised when version conflicts occur between Local JARs and Maven Packages."""
    pass
