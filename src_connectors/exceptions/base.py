class ConnectorBaseError(Exception):
    """Base exception class for all errors raised by src_connectors."""
    pass


class ConfigurationError(ConnectorBaseError):
    """Raised when there is an invalid configuration parameter."""
    pass
