from abc import ABC, abstractmethod
from typing import Dict, List, Any


class SparkBaseComponent(ABC):
    """
    Abstract base class for all Spark configuration components and adapters.

    This class defines a standardized interface for collecting Spark runtime
    configurations, remote Maven packages, and local JAR file paths across
    different connectors.
    """

    @abstractmethod
    def get_spark_config(self) -> Dict[str, Any]:
        """
        Generates a dictionary of Spark runtime configuration properties.

        Returns:
            Dict[str, Any]: Key-value pairs for 'spark.*' configuration settings.
        """
        pass

    @abstractmethod
    def get_required_spark_packages(self) -> List[str]:
        """
        Returns a list of required remote Maven package coordinates (group:artifact:version).

        Returns:
            List[str]: Cleaned Maven dependency strings to be downloaded via Ivy/Maven.
        """
        pass

    @abstractmethod
    def get_required_local_jars(self) -> List[str]:
        """
        Returns a list of local JAR file paths available on the system filesystem.

        Returns:
            List[str]: Absolute or resolved local paths to .jar files.
        """
        pass
