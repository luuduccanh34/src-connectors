from abc import ABC, abstractmethod
from typing import List, Dict

class SparkBaseComponent(ABC):
    """
    Abstract base class for Spark components.

    This class defines the interface for components that require
    specific Spark configurations and external packages.
    """

    @abstractmethod
    def get_spark_config(self) -> Dict:
        """
        Return Spark configuration settings.

        Returns:
            dict: A dictionary of Spark configuration keys and values.
        """
        pass

    @abstractmethod
    def get_required_spark_packages(self) -> List:
        """
        Return a list of required Spark packages.

        Returns:
            list: A list of Maven coordinates or package names.
        """
        pass