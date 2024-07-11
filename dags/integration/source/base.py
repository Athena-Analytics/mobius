"""Module is basement for Source."""
from abc import ABC, abstractmethod


class BaseSource(ABC):
    """
    Define some abstract methods that have
    [
        read
        exist
    ]
    """
    @abstractmethod
    def read(self, *args, **kwargs):
        """Implement to define how the connector read data to the source"""

    @abstractmethod
    def exist(self, *args, **kwargs):
        """Implement to define how the connector check if a table exists"""
