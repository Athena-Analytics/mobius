"""Module is basement for Destination."""
from abc import ABC, abstractmethod


class BaseDestination(ABC):
    """
    Define some abstract methods that have
    [
        write
        copy_write
    ]
    """
    @abstractmethod
    def write(self, *args, **kwargs):
        """Implement to define how the connector insert data to the destination"""

    @abstractmethod
    def copy_write(self, *args, **kwargs):
        """Implement to define how the connector insert data to the destination"""
