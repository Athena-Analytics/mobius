"""Module is basement for Destination."""

from abc import ABC, abstractmethod


class BaseDestination(ABC):
    """
    Define some abstract methods that have
    [
        write
        exist
    ]
    """

    @abstractmethod
    def write(self, *args, **kwargs):
        """Implement to define how the connector writes data to the destination"""
