"""Module is basement for Source."""
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseSource(ABC):
    """
    Define some abstract methods that have
    [
        read
        exist
    ]
    """
    @abstractmethod
    def read(self, sql: str):
        """Implement to define how the connector read data to the source"""

    @abstractmethod
    def exist(self, table_name: str, table_schema: str = None):
        """Implement to define how the connector check if table exists"""
