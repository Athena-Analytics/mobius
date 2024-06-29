"""Module is basement for Destination"""
import logging
from abc import ABC, abstractmethod

from pandas import DataFrame

logger = logging.getLogger(__name__)


class BaseDestination(ABC):
    """
    Define some abstact methods that have
    [
        write
        copy_write
    ]
    """
    @abstractmethod
    def write(self, df: DataFrame, table_name: str, table_schema: str = None, cols_mapping: dict = None):
        """Implement to define how the connector insert data to the destination"""

    @abstractmethod
    def copy_write(self, df: DataFrame, table_name: str, table_schema: str = None, cols_mapping: dict = None):
        """Implement to define how the connector insert data to the destination"""
