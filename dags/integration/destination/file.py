"""Module is Destination of File."""

import logging
import os

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.hooks.filesystem import FSHook

from integration.destination.base import BaseDestination

logger = logging.getLogger(__name__)


class FileDestination(BaseDestination):
    """
    Define how to write data into PostgreSQL
    """

    def __init__(self):
        fs_hook = FSHook()
        self._path = fs_hook.get_path()
        logger.info("current path is %s", self._path)

    def _combine_path_and_file(
        self, file_name: str, sub_path: str | None = None
    ) -> str:
        """
        Combine path and file name
        """
        if sub_path is None:
            return f"{self._path}/{file_name}"
        if sub_path.endswith("/"):
            raise ValueError(f"sub_path must not endwith '/', but got {sub_path}")
        logger.info("sub_path is %s", sub_path)
        return f"{self._path}/{sub_path}/{file_name}"

    def exist(self, file_name: str, sub_path: str | None = None) -> bool:
        """
        Check if a file exists
        """
        if not os.path.exists(self._combine_path_and_file(file_name, sub_path)):
            raise ValueError(f"file must be existent, but got {file_name}")
        return True

    def write(
        self,
        data: str | list[str],
        file_name: str,
        sub_path: str | None = None,
    ) -> int:
        """
        Write data using contextlib
        """
        try:
            assert self.exist(file_name, sub_path)

            with open(
                self._combine_path_and_file(file_name, sub_path), "w", encoding="utf-8"
            ) as f:
                if isinstance(data, list):
                    for i in data:
                        f.write(i)
                f.write(data)
            return 1
        except AirflowException as e:
            logger.error(e)

    def write_csv(
        self,
        df: pd.DataFrame,
        file_name: str,
        mode: str = "x",
        sub_path: str | None = None,
    ) -> int:
        """
        Write data using to_csv
        """
        try:
            df.to_csv(
                self._combine_path_and_file(file_name, sub_path), index=False, mode=mode
            )
            return 1
        except AirflowException as e:
            logger.error(e)

    def write_json(
        self,
        df: pd.DataFrame,
        file_name: str,
        mode: str = "x",
        sub_path: str | None = None,
    ) -> int:
        """
        Write data using to_json
        """
        try:
            df.to_json(
                self._combine_path_and_file(file_name, sub_path),
                orient="records",
                index=False,
                mode=mode,
            )
            return 1
        except AirflowException as e:
            logger.error(e)
