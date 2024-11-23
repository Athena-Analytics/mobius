"""Module is Source of Local File System."""

import logging
import os

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.hooks.filesystem import FSHook
from integration.source.base import BaseSource

logger = logging.getLogger(__name__)


class FileSource(BaseSource):
    """
    Define how to read data from local system
    """

    def __init__(self):
        fs_hook = FSHook()
        self._path = fs_hook.get_path()
        logger.info("current path is %s", self._path)

    @staticmethod
    def _check_file_ext(file_name: str, ext: list) -> bool:
        """
        Check if the extension of file is valid
        """
        file_ext = os.path.splitext(file_name)[1]
        logger.info("file_ext is %s", file_ext)
        return file_ext in ext

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
        return os.path.exists(self._combine_path_and_file(file_name, sub_path))

    def read(self, file_name: str, sub_path: str | None = None):
        """
        Fetch data using contextlib
        """
        try:
            assert self.exist(file_name, sub_path)

            with open(
                self._combine_path_and_file(file_name, sub_path), "r", encoding="utf-8"
            ) as f:
                for line in f.readlines():
                    yield line
        except AirflowException as e:
            logger.error(e)

    def read_csv(self, file_name: str, sub_path: str | None = None) -> pd.DataFrame:
        """
        Fetch data using read_csv
        """
        try:
            if not self._check_file_ext(file_name, [".csv", ".txt"]):
                raise TypeError("file ext must be csv or txt")

            return pd.read_csv(
                self._combine_path_and_file(file_name, sub_path), float_precision="high"
            )
        except AirflowException as e:
            logger.error(e)

    def read_json(self, file_name: str, sub_path: str | None = None) -> pd.DataFrame:
        """
        Fetch data using read_json
        """
        try:
            if not self._check_file_ext(file_name, [".json"]):
                raise TypeError("file ext must be json")

            return pd.read_json(
                self._combine_path_and_file(file_name, sub_path),
                orient="records",
                precise_float=True,
            )
        except AirflowException as e:
            logger.error(e)
