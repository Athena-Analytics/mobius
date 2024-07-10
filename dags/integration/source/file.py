"""Module is Source of Local File System."""
import logging

import pandas as pd

from integration.source.base import BaseSource

logger = logging.getLogger(__name__)


class FileSource(BaseSource):
    """
    Define how to read data from local system
    """
    def __init__(self):
        from airflow.hooks.filesystem import FSHook
        fs_hook = FSHook()
        self._path = fs_hook.get_path()
        logger.info("current path is %s", self._path)

    @staticmethod
    def _check_file_ext(file_name: str, ext: list) -> bool:
        """
        Check if the extension of file is valid
        """
        import os
        file_ext = os.path.splitext(file_name)[1]
        logger.info("file_ext is %s", file_ext)
        return file_ext in ext

    def _combine_path_and_file(self, file_name: str, sub_path: str|None = None) -> str:
        """
        Combine path and file name
        """
        if sub_path.endswith("/"):
            raise ValueError(f"sub_path must not endwith '/', but got {sub_path}")
        if sub_path is None:
            return f"{self._path}/{file_name}"
        logger.info("sub_path is %s", sub_path)
        return f"{self._path}/{sub_path}/{file_name}"

    def exist(self, file_name: str, sub_path: str|None = None) -> bool:
        """
        Define if file e exists in local file system
        """
        import os
        return os.path.exists(self._combine_path_and_file(file_name, sub_path))

    def read(self, file_name: str, sub_path: str|None = None):
        """
        Define how to fetch data from local file using contextlib
        """
        try:
            with open(self._combine_path_and_file(file_name, sub_path), "rb") as f:
                for line in f.readlines():
                    yield line
        except Exception as e:
            raise e

    def read_csv(self, file_name: str, sub_path: str|None = None) -> pd.DataFrame:
        """
        Define how to fetch data from local csv file using read_csv
        """
        try:
            if not self.exist(file_name, "file"):
                raise ValueError(f"file_name must be existent, but got {file_name}")

            if not self._check_file_ext(file_name, [".csv", ".txt"]):
                raise TypeError("file ext must be csv or txt")

            return pd.read_csv(self._combine_path_and_file(file_name, sub_path))
        except Exception as e:
            raise e

    def read_json(self, file_name: str, sub_path: str|None = None) -> pd.DataFrame:
        """
        Define how to fetch data from local csv file using read_csv
        """
        try:
            if not self.exist(file_name, "file"):
                raise ValueError(f"file_name must be existent, but got {file_name}")

            if not self._check_file_ext(file_name, [".json"]):
                raise TypeError("file ext must be json")

            return pd.read_json(self._combine_path_and_file(file_name, sub_path))
        except Exception as e:
            raise e
