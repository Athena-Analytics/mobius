"""Unit test is designed to test file destination."""

import unittest

import pandas as pd

from dags.integration.destination.file import FileDestination


class TestFileDestination(unittest.TestCase):
    """
    Test destination file
    """

    def setUp(self):
        self.file_destination = FileDestination()

    def test_combine_path_and_file_with_sub_path(self):
        """
        Test method combine_path_and_file with sub_path
        """
        file = self.file_destination._combine_path_and_file("dim_date.csv", "file")
        self.assertEqual(file, "/opt/airflow/include/file/dim_date.csv")

    def test_combine_path_and_file_without_sub_path(self):
        """
        Test method combine_path_and_file without sub_path
        """
        file = self.file_destination._combine_path_and_file("test.log")
        self.assertEqual(file, "/opt/airflow/include/test.log")

    def test_should_write(self):
        """
        Test method write
        """
        result = self.file_destination.write("test fs connection\n", "test.log", "mock")
        self.assertEqual(result, 1)

    def test_should_write_csv(self):
        """
        Test method write_csv
        """
        df = pd.DataFrame({"a": [1], "b": ["test"]})
        result = self.file_destination.write_csv(df, "test.csv", "w", "mock")
        self.assertEqual(result, 1)

    def test_should_write_json(self):
        """
        Test method write_json
        """
        df = pd.DataFrame({"a": [1], "b": ["test"]})
        result = self.file_destination.write_json(df, "test.json", "w", "mock")
        self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()
