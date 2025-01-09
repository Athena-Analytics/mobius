"""Unit test is designed to test file source."""

import unittest

from dags.integration.source.file import FileSource


class TestFileSource(unittest.TestCase):
    """
    Test source file
    """

    def setUp(self):
        self.file_source = FileSource()

    def test_check_file_ext(self):
        """
        Test method check_file_ext
        """
        assert self.file_source._check_file_ext("test.log", [".log"])

    def test_combine_path_and_file_with_sub_path(self):
        """
        Test method combine_path_and_file with sub_path
        """
        file = self.file_source._combine_path_and_file("dim_date.csv", "file")
        self.assertEqual(file, "/opt/airflow/include/file/dim_date.csv")

    def test_combine_path_and_file_without_sub_path(self):
        """
        Test method combine_path_and_file without sub_path
        """
        file = self.file_source._combine_path_and_file("test.log")
        self.assertEqual(file, "/opt/airflow/include/test.log")

    def test_should_exist(self):
        """
        Test method exist
        """
        self.assertTrue(self.file_source.exist("dim_date.sql", "sql/ddl"))

    def test_should_read(self):
        """
        Test method read
        """
        result = self.file_source.read("test.log", "mock")
        self.assertEqual(list(result)[0], "test fs connection\n")

    def test_should_read_csv(self):
        """
        Test method read_csv
        """
        result = self.file_source.read_csv("test.csv", "mock")
        self.assertEqual(result.columns[0], "a")
        self.assertEqual(result.columns[1], "b")
        self.assertEqual(result["a"].values[0], 1)
        self.assertEqual(result["b"].values[0], "test")

    def test_should_read_json(self):
        """
        Test method read_json
        """
        result = self.file_source.read_json("test.json", "mock")
        self.assertEqual(result.columns[0], "a")
        self.assertEqual(result.columns[1], "b")
        self.assertEqual(result["a"].values[0], 1)
        self.assertEqual(result["b"].values[0], "test")


if __name__ == "__main__":
    unittest.main()
