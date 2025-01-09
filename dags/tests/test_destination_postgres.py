"""Unit test is designed to test postgres source."""

import unittest

import pandas as pd

from dags.integration.destination.postgres import PGDestination


class TestPGDestination(unittest.TestCase):
    """
    Test source file
    """

    def setUp(self):
        self.pg_destination = PGDestination("dev")

    def test_should_fix_columns(self):
        """
        Test method _fix_columns
        """
        df = pd.DataFrame({"a": [1], "b": ["test"]})
        result = self.pg_destination._fix_columns(df, {"a": "aa", "b": "bb"})
        self.assertEqual(result.columns[0], "aa")
        self.assertEqual(result.columns[1], "bb")

    def test_should_write(self):
        """
        Test method write
        """
        df = pd.DataFrame({"a": [1], "b": ["test"]})
        result = self.pg_destination.write(df, "temp_table", "public")
        self.assertEqual(result, 1)

    def test_should_copy_write(self):
        """
        Test method copy_write
        """
        df = pd.DataFrame({"a": [1], "b": ["test"]})
        result = self.pg_destination.copy_write(df, "temp_table", "public")
        self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()
