"""Unit test is designed to test postgres source."""

import unittest

from dags.integration.source.postgres import PGSource


class TestPGSource(unittest.TestCase):
    """
    Test source file
    """

    def setUp(self):
        self.pg_source = PGSource("dev")

    def test_should_exist(self):
        """
        Test method exist
        """
        self.assertTrue(self.pg_source.exist("dim_date", "dim"))

    def test_should_read(self):
        """
        Test method read
        """
        result = self.pg_source.read("SELECT 1")
        self.assertEqual(result.values[0], 1)

    def test_should_read_with_sql_file(self):
        """
        Test method read with sql file
        """
        result = self.pg_source.read("/opt/airflow/include/mock/test.sql")
        self.assertEqual(result.values[0], 2)


if __name__ == "__main__":
    unittest.main()
