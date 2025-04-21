"""Unit test is designed to test postgres source."""

import pytest

from dags.integration.source.postgres import PGSource


@pytest.fixture()
def resource():
    """
    Setup and Teardown for test
    """
    pg_source = PGSource("dev")

    yield pg_source


def test_should_exist(resource):
    """
    Test method exist
    """
    assert resource.exist("dim_date", "dim")


def test_should_read(resource):
    """
    Test method read
    """
    result = resource.read("SELECT 1")
    assert result.values[0] == 1


def test_should_read_with_sql_file(resource):
    """
    Test method read with sql file
    """
    result = resource.read("/opt/airflow/include/mock/test.sql")
    assert result.values[0] == 2
