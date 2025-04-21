"""Unit test is designed to test postgres source."""

import pandas as pd
import pytest

from dags.integration.destination.postgres import PGDestination


@pytest.fixture()
def resource():
    """
    Setup and Teardown for test
    """
    pg_destination = PGDestination("dev")
    pg_destination._execute("/opt/airflow/include/mock/temp_table.sql")

    yield pg_destination

    pg_destination._execute("DROP TABLE public.temp_table;")


def test_should_fix_columns(resource):
    """
    Test method _fix_columns
    """
    df = pd.DataFrame({"name": ["bob"], "age": [1]})
    result = resource._fix_columns(df, {"name": "name_1", "age": "age_1"})
    assert result.columns[0] == "name_1"
    assert result.columns[1] == "age_1"


def test_should_write(resource):
    """
    Test method write
    """
    df = pd.DataFrame({"name": ["bob"], "age": [1]})
    result = resource.write(df, "temp_table", "public")
    assert result == 1


def test_should_copy_write(resource):
    """
    Test method copy_write
    """
    df = pd.DataFrame({"name": ["bob"], "age": [1]})
    result = resource.copy_write(df, "temp_table", "public")
    assert result == 1
