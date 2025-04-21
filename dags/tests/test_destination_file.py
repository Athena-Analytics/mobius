"""Unit test is designed to test file destination."""

import pandas as pd
import pytest

from dags.integration.destination.file import FileDestination


@pytest.fixture()
def resource():
    """
    Setup and Teardown for test
    """
    file_destination = FileDestination()

    yield file_destination


def test_combine_path_and_file_with_sub_path(resource):
    """
    Test method combine_path_and_file with sub_path
    """
    file = resource._combine_path_and_file("dim_date.csv", "file")
    assert file == "/opt/airflow/include/file/dim_date.csv"


def test_combine_path_and_file_without_sub_path(resource):
    """
    Test method combine_path_and_file without sub_path
    """
    file = resource._combine_path_and_file("test.log")
    assert file == "/opt/airflow/include/test.log"


def test_should_write(resource):
    """
    Test method write
    """
    result = resource.write("test fs connection\n", "test.log", "w", "mock")
    assert result == 1


def test_should_write_csv(resource):
    """
    Test method write_csv
    """
    df = pd.DataFrame({"a": [1], "b": ["test"]})
    result = resource.write_csv(df, "test.csv", "w", "mock")
    assert result == 1


def test_should_write_json(resource):
    """
    Test method write_json
    """
    df = pd.DataFrame({"a": [1], "b": ["test"]})
    result = resource.write_json(df, "test.json", "w", "mock")
    assert result == 1
