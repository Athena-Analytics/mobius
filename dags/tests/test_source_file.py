"""Unit test is designed to test file source."""

import pytest

from dags.integration.source.file import FileSource


@pytest.fixture()
def resource():
    """
    Setup and Teardown for test
    """
    file_source = FileSource()

    yield file_source


def test_check_file_ext(resource):
    """
    Test method check_file_ext
    """
    assert resource._check_file_ext("test.log", [".log"])


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


def test_should_exist(resource):
    """
    Test method exist
    """
    assert resource.exist("dim_date.sql", "sql/ddl")


def test_should_read(resource):
    """
    Test method read
    """
    result = resource.read("test.log", "mock")
    assert list(result)[0] == "test fs connection\n"


def test_should_read_csv(resource):
    """
    Test method read_csv
    """
    result = resource.read_csv("test.csv", "mock")
    assert result.columns[0] == "a"
    assert result.columns[1] == "b"
    assert result["a"].values[0] == 1
    assert result["b"].values[0] == "test"


def test_should_read_json(resource):
    """
    Test method read_json
    """
    result = resource.read_json("test.json", "mock")
    assert result.columns[0] == "a"
    assert result.columns[1] == "b"
    assert result["a"].values[0] == 1
    assert result["b"].values[0] == "test"
