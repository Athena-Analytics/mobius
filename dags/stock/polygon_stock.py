"""Dag fetches stock data from Polygon every Monday."""
import json
import logging

import pendulum
import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.hooks.filesystem import FSHook

from utils.common_utils import get_task_date
from integration.source.polygon import aggregates_bars_of_stock
from integration.destination.postgres import PGDestination

logger = logging.getLogger(__name__)


@dag(
    "polygon_stock",
    default_args={
        "depends_on_past": False
    },
    description="fetch stock data from polygon api.",
    start_date=pendulum.datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["stock"],
    params={
        "env": Param(
            "prod",
            type="string",
            title="Select one Value.",
            enum=["prod", "dev"]
        ),
        "sync_mode": Param(
            "Incremental Append",
            type="string",
            title="Select on Value.",
            enum=["Incremental Append", "Full Refresh Append"]
        )
    }
)
def polygon_stock():
    """
    ## Build a table with data of stocks that has columns
    - id
    - ticker
    - c
    - h
    - l
    - n
    - o
    - t
    - v
    - vm
    - raw_id
    - create_time
    - update_time
    > [Polygon API Documentation](https://polygon.io/docs/stocks/getting-started)
    """
    @task.branch()
    def branch_sync(**kwargs) -> str:
        params = kwargs["params"]
        sync_mode = params["sync_mode"]

        if sync_mode == "Incremental Append":
            return "current_task_group.get_task_date"
        elif sync_mode == "Full Refresh Append":
            return "historic_task_group.extract_history"
        else:
            raise AirflowException("sync_mode only support Incremental Append and Full Refresh Append")

    @task()
    def extract_history(file: str) -> pd.DataFrame:
        fs_hook = FSHook()
        path = fs_hook.get_path()
        return pd.read_csv(f"{path}/{file}")

    @task()
    def transform_history(df: pd.DataFrame) -> pd.DataFrame:
        try:
            if df is None:
                raise TypeError("df is None")

            df["t"] = df["t"].apply(lambda x: pendulum.parse(x).int_timestamp * 1000)
            return df
        except Exception as e:
            raise AirflowException(f"unknown error: {e}") from e

    @task()
    def load_raw(json_data: json, table_name: str, **kwargs) -> int:
        params = kwargs["params"]
        env = params["env"]

        import re

        try:
            if json_data is None:
                raise TypeError("json_data is None")

            pattern = re.compile("_raw")
            if pattern.search(table_name):
                pg_destination = PGDestination(env)
                json_str = json.dumps(json_data)
                df = pd.DataFrame.from_dict({"metadata": [json_str]})
                raw_id = pg_destination.write(df, table_name, "stock")
                return raw_id
            else:
                raise ValueError(f"table_name must have _raw, but got {table_name}")
        except Exception as e:
            raise AirflowException(f"unknown error: {e}") from e

    @task()
    def extract(from_date: str, to_date: str) -> json:
        aggregates_bars = aggregates_bars_of_stock("TQQQ", 1, "day", from_date, to_date)
        return aggregates_bars

    @task()
    def transform(json_data: json, raw_id: int) -> pd.DataFrame:
        try:
            if json_data is None:
                raise TypeError("json_data is None")

            if not isinstance(raw_id, int):
                raise TypeError("raw_id must be int")

            if raw_id < 1:
                raise ValueError(f"raw_id must be bigger than 1, but got {raw_id}")

            df = pd.DataFrame(json_data["results"])

            df["ticker"] = json_data["ticker"]
            df["raw_id"] = raw_id

            return df
        except Exception as e:
            raise AirflowException(f"unknown error: {e}") from e

    @task()
    def load(data, table_name: str, **kwargs) -> int:
        params = kwargs["params"]
        env = params["env"]

        try:
            if data is None:
                raise TypeError("data is None")

            pg_destination = PGDestination(env)
            result = pg_destination.copy_write(data, table_name, "stock", None)

            return result
        except Exception as e:
            raise AirflowException(f"unknown error: {e}") from e

    branch_sync_op = branch_sync()

    @task_group()
    def current_task_group():
        task_date = get_task_date(0, 1)
        metadata = extract(task_date["start_date"], task_date["end_date"])
        raw_id = load_raw(metadata, "_raw_polygon_stock_aggregates_bars")
        load(transform(metadata, raw_id), "polygon_stock_aggregates_bars")

    @task_group()
    def historic_task_group():
        historic_data = extract_history("polygon_tqqq_historic_data.csv")
        load(transform_history(historic_data), "polygon_stock_aggregates_bars")

    current_task = current_task_group()
    historic_task = historic_task_group()

    branch_sync_op >> [current_task, historic_task]


dag_object = polygon_stock()


if __name__ == "__main__":
    dag_object.test()
