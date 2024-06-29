"""Dag fetches stock data from FMP every Monday."""
import json
import logging

import pendulum
import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.models.param import Param

from utils.common_utils import get_task_date
from integration.source.fmp import historical_price_full_of_stock
from integration.destination.postgres import PGDestination

logger = logging.getLogger(__name__)


@dag(
    "fmp_stock",
    default_args={
        "depends_on_past": True
    },
    description="fetch stock data from fmp api",
    start_date=pendulum.datetime(2024, 1, 1),
    schedule="0 0 * * 1",
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
def fmp_stock():
    """
    ## Build a table with data of stocks that has columns
    - id
    - symbol
    - date
    - open
    - high
    - low
    - close
    - adjust_close
    - volume
    - unadjusted_volume
    - change
    - change_percent
    - vwap
    - label
    - change_over_time
    - raw_id
    - create_time
    - update_time
    > [FMP API Documentation](https://site.financialmodelingprep.com/developer/docs)
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
            raise AirflowException("sync_mode only support Incremental Append and Full Refresh Append.")

    @task()
    def extract_history(file: str) -> pd.DataFrame:
        from airflow.hooks.filesystem import FSHook
        fs_hook = FSHook()
        path = fs_hook.get_path()
        return pd.read_csv(f"{path}/file/{file}")

    @task()
    def load_raw(json_data: json, table_name: str, **kwargs) -> int:
        params = kwargs["params"]
        env = params["env"]

        try:
            if json_data is None:
                raise TypeError("json_data is None")

            if table_name.startswith("_raw"):
                pg_destination = PGDestination(env)
                json_str = json.dumps(json_data)
                df = pd.DataFrame.from_dict({"metadata": [json_str]})
                raw_id = pg_destination.write(df, table_name, "stock", None)
                return raw_id
            else:
                raise ValueError(f"table_name must start with _raw, but got {table_name}")
        except Exception as e:
            raise AirflowException(f"unkown error: {e}") from e

    @task()
    def extract(from_date: str, to_date: str) -> json:
        historical_price_full = historical_price_full_of_stock("TQQQ", from_date, to_date)
        return historical_price_full

    @task()
    def transform(json_data: json, raw_id: int) -> pd.DataFrame:
        try:
            if json_data is None:
                raise TypeError("json_data is None")

            if not isinstance(raw_id, int):
                raise TypeError("raw_id must be int")

            if raw_id < 1:
                raise ValueError(f"raw_id must be bigger than 1, but got {raw_id}")

            df = pd.DataFrame(json_data["historical"]).sort_values(by=["date"])

            df.loc[:, "symbol"] = json_data["symbol"]
            df.loc[:, "raw_id"] = raw_id

            return df
        except Exception as e:
            raise AirflowException(f"unkown error: {e}") from e

    @task()
    def load(df: pd.DataFrame, table_name: str, **kwargs) -> int:
        params = kwargs["params"]
        env = params["env"]

        try:
            if df is None:
                raise TypeError("df is None")

            pg_destination = PGDestination(env)
            cols_mapping = {
                "adjClose": "adjust_close",
                "unadjustedVolume": "unadjusted_volume",
                "changePercent": "change_percent",
                "changeOverTime": "change_over_time",
            }
            result = pg_destination.copy_write(df, table_name, "stock", cols_mapping)

            return result
        except Exception as e:
            raise AirflowException(f"unkown error: {e}") from e

    branch_sync_op = branch_sync()

    @task_group()
    def current_task_group():
        task_date = get_task_date(0, 1)
        metadata = extract(task_date["start_date"], task_date["end_date"])
        raw_id = load_raw(metadata, "_raw_fmp_stock_aggregates_bars")
        current_df = transform(metadata, raw_id)
        load(current_df, "fmp_stock_aggregates_bars")

    @task_group()
    def historic_task_group():
        historic_df = extract_history("fmp_tqqq_historic_data.csv")
        load(historic_df, "fmp_stock_aggregates_bars")

    current_task = current_task_group()
    historic_task = historic_task_group()

    branch_sync_op >> [current_task, historic_task]


dag_object = fmp_stock()


if __name__ == "__main__":
    dag_object.test()
