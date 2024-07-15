"""Dag fetches stock data from FMP every Monday."""

import json
import logging

import pandas as pd
import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier

from integration.destination.postgres import PGDestination
from integration.source.file import FileSource
from integration.source.fmp import FMPSource
from integration.source.postgres import PGSource
from stock.slack_blocks import dag_failure_slack_blocks
from utils.common_utils import get_task_date

logger = logging.getLogger(__name__)

dag_failure_slack_webhook_notification = SlackWebhookNotifier(
    slack_webhook_conn_id="slack_webhook_mobius",
    text="The dag {{ dag.dag_id }} failed",
    blocks=dag_failure_slack_blocks,
)

FMP_STOCK_PRICE = Dataset("postgres://localhost:5432/postgres/stock/fmp_stock_price")


@dag(
    "fmp_stock",
    default_args={"depends_on_past": True},
    description="fetch stock data from fmp api.",
    start_date=pendulum.datetime(2024, 7, 15),
    schedule="0 0 * * 1-5",
    catchup=True,
    tags=["stock"],
    params={
        "env": Param(
            "prod", type="string", title="Select one Value.", enum=["prod", "dev"]
        ),
        "sync_mode": Param(
            "Incremental Append",
            type="string",
            title="Select on Value.",
            enum=["Incremental Append", "Full Refresh Append"],
        ),
    },
    on_failure_callback=[dag_failure_slack_webhook_notification],
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
            return "is_holiday"
        if sync_mode == "Full Refresh Append":
            return "historic_task_group.extract_history"
        raise AirflowException(
            "sync_mode only support Incremental Append and Full Refresh Append"
        )

    @task.branch()
    def is_holiday(**kwargs) -> str:
        start_date = kwargs["data_interval_start"]
        params = kwargs["params"]
        env = params["env"]

        pg_source = PGSource(env)
        sql = f"SELECT holiday_date::text FROM stock.stock_holidays WHERE holiday_year = {start_date.year};"
        holiday_date_df = pg_source.read(sql)

        logger.info("holiday is %s", holiday_date_df["holiday_date"].values)

        if start_date.to_date_string() in holiday_date_df["holiday_date"].values:
            return None
        return "current_task_group.get_task_date"

    @task()
    def extract_history(file_name: str) -> pd.DataFrame:
        file_source = FileSource()
        return file_source.read_csv(file_name, "file")

    @task()
    def load_raw(json_data: json, table_name: str, **kwargs) -> int:
        params = kwargs["params"]
        env = params["env"]

        try:
            if json_data is None:
                raise TypeError("json_data is None")

            if not table_name.startswith("_raw"):
                raise ValueError(
                    f"table_name must start with _raw, but got {table_name}"
                )

            pg_destination = PGDestination(env)
            json_str = json.dumps(json_data)
            df = pd.DataFrame.from_dict({"metadata": [json_str]})
            raw_id = pg_destination.write(df, table_name, "stock", None)
            return raw_id
        except Exception as e:
            raise AirflowException(f"unknown error: {e}") from e

    @task()
    def extract(symbol: str, from_date: str, to_date: str) -> json:
        fmp_source = FMPSource()
        stock_price_weekly = fmp_source.historical_price_full_of_stock(
            symbol, from_date, to_date
        )
        return stock_price_weekly

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
            raise AirflowException(f"unknown error: {e}") from e

    @task(outlets=[FMP_STOCK_PRICE])
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
            raise AirflowException(f"unknown error: {e}") from e

    branch_sync_op = branch_sync()
    is_holiday_op = is_holiday()

    @task_group()
    def current_task_group():
        task_date = get_task_date(0, 1)
        metadata = extract("TQQQ", task_date["start_date"], task_date["end_date"])
        raw_id = load_raw(metadata, "_raw_fmp_stock_price")
        current_df = transform(metadata, raw_id)
        load(current_df, "fmp_stock_price")

    @task_group()
    def historic_task_group():
        historic_df = extract_history("fmp_historic_data.csv")
        load(historic_df, "fmp_stock_price")

    current_task = current_task_group()
    historic_task = historic_task_group()

    branch_sync_op >> [is_holiday_op, historic_task]
    is_holiday_op >> current_task


dag_object = fmp_stock()


if __name__ == "__main__":
    conf = {"env": "dev", "sync_mode": "Incremental Append"}
    dag_object.test(execution_date=pendulum.datetime(2024, 7, 2), run_conf=conf)
