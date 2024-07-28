"""Dag fetches stock data from FMP every Monday."""

import json
import logging

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier

from integration.destination.postgres import PGDestination
from integration.source.fmp import FMPSource
from stock.slack_blocks import dag_failure_slack_blocks

logger = logging.getLogger(__name__)

dag_failure_slack_webhook_notification = SlackWebhookNotifier(
    slack_webhook_conn_id="slack_webhook_mobius",
    text="The dag {{ dag.dag_id }} failed",
    blocks=dag_failure_slack_blocks,
)


@dag(
    "stock_holidays",
    default_args={"depends_on_past": True},
    description="fetch stock holidays from fmp api.",
    start_date=pendulum.today(),
    schedule=None,
    catchup=True,
    tags=["stock"],
    params={
        "env": Param(
            "prod", type="string", title="Select one Value.", enum=["prod", "dev"]
        )
    },
    on_failure_callback=[dag_failure_slack_webhook_notification],
)
def stock_holidays():
    """
    ## Build a table with holidays of stock that has columns
    - id
    - exchange
    - holiday_year
    - holiday_name
    - holiday_date
    - create_time
    - update_time
    > [FMP API Documentation](https://site.financialmodelingprep.com/developer/docs)
    """

    @task()
    def extract(exchange: str) -> json:
        fmp_source = FMPSource()
        holidays = fmp_source.holidays_of_stock(exchange)
        return holidays

    @task()
    def transform(json_data: json) -> pd.DataFrame:

        def get_holidays_detail(stock_market_holidays: dict) -> pd.DataFrame:
            holiday_name = []
            holiday_date = []
            for key, value in stock_market_holidays.items():
                if key == "year":
                    continue
                holiday_name.append(key)
                holiday_date.append(value)
            df = pd.DataFrame(
                {"holiday_name": holiday_name, "holiday_date": holiday_date}
            )
            df.loc[:, "holiday_year"] = stock_market_holidays["year"]
            return df.sort_values(by=["holiday_date"])

        try:
            if json_data is None:
                raise TypeError("json_data is None")

            holidays_df = pd.concat(
                (get_holidays_detail(i) for i in json_data["stockMarketHolidays"])
            )

            holidays_df.loc[:, "exchange"] = json_data["stockExchangeName"]

            return holidays_df
        except Exception as e:
            raise AirflowException(f"unknown error: {e}") from e

    @task()
    def load(df: pd.DataFrame, table_name: str, **kwargs) -> int:
        params = kwargs["params"]
        env = params["env"]

        try:
            if df is None:
                raise TypeError("df is None")

            pg_destination = PGDestination(env)
            result = pg_destination.copy_write(df, table_name, "stock")

            return result
        except Exception as e:
            raise AirflowException(f"unknown error: {e}") from e

    holidays_detail = extract("NASDAQ")
    holidays_detail_df = transform(holidays_detail)
    load(holidays_detail_df, "stock_holidays")


dag_object = stock_holidays()


if __name__ == "__main__":
    conf = {"env": "dev"}
    dag_object.test(run_conf=conf)
