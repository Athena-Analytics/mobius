"""Dag get option price base on stock price."""

import json
import logging
from decimal import Decimal

import pandas as pd
import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier

from integration.destination.postgres import PGDestination
from integration.source.postgres import PGSource
from stock.slack_blocks import dag_failure_slack_blocks

logger = logging.getLogger(__name__)

dag_failure_slack_webhook_notification = SlackWebhookNotifier(
    slack_webhook_conn_id="slack_webhook_mobius",
    text="The dag {{ dag.dag_id }} failed",
    blocks=dag_failure_slack_blocks,
)

FMP_STOCK_PRICE = Dataset("postgres://localhost:5432/postgres/stock/fmp_stock_price")


@dag(
    "option_price",
    default_args={"depends_on_past": True, "wait_for_downstream": True},
    description="get option price base on stock price.",
    start_date=pendulum.today(),
    schedule=[FMP_STOCK_PRICE],
    catchup=False,
    tags=["stock"],
    params={
        "env": Param(
            "prod", type="string", title="Select one Value.", enum=["prod", "dev"]
        )
    },
    on_failure_callback=[dag_failure_slack_webhook_notification],
)
def option_price():
    """
    ## Build a table for option which has columns
    - id
    - symbol
    - date
    - open
    - high
    - low
    - close
    - price_detail
    - create_time
    - update_time
    > [FMP API Documentation](https://site.financialmodelingprep.com/developer/docs)
    """

    @task()
    def get_stock_price(symbol: str, **kwargs) -> pd.DataFrame:
        params = kwargs["params"]
        env = params["env"]
        start_date = kwargs["data_interval_start"]
        week_start = start_date.start_of("week").to_date_string()

        pg_source = PGSource(env)
        df = pg_source.read(
            "/opt/airflow/include/sql/is_first_trade_date.sql",
            {"symbol": symbol, "week_start": week_start},
        )

        if len(df) == 0:
            raise ValueError(f"{symbol} must have data, but got None")

        return df

    @task.branch()
    def is_first_trade_date(df: pd.DataFrame) -> str | None:

        if df["is_exists"].values[0] == 1:
            return None

        return "transform"

    @task()
    def transform(df: pd.DataFrame) -> pd.DataFrame:

        def get_price_detail(price: float) -> str:
            decimal_price = Decimal(str(price))
            return json.dumps(
                {
                    "long_price_weekly": str(decimal_price * Decimal("1.1")),
                    "short_price_weekly": str(decimal_price * Decimal("0.9")),
                    "long_price_monthly": str(decimal_price * Decimal("1.3")),
                    "short_price_monthly": str(decimal_price * Decimal("0.7")),
                }
            )

        try:
            if df is None:
                raise TypeError("df is None")

            df["option_price_detail"] = df["open"].apply(get_price_detail)

            return df[
                [
                    "symbol",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "option_price_detail",
                ]
            ]
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
            result = pg_destination.write(df, table_name, "stock")

            return result
        except Exception as e:
            raise AirflowException(f"unknown error: {e}") from e

    @task()
    def send_to_slack(df: pd.DataFrame):
        from airflow.providers.slack.hooks.slack import SlackHook

        trade_date = df["date"].values[0]
        option_price_detail = json.loads(df["option_price_detail"].values[0])
        long_price_weekly = option_price_detail["long_price_weekly"]
        short_price_weekly = option_price_detail["short_price_weekly"]
        long_price_monthly = option_price_detail["long_price_monthly"]
        short_price_monthly = option_price_detail["short_price_monthly"]

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Option Price Detail",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*trade_date:* {trade_date}"},
                    {
                        "type": "mrkdwn",
                        "text": f"*long_price_weekly:* {long_price_weekly}\n*short_price_weekly:* {short_price_weekly}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*long_price_monthly:* {long_price_monthly}\n*short_price_monthly:* {short_price_monthly}",
                    },
                ],
            },
        ]
        slack_hook = SlackHook(slack_conn_id="slack_api_mobius")
        slack_hook.client.chat_postMessage(
            text="send option price detail",
            channel="#option-price-alert",
            blocks=blocks,
        )

    stock_price_df = get_stock_price("TQQQ")

    branch_op = is_first_trade_date(stock_price_df)

    option_price_df = transform(stock_price_df)
    load(option_price_df, "option_price")
    send_to_slack(option_price_df)

    branch_op >> option_price_df


dag_object = option_price()
