
"""Dag get option price base on stock price."""
import json
import logging
from decimal import Decimal

import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier

from stock.slack_blocks import dag_failure_slack_blocks
from utils.common_utils import get_task_date
from integration.source.fmp import historical_price_full_of_stock
from integration.source.postgres import PGSource
from integration.destination.postgres import PGDestination

logger = logging.getLogger(__name__)

dag_failure_slack_webhook_notification = SlackWebhookNotifier(
    slack_webhook_conn_id="slack_webhook_mobius",
    text="The dag {{ dag.dag_id }} failed",
    blocks=dag_failure_slack_blocks
)


@dag(
    "option_price",
    default_args={
        "depends_on_past": True
    },
    description="get option price base on stock price.",
    start_date=pendulum.datetime(2024, 7, 1),
    schedule="0 0 * * 1-5",
    catchup=True,
    tags=["stock"],
    params={
        "env": Param(
            "prod", 
            type="string",
            title="Select one Value.",
            enum=["prod", "dev"]
        )
    },
    on_failure_callback=[dag_failure_slack_webhook_notification]
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
    @task.branch()
    def is_first_trade_date(symbol: str, **kwargs) -> str:

        start_date = kwargs["data_interval_start"]
        params = kwargs["params"]
        env = params["env"]

        pg_source = PGSource(env)
        df = pg_source.read(f"SELECT * FROM stock.option_price WHERE symbol = '{symbol}' ORDER BY id DESC LIMIT 1")
        if len(df) == 0:
            raise ValueError(f"{symbol} must have data, but got None")

        latest_trade_date = pendulum.parse(str(df["date"].values[0]))

        old_week_start = latest_trade_date.start_of("week")
        week_start = start_date.start_of("week")

        logger.info("old_week_start: %s, week_start: %s", old_week_start, week_start)

        if old_week_start == week_start:
            return None

        return "get_task_date"

    @task()
    def extract(symbol: str, from_date: str, to_date: str) -> json:
        stock_price_daily = historical_price_full_of_stock(symbol, from_date, to_date)
        return stock_price_daily

    @task()
    def transform(json_data: json) -> pd.DataFrame:

        def get_price_detail(price: float) -> str:
            decimal_price = Decimal(str(price))
            return json.dumps({
                "long_price_weekly": str(decimal_price * Decimal('1.1')),
                "short_price_weekly": str(decimal_price * Decimal('0.9')),
                "long_price_monthly": str(decimal_price * Decimal('1.3')),
                "short_price_monthly": str(decimal_price * Decimal('0.7')),
            })

        try:
            if json_data is None:
                raise TypeError("json_data is None")

            df = pd.DataFrame(json_data["historical"])
            df.loc[:, "symbol"] = json_data["symbol"]
            df["price_detail"] = df["open"].apply(get_price_detail)

            return df[["symbol", "date", "open", "high", "low", "close", "price_detail"]]
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
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        price_detail = json.loads(df["price_detail"].values[0])
        long_price_weekly = price_detail["long_price_weekly"]
        short_price_weekly = price_detail["short_price_weekly"]
        long_price_monthly = price_detail["long_price_monthly"]
        short_price_monthly = price_detail["short_price_monthly"]

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Option Price Detail",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*long_price_weekly:* {long_price_weekly}\n*short_price_weekly:* {short_price_weekly}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*long_price_monthly:* {long_price_monthly}\n*short_price_monthly:* {short_price_monthly}"
                    }
                ]
            }
        ]
        slack_webhook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook_mobius")
        slack_webhook.client.send(text="option price detail", blocks=blocks)


    stock_symbol = "TQQQ"
    branch_op = is_first_trade_date(stock_symbol)

    task_date = get_task_date(0, 1)
    stock_price_daily = extract(stock_symbol, task_date["start_date"], task_date["end_date"])
    stock_price_daily_df = transform(stock_price_daily)
    load(stock_price_daily_df, "option_price")
    send_to_slack(stock_price_daily_df)

    branch_op >> task_date


dag_object = option_price()


if __name__ == "__main__":
    conf = {"env": "dev"}
    dag_object.test(run_conf=conf)
