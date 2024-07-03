"""Module is Source of polygon."""
import json
import logging

from airflow.models import Variable

logger = logging.getLogger(__name__)


def aggregates_bars_of_stock(stocks_ticker: str,
                             multiplier: int,
                             timespan: str,
                             from_date: str,
                             to_date: str) -> json:
    """
    Fetch Stock Data from Polygon
    """
    try:
        import requests

        base_url = "https://api.polygon.io"
        url = f"{base_url}/v2/aggs/ticker/{stocks_ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        api_key = Variable.get("POLYGON_API_KEY")

        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 120,
            "apiKey": api_key
        }

        logger.info("begin fetching data from the %s", url)

        r = requests.get(url=url, params=params, timeout=60)
        aggregates_bars = r.json()

        logger.info("stop fetching data from the %s", url)

        if r.status_code == 200:
            if aggregates_bars["resultsCount"] == 0:
                raise ValueError("stock data is empty, please check if date in params is valid trading date")
            return r.json()
        else:
            raise requests.RequestException(f"request fail, status code is {r.status_code}, response is {r.json()}")
    except Exception as e:
        raise e
