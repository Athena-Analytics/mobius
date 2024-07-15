"""Module is Source of FMP."""

import json
import logging

from airflow.models import Variable

logger = logging.getLogger(__name__)


class FMPSource:
    """
    Define how to fetch data from FMP
    """

    def __init__(self):
        self._base_url = "https://financialmodelingprep.com/api/v3"
        self._api_key = Variable.get("FMP_API_KEY")

    def holidays_of_stock(self, exchange: str = "NASDAQ"):
        """
        Fetch stock holidays
        """
        try:
            import requests

            url = f"{self._base_url}/is-the-market-open"
            params = {"exchange": exchange, "apikey": self._api_key}

            logger.info("begin fetching holidays from the %s", url)

            r = requests.get(url=url, params=params, timeout=60)
            holidays = r.json()

            logger.info("stop fetching holidays from the %s", url)

            if r.status_code != 200:
                raise requests.RequestException(
                    f"request fail, status code is {r.status_code}, response is {r.json()}"
                )

            if len(holidays) == 0:
                raise ValueError(
                    "holidays must have data, please check if exchange is valid"
                )

            return r.json()
        except Exception as e:
            raise e

    def historical_price_full_of_stock(
        self, symbol: str, from_date: str, to_date: str
    ) -> json:
        """
        Fetch historical stock data
        """
        try:
            import requests

            url = f"{self._base_url}/historical-price-full/{symbol}"
            params = {"apikey": self._api_key, "from": from_date, "to": to_date}

            logger.info("begin fetching data from the %s", url)

            r = requests.get(url=url, params=params, timeout=60)
            historical_price_full = r.json()

            logger.info("stop fetching data from the %s", url)

            if r.status_code != 200:
                raise requests.RequestException(
                    f"request fail, status code is {r.status_code}, response is {r.json()}"
                )

            if len(historical_price_full) == 0:
                raise ValueError(
                    "historical_price_full must have data, please check if date in params is valid"
                )

            return r.json()
        except Exception as e:
            raise e
