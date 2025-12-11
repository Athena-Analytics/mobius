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
        self._base_url = "https://financialmodelingprep.com/stable"
        self._api_key = Variable.get("FMP_API_KEY")

    def holidays_of_stock(
        self,
        exchange: str = "NASDAQ",
        from_date: str = "2025-01-01",
        to_date: str = "2025-12-31",
    ) -> json:
        """
        Fetch stock holidays
        """
        try:
            import requests

            url = f"{self._base_url}/holidays-by-exchange"
            params = {
                "exchange": exchange,
                "apikey": self._api_key,
                "from": from_date,
                "to": to_date,
            }

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

            url = f"{self._base_url}/historical-price-eod/full"
            params = {
                "symbol": symbol,
                "apikey": self._api_key,
                "from": from_date,
                "to": to_date,
            }

            logger.info(
                "begin fetching data from the %s, and params is %s", url, params
            )

            r = requests.get(url=url, params=params, timeout=60)

            if r.status_code != 200:
                raise requests.RequestException(
                    f"request fail, status code is {r.status_code}, response is {r}"
                )

            historical_price_full = r.json()

            logger.info("stop fetching data from the %s, and params is %s", url, params)

            if len(historical_price_full) == 0:
                raise ValueError(
                    "historical_price_full must have data, please check if date in params is valid"
                )

            return historical_price_full
        except Exception as e:
            raise e
