"""Module is Source of FMP."""
import json
import logging

from airflow.models import Variable

logger = logging.getLogger(__name__)


def historical_price_full_of_stock(stocks_ticker: str,
                                   from_date: str,
                                   to_date: str) -> json:
    """
    Fetch Stock Data from FMP
    """
    try:
        import requests

        base_url = "https://financialmodelingprep.com/api/v3/historical-price-full"
        url = f"{base_url}/{stocks_ticker}"
        api_key = Variable.get("FMP_API_KEY")

        params = {
            "apikey": api_key,
            "from": from_date,
            "to": to_date
        }

        logger.info("begin fetching data from the %s", url)

        r = requests.get(url=url, params=params, timeout=60)
        historical_price_full = r.json()

        logger.info("stop fetching data from the %s", url)

        if r.status_code == 200:
            if len(historical_price_full) == 0:
                raise ValueError("stock data is empty, please check if date in params is valid trading date")
            return r.json()
        else:
            raise requests.RequestException(f"request fail, status code is {r.status_code}, response is {r.json()}")
    except Exception as e:
        raise e
