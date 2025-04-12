from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from httpx import AsyncClient, TransportError

from ..utils import retry


@dataclass
class FundingRate:
    symbol: str
    fundingRate: Decimal
    nextFundingTime: datetime
    lastPrice: Decimal


BASE_URL = "https://api.bybit.com"


@retry(catch_exceptions=(TransportError,))
async def get_funding_rate():
    """
    https://bybit-exchange.github.io/docs/v5/market/tickers
    """

    endpoint = BASE_URL + "/v5/market/tickers"
    params = {"category": "linear"}
    async with AsyncClient() as c:
        response = await c.get(endpoint, params=params)
        if response.status_code != 200:
            raise ValueError(response.text)
        return [
            FundingRate(
                symbol=row["symbol"],
                fundingRate=Decimal(row["fundingRate"]),
                nextFundingTime=datetime.fromtimestamp(
                    int(row["nextFundingTime"]) / 1000
                ),
                lastPrice=Decimal(row["lastPrice"]),
            )
            for row in response.json()["result"]["list"]
            if row["fundingRate"]
        ]
