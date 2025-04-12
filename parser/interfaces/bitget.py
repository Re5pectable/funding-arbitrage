from dataclasses import dataclass
from decimal import Decimal

from httpx import AsyncClient, TransportError

from ..utils import retry

BASE_URL = "https://api.bitget.com"


@dataclass
class FundingRate:
    symbol: str
    fundingRate: Decimal
    lastPrice: Decimal


@retry(catch_exceptions=(TransportError,))
async def get_funding_rate():
    """
    https://www.bitget.com/api-doc/contract/market/Get-All-Symbol-Ticker
    """

    endpoint = BASE_URL + "/api/v2/mix/market/tickers"
    params = {"productType": "USDT-FUTURES"}
    async with AsyncClient() as c:
        response = await c.get(endpoint, params=params)
        if response.status_code != 200:
            raise ValueError(response.text)
        return [
            FundingRate(
                symbol=row["symbol"],
                fundingRate=Decimal(row["fundingRate"]),
                lastPrice=Decimal(row["lastPr"]),
            )
            for row in response.json()["data"]
        ]
