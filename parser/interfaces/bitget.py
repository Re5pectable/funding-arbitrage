from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime

from httpx import AsyncClient, TransportError

from ..utils import retry, errors

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
            raise errors.ExchangeAPICallException(response.text)
        return [
            FundingRate(
                symbol=row["symbol"],
                fundingRate=Decimal(row["fundingRate"]),
                lastPrice=Decimal(row["lastPr"]),
            )
            for row in response.json()["data"]
        ]

async def get_next_funding_time(symbol: str):
    """
    https://www.bitget.com/api-doc/contract/market/Get-Symbol-Next-Funding-Time
    """
    params = {"productType": "USDT-FUTURES", "symbol": symbol}
    endpoint = BASE_URL + "/api/v2/mix/market/funding-time"
    async with AsyncClient() as c:
        response = await c.get(endpoint, params=params)
        if response.status_code != 200:
            raise errors.ExchangeAPICallException(response.text)
        nft = response.json()["data"][0]["nextFundingTime"]
        return datetime.fromtimestamp(int(nft) / 1000)
        
    