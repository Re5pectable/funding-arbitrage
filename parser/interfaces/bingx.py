from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from httpx import AsyncClient, TransportError

from ..utils import retry, errors

@dataclass
class FundingRate:
    symbol: str
    fundingRate: Decimal
    nextFundingTime: datetime
    lastPrice: Decimal


BASE_URL = "https://open-api.bingx.com"

@retry(catch_exceptions=(TransportError,))
async def get_funding_rate():
    """
    https://bingx-api.github.io/docs/#/en-us/swapV2/market-api.html#Get%20Funding%20Rate%20History
    """

    endpoint = BASE_URL + "/openApi/swap/v2/quote/premiumIndex"
    async with AsyncClient() as c:
        response = await c.get(endpoint)
        if response.status_code != 200:
            raise errors.ExchangeAPICallException(response.text)
        return [
            FundingRate(
                symbol=row["symbol"],
                fundingRate=Decimal(row["lastFundingRate"]),
                lastPrice=Decimal(row["markPrice"]),
                nextFundingTime=datetime.fromtimestamp(row["nextFundingTime"] / 1000)
            )
            for row in response.json()["data"]
        ]
    