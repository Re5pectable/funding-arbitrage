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


BASE_URL = "https://open-api.bingx.com"

@retry(catch_exceptions=(TransportError,))
async def get_funding_rate():
    """
    https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex
    """

    endpoint = BASE_URL + "/openApi/swap/v2/quote/premiumIndex"
    async with AsyncClient() as c:
        response = await c.get(endpoint)
        if response.status_code != 200:
            raise ValueError(response.text)
        return print(response)
