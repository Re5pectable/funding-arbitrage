from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from time import time

from httpx import AsyncClient, TransportError

from ..utils import retry, errors


@dataclass
class FundingRate:
    symbol: str
    fundingRate: Decimal
    nextFundingTime: datetime
    lastPrice: Decimal


BASE_URL = "https://api-futures.kucoin.com"


@retry(catch_exceptions=(TransportError,))
async def get_funding_rate():
    """
    https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-all-symbols
    """

    endpoint = BASE_URL + "/api/v1/contracts/active"
    async with AsyncClient() as c:
        now = time()
        response = await c.get(endpoint)
        if response.status_code != 200:
            raise errors.ExchangeAPICallException(response.text)
        return [
            FundingRate(
                symbol=row["baseCurrency"] + row["quoteCurrency"],
                lastPrice=Decimal(str(row["markPrice"])),
                fundingRate=Decimal(str(row["fundingFeeRate"])),
                nextFundingTime=datetime.fromtimestamp((row["nextFundingRateTime"] / 1000 + now)),
            )
            for row in response.json()["data"]
            if row["fundingFeeRate"]
        ]
