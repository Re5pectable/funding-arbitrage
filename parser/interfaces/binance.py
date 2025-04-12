from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from httpx import AsyncClient, TransportError

from ..utils import retry

FBASE_URL = "https://fapi.binance.com"


@dataclass
class FundingRate:
    symbol: str
    markPrice: Decimal
    indexPrice: Decimal
    estimatedSettlePrice: Decimal
    lastFundingRate: Decimal
    interestRate: Decimal
    nextFundingTime: datetime
    time: datetime


@retry(catch_exceptions=(TransportError,))
async def get_funding_rate() -> list[FundingRate]:
    endpoint = FBASE_URL +  "/fapi/v1/premiumIndex"
    async with AsyncClient() as c:
        response = await c.get(endpoint)
        if response.status_code != 200:
            raise ValueError(response.text)
        [
            FundingRate(
                symbol=row["symbol"],
                markPrice=Decimal(row["markPrice"]),
                indexPrice=Decimal(row["indexPrice"]),
                estimatedSettlePrice=Decimal(row["estimatedSettlePrice"]),
                lastFundingRate=Decimal(row["lastFundingRate"]),
                interestRate=Decimal(row["interestRate"]),
                nextFundingTime=datetime.fromtimestamp(row["nextFundingTime"] / 1000),
                time=datetime.fromtimestamp(row["time"] / 1000),
            )
            for row in response.json()
        ]