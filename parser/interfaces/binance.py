from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from httpx import AsyncClient, TransportError

from ..utils import retry

FBASE_URL = "https://fapi.binance.com"


@dataclass
class FundingRate:
    symbol: str
    lastPrice: Decimal
    indexPrice: Decimal
    estimatedSettlePrice: Decimal
    fundingRate: Decimal
    nextFundingTime: datetime


@retry(catch_exceptions=(TransportError,))
async def get_funding_rate() -> list[FundingRate]:
    """
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-Info
    """
    endpoint = FBASE_URL + "/fapi/v1/premiumIndex"
    async with AsyncClient() as c:
        response = await c.get(endpoint)
        if response.status_code != 200:
            raise ValueError(response.text)
        return [
            FundingRate(
                symbol=row["symbol"],
                lastPrice=Decimal(row["markPrice"]),
                indexPrice=Decimal(row["indexPrice"]),
                estimatedSettlePrice=Decimal(row["estimatedSettlePrice"]),
                fundingRate=Decimal(row["lastFundingRate"]),
                nextFundingTime=datetime.fromtimestamp(row["nextFundingTime"] / 1000),
            )
            for row in response.json()
        ]


async def get_order_book(symbol: str):
    """
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book
    """
    return {
        "lastUpdateId": 1027024,
        "E": 1589436922972,
        "T": 1589436922959,
        "bids": [["4.00000000", "431.00000000"]],
        "asks": [["4.00000200", "12.00000000"]],
    }
