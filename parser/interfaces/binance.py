from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from httpx import AsyncClient, TransportError

from ..utils import retry, errors, OrderBook, Order


@dataclass
class FundingRate:
    symbol: str
    lastPrice: Decimal
    indexPrice: Decimal
    estimatedSettlePrice: Decimal
    fundingRate: Decimal
    nextFundingTime: datetime


@retry(catch=(TransportError,))
async def _request(method: str, uri: str, params=None):
    endpoint = "https://fapi.binance.com" + uri
    async with AsyncClient() as c:
        response = await c.request(method, endpoint, params=params)
        if response.status_code != 200:
            raise errors.ExchangeAPICallException(response.text)
        return response


async def get_funding_rate() -> list[FundingRate]:
    """
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-Info
    """
    response = await _request("GET", "/fapi/v1/premiumIndex")
    return [
        FundingRate(
            symbol=row["symbol"],
            lastPrice=Decimal(row["markPrice"]),
            indexPrice=Decimal(row["indexPrice"]),
            estimatedSettlePrice=Decimal(row["estimatedSettlePrice"]),
            fundingRate=Decimal(row["lastFundingRate"]),
            nextFundingTime=datetime.fromtimestamp(row["nextFundingTime"] / 1000),
        )
        for row in response.json() if float(row["estimatedSettlePrice"])
    ]


async def get_orderbook(symbol: str):
    """
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book
    """
    response = await _request("GET", "/fapi/v1/depth", {"symbol": symbol, "limit": 100})
    data = response.json()
    return OrderBook(
        bids=[Order(Decimal(price), Decimal(size)) for price, size in data["bids"]],
        asks=[Order(Decimal(price), Decimal(size)) for price, size in data["asks"]],
    )
