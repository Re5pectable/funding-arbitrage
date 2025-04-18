from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from httpx import AsyncClient, TransportError

from ..utils import retry, errors, OrderBook, Order


@dataclass
class FundingRate:
    symbol: str
    fundingRate: Decimal
    nextFundingTime: datetime
    lastPrice: Decimal


@retry(catch=(TransportError,))
async def _request(method: str, uri: str, params=None):
    endpoint = "https://api-futures.kucoin.com" + uri
    async with AsyncClient() as c:
        response = await c.request(method, endpoint, params=params)
        if response.status_code != 200:
            raise errors.ExchangeAPICallException(response.text)
        return response


async def get_funding_rate():
    """
    https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-all-symbols
    """
    now = datetime.now(timezone(timedelta(hours=0))).timestamp()
    response = await _request("GET", "/api/v1/contracts/active")
    return [
        FundingRate(
            symbol=row["symbol"],
            lastPrice=Decimal(str(row["markPrice"])),
            fundingRate=Decimal(str(row["fundingFeeRate"])),
            nextFundingTime=datetime.fromtimestamp(
                (row["nextFundingRateTime"] / 1000 + now)
            ),
        )
        for row in response.json()["data"]
        if row["fundingFeeRate"]
    ]


async def get_orderbook(symbol: str):
    """
    https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-part-orderbook
    """
    response = await _request("GET", "/api/v1/level2/depth100", {"symbol": symbol})
    data = response.json()["data"]
    return OrderBook(
        bids=[
            Order(Decimal(str(price)), Decimal(str(size)))
            for price, size in data["bids"]
        ],
        asks=[
            Order(Decimal(str(price)), Decimal(str(size)))
            for price, size in data["asks"]
        ],
    )
