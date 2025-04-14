from dataclasses import dataclass
from datetime import datetime
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
    endpoint = "https://api.bybit.com" + uri
    async with AsyncClient() as c:
        response = await c.request(method, endpoint, params=params)
        if response.status_code != 200:
            raise errors.ExchangeAPICallException(response.text)
        return response


async def get_funding_rate():
    """
    https://bybit-exchange.github.io/docs/v5/market/tickers
    """
    response = await _request("GET", "/v5/market/tickers", {"category": "linear"})
    return [
        FundingRate(
            symbol=row["symbol"],
            fundingRate=Decimal(row["fundingRate"]),
            nextFundingTime=datetime.fromtimestamp(int(row["nextFundingTime"]) / 1000),
            lastPrice=Decimal(row["lastPrice"]),
        )
        for row in response.json()["result"]["list"]
        if row["fundingRate"]
    ]


async def get_orderbook(symbol: str):
    """
    https://bybit-exchange.github.io/docs/v5/market/orderbook
    """
    response = await _request(
        "GET",
        "/v5/market/orderbook",
        {"category": "linear", "symbol": symbol, "limit": 100},
    )
    data = response.json()["result"]
    return OrderBook(
        bids=[Order(Decimal(price), Decimal(size)) for price, size in data["b"]],
        asks=[Order(Decimal(price), Decimal(size)) for price, size in data["a"]],
    )
