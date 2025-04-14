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


@dataclass
class Order:
    price: Decimal
    size: Decimal


@dataclass
class OrderBook:
    bids: list[Order]
    asks: list[Order]


@retry(catch=(TransportError,))
async def _request(method: str, uri: str, params=None):
    endpoint = "https://api.gateio.ws" + uri
    async with AsyncClient() as c:
        response = await c.request(method, endpoint, params=params)
        if response.status_code != 200:
            raise errors.ExchangeAPICallException(response.text)
        return response


async def get_funding_rate():
    """
    https://www.gate.io/docs/developers/apiv4/#list-all-futures-contracts
    """
    response = await _request("GET", "/api/v4/futures/usdt/contracts")
    return [
        FundingRate(
            symbol=row["name"],
            fundingRate=Decimal(row["funding_rate"]),
            lastPrice=Decimal(row["last_price"]),
            nextFundingTime=datetime.fromtimestamp(row["funding_next_apply"]),
        )
        for row in response.json()
    ]


async def get_orderbook(symbol: str):
    """
    https://www.gate.io/docs/developers/apiv4/#futures-order-book
    """
    response = await _request(
        "GET", "/api/v4/futures/usdt/order_book", params={"contract": symbol}
    )
    data = response.json()
    return OrderBook(
        bids=[Order(Decimal(row["p"]), Decimal(row["s"])) for row in data["bids"]],
        asks=[Order(Decimal(row["p"]), Decimal(row["s"])) for row in data["asks"]],
    )
