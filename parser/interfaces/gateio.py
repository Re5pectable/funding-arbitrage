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


BASE_URL = "https://api.gateio.ws"


@retry(catch=(TransportError,))
async def get_funding_rate():
    """
    https://www.gate.io/docs/developers/apiv4/#list-all-futures-contracts
    """
    endpoint = BASE_URL + "/api/v4/futures/usdt/contracts"
    async with AsyncClient() as c:
        response = await c.get(endpoint)
        if response.status_code != 200:
            raise errors.ExchangeAPICallException(response.text)
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
    return {
        "id": 123456,
        "current": 1623898993.123,
        "update": 1623898993.121,
        "asks": [{"p": "1.52", "s": 100}, {"p": "1.53", "s": 40}],
        "bids": [{"p": "1.17", "s": 150}, {"p": "1.16", "s": 203}],
    }
