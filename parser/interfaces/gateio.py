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


BASE_URL = "https://api.gateio.ws"

@retry(catch_exceptions=(TransportError,))
async def get_funding_rate():
    """
    https://www.gate.io/docs/developers/apiv4/#list-all-futures-contracts
    """
    endpoint = BASE_URL + "/api/v4/futures/usdt/contracts"
    async with AsyncClient() as c:
        response = await c.get(endpoint)
        if response.status_code != 200:
            raise ValueError(response.text)
        return [
            FundingRate(
                symbol=row["name"],
                fundingRate=Decimal(row["funding_rate"]),
                lastPrice=Decimal(row["last_price"]),
                nextFundingTime=datetime.fromtimestamp(row["funding_next_apply"]),
            )
            for row in response.json()
        ]
