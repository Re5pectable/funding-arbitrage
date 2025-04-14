from dataclasses import dataclass
from decimal import Decimal

from httpx import AsyncClient, TransportError

from ..utils import retry, errors


@dataclass
class FundingRate:
    symbol: str
    fundingRate: Decimal
    lastPrice: Decimal


BASE_URL = "https://futures.kraken.com/derivatives"


@retry(catch_exceptions=(TransportError,))
async def get_funding_rate():
    """
    https://docs.kraken.com/api/docs/futures-api/trading/get-tickers
    """

    endpoint = BASE_URL + "/api/v3/tickers"
    async with AsyncClient() as c:
        params = {"contractType": "futures_vanilla"}
        response = await c.get(endpoint, params=params)
        if response.status_code != 200:
            raise errors.ExchangeAPICallException(response.text)
        return [
            FundingRate(
                symbol=row["symbol"],
                lastPrice=row.get("last"),
                fundingRate=row.get("fundingRate"),
            )
            for row in response.json()["tickers"] if not row.get("suspended")
        ]
