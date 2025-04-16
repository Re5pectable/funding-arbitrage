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

@dataclass
class OrderBook: 
    symbol: str
    bestBidPrice: Decimal
    bestBidQuote: Decimal
    bestAskPrice: Decimal
    bestAskQuote: Decimal


BASE_URL = "https://open-api.bingx.com"

@retry(catch_exceptions=(TransportError,))
async def get_funding_rate():
    """
    https://bingx-api.github.io/docs/#/en-us/swapV2/market-api.html#Get%20Funding%20Rate%20History
    """

    endpoint = BASE_URL + "/openApi/swap/v2/quote/premiumIndex"
    async with AsyncClient() as c:
        response = await c.get(endpoint)
        if response.status_code != 200:
            raise ValueError(response.text)
        return [
            FundingRate(
                symbol=row["symbol"],
                fundingRate=Decimal(row["lastFundingRate"]),
                lastPrice=Decimal(row["markPrice"]),
                nextFundingTime=datetime.fromtimestamp(row["nextFundingTime"] / 1000)
            )
            for row in response.json()["data"]
        ]
    
@retry(catch_exceptions=(TransportError,))
async def get_orderbook(symbol: str, limit: 5):
    """
    https://bingx-api.github.io/docs/#/en-us/swapV2/market-api.html#Order%20Book
    """
    
    endpoint = BASE_URL + "/openApi/swap/v2/quote/depth"
    params = { "symbol": symbol, "limit": limit }
    async with AsyncClient() as client:
        response = await client.get(endpoint, params=params)
        if response.status_code != 200:
            raise ValueError(f"Error fetching orderbook: {response.text}")
        data = response.json()
        if data.get("code") != 0:
            raise ValueError(f"API error: {data.get('msg')}")
        ob_data = data["data"]
        best_bid = ob_data["bids"][0]
        best_ask = ob_data["asks"][0]
        return OrderBook(
            symbol=symbol,
            bestBidPrice=Decimal(best_bid[0]),
            bestBidQuote=Decimal(best_bid[0])*Decimal(best_bid[1]),
            bestAskPrice=Decimal(best_ask[0]),
            bestAskQuote=Decimal(best_ask[0])*Decimal(best_ask[1])
        )

    