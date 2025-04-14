from httpx import AsyncClient, TransportError

from ..utils import errors

BASE_URL = "https://www.okx.com/"

# не нашел способа достать bulk

async def get_funding_rate(instrument_id: str):
    """
    https://www.okx.com/docs-v5/en/#public-data-rest-api-get-funding-rate
    """
    endpoint = BASE_URL + "/api/v5/public/funding-rate"
    params = {"instId": instrument_id}
    async with AsyncClient() as c:
        response = await c.get(endpoint, params=params)
        if response.status_code != 200:
            raise errors.ExchangeAPICallException(response.text)
        return response.json()
