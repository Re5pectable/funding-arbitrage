from httpx import AsyncClient, TransportError

BASE_URL = "https://www.okx.com/"

async def get_funding_rate():
    endpoint = BASE_URL + "/api/v5/public/funding-rate"
    async with AsyncClient() as c:
        response = await c.get(endpoint)
        if response.status_code != 200:
            raise ValueError(response.text)
        return response.json()
