from httpx import AsyncClient, Proxy, TransportError

from ..utils import retry


class UnsuccessfulRequestException(Exception):
    pass


class BaseExchange:

    proxy: Proxy
    base_endpoint: str
    
    def __init__(self, base_endpoint: str, proxy: Proxy = None):
        self.base_endpoint = base_endpoint
        self.proxy = proxy

    @retry(catch=(TransportError,))
    async def _call(self, method: str, uri: str, params=None):
        async with AsyncClient(proxy=self.proxy) as c:
            endpoint = self.base_endpoint + uri
            response = await c.request(method, endpoint, params=params)
            if response.status_code != 200:
                raise UnsuccessfulRequestException(response.text)
            return response
