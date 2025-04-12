from .interfaces import okx, bybit
from pprint import pprint

async def main():
    pprint(await bybit.get_funding_rate())
