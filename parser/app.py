from .interfaces import okx, bybit, bitget
from pprint import pprint

async def main():
    pprint(await bitget.get_funding_rate())
