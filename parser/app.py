from .interfaces import okx, bybit, bitget, binance
from pprint import pprint

async def main():
    for name, cor in (
        ("bitget", bitget.get_funding_rate()),
        ("bybit", bybit.get_funding_rate()),
        ("binance", binance.get_funding_rate()),
    ):
        response = await cor
        print('\n' + name)
        pprint(response[:2])
