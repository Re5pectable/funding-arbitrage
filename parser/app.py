from .interfaces import okx, bybit, bitget, binance, gateio
from pprint import pprint


async def main():
    for name, cor in (
        ("bitget", bitget.get_funding_rate()),
        ("bybit", bybit.get_funding_rate()),
        ("binance", binance.get_funding_rate()),
        ("gateio", gateio.get_funding_rate()),
    ):
        response = await cor
        print(name + "\n")
        pprint(response[:2])
        print("-" * 70)
