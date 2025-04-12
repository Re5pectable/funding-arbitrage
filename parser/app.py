from .interfaces import okx, bybit, bitget, binance, gateio
from decimal import Decimal
import pandas as pd
from dataclasses import asdict
from .utils import fix_decimals_in_symbols


async def _gather_binance():
    data = await binance.get_funding_rate()
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df.set_index("symbol", inplace=True)
    return df[["lastPrice", "fundingRate", "nextFundingTime"]]


async def _gather_bybit():
    data = await bybit.get_funding_rate()
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    return df.set_index("symbol")


async def _gather_gateio():
    data = await gateio.get_funding_rate()
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["symbol"] = df["symbol"].str[:-5] + df["symbol"].str[-4:]
    return df.set_index("symbol")


async def main():
    df = pd.DataFrame()
    
    for prefix, func in [
        ("binance_", _gather_binance),
        ("bybit_", _gather_bybit),
        ("gateio_", _gather_gateio),
    ]:
        data = await func()
        data = data[sorted(data.columns)].add_prefix(prefix)
        df = pd.concat((df, data), axis=1)

    print(df)
