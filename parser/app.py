from dataclasses import asdict
from pprint import pprint

import pandas as pd

from . import settings  # noqa: F401
from .interfaces import binance, bitget, bybit, gateio, okx
from .test_data import test_data
from .utils import find_diff, fix_decimals_in_symbols


async def _gather_binance():
    # data = await binance.get_funding_rate()
    data = test_data.BINANCE_FUNDRATE
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"]
    return df[["id", "lastPrice", "fundingRate", "nextFundingTime", "symbol"]]


async def _gather_bybit():
    # data = await bybit.get_funding_rate()
    data = test_data.BYBIT_FUNDRATE
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"]
    return df


async def _gather_gateio():
    # data = await gateio.get_funding_rate()
    data = test_data.GATEIO_FUNDRATE
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"].str[:-5] + df["symbol"].str[-4:]
    return df


async def main():
    df = pd.DataFrame()
    columns = ["id", "exchange", "fundingRate", "lastPrice", "nextFundingTime", "symbol"]
    
    for exchange_name, func in [
        ("binance", _gather_binance),
        ("bybit", _gather_bybit),
        ("gateio", _gather_gateio),
    ]:
        data = await func()
        data["exchange"] = exchange_name
        df = pd.concat((df, data), axis=0)
    
    df = df.reset_index()
    df = df[columns]
    
    r = find_diff(df, max_hours_diff=2)
    pprint(r)
