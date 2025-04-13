from dataclasses import asdict

import pandas as pd
pd.set_option("display.max_rows", None)
from .interfaces import binance, bitget, bybit, gateio, okx, bingx
from .utils import fix_decimals_in_symbols, find_diff


async def _gather_binance():
    data = await binance.get_funding_rate()
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"]
    return df[["id", "lastPrice", "fundingRate", "nextFundingTime", "symbol"]]

async def _gather_bingx():
    data = await bingx.get_funding_rate()
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"].str[:-5] + df["symbol"].str[-4:]
    return df

async def _gather_bybit():
    data = await bybit.get_funding_rate()
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"]
    return df


async def _gather_gateio():
    data = await gateio.get_funding_rate()
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
        ("bingx", _gather_bingx)
    ]:
        data = await func()
        data["exchange"] = exchange_name
        df = pd.concat((df, data), axis=0)
    
    df = df.reset_index()
    df = df[columns]
    
    print(find_diff(df))
