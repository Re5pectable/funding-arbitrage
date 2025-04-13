import asyncio
import time
from dataclasses import asdict
from decimal import Decimal
from pprint import pprint

import pandas as pd

from . import settings  # noqa: F401
from .interfaces import (  # noqa: F401
    binance,
    bitget,
    bybit,
    gateio,
    kraken,
    kucoin,
    okx,
)
from .test_data import test_data
from .utils import find_diff, fix_decimals_in_symbols


async def _gather_binance(mock=False):
    data = await binance.get_funding_rate() if not mock else test_data.BINANCE_FUNDRATE
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

async def _gather_bybit(mock=False):
    data = await bybit.get_funding_rate() if not mock else test_data.BYBIT_FUNDRATE
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"]
    return df


async def _gather_gateio(mock=False):
    data = await gateio.get_funding_rate() if not mock else test_data.GATEIO_FUNDRATE
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"].str[:-5] + df["symbol"].str[-4:]
    return df


async def _gather_kucoin(mock=False):
    data = await kucoin.get_funding_rate() if not mock else test_data.GATEIO_FUNDRATE
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"]
    return df


async def main():
    df = pd.DataFrame()
    columns = [
        "id",
        "index",
        "exchange",
        "fundingRate",
        "lastPrice",
        "nextFundingTime",
        "symbol",
    ]
    
    gather_tasks = [
        ("binance", _gather_binance),
        ("bybit", _gather_bybit),
        ("gateio", _gather_gateio),
        ("kucoin", _gather_kucoin),
    ]

    results = await asyncio.gather(*[func(mock=False) for _, func in gather_tasks])

    df = pd.concat(
        [result.assign(exchange=name) for (name, _), result in zip(gather_tasks, results)],
        axis=0,
        ignore_index=True
    ).reset_index()

    df = df[columns]

    r = find_diff(df, threshold=Decimal("0.001"), max_hours_diff=2)
    print(r)
    print(df)
    indexes = r[["index_a", "index_b"]].values.ravel()
    print(df[df.index.isin(indexes)])
    
    
