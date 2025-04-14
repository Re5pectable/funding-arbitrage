import asyncio
from dataclasses import asdict
from decimal import Decimal

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
    bingx,
)
from .test_data import test_data
from .utils import find_diff, fix_decimals_in_symbols, avg_orderbook_price


async def _gather_binance(mock=False):
    data = await binance.get_funding_rate() if not mock else test_data.BINANCE_FUNDRATE
    df = pd.DataFrame([asdict(row) for row in data])
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"]
    return df[["id", "lastPrice", "fundingRate", "nextFundingTime", "symbol"]]


async def _gather_bingx(mock=False):
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
        # ("bingx", _gather_bingx),
    ]

    results: list[pd.DataFrame] = await asyncio.gather(
        *[func(mock=False) for _, func in gather_tasks]
    )
    dfs = [
        result.assign(exchange=name) for (name, _), result in zip(gather_tasks, results)
    ]
    df = pd.concat(dfs, axis=0, ignore_index=True).reset_index()
    df = df[columns]

    r = find_diff(df)
    print("-" * 100)
    print(r)
    print("-" * 100, '\n')
    gap = r.loc[(r["price_diff"] > 1.01) | (r["price_diff"] < 0.99)]
    print(gap)
    return
    indexes = r[["index_a", "index_b"]].values.ravel()

    orderbooks = {
        row["index"]: {
            "exchange": row["exchange"],
            "symbol": row["symbol"],
        }
        for row in df[df.index.isin(indexes)][["symbol", "index", "exchange"]].to_dict(
            "records"
        )
    }
    exch_map = {
        "binance": binance,
        "bybit": bybit,
        "gateio": gateio,
        "kucoin": kucoin
    }
    for key in orderbooks.keys():
        exchange = orderbooks[key]["exchange"]
        symbol = orderbooks[key]["symbol"]
        orderbooks[key]["orderbook"] = await exch_map[exchange].get_orderbook(symbol)
        
    print(r.columns)
        
    # df["100usd_a"] = 
    
    # df = df.apply()
    # print("BIDS", bids)
    # print("ASKS", asks)
    
    # print(avg_orderbook_price(orderbooks[first_symbol]["orderbook"], 100, "buy"))

    # pprint(orderbooks)
