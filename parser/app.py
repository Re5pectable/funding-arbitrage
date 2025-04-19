import asyncio
from dataclasses import asdict
from decimal import Decimal
from random import uniform

import pandas as pd

from . import settings  # noqa: F401
from .interfaces import (  # noqa: F401
    binance,
    bitget,
    bybit,
    gateio,
    kucoin,
    okx,
    bingx
)
from .test_data import test_data
from .utils import find_diff, fix_decimals_in_symbols, avg_orderbook_price, calculate_avg_price


async def _gather_binance(mock=False):
    data = await binance.get_funding_rate() if not mock else test_data.BINANCE_FUNDRATE
    df = pd.DataFrame([asdict(row) for row in data])
    df["exchange_symbol"] = df["symbol"]
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"]
    return df[["id", "lastPrice", "fundingRate", "nextFundingTime", "exchange_symbol"]]


async def _gather_bingx(mock=False):
    data = await bingx.get_funding_rate()
    df = pd.DataFrame([asdict(row) for row in data])
    df["exchange_symbol"] = df["symbol"]
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"].str[:-5] + df["symbol"].str[-4:]
    return df


async def _gather_bybit(mock=False):
    data = await bybit.get_funding_rate() if not mock else test_data.BYBIT_FUNDRATE
    df = pd.DataFrame([asdict(row) for row in data])
    df["exchange_symbol"] = df["symbol"]
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"]
    return df


async def _gather_gateio(mock=False):
    data = await gateio.get_funding_rate() if not mock else test_data.GATEIO_FUNDRATE
    df = pd.DataFrame([asdict(row) for row in data])
    df["exchange_symbol"] = df["symbol"]
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"].str[:-5] + df["symbol"].str[-4:]
    return df


async def _gather_kucoin(mock=False):
    data = await kucoin.get_funding_rate() if not mock else test_data.GATEIO_FUNDRATE
    df = pd.DataFrame([asdict(row) for row in data])
    df["exchange_symbol"] = df["symbol"]
    df = df.apply(fix_decimals_in_symbols, axis=1)
    df["id"] = df["symbol"].str[:-1]
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
        "exchange_symbol"
    ]

    gather_tasks = [
        ("binance", _gather_binance),
        ("bybit", _gather_bybit),
        ("gateio", _gather_gateio),
        ("kucoin", _gather_kucoin),
        ("bingx", _gather_bingx)
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
    gap = r.loc[(r["price_diff"] > 1.01) | (r["price_diff"] < 0.99)]
    print(gap)

    indexes = gap[["index_a", "index_b"]].values.ravel()

    orderbooks = {
        row["index"]: {
            "exchange": row["exchange"],
            "exchange_symbol": row["exchange_symbol"],
            "id": row["id"]
        }
        for row in df[df.index.isin(indexes)][["id", "exchange_symbol", "index", "exchange"]].to_dict(
            "records"
        )
    }
    exch_map = {
        "binance": binance,
        "bybit": bybit,
        "gateio": gateio,
        "kucoin": kucoin,
        "bingx": bingx
    }

    async def task(key):
        await asyncio.sleep(uniform(0,4))
        exchange = orderbooks[key]["exchange"]
        symbol = orderbooks[key]["exchange_symbol"]
        try:
            orderbooks[key]["orderbook"] = await exch_map[exchange].get_orderbook(symbol)
            print(key, exchange, symbol, "success")
        except Exception as e:
            print(key, exchange, symbol, "failed", e)

    await asyncio.gather(*[task(key) for key in orderbooks.keys()])
    
    print(gap.apply(lambda row: calculate_avg_price(row, orderbooks), axis=1))

    # orderbooks_df = pd.DataFrame(orderbooks).T
    # print(orderbooks_df)
    # gap['optimal_order_size'] = gap.apply(lambda row: calculate_arbitrage_order_size(row, orderbooks_df), axis=1)
    # print("gap", gap)


