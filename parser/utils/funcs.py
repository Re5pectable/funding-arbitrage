import re
from decimal import Decimal
from typing import Literal
from pandas import DataFrame, Series, concat
from .entities import OrderBook


SYMBOL_DECIMALS_PATTERN = re.compile(r"^(10+)(.*)$")


def fix_decimals_in_symbols(row: Series):
    match = SYMBOL_DECIMALS_PATTERN.match(row["symbol"])
    if match:
        numeric_prefix_str = match.group(1)
        row["symbol"] = match.group(2)
        row["lastPrice"] = Decimal(row["lastPrice"]) / Decimal(numeric_prefix_str)

    return row


def find_diff(df: DataFrame) -> DataFrame:
    result_rows = []

    for id_, group in df.groupby("id"):
        if len(group) < 2:
            continue

        merged = group.merge(group, how="cross", suffixes=("_a", "_b"))
        merged = merged[merged["exchange_a"] < merged["exchange_b"]]

        merged["fundingRate_diff"] = (
            merged["fundingRate_a"] - merged["fundingRate_b"]
        ).abs()
        merged["price_diff"] = merged["lastPrice_a"] / merged["lastPrice_b"]
        merged["time_diff_seconds"] = (
            (merged["nextFundingTime_a"] - merged["nextFundingTime_b"])
            .abs()
            .dt.total_seconds()
        )
        result_rows.append(merged)

    if result_rows:
        return concat(result_rows, ignore_index=True)

def avg_orderbook_price(
    orderbook: OrderBook,
    amount: float,
    side: Literal["buy", "sell"],
) -> float:
    reverse = side == "sell"
    book = sorted(
        orderbook.asks if side == "buy" else orderbook.bids,
        key=lambda x: x.price,
        reverse=reverse,
    )

    remaining = Decimal(str(amount))
    total_cost = Decimal("0")

    for row in book:
        print(row)
        if remaining <= row.size:
            total_cost += remaining * row.price
            remaining = 0
            break
        else:
            total_cost += row.size * row.price
            remaining -= row.size

    if remaining > 0:
        return None

    average_price = total_cost / Decimal(str(amount))
    return float(average_price)

def calculate_avg_price(row: Series, orderbook: dict):
    for amount in [50, 100, 200, 500]:
        if row["lastPrice_a"] > row["lastPrice_b"]:
            avg_sell_a = avg_orderbook_price(orderbook[row["index_a"]]["orderbook"], amount, "sell")
            avg_buy_b =  avg_orderbook_price(orderbook[row["index_b"]]["orderbook"], amount, "buy")
            row[f"priceDiff{amount}"] = avg_sell_a / avg_buy_b
        else:
            avg_buy_a = avg_orderbook_price(orderbook[row["index_a"]]["orderbook"], amount, "buy")
            avg_sell_b =  avg_orderbook_price(orderbook[row["index_b"]]["orderbook"], amount, "sell")
            row[f"priceDiff{amount}"] = avg_sell_b / avg_buy_a

    return row