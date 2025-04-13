import re
from decimal import Decimal
from typing import Literal
from pprint import pprint
from pandas import DataFrame, Series, concat, merge, to_datetime
from time import time


SYMBOL_DECIMALS_PATTERN = re.compile(r"^(10+)(.*)$")


def fix_decimals_in_symbols(row: Series):
    s = row["symbol"]
    match = SYMBOL_DECIMALS_PATTERN.match(s)
    if match:
        numeric_prefix_str = match.group(1)
        row["symbol"] = match.group(2)
        row["lastPrice"] = Decimal(row["lastPrice"]) / Decimal(numeric_prefix_str)

    return row

def find_diff(
    df: DataFrame,
    threshold: Decimal = Decimal("0.001"),
    max_hours_diff: int = 4
) -> DataFrame:
    df = df.copy()
    max_seconds_diff = max_hours_diff * 3600

    result_rows = []

    for id_, group in df.groupby("id"):
        if len(group) < 2:
            continue
        
        merged = group.merge(group, how="cross", suffixes=("_a", "_b"))
        merged = merged[merged["exchange_a"] < merged["exchange_b"]]

        merged["fundingRate_diff"] = (merged["fundingRate_a"] - merged["fundingRate_b"]).abs()
        merged["time_diff_seconds"] = (merged["nextFundingTime_a"] - merged["nextFundingTime_b"]).abs().dt.total_seconds()

        mask = (merged["fundingRate_diff"] > threshold) & (merged["time_diff_seconds"] <= max_seconds_diff)
        result_rows.append(merged[mask])

    if result_rows:
        return concat(result_rows, ignore_index=True)



def calculate_average_price_from_book(
    bids: list[list[float]],
    asks: list[list[float]],
    amount: float,
    side: Literal["buy", "sell"],
) -> float:
    orderbook = asks if side == "buy" else bids
    reverse = side == "sell"
    book = sorted(orderbook, key=lambda x: Decimal(str(x[0])), reverse=reverse)

    remaining = Decimal(str(amount))
    total_cost = Decimal("0")

    for price_str, qty_str in book:
        price = Decimal(str(price_str))
        qty = Decimal(str(qty_str))

        if remaining <= qty:
            total_cost += remaining * price
            break
        else:
            total_cost += qty * price
            remaining -= qty

    if remaining > 0:
        raise ValueError("Not enough liquidity to fulfill the order")

    average_price = total_cost / Decimal(str(amount))
    return float(average_price)
