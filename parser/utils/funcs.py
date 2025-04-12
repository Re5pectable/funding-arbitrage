import re
from decimal import Decimal
from typing import Literal

from pandas import DataFrame, Series

SYMBOL_DECIMALS_PATTERN = re.compile(r"^(10+)(.*)$")


def fix_decimals_in_symbols(row: Series):
    s = row["symbol"]
    match = SYMBOL_DECIMALS_PATTERN.match(s)
    if match:
        numeric_prefix_str = match.group(1)
        row["symbol"] = match.group(2)
        row["lastPrice"] = Decimal(row["lastPrice"]) / Decimal(numeric_prefix_str)

    return row


def find_diff(df: DataFrame, threshold: Decimal = Decimal("0.001")) -> DataFrame:
    def is_significantly_diff(gr):
        print(gr)
        rates = gr["fundingRate"].dropna()
        if len(rates) < 2:
            return False
        max_rate = max(rates)
        min_rate = min(rates)
        return (max_rate - min_rate) > threshold

    symbols = df.groupby("id").filter(is_significantly_diff)

    return symbols.sort_values("id")


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
