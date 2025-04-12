from pandas import Series, DataFrame
import re
from decimal import Decimal

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
        rates = gr["fundingRate"].dropna()
        if len(rates) < 2:
            return False
        max_rate = max(rates)
        min_rate = min(rates)
        return (max_rate - min_rate) > threshold

    symbols = (
        df.groupby("id")
        .filter(is_significantly_diff)
    )

    return symbols.sort_values("id")
