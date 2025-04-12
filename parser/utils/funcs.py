from pandas import Series
import re
from decimal import Decimal

SYMBOL_DECIMALS_PATTERN = re.compile(r'^(10+)(.*)$')

def fix_decimals_in_symbols(row: Series):
    s = row["symbol"]
    match = SYMBOL_DECIMALS_PATTERN.match(s)
    if match:
        numeric_prefix_str = match.group(1)
        row["symbol"] = match.group(2)
        row["lastPrice"] = Decimal(row["lastPrice"]) / Decimal(numeric_prefix_str)

    return row