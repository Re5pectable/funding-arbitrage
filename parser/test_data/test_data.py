from datetime import datetime, timedelta
from decimal import Decimal
from random import randint, uniform

from .. import settings  # noqa: F401
from ..interfaces import binance, bybit, gateio
import json

symbols = {
    row["symbol"]: float(row["price"]) for row in json.load(open("./parser/test_data/data.json", "r"))
}
now = datetime.now().replace(microsecond=0, second=0, minute=0)

def get_random_decimal(start, end, decimals: int = 8):
    return Decimal(str(round(uniform(start, end), decimals)))
    

BINANCE_FUNDRATE = [
    binance.FundingRate(
        symbol=symbol,
        lastPrice=get_random_decimal(price * 0.95, price * 1.05),
        indexPrice=get_random_decimal(price * 0.95, price * 1.05),
        estimatedSettlePrice=get_random_decimal(price * 0.95, price * 1.05),
        fundingRate=get_random_decimal(-0.001, 0.001),
        nextFundingTime=now+timedelta(hours=randint(2, 5)),
    )
    for symbol, price in symbols.items()
]
BYBIT_FUNDRATE = [
    bybit.FundingRate(
        symbol=symbol,
        lastPrice=get_random_decimal(price * 0.95, price * 1.05),
        fundingRate=get_random_decimal(-0.001, 0.001),
        nextFundingTime=now + timedelta(hours=randint(2, 5)),
    )
    for symbol, price in symbols.items()
]
GATEIO_FUNDRATE = [
    gateio.FundingRate(
        symbol=symbol[:-4] + "_" + symbol[-4:],
        lastPrice=get_random_decimal(price * 0.95, price * 1.05),
        fundingRate=get_random_decimal(-0.001, 0.001),
        nextFundingTime=now + timedelta(hours=randint(2, 5)),
    )
    for symbol, price in symbols.items()
]
