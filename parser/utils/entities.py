from dataclasses import dataclass
from decimal import Decimal

@dataclass
class Order:
    price: Decimal
    size: Decimal


@dataclass
class OrderBook:
    bids: list[Order]
    asks: list[Order]