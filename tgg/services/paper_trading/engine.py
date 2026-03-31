from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PaperOrder:
    symbol: str
    side: str
    qty: float
    price: float
    stop_loss: float
    take_profit: float


def build_paper_order(symbol: str, score: float, price: float) -> PaperOrder:
    qty = max(1.0, round((1000 * min(score, 1.0)) / max(price, 1), 4))
    return PaperOrder(
        symbol=symbol,
        side="BUY",
        qty=qty,
        price=price,
        stop_loss=round(price * 0.98, 4),
        take_profit=round(price * 1.04, 4),
    )
