from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import urlopen


@dataclass
class MarketBar:
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str = "stooq"


class StooqCollector:
    """Legal-access daily bars from Stooq CSV endpoint."""

    BASE_URL = "https://stooq.com/q/l/"

    def fetch_latest(self, symbol: str) -> MarketBar | None:
        normalized = symbol.lower().replace("-", "")
        if normalized.endswith("usd") and len(normalized) > 3:
            normalized = normalized[:-3] + "usd"
        query = {
            "s": f"{normalized}.us" if symbol.isalpha() and len(symbol) <= 5 else normalized,
            "f": "sd2t2ohlcv",
            "h": "",
            "e": "csv",
        }
        url = f"{self.BASE_URL}?{urlencode(query)}"
        with urlopen(url, timeout=10) as response:
            body = response.read().decode("utf-8")

        lines = body.strip().splitlines()
        if len(lines) < 2 or "N/D" in lines[1]:
            return None
        row = lines[1].split(",")
        ts = datetime.strptime(f"{row[1]} {row[2]}", "%Y-%m-%d %H:%M:%S")
        return MarketBar(
            symbol=symbol,
            ts=ts,
            open=float(row[3]),
            high=float(row[4]),
            low=float(row[5]),
            close=float(row[6]),
            volume=float(row[7]),
        )
