from __future__ import annotations

from dataclasses import asdict
from typing import Iterable

from tgg.services.signal_engine.basic_signals import SignalResult


def rank(signals: Iterable[SignalResult]) -> list[dict]:
    ranked = sorted(signals, key=lambda s: (s.score, s.confidence), reverse=True)
    return [asdict(item) for item in ranked]
