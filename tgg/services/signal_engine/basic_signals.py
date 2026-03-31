from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SignalResult:
    symbol: str
    signal_type: str
    score: float
    confidence: float
    explanation: str
    provenance: str
    risk_summary: str
    feature_snapshot: str
    open_reason: str


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def momentum_context_signal(
    *,
    symbol: str,
    close: float,
    open_px: float,
    volume: float,
    prev_close: float | None,
    avg_volume: float | None,
) -> SignalResult | None:
    if open_px <= 0:
        return None

    intraday_return = (close - open_px) / open_px
    gap_pct = ((open_px - prev_close) / prev_close) if prev_close and prev_close > 0 else 0.0
    rel_volume = volume / avg_volume if avg_volume and avg_volume > 0 else 1.0
    session_context = "gap_up" if gap_pct > 0.01 else "gap_down" if gap_pct < -0.01 else "flat_open"

    unusual_volume = rel_volume >= 1.8 or volume >= 500000
    momentum_ok = intraday_return >= 0.01

    score = 0.0
    score += _clamp(intraday_return * 8, -0.5, 0.6)
    score += 0.25 if unusual_volume else 0.0
    score += 0.15 if session_context == "gap_up" else 0.05 if session_context == "flat_open" else -0.1
    score = _clamp(score, 0.0, 1.0)

    if score < 0.45 or not momentum_ok:
        return None

    confidence = _clamp(0.35 + score * 0.45 + (0.08 if unusual_volume else 0.0), 0.35, 0.9)
    reasons = [
        f"intraday_return={intraday_return:.2%}",
        f"gap={gap_pct:.2%}",
        f"rel_volume={rel_volume:.2f}x",
        f"session={session_context}",
    ]

    explanation = (
        f"{symbol} bullish momentum with {intraday_return:.2%} intraday move, "
        f"{rel_volume:.2f}x relative volume, and {session_context.replace('_', ' ')}."
    )
    provenance = "stooq:open,close,volume + local_ohlcv_history:prev_close,avg_volume + rules:v0.2"
    risk_summary = "Momentum can reverse quickly; invalidate below session low or if relative volume fades."
    feature_snapshot = ", ".join(reasons)
    open_reason = "Rule v0.2 triggered by momentum + volume context + session gap structure"

    return SignalResult(
        symbol=symbol,
        signal_type="MOMENTUM_CONTEXT",
        score=score,
        confidence=confidence,
        explanation=explanation,
        provenance=provenance,
        risk_summary=risk_summary,
        feature_snapshot=feature_snapshot,
        open_reason=open_reason,
    )
