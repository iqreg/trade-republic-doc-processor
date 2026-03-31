from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RiskConfig:
    max_risk_per_trade_pct: float = 0.01
    max_daily_drawdown_pct: float = 0.03
    max_open_positions: int = 10


@dataclass
class RiskDecision:
    allowed: bool
    reason: str


def evaluate_order(open_positions: int, kill_switch: bool, approved: bool, cfg: RiskConfig) -> RiskDecision:
    if kill_switch:
        return RiskDecision(False, "Kill switch engaged")
    if open_positions >= cfg.max_open_positions:
        return RiskDecision(False, "Max open positions reached")
    if not approved:
        return RiskDecision(False, "Approval missing")
    return RiskDecision(True, "Allowed")
