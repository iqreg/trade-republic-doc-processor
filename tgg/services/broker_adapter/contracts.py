from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from tgg.services.live_trading_control_layer.control import live_execution_allowed


class Environment(str, Enum):
    DEV = "dev"
    PAPER = "paper"
    LIVE = "live"


@dataclass
class BrokerOrderIntent:
    symbol: str
    side: str
    qty: float
    order_type: str
    limit_price: float | None = None


@dataclass
class BrokerExecutionResult:
    accepted: bool
    broker_order_id: str
    message: str


class BrokerAdapter:
    def submit_order(self, order: BrokerOrderIntent) -> BrokerExecutionResult:
        raise NotImplementedError


class DryRunBrokerAdapter(BrokerAdapter):
    """Default adapter. Never places real orders."""

    def submit_order(self, order: BrokerOrderIntent) -> BrokerExecutionResult:
        return BrokerExecutionResult(True, f"dryrun-{order.symbol}", "Dry-run accepted (no live order sent)")


class LiveBrokerAdapterPlaceholder(BrokerAdapter):
    """Placeholder only: live broker execution is intentionally unavailable in MVP."""

    def submit_order(self, order: BrokerOrderIntent) -> BrokerExecutionResult:
        return BrokerExecutionResult(False, "", "Live adapter is intentionally disabled in MVP")


def get_default_broker_adapter(approvals_ok: bool = False) -> BrokerAdapter:
    """Returns a dry-run adapter unless all live execution gates pass.

    Even when live gates pass, MVP still returns a placeholder adapter that rejects orders.
    """
    if live_execution_allowed(approvals_ok=approvals_ok):
        return LiveBrokerAdapterPlaceholder()
    return DryRunBrokerAdapter()
