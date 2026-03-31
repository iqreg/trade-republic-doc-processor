from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class LiveTradingControl:
    enabled: bool
    require_two_step_approval: bool
    kill_switch: bool


def _as_bool(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def load_control() -> LiveTradingControl:
    # Safety defaults: live trading OFF and kill switch ON unless explicitly changed.
    return LiveTradingControl(
        enabled=_as_bool("ENABLE_LIVE_TRADING", "false"),
        require_two_step_approval=_as_bool("REQUIRE_TWO_STEP_APPROVAL", "true"),
        kill_switch=_as_bool("KILL_SWITCH", "true"),
    )


def live_execution_allowed(*, approvals_ok: bool) -> bool:
    control = load_control()
    if not control.enabled:
        return False
    if control.kill_switch:
        return False
    if control.require_two_step_approval and not approvals_ok:
        return False
    return approvals_ok
