from __future__ import annotations

from tgg.services.broker_adapter.contracts import BrokerOrderIntent, DryRunBrokerAdapter, get_default_broker_adapter
from tgg.services.live_trading_control_layer.control import live_execution_allowed, load_control
from tgg.services.notifications.telegram import TelegramNotifier


def test_live_control_defaults(monkeypatch) -> None:
    monkeypatch.delenv("ENABLE_LIVE_TRADING", raising=False)
    monkeypatch.delenv("REQUIRE_TWO_STEP_APPROVAL", raising=False)
    monkeypatch.delenv("KILL_SWITCH", raising=False)

    control = load_control()
    assert control.enabled is False
    assert control.require_two_step_approval is True
    assert control.kill_switch is True
    assert live_execution_allowed(approvals_ok=True) is False


def test_broker_default_is_dry_run() -> None:
    adapter = get_default_broker_adapter(approvals_ok=False)
    assert isinstance(adapter, DryRunBrokerAdapter)


def test_live_adapter_still_rejects_in_mvp(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_LIVE_TRADING", "true")
    monkeypatch.setenv("REQUIRE_TWO_STEP_APPROVAL", "false")
    monkeypatch.setenv("KILL_SWITCH", "false")
    adapter = get_default_broker_adapter(approvals_ok=True)
    result = adapter.submit_order(BrokerOrderIntent(symbol="AAPL", side="BUY", qty=1, order_type="market"))
    assert result.accepted is False
    assert "disabled" in result.message.lower()


def test_telegram_missing_token_returns_false(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_TELEGRAM", "true")
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    notifier = TelegramNotifier()
    assert notifier.send("test") is False
