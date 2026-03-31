from tgg.services.paper_trading.engine import build_paper_order
from tgg.services.risk_engine.guards import RiskConfig, evaluate_order
from tgg.services.signal_engine.basic_signals import momentum_context_signal


def test_momentum_signal_triggers_with_volume_context() -> None:
    signal = momentum_context_signal(
        symbol="AAPL",
        close=103,
        open_px=100,
        volume=200000,
        prev_close=99,
        avg_volume=90000,
    )
    assert signal is not None
    assert signal.signal_type == "MOMENTUM_CONTEXT"


def test_signal_includes_explainability_fields() -> None:
    signal = momentum_context_signal(
        symbol="MSFT",
        close=104,
        open_px=100,
        volume=600000,
        prev_close=100,
        avg_volume=200000,
    )
    assert signal is not None
    assert "rel_volume" in signal.feature_snapshot
    assert "momentum" in signal.explanation.lower()
    assert "rules:v0.2" in signal.provenance


def test_risk_engine_blocks_missing_approval() -> None:
    cfg = RiskConfig(max_open_positions=3)
    decision = evaluate_order(open_positions=1, kill_switch=False, approved=False, cfg=cfg)
    assert not decision.allowed


def test_paper_order_has_bounds() -> None:
    order = build_paper_order("TSLA", score=0.5, price=100)
    assert order.stop_loss < order.price < order.take_profit
