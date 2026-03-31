from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("sqlalchemy")
from fastapi.testclient import TestClient

from tgg.apps.api.main import app
from tgg.services.collectors.stooq_collector import MarketBar
from tgg.services.market_data.db import init_db, session
from tgg.services.market_data.models import AuditLog, OHLCV, PaperTrade, Signal


def test_run_once_happy_path(monkeypatch) -> None:
    def fake_fetch_latest(self, symbol: str):
        return MarketBar(
            symbol=symbol,
            ts=datetime(2026, 1, 1, 12, 0, 0),
            open=100.0,
            high=105.0,
            low=99.0,
            close=104.0,
            volume=250000,
        )

    monkeypatch.setenv("WATCHLIST", "AAPL")
    monkeypatch.setenv("ENABLE_TELEGRAM", "false")
    monkeypatch.setattr("tgg.services.collectors.stooq_collector.StooqCollector.fetch_latest", fake_fetch_latest)

    init_db()
    with session() as db:
        db.add(
            OHLCV(
                symbol="AAPL",
                ts=datetime(2025, 12, 31, 12, 0, 0),
                open=98.0,
                high=101.0,
                low=97.0,
                close=99.0,
                volume=100000,
                source="test",
            )
        )
        db.commit()

    with TestClient(app) as client:
        response = client.post("/run-once")
        assert response.status_code == 200
        payload = response.json()
        assert payload["count"] == 1
        assert payload["signals"][0]["signal_type"] == "MOMENTUM_CONTEXT"
        assert "rel_volume" in payload["signals"][0]["feature_snapshot"]


def test_paper_trade_and_audit_written() -> None:
    init_db()
    with session() as db:
        trade = db.query(PaperTrade).order_by(PaperTrade.id.desc()).first()
        assert trade is not None
        assert trade.signal_snapshot
        assert trade.open_reason
        assert db.query(Signal).count() >= 1
        assert db.query(AuditLog).filter(AuditLog.action == "paper_trade_created").count() >= 1
