from __future__ import annotations

from fastapi import FastAPI

from tgg.apps.worker.pipeline import run_once
from tgg.services.config import load_env_file
from tgg.services.broker_adapter.contracts import get_default_broker_adapter
from tgg.services.live_trading_control_layer.control import load_control
from tgg.services.market_data.db import init_db, session
from tgg.services.market_data.models import OHLCV, PaperTrade, Signal

load_env_file()

app = FastAPI(title="Trader-Ghost-Gregory API", version="0.1.1")


@app.on_event("startup")
def startup() -> None:
    init_db()


@app.get("/health")
def health() -> dict:
    control = load_control()
    adapter = get_default_broker_adapter(approvals_ok=False)
    return {
        "status": "ok",
        "mode": "analysis/paper",
        "live_trading_enabled": control.enabled,
        "kill_switch": control.kill_switch,
        "broker_mode": adapter.__class__.__name__,
    }


@app.post("/run-once")
def run_pipeline_once() -> dict:
    ranked = run_once()
    return {"signals": ranked, "count": len(ranked)}


@app.get("/status")
def status() -> dict:
    with session() as db:
        return {
            "ohlcv_count": db.query(OHLCV).count(),
            "signal_count": db.query(Signal).count(),
            "paper_trade_count": db.query(PaperTrade).count(),
        }
