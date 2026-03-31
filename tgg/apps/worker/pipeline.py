from __future__ import annotations

import os

from sqlalchemy import desc

from tgg.services.collectors.stooq_collector import StooqCollector
from tgg.services.config import load_env_file
from tgg.services.market_data.db import init_db, session
from tgg.services.market_data.models import AuditLog, Instrument, OHLCV, PaperTrade, Signal
from tgg.services.notifications.telegram import TelegramNotifier
from tgg.services.paper_trading.engine import build_paper_order
from tgg.services.recommendation_engine.ranker import rank
from tgg.services.signal_engine.basic_signals import momentum_context_signal


def _history_context(db, symbol: str) -> tuple[float | None, float | None]:
    rows = db.query(OHLCV).filter(OHLCV.symbol == symbol).order_by(desc(OHLCV.ts)).limit(20).all()
    if not rows:
        return None, None
    prev_close = rows[0].close
    avg_volume = sum(r.volume for r in rows) / len(rows)
    return prev_close, avg_volume


load_env_file()


def run_once() -> list[dict]:
    init_db()
    collector = StooqCollector()
    notifier = TelegramNotifier()
    watchlist = [x.strip().upper() for x in os.getenv("WATCHLIST", "AAPL,MSFT,TSLA").split(",") if x.strip()]

    captured = []
    with session() as db:
        for symbol in watchlist:
            prev_close, avg_volume = _history_context(db, symbol)
            bar = collector.fetch_latest(symbol)
            if not bar:
                continue

            if not db.query(Instrument).filter(Instrument.symbol == symbol).first():
                db.add(Instrument(symbol=symbol, asset_class="equity"))

            db.add(
                OHLCV(
                    symbol=symbol,
                    ts=bar.ts,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    source=bar.source,
                )
            )

            signal = momentum_context_signal(
                symbol=symbol,
                close=bar.close,
                open_px=bar.open,
                volume=bar.volume,
                prev_close=prev_close,
                avg_volume=avg_volume,
            )
            if signal:
                db.add(
                    Signal(
                        symbol=signal.symbol,
                        signal_type=signal.signal_type,
                        score=signal.score,
                        confidence=signal.confidence,
                        explanation=signal.explanation,
                        provenance=signal.provenance,
                        risk_summary=signal.risk_summary,
                        feature_snapshot=signal.feature_snapshot,
                    )
                )
                order = build_paper_order(symbol, signal.score, bar.close)
                db.add(
                    PaperTrade(
                        symbol=order.symbol,
                        side=order.side,
                        qty=order.qty,
                        entry_price=order.price,
                        stop_loss=order.stop_loss,
                        take_profit=order.take_profit,
                        signal_snapshot=signal.feature_snapshot,
                        open_reason=signal.open_reason,
                    )
                )
                db.add(AuditLog(action="paper_trade_created", payload=f"{symbol}:{order.qty}:{signal.feature_snapshot}"))
                notifier.send(
                    f"TGG {signal.signal_type} {symbol} score={signal.score:.2f} conf={signal.confidence:.2f} | {signal.feature_snapshot}"
                )
                captured.append(signal)

        db.commit()

    return rank(captured)


if __name__ == "__main__":
    print(run_once())
