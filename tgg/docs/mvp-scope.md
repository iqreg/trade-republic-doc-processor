# MVP Scope (v0.1)

## Included
- Watchlist ingestion for legal/public market bars.
- Symbol normalization and OHLCV storage.
- Basic momentum breakout signal.
- Explainable recommendation payloads (score, confidence, provenance, risk summary).
- Telegram alerting integration.
- Paper trade simulation and journaling.
- API health/status/run endpoints.
- Broker interface contracts and dry-run adapter.
- Live-trading safety architecture (disabled path, approval hooks, kill switch).

## Excluded
- Fully automated live order placement.
- High-frequency real-time feeds requiring paid licenses.
- Advanced NLP models for news clustering (stubbed phase 2+).
- Full dashboard UI implementation.
- Production-grade auth/RBAC (planned before live readiness).
