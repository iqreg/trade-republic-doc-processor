# TGG System Architecture

## Runtime/deployment model
- Primary local mode: **native macOS** (API + worker direct, SQLite fallback).
- Secondary: hybrid mode (native app + optional Postgres/Redis).
- Optional: docker-compose full stack for reproducible deployments.

## Module Diagram
```text
collector -> market_data -> signal_engine -> recommendation_engine -> notifications
                 |               |                 |
                 v               v                 v
              storage        strategy_lab      paper_trading
                 |                                  |
                 +-> risk_engine <-> broker_adapter <-> live_control
```

## Signal quality flow (current)
1. Collect OHLCV.
2. Load local rolling history (`prev_close`, `avg_volume`).
3. Compute momentum + relative-volume + unusual-volume + gap/session features.
4. Produce explainable signal with calibrated confidence.
5. Persist signal features and paper-trade rationale for later evaluation.

## Safety boundaries
- Live execution path remains blocked by default flags and kill-switch.
- Default broker adapter is dry-run only.
- Placeholder live adapter rejects execution in MVP.
