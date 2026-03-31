# API Design (v0.1)

## `GET /health`
Returns service health and operating mode.

```json
{"status":"ok","mode":"analysis/paper","live_trading":false}
```

## `GET /status`
Returns aggregate counts from OHLCV, signals, and paper trades.

## `POST /run-once`
Runs one end-to-end collection -> signal -> paper-trade cycle and returns ranked signals.

## Future API additions
- `/recommendations`
- `/paper-trades`
- `/risk/events`
- `/broker/readiness`
- `/approvals`
