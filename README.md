# Trader-Ghost-Gregory (TGG)

TGG is a modular market-intelligence and execution-preparation platform focused on analysis, alerting, and paper-trading.

> ⚠️ Live trading is still disabled. Real broker execution is NOT enabled in MVP.

## Runtime modes

### 1) Native mode (primary for MacBook Pro)
- API + worker run directly on macOS.
- Default local DB is SQLite (`tgg_native.db`) for low resource usage.
- Best for first real testing, debugging, and fast iteration.

### 2) Hybrid mode
- App runs natively, but you may use native/local PostgreSQL or Redis (or containerized infra only).
- Useful when validating production-like dependencies while keeping app debugging local.

### 3) Docker mode (optional)
- Full stack in containers via `docker compose`.
- Useful for reproducibility, cross-machine rollout, and deployment parity.

## Branch policy
- Canonical development branch: `work`.
- Legacy backup branch: `main`.

## Quick start (native mode on macOS)
```bash
cd tgg
make native-setup
make native-test
make native-run
```

In another terminal:
```bash
cd tgg
make worker
```

Then verify:
```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/status
curl -X POST http://127.0.0.1:8000/run-once
```

## Telegram in native mode
1. Edit `tgg/.env`.
2. Set:
   - `ENABLE_TELEGRAM=true`
   - `TELEGRAM_BOT_TOKEN=<your token>`
   - `TELEGRAM_CHAT_ID=<target chat id>`
3. Re-run worker or `/run-once`.

If token/chat ID is missing, notifier safely no-ops.

## What works natively now
- Health/status/run-once API path.
- Watchlist ingestion and normalized OHLCV storage.
- Signal generation with momentum + volume + gap/session context.
- Paper-trade creation with feature snapshot and open-reason traceability.
- Dry-run broker default and live adapter rejection behavior.

## What still depends on containers (optional)
- Reproducible Postgres/Redis stack via `docker compose`.
- Cross-machine environment parity.

## Documentation
- `tgg/docs/runtime-modes.md`
- `tgg/docs/local-setup-macos.md`
- `tgg/docs/architecture.md`
- `tgg/docs/source-matrix.md`
- `tgg/docs/live-trading-safety.md`
- `tgg/docs/roadmap.md`
