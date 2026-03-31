# Local Setup Guide (macOS, Native-first)

## Prerequisites
- macOS + Python 3.11+
- `make`

## Setup
```bash
cd tgg
make native-setup
```

This creates `.venv`, installs requirements, and copies `.env.native.example` to `.env` if missing.

## Run API
```bash
cd tgg
make native-run
```

## Run worker (second terminal)
```bash
cd tgg
make worker
```

## Validate endpoints
```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/status
curl -X POST http://127.0.0.1:8000/run-once
```

## Native test run
```bash
cd tgg
make native-test
```

## Optional Docker (not required)
```bash
cd tgg
make docker-up
```

## Telegram (optional)
Set in `.env`:
- `ENABLE_TELEGRAM=true`
- `TELEGRAM_BOT_TOKEN=...`
- `TELEGRAM_CHAT_ID=...`

## Safety reminder
Live trading remains disabled in native mode and all other modes.
