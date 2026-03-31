# Runtime Modes

## Native mode (preferred for first Mac testing)
- App: native Python processes (`uvicorn` + worker).
- DB: SQLite fallback by default.
- Redis: optional (not required for MVP path).
- Best for low RAM/CPU use and easy breakpoints/logging.

## Hybrid mode
- App: native Python.
- Infra: optional PostgreSQL/Redis (native services or containers).
- Best for incremental infra realism while preserving fast local debugging.

## Docker mode (optional)
- App + infra in Compose.
- Best for reproducible deployments and team consistency.

## Safety behavior across all modes
- `ENABLE_LIVE_TRADING=false` by default.
- `KILL_SWITCH=true` by default.
- Broker default adapter remains dry-run.
- Live adapter remains placeholder and rejects execution.
