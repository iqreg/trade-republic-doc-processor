# AGENTS Instructions for Trader-Ghost-Gregory (TGG)

## Mission
TGG is a market-intelligence and execution-preparation platform. It is **not** a profit guarantee system.

## Non-negotiables
- Live trading code paths must remain disabled by default.
- Paper trading is default.
- Every recommendation must include explainability, confidence, provenance, and risk summary.
- Do not add connectors that violate terms of service.
- Secrets must never be hardcoded.

## Coding standards
- Python 3.11+
- Type hints required for public functions.
- Keep modules small and composable.
- Prefer explicit dataclasses / pydantic models over untyped dicts for cross-module contracts.

## Safety standards
- Any broker adapter implementation must pass through risk and approval gates.
- All broker-facing intents/actions must be logged into an audit trail.
- Keep kill-switch checks as close to order submission boundaries as possible.

## Testing
- Add unit tests for all signal/risk/paper-trading logic changes.
- Any new API endpoint should have at least one test.
