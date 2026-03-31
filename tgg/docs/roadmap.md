# TGG Roadmap

## Current state
- Canonical branch: `work`.
- Primary local run mode: native macOS.
- Docker kept optional for reproducible rollout.

## Completed in current milestone
- Native-first developer commands and setup docs.
- Signal quality improvements (relative volume, unusual volume, gap/session context).
- Better confidence calibration and explainability.
- Paper-trade traceability fields (`signal_snapshot`, `open_reason`).

## Next milestone (recommended)
### OpenClaw-safe integration prep
- Add a read-only reporting/export interface for TGG outputs.
- Keep OpenClaw integration in observer mode first (no execution hooks).
- Add strict allowlist for what data leaves TGG and audit each export event.

## Later
- Strategy lab/backtesting depth.
- News/macro enrichment.
- Vector memory integration.
- Broker hardening (still disabled-by-default for live).
