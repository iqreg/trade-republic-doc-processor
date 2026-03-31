# Live Trading Safety Design

> Live broker execution remains disabled in MVP (native, hybrid, and docker modes).

## Defaults
- `ENABLE_LIVE_TRADING=false`
- `REQUIRE_TWO_STEP_APPROVAL=true`
- `KILL_SWITCH=true`

## Enforcement
- Control layer vetoes execution unless explicit live enable + approvals + kill-switch off.
- Default broker adapter is dry-run.
- Placeholder live adapter rejects all execution attempts.

## Non-goals
- No real order placement in current milestone.
- No bypass of risk or approval gates.
