# Source Evaluation Matrix

| Source | Coverage | Access | MVP usage now | Native-mode suitability |
|---|---|---|---|---|
| Stooq public CSV | Price/volume bars | HTTP CSV | Active (collector baseline) | High |
| SEC EDGAR | Filings/insider | Official API/feed | Planned | High |
| FRED/BLS/BEA | Macro indicators | Official APIs | Planned | High |
| Public RSS feeds | News headlines | RSS/API | Planned | High |
| TradingView | Chart/social | Licensed/API only | Not used in MVP automation | Low without license |
| Trade Republic internal | Account data | Private interfaces | Not used automatically | Unknown pending legal review |

## Current note
- Native mode does not require Docker to use Stooq + local SQLite for first testing.
