# Changelog

All notable changes to the Market Intelligence Suite are documented here.

---

## [1.0.0] — April 2026

### Added
- Liquidity Regime Dashboard (LiquidityRegime) — FRED-based Fed BS/TGA/RRP
  decomposition, HY Z-score, yield curve composite. Saturday 8 PM refresh.
- COT Positioning Dashboard (COTRegime) — 12 futures instruments, 52-week
  rolling Z-scores, Bullish/Bearish Divergence signals. Friday 6 PM refresh.
- Macro Regime Dashboard (MacroRegime) — GDP/CPI/unemployment/PMI composite
  scoring. Sunday 5 AM refresh.
- DCF Valuation Dashboard (DCFRegime) — 503 S&P 500 stocks, Three Pillars
  framework, stress testing. Sunday 5 AM refresh.
- Market Sentiment Dashboard (SentimentRegime) — CBOE P/C, CNN Fear & Greed,
  AAII composite. Saturday 5:30 AM refresh.
- Full automation via Windows Task Scheduler + Power BI Personal Gateway
- Trading Confluence Panel on Liquidity Dashboard pulling signals from all
  four dashboards into per-instrument bias table

### Architecture
- Python ETL pipelines for all five dashboards
- SQL Server 2019 local instance with separate database per dashboard
- Power BI Desktop/Service with scheduled refresh
- Post-upsert data validation with null rate checks on every cycle