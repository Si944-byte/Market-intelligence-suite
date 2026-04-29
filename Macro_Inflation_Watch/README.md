# Macro Inflation Watch

ETL pipeline for the Macro Regime Dashboard.

**Database:** MacroRegime | **Script:** macro_etl.py
**Refresh:** Sunday 5:00 AM ETL | 6:00 AM Power BI
**Data sources:** FRED API, RapidAPI YFinance

## Metrics Tracked
- CPI (3-month smoothing)
- GDP (6-month smoothing)
- Unemployment rate
- Fed Funds Rate
- S&P 500 via RapidAPI YFinance
- PMI proxy via IPMAN

## Regime Output
Expansion / Slowdown / Contraction / Recovery
Composite +1/0/-1 scoring across four macro signals.
All Power BI transforms done in SQL views.

## Setup
pip install -r requirements.txt
Configure FRED_API_KEY, RAPIDAPI_KEY, and SQL credentials.
