# Liquidity Hub

ETL pipeline for the Liquidity Regime Dashboard.

**Database:** LiquidityRegime | **Script:** liquidity_etl.py
**Refresh:** Saturday 8:00 PM ETL | 9:00 PM Power BI
**Data source:** FRED API (all series, no other dependencies)

## FRED Series
| Series | Metric | Cadence |
|--------|--------|---------|
| WALCL | Fed Balance Sheet | Weekly Thu |
| WTREGEN | Treasury TGA | Weekly Thu |
| WLRRAL | Reverse Repo RRP | Weekly Thu |
| BAMLH0A0HYM2 | HY Credit Spread | Daily |
| BAMLC0A0CM | IG Credit Spread | Daily |
| DFF | Fed Funds Rate | Daily |
| SOFR | SOFR Rate | Daily |
| T10YFF | 10Y minus Fed Funds | Daily |

## Key Formulas
- Net Liquidity = Fed Balance Sheet - TGA - RRP
- HY Z-Score = (Current - 2yr Mean) / 2yr StdDev
- Composite = (Net Liq x 0.50) + (HY Z-Score x 0.30) + (Yield Curve x 0.20)
- Gauge = ((Score + 2) / 4) x 100 -- Range 0 to 100

## SQL Architecture
Staging: stg_FedBalanceSheet, stg_CreditSpreads, stg_MoneyMarket, DimDate
Views: vw_NetLiquidity, vw_CreditSpreads, vw_MoneyMarket, vw_LiquidityRegime, vw_RegimeHistory

## Setup
pip install -r requirements.txt
Configure FRED_API_KEY and SQL credentials in script before running.
