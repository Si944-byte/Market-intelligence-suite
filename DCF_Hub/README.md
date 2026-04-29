# DCF Hub

ETL pipeline for the DCF Valuation Dashboard.

**Database:** DCFRegime | **Script:** dcf_etl.py
**Refresh:** Sunday 5:00 AM ETL | 6:00 AM Power BI
**Data source:** RapidAPI Yahoo Finance

## Coverage
All 503 S&P 500 constituents. Full DCF per stock.

## Framework
Three Pillars: Valuation and Research Engine, Stress-Testing Engine

## Key Outputs
- Valuation gap (DCF vs market price)
- Durability classification, Outlier flag
- Sensitivity Score per stock, Gap Buckets table
- Stress test pass/fail

## Key Finding
48.5% of BUY signals fail conservative stress testing.
Financials flagged for Phase 2 P/B/ROE treatment.

## Setup
pip install -r requirements.txt
Configure RAPIDAPI_KEY and SQL credentials.
