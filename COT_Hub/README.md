# COT Hub

ETL pipeline for the COT Positioning Dashboard.

**Database:** COTRegime | **Script:** cot_etl.py
**Refresh:** Friday 6:00 PM ETL | 7:00 PM Power BI
**Data source:** CFTC.gov public ZIPs (no API key required)

## Instruments Tracked (12 total)
Legacy report: ES/MES, NQ/MNQ, YM/MYM, ZN, ZB, 6E
Disaggregated: CL/MCL, GC/MGC, SI/SIL, ZC, ZS, NG

## Key Metrics
- 52-week rolling Z-scores (spec and commercial)
- Bullish/Bearish Divergence at |Z| > 1.0
- Divergence Strength = |Spec Z| + |Comm Z|
- 10,000+ row fact table rebuilt weekly

## Setup
pip install -r requirements.txt
Configure SQL credentials. No API key needed for CFTC data.
