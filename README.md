# Market Intelligence Suite

A personal institutional-grade market intelligence system built for
systematic futures trading. Five interconnected dashboards covering
liquidity conditions, macro regime, DCF valuation, market sentiment,
and COT institutional positioning — all feeding into a unified weekly
pre-trade decision framework.

Built entirely from scratch: Python ETL pipelines, SQL Server 2019,
Power BI Desktop/Service, and Windows Task Scheduler automation.
No third-party BI templates or pre-built connectors.

---

## Dashboards

### 1. Liquidity Regime Dashboard
**Database:** LiquidityRegime | **Refresh:** Saturday 8:00 PM (ETL) | 9:00 PM (Power BI)

<img width="1160" height="811" alt="Screenshot 2026-04-29 140534" src="https://github.com/user-attachments/assets/e99d59f4-660b-4c6d-a2b8-c7a734114062" />

Tracks systemic liquidity conditions via Fed balance sheet decomposition,
credit spreads, and yield curve. Produces a 0–100 composite gauge and
Expanding / Neutral / Contracting regime classification.

**Data source:** FRED (all series, daily + weekly cadence)

| FRED Series | Metric | Cadence |
|-------------|--------|---------|
| WALCL | Fed Balance Sheet | Weekly Thu |
| WTREGEN | Treasury TGA | Weekly Thu |
| WLRRAL | Reverse Repo (RRP) | Weekly Thu |
| BAMLH0A0HYM2 | HY Credit Spread | Daily |
| BAMLC0A0CM | IG Credit Spread | Daily |
| DFF | Fed Funds Rate | Daily |
| SOFR | SOFR Rate | Daily |
| T10YFF | 10Y minus Fed Funds | Daily |

**Key formulas:**
- Net Liquidity = Fed Balance Sheet − TGA − RRP
- HY Z-Score = (Current HY Spread − 2yr Mean) / 2yr StdDev
- Composite Score = (Net Liq × 0.50) + (HY Z-Score × 0.30) + (Yield Curve × 0.20)
- Gauge = ((Score + 2) / 4) × 100 → Range: 0–100

**Regime scoring:**

| Regime | Gauge | Composite | Trade Bias |
|--------|-------|-----------|------------|
| Expanding | 69–100 | > 0.75 | Risk-On — Lean Long |
| Neutral-Positive | 50–69 | > 0.00 | Mild Risk-On Bias |
| Neutral | 37–50 | > -0.50 | No Liquidity Edge |
| Neutral-Negative | 25–37 | > -1.00 | Caution — Reduce Size |
| Contracting | 0–25 | ≤ -1.00 | Risk-Off — ZN/ZB |

**SQL architecture:**
- Staging: stg_FedBalanceSheet (weekly), stg_CreditSpreads (daily), stg_MoneyMarket (daily), DimDate (static)
- Views: vw_NetLiquidity, vw_CreditSpreads, vw_MoneyMarket, vw_LiquidityRegime, vw_RegimeHistory

**5 pages:** Liquidity Regime Dashboard · Net Liquidity Deep Dive ·
Credit & Money Markets · Trading Confluence Panel · Methodology

---

### 2. Macro Regime Dashboard
**Database:** MacroRegime | **Refresh:** Sunday 5:00 AM (ETL) | 6:00 AM (Power BI)

<img width="1359" height="765" alt="Screenshot 2026-04-29 144726" src="https://github.com/user-attachments/assets/f908b0be-5e20-477a-895d-f8016106631b" />

Tracks the macroeconomic environment across four regime states:
Expansion, Slowdown, Contraction, Recovery.

- **Data sources:** FRED (CPI, unemployment, FFR), RapidAPI YFinance (S&P 500), IPMAN as PMI proxy
- **Key metrics:** 6-month GDP smoothing, 3-month CPI smoothing, composite macro score with +1/0/−1 signal system
- **SQL views:** All Power BI transforms done in SQL — no DAX heavy lifting
- **Output:** Market bias score, regime classification, multi-page Power BI dashboard

---

### 3. DCF Valuation Dashboard
**Database:** DCFRegime | **Refresh:** Sunday 5:00 AM (ETL) | 6:00 AM (Power BI)

<img width="1443" height="812" alt="Screenshot 2026-04-29 143031" src="https://github.com/user-attachments/assets/5e5b76c3-323d-4808-8456-3e7884543080" />

Covers all 503 S&P 500 stocks via RapidAPI Yahoo Finance.

- **Framework:** Three Pillars — Valuation & Research Engine, Stress-Testing Engine
- **Key features:** Durability classification, outlier flagging, Sensitivity Score per stock, Gap Buckets reference table
- **Key finding:** 48.5% of BUY signals fail conservative stress testing. Financials sector flagged for Phase 2 P/B/ROE treatment
- **Output:** Stock-level valuation gap, buy/hold/sell signals, stress test pass/fail, multi-page Power BI dashboard

---

### 4. Market Sentiment Dashboard
**Database:** SentimentRegime | **Refresh:** Saturday 5:30 AM (ETL) | 6:30 AM (Power BI)

<img width="1440" height="812" alt="Screenshot 2026-04-29 144821" src="https://github.com/user-attachments/assets/9a7b888e-6d3b-4101-bae1-ea780fc36884" />

Tracks market crowd psychology across multiple sentiment indicators.

- **Data sources:** CBOE (put/call ratio), CNN Fear & Greed, AAII sentiment survey
- **Key metrics:** Composite sentiment score, regime classification, historical percentile context
- **Output:** Sentiment regime label, composite score trending, Power BI dashboard with historical context

---

### 5. COT Positioning Dashboard
**Database:** COTRegime | **Refresh:** Friday 6:00 PM (ETL) | 7:00 PM (Power BI)

<img width="1282" height="722" alt="Screenshot 2026-04-29 140924" src="https://github.com/user-attachments/assets/f7943f2b-bf74-439d-9620-7d6b7bb98b6b" />

Tracks CFTC Commitment of Traders institutional positioning for 12
futures markets across Legacy and Disaggregated report formats.

- **Instruments:** ES, NQ, YM, ZN, ZB, 6E, CL, GC, SI, ZC, ZS, NG
- **Key metrics:** 52-week rolling Z-scores for spec and commercial positioning, Bullish/Bearish Divergence signals, Divergence Strength score
- **ETL:** Downloads ~20 CFTC annual ZIPs per cycle, validates null rates, rebuilds 10,000+ row fact table
- **Output:** Positioning heatmap, divergence confluence panel, historical extremes, five-page Power BI dashboard

See `docs/COT_Dashboard_Guide.docx` for full COT methodology.

---

## How It Works Together

Each dashboard feeds a layer of the pre-trade decision framework:

```
Liquidity        → Is the tide rising or falling? (Fed BS, TGA, RRP, spreads)
        |
Macro Regime     → What is the macroeconomic environment?
        |
COT Positioning  → What are institutions positioned to do?
        |
Sentiment        → What is the crowd's emotional state?
        |
DCF Valuation    → Are equity prices justified? (position sizing)
        |
Break of Structure setup → Entry trigger (TradingView, 4H/1H/5M)
```

The Trading Confluence Panel on the Liquidity Dashboard pulls signals
from all four other dashboards into a single per-instrument bias table,
producing an Overall Confluence score and Confluence Regime Display.

**Signal hierarchy:**
- Liquidity expanding + Macro expansion = maximum risk-on conviction
- Any contraction signal = reduce size regardless of other signals
- All four dashboards aligned = full position size
- Mixed signals = minimum size or stand aside

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| ETL | Python 3 (pandas, requests, pyodbc, numpy, fredapi) |
| Database | SQL Server 2019 (local instance, DESKTOP-1CRNFTD) |
| Visualization | Power BI Desktop + Power BI Service |
| Automation | Windows Task Scheduler + Personal Gateway |
| Data Sources | FRED API, RapidAPI (Yahoo Finance), CFTC.gov, CBOE |

---

## Automation Schedule

| Dashboard | ETL | Power BI Refresh |
|-----------|-----|-----------------|
| COT Positioning | Friday 6:00 PM | Friday 7:00 PM |
| Liquidity | Saturday 8:00 PM | Saturday 9:00 PM |
| Sentiment | Saturday 5:30 AM | Saturday 6:30 AM |
| Macro Regime | Sunday 5:00 AM | Sunday 6:00 AM |
| DCF Valuation | Sunday 5:00 AM | Sunday 6:00 AM |

All automation runs via Windows Task Scheduler calling batch files,
with Power BI Service scheduled refresh via Personal Gateway
connecting to local SQL Server.

---

## Repository Structure

```
market-intelligence-suite/
├── Liquidity Hub/           # Liquidity regime ETL
│   ├── liquidity_etl.py
│   ├── run_liquidity_etl.bat
│   └── requirements.txt
├── Macro Inflation Watch/   # Macro regime ETL
│   ├── macro_etl.py
│   ├── run_macro_etl.bat
│   └── requirements.txt
├── COT Hub/                 # COT positioning ETL
│   ├── cot_etl.py
│   ├── run_cot_etl.bat
│   └── requirements.txt
├── Sentiment Hub/           # Sentiment ETL
│   ├── sentiment_etl.py
│   ├── run_sentiment_etl.bat
│   └── requirements.txt
├── DCF Hub/                 # DCF valuation ETL
│   ├── dcf_etl.py
│   ├── run_dcf_etl.bat
│   └── requirements.txt
├── docs/                    # Dashboard guides and stress tests
│   └── COT_Dashboard_Guide.docx
└── skills/                  # Claude AI skills for future builds
    └── cot-etl-pipeline/
        └── SKILL.md
```

---

## Setup

Each ETL script requires Python 3.x and SQL Server 2019 with the
corresponding database created. Install dependencies per dashboard:

```bash
cd "COT Hub"
pip install -r requirements.txt
```

Configure credentials by replacing placeholder values in each ETL
script before running:

```python
SQL_SERVER   = "YOUR_SQL_SERVER"
SQL_PASSWORD = "YOUR_SQL_PASSWORD"
FRED_API_KEY = "YOUR_FRED_API_KEY"      # Liquidity, Macro
RAPIDAPI_KEY = "YOUR_RAPIDAPI_KEY"      # Macro, DCF
```

---

*Built for personal systematic trading research.
Not financial advice.*
