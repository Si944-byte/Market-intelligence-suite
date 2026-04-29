---
name: cot-etl-pipeline
description: >
  Build, debug, and maintain a CFTC Commitment of Traders (COT) ETL pipeline
  that downloads weekly positioning data, computes Z-scores and divergence
  signals, loads into SQL Server, and connects to Power BI. Use this skill
  whenever the user is working with COT data, CFTC futures positioning,
  building a COT dashboard, writing or fixing a COT ETL script, setting up
  scheduled refresh for COT data, or asking about spec vs. commercial
  divergence signals for futures trading. Also triggers for questions about
  COT Z-score methodology, Legacy vs. Disaggregated CFTC report formats,
  CFTC instrument codes, or building a positioning intelligence dashboard
  for futures markets. If the user mentions COT, CFTC, commitment of
  traders, non-commercial positioning, managed money, or commercial hedgers
  in a trading or data context, use this skill.
---

# COT ETL Pipeline Skill

End-to-end workflow for building and maintaining a CFTC COT data pipeline
feeding a Power BI positioning dashboard. Covers data sourcing, ETL
architecture, SQL schema, Z-score math, divergence signal logic, Power BI
build, and weekly automation.

---

## Architecture Overview

```
CFTC.gov (free ZIPs, weekly)
    |
cot_etl.py  (Python ETL)
    |
SQL Server -> raw_cot (staging) -> cot_weekly (fact) -> 3 views
    |
Power BI Desktop -> Power BI Service (Friday 7 PM refresh)
```

**Two ZIP series to download:**
- Legacy (equity index, rates, FX): `https://www.cftc.gov/files/dea/history/deacot{YEAR}.zip`
- Disaggregated (energy, metals, ags): `https://www.cftc.gov/files/dea/history/fut_disagg_txt_{YEAR}.zip`

---

## CFTC Instrument Master

| Symbol | CFTC Code | Report Type | Pre-May 2023 ID | Post-May 2023 ID |
|--------|-----------|-------------|-----------------|------------------|
| ES/MES | 13874A->13874+ | Legacy | 13874A | 13874+ |
| NQ/MNQ | 209742->20974+ | Legacy | 209742 | 20974+ |
| YM/MYM | 12460A->12460+ | Legacy | 12460A | 12460+ |
| ZN | 043602 | Legacy | 043602 | -- |
| ZB | 020601 | Legacy | 020601 | -- |
| 6E | 099741 | Legacy | 099741 | -- |
| CL/MCL | 067651 | Disagg | 067651 | -- |
| GC/MGC | 088691 | Disagg | 088691 | -- |
| SI/SIL | 084691 | Disagg | 084691 | -- |
| ZC | 002602 | Disagg | 002602 | -- |
| ZS | 005602 | Disagg | 005602 | -- |
| NG | 023651 | Disagg | 023651 | -- |

**May 2023 note:** CFTC merged e-mini + standard + micro equity index
contracts into consolidated rows. The ETL must handle both pre and
post-consolidation CFTC IDs via a reverse lookup dict.

---

## Critical Column Name Differences

The two ZIP formats use **different column naming conventions**.
This mismatch is the #1 source of silent null data errors.

| Field | Legacy CSV column | Disagg CSV column |
|-------|------------------|-------------------|
| Date | `As of Date in Form YYMMDD` (spaces) | `As_of_Date_In_Form_YYMMDD` (underscores, capital I in "In") |
| Code | `CFTC Contract Market Code` (spaces) | `CFTC_Contract_Market_Code` (underscores) |
| OI | `Open Interest (All)` | `Open_Interest_All` |
| Spec long | `Noncommercial Positions-Long (All)` | `M_Money_Positions_Long_All` |
| Spec short | `Noncommercial Positions-Short (All)` | `M_Money_Positions_Short_All` |
| Comm long | `Commercial Positions-Long (All)` | `Prod_Merc_Positions_Long_All` |
| Comm short | `Commercial Positions-Short (All)` | `Prod_Merc_Positions_Short_All` |
| Swap short | -- | `Swap__Positions_Short_All` (DOUBLE underscore) |

Always verify column names from a live file before hardcoding.
Run a diagnostic script on a recent ZIP to print actual df.columns.

---

## Z-Score Methodology

```
Z = (Current Net Position - 52-Week Rolling Mean) / 52-Week Rolling Std Dev
```

- **Net Position** = Longs - Shorts for the relevant trader class
- **Window:** 52 weeks, min_periods=10
- **Tiers:**
  - Z > +1.5  -> Extreme Long  (~7% of weeks)
  - Z > +0.5  -> Long
  - Z +/-0.5  -> Neutral
  - Z < -0.5  -> Short
  - Z < -1.5  -> Extreme Short (~7% of weeks)

**Divergence Signal:**
```
Bullish  = Spec Z < -1.0 AND Comm Z > +1.0
Bearish  = Spec Z > +1.0 AND Comm Z < -1.0
Strength = |Spec Z| + |Comm Z|
```

---

## SQL Schema

```sql
-- Staging table
raw_cot: report_date, cftc_code, symbol, report_type,
         nc_long, nc_short, comm_long, comm_short,      -- Legacy
         nonrept_long, nonrept_short, open_interest,
         mm_long, mm_short, prod_long, prod_short,      -- Disagg
         swap_long, swap_short, other_long, other_short

-- Fact table (add computed columns post-build)
cot_weekly: all staging cols PLUS:
  noncomm_zscore, comm_zscore, mm_zscore, prod_zscore,
  net_noncomm, net_comm, net_nonrept,
  net_managed_money, net_producer, net_swap,
  positioning_label,
  primary_zscore    = COALESCE(noncomm_zscore, mm_zscore),
  commercial_zscore = COALESCE(comm_zscore, prod_zscore),
  net_position_primary = COALESCE(net_noncomm, net_managed_money)

-- Views
vw_cot_latest    -- latest row per instrument (KPI cards)
vw_cot_history   -- full history + 13-week smoothed Z
vw_cot_extremes  -- top 5 long + short per instrument
```

Always TRUNCATE raw_cot and cot_weekly before each upsert cycle.
MERGE alone does not fix rows that were inserted with nulls from a
prior broken parse run.

---

## ETL Build Steps

1. **Diagnose** -- download one ZIP of each type, print all column names
2. **Build parser functions** -- separate parse_legacy_zip() and
   parse_disagg_zip() with correct column maps for each format
3. **Build _safe_int()** -- must unwrap numpy scalars before conversion
4. **Build upsert with clean_val()** -- sanitize at tuple level only
5. **Add TRUNCATE** -- before both raw_cot and cot_weekly upserts
6. **Add validation** -- post-upsert null checks per instrument
7. **Add computed columns** -- primary_zscore, commercial_zscore,
   net_position_primary via ALTER TABLE + UPDATE
8. **Add --rebuild-master-only flag** -- skip download, rebuild from raw_cot

---

## Critical Code Patterns

### _safe_int -- numpy scalar unwrapping (REQUIRED)
```python
def _safe_int(val):
    if val is None: return None
    if hasattr(val, 'item'): val = val.item()  # unwrap numpy
    if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
        return None
    try:
        return int(float(str(val).replace(',', '').strip()))
    except: return None
```

### clean_val -- tuple-level sanitization for pyodbc (REQUIRED)
```python
def clean_val(col, val):
    if hasattr(val, 'item'): val = val.item()
    if val is None: return None
    if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
        return None
    if col in float_cols:
        try:
            f = float(val)
            return None if (np.isnan(f) or np.isinf(f)) else round(f, 4)
        except: return None
    if col in int_cols:
        try:
            f = float(val)
            return None if (np.isnan(f) or np.isinf(f)) else int(f)
        except: return None
    return val
```

Do NOT sanitize at the DataFrame level -- pandas silently reconverts
None back to NaN in float columns, destroying valid values.

---

## Common Bugs and Root Causes

| Bug | Root Cause | Fix |
|-----|-----------|-----|
| Disagg instruments show Z=nan | Column name mismatch -- wrong names in col_map | Run diagnostic, verify from live df.columns |
| net_managed_money NULL in SQL | numpy float64 fails int(str(val)) via ValueError | Unwrap numpy with .item() before conversion |
| pyodbc ProgrammingError float | pandas .where(other=None) stores None as NaN | Sanitize at tuple level via clean_val(), never DataFrame |
| Old null data survives ETL fix | MERGE updates rows but null->null is a no-op | TRUNCATE both tables before every upsert |
| All disagg nan, all legacy OK | Different format -> different branch in build_cot_master | Add per-instrument debug logging to verify rtype |
| Power BI table shows all rows | Missing is_latest visual-level filter | Add is_latest=TRUE as visual filter |
| Signal card wrong label | NaN is truthy: NaN or mm_zscore = NaN | Derive label from Z inside the measure, not from a separate measure |

---

## Power BI Key Patterns

### Avoid relationship issues with vw_cot_extremes
Create an Instruments bridge table (Enter Data, 12 rows) with
relationships to cot_weekly[symbol] and vw_cot_extremes[symbol].
Use Instruments[symbol] as the slicer field on the Extremes page.

### Positioning color from row-level data (not measure)
For extreme readings tables, use a calculated column on vw_cot_extremes:
```dax
Label Color = SWITCH(vw_cot_extremes[positioning_label],
    "Extreme Long", "#1E8449", "Long", "#27AE60",
    "Neutral", "#7F8C8D", "Short", "#E67E22",
    "Extreme Short", "#C0392B", "#7F8C8D")
```
Apply as Font color -> Field value. Never use the [Positioning Color]
measure for table rows -- it reads the selected instrument's current
position, not the row's own label.

### Bar chart tier coloring
Use Positioning Label Ordered as the Legend field (not symbol).
This creates one series per tier with fixed colors, avoiding the
"color greyed out" issue from trying to color individual bars.

### Distribution chart
Use 5 separate COUNT measures (one per tier) as the Y-axis values
with no X-axis category. Set each series color manually. This is
cleaner than a bucket column approach.

---

## Dashboard Color Palette

- Canvas: #1A1A2E | Cards: #16213E | Header: #0D1B2A
- Extreme Long: #1E8449 | Long: #27AE60
- Neutral: #7F8C8D
- Short: #E67E22 | Extreme Short: #C0392B
- Accent blue: #378ADD

---

## Automation

```bat
@echo off
cd /d "C:\Users\TJs PC\OneDrive\Desktop\Projects\COT Hub"
python cot_etl.py
```

Task Scheduler: Friday 6 PM, run as admin, highest privileges,
uncheck AC power requirement, stop if already running.
Power BI Service: Friday 7 PM via Personal Gateway, failure email on.

---

## Signal Hierarchy (Full Suite)

1. Macro Regime -- defines environment (highest weight)
2. COT Positioning -- confirms/contradicts directional bias (this)
3. Sentiment -- crowd psychology overlay
4. DCF Valuation -- equity position sizing

COT is never a trade entry trigger. It is a bias and size layer
stacked on top of Break of Structure setups.
