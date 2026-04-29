"""
liquidity_etl.py
================
Market Intelligence Suite — Liquidity Dashboard ETL
Schedule:   Saturday 8:00 PM via Task Scheduler
Author:     TJ

Data Sources:
    FRED API — Fed Balance Sheet, TGA, RRP, HY/IG Spreads,
                SOFR, Fed Funds, 10Y-FF Spread

FRED Series:
    WALCL          — Fed Total Assets (Balance Sheet) weekly $M → converted to $B
    WTREGEN        — Treasury General Account weekly $M → $B
    WLRRAL         — Reverse Repo (ON RRP) weekly $M → $B
    BAMLH0A0HYM2   — HY OAS Credit Spread (ICE BofA) daily %
    BAMLC0A0CM     — IG OAS Credit Spread (ICE BofA) daily %
    SOFR           — Secured Overnight Financing Rate daily %
    DFF            — Effective Fed Funds Rate daily %
    T10YFF         — 10Y Treasury minus Fed Funds bps daily

Usage:
    pip install requests pandas pyodbc -q
    python liquidity_etl.py

"""

import requests
import pandas as pd
import pyodbc
import logging
import sys
import os
from datetime import datetime, date

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

SQL_SERVER   = "YOUR_SQL_SERVER"
SQL_DATABASE = "LiquidityRegime"
SQL_USER     = "macro_user"
SQL_PASSWORD = "YOUR_SQL_PASSWORD"

FRED_API_KEY = "YOUR_FRED_API_KEY"
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"

# Full history load — SQL holds everything, Power BI filters to 2014+
FRED_START   = "2002-01-01"
FRED_END     = datetime.today().strftime("%Y-%m-%d")

# FRED series definitions
# (series_id, target_table, value_column, units, description)
FRED_WEEKLY = [
    ("WALCL",   "stg_FedBalanceSheet", "fed_balance_sheet_b", "millions_to_billions",
     "Fed Total Assets"),
    ("WTREGEN", "stg_FedBalanceSheet", "tga_b",               "millions_to_billions",
     "Treasury General Account"),
    ("WLRRAL",  "stg_FedBalanceSheet", "reverse_repo_b",      "millions_to_billions",
     "ON Reverse Repo"),
]

FRED_DAILY = [
    ("BAMLH0A0HYM2", "stg_CreditSpreads",  "hy_spread_pct",      "direct",
     "HY OAS Spread"),
    ("BAMLC0A0CM",   "stg_CreditSpreads",  "ig_spread_pct",      "direct",
     "IG OAS Spread"),
    ("SOFR",         "stg_MoneyMarket",    "sofr_rate_pct",      "direct",
     "SOFR Rate"),
    ("DFF",          "stg_MoneyMarket",    "fed_funds_rate_pct", "direct",
     "Effective Fed Funds Rate"),
    ("T10YFF",       "stg_MoneyMarket",    "t10y_ff_spread_bps", "pct_to_bps",
     "10Y minus Fed Funds"),
]

LOG_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "liquidity_etl_log.txt"
)

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def get_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


# ─────────────────────────────────────────────────────────────
# FRED EXTRACTION
# ─────────────────────────────────────────────────────────────

def fetch_fred(series_id: str, start: str, end: str) -> pd.DataFrame:
    """Pull a FRED series and return a clean DataFrame with date + value."""
    params = {
        "series_id":          series_id,
        "api_key":            FRED_API_KEY,
        "file_type":          "json",
        "observation_start":  start,
        "observation_end":    end,
        "sort_order":         "asc",
    }
    log.info(f"  Fetching FRED: {series_id}")
    resp = requests.get(FRED_BASE, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if "observations" not in data or not data["observations"]:
        log.warning(f"  No observations returned for {series_id}")
        return pd.DataFrame(columns=["series_date", "value"])

    rows = []
    for obs in data["observations"]:
        if obs["value"] == ".":   # FRED missing value marker
            continue
        rows.append({
            "series_date": obs["date"],
            "value":       float(obs["value"])
        })

    df = pd.DataFrame(rows)
    df["series_date"] = pd.to_datetime(df["series_date"]).dt.date
    log.info(f"  {series_id}: {len(df)} observations ({df['series_date'].min()} → {df['series_date'].max()})")
    return df


def apply_unit_conversion(df: pd.DataFrame, conversion: str) -> pd.DataFrame:
    """Apply unit conversion to value column."""
    if conversion == "millions_to_billions":
        df["value"] = df["value"] / 1000.0
    elif conversion == "pct_to_bps":
        # T10YFF comes as percentage points (e.g. 1.30 = 130 bps)
        df["value"] = df["value"] * 100.0
    # "direct" — no conversion needed
    return df


# ─────────────────────────────────────────────────────────────
# UPSERT LOGIC
# ─────────────────────────────────────────────────────────────

def upsert_fed_balance_sheet(conn, walcl_df: pd.DataFrame,
                              tga_df: pd.DataFrame,
                              rrp_df: pd.DataFrame) -> int:
    """
    Merge all three weekly series into stg_FedBalanceSheet.
    Uses WALCL dates as the spine — TGA and RRP joined in.
    Returns number of rows upserted.
    """
    # Build combined DataFrame on WALCL date spine
    df = walcl_df.rename(columns={"value": "fed_balance_sheet_b"})
    df = df.merge(
        tga_df.rename(columns={"value": "tga_b"}),
        on="series_date", how="left"
    )
    df = df.merge(
        rrp_df.rename(columns={"value": "reverse_repo_b"}),
        on="series_date", how="left"
    )
    df = df.dropna(subset=["fed_balance_sheet_b"])

    cursor = conn.cursor()
    upserted = 0

    for _, row in df.iterrows():
        cursor.execute("""
            MERGE dbo.stg_FedBalanceSheet AS target
            USING (SELECT ? AS series_date) AS source
                ON target.series_date = source.series_date
            WHEN MATCHED THEN
                UPDATE SET
                    fed_balance_sheet_b = ?,
                    tga_b               = ?,
                    reverse_repo_b      = ?,
                    loaded_at           = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (series_date, fed_balance_sheet_b, tga_b, reverse_repo_b)
                VALUES (?, ?, ?, ?);
        """,
            str(row["series_date"]),
            round(float(row["fed_balance_sheet_b"]), 3),
            round(float(row["tga_b"]), 3) if pd.notna(row["tga_b"]) else None,
            round(float(row["reverse_repo_b"]), 3) if pd.notna(row["reverse_repo_b"]) else None,
            str(row["series_date"]),
            round(float(row["fed_balance_sheet_b"]), 3),
            round(float(row["tga_b"]), 3) if pd.notna(row["tga_b"]) else None,
            round(float(row["reverse_repo_b"]), 3) if pd.notna(row["reverse_repo_b"]) else None,
        )
        upserted += 1

    conn.commit()
    return upserted


def upsert_credit_spreads(conn, hy_df: pd.DataFrame,
                           ig_df: pd.DataFrame) -> int:
    """Merge HY and IG spreads into stg_CreditSpreads."""
    df = hy_df.rename(columns={"value": "hy_spread_pct"})
    df = df.merge(
        ig_df.rename(columns={"value": "ig_spread_pct"}),
        on="series_date", how="outer"
    ).sort_values("series_date")

    cursor = conn.cursor()
    upserted = 0

    for _, row in df.iterrows():
        cursor.execute("""
            MERGE dbo.stg_CreditSpreads AS target
            USING (SELECT ? AS series_date) AS source
                ON target.series_date = source.series_date
            WHEN MATCHED THEN
                UPDATE SET
                    hy_spread_pct = ?,
                    ig_spread_pct = ?,
                    loaded_at     = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (series_date, hy_spread_pct, ig_spread_pct)
                VALUES (?, ?, ?);
        """,
            str(row["series_date"]),
            round(float(row["hy_spread_pct"]), 4) if pd.notna(row["hy_spread_pct"]) else None,
            round(float(row["ig_spread_pct"]), 4) if pd.notna(row["ig_spread_pct"]) else None,
            str(row["series_date"]),
            round(float(row["hy_spread_pct"]), 4) if pd.notna(row["hy_spread_pct"]) else None,
            round(float(row["ig_spread_pct"]), 4) if pd.notna(row["ig_spread_pct"]) else None,
        )
        upserted += 1

    conn.commit()
    return upserted


def upsert_money_market(conn, sofr_df: pd.DataFrame,
                         dff_df: pd.DataFrame,
                         t10yff_df: pd.DataFrame) -> int:
    """Merge SOFR, Fed Funds, and yield curve into stg_MoneyMarket."""
    # DFF as spine — most complete daily series
    df = dff_df.rename(columns={"value": "fed_funds_rate_pct"})
    df = df.merge(
        sofr_df.rename(columns={"value": "sofr_rate_pct"}),
        on="series_date", how="left"
    )
    df = df.merge(
        t10yff_df.rename(columns={"value": "t10y_ff_spread_bps"}),
        on="series_date", how="left"
    )
    df = df.dropna(subset=["fed_funds_rate_pct"])

    cursor = conn.cursor()
    upserted = 0

    for _, row in df.iterrows():
        cursor.execute("""
            MERGE dbo.stg_MoneyMarket AS target
            USING (SELECT ? AS series_date) AS source
                ON target.series_date = source.series_date
            WHEN MATCHED THEN
                UPDATE SET
                    sofr_rate_pct      = ?,
                    fed_funds_rate_pct = ?,
                    t10y_ff_spread_bps = ?,
                    loaded_at          = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (series_date, sofr_rate_pct, fed_funds_rate_pct, t10y_ff_spread_bps)
                VALUES (?, ?, ?, ?);
        """,
            str(row["series_date"]),
            round(float(row["sofr_rate_pct"]), 4) if pd.notna(row["sofr_rate_pct"]) else None,
            round(float(row["fed_funds_rate_pct"]), 4),
            round(float(row["t10y_ff_spread_bps"]), 2) if pd.notna(row["t10y_ff_spread_bps"]) else None,
            str(row["series_date"]),
            round(float(row["sofr_rate_pct"]), 4) if pd.notna(row["sofr_rate_pct"]) else None,
            round(float(row["fed_funds_rate_pct"]), 4),
            round(float(row["t10y_ff_spread_bps"]), 2) if pd.notna(row["t10y_ff_spread_bps"]) else None,
        )
        upserted += 1

    conn.commit()
    return upserted


# ─────────────────────────────────────────────────────────────
# DATE DIMENSION
# ─────────────────────────────────────────────────────────────

def build_dim_date(conn):
    """Generate DimDate from 2002-01-01 through 2035-12-31."""
    cursor = conn.cursor()

    # Check if already populated
    cursor.execute("SELECT COUNT(*) FROM dbo.DimDate")
    count = cursor.fetchone()[0]
    if count > 0:
        log.info(f"  DimDate already populated ({count} rows) — skipping")
        return

    log.info("  Building DimDate (2002–2035)...")
    dates = pd.date_range("2002-01-01", "2035-12-31", freq="D")
    batch = []

    for d in dates:
        batch.append((
            int(d.strftime("%Y%m%d")),       # date_key
            d.date(),                         # full_date
            d.year,                           # year_num
            d.quarter,                        # quarter_num
            d.month,                          # month_num
            d.strftime("%B"),                 # month_name
            d.strftime("%b"),                 # month_short
            int(d.strftime("%V")),            # week_num (ISO)
            d.isoweekday(),                   # day_of_week (1=Mon)
            d.strftime("%A"),                 # day_name
            1 if d.isoweekday() <= 5 else 0, # is_weekday
            d.strftime("%Y-%m"),              # year_month
            d.year,                           # fiscal_year
            f"Q{d.quarter} {d.year}",         # quarter_label
        ))

    cursor.executemany("""
        INSERT INTO dbo.DimDate (
            date_key, full_date, year_num, quarter_num, month_num,
            month_name, month_short, week_num, day_of_week, day_name,
            is_weekday, year_month, fiscal_year, quarter_label
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, batch)

    conn.commit()
    log.info(f"  DimDate: {len(batch)} rows inserted")


# ─────────────────────────────────────────────────────────────
# VERIFICATION
# ─────────────────────────────────────────────────────────────

def verify(conn):
    """Quick sanity check — log latest row from each staging table and regime view."""
    cursor = conn.cursor()

    checks = [
        ("stg_FedBalanceSheet", "series_date",
         "SELECT TOP 1 series_date, fed_balance_sheet_b, tga_b, reverse_repo_b "
         "FROM dbo.stg_FedBalanceSheet ORDER BY series_date DESC"),

        ("stg_CreditSpreads", "series_date",
         "SELECT TOP 1 series_date, hy_spread_pct, ig_spread_pct "
         "FROM dbo.stg_CreditSpreads ORDER BY series_date DESC"),

        ("stg_MoneyMarket", "series_date",
         "SELECT TOP 1 series_date, sofr_rate_pct, fed_funds_rate_pct, t10y_ff_spread_bps "
         "FROM dbo.stg_MoneyMarket ORDER BY series_date DESC"),

        ("vw_LiquidityRegime", "series_date",
         "SELECT TOP 1 series_date, net_liquidity_b, composite_score, gauge_value, "
         "liquidity_regime, trade_bias "
         "FROM dbo.vw_LiquidityRegime ORDER BY series_date DESC"),
    ]

    log.info("")
    log.info("─" * 60)
    log.info("VERIFICATION")
    log.info("─" * 60)

    for table, _, query in checks:
        try:
            cursor.execute(query)
            row = cursor.fetchone()
            cols = [d[0] for d in cursor.description]
            if row:
                result = " | ".join(f"{c}: {v}" for c, v in zip(cols, row))
                log.info(f"  ✅ {table}: {result}")
            else:
                log.warning(f"  ⚠️  {table}: No rows returned")
        except Exception as e:
            log.error(f"  ❌ {table}: {e}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    run_start = datetime.now()
    log.info("=" * 60)
    log.info("LIQUIDITY ETL — START")
    log.info(f"Run time:  {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"FRED range: {FRED_START} → {FRED_END}")
    log.info("=" * 60)

    # ── 1. Connect ────────────────────────────────────────────
    log.info("Connecting to SQL Server...")
    try:
        conn = get_connection()
        log.info(f"  ✅ Connected to {SQL_SERVER} / {SQL_DATABASE}")
    except Exception as e:
        log.error(f"  ❌ Connection failed: {e}")
        sys.exit(1)

    # ── 2. DimDate ────────────────────────────────────────────
    log.info("")
    log.info("Building DimDate...")
    build_dim_date(conn)

    # ── 3. Fed Balance Sheet (weekly) ─────────────────────────
    log.info("")
    log.info("Fetching Fed Balance Sheet components (weekly)...")
    try:
        walcl = fetch_fred("WALCL",   FRED_START, FRED_END)
        tga   = fetch_fred("WTREGEN", FRED_START, FRED_END)
        rrp   = fetch_fred("WLRRAL",  FRED_START, FRED_END)

        walcl = apply_unit_conversion(walcl, "millions_to_billions")
        tga   = apply_unit_conversion(tga,   "millions_to_billions")
        rrp   = apply_unit_conversion(rrp,   "millions_to_billions")

        n = upsert_fed_balance_sheet(conn, walcl, tga, rrp)
        log.info(f"  ✅ stg_FedBalanceSheet: {n} rows upserted")
    except Exception as e:
        log.error(f"  ❌ Fed Balance Sheet failed: {e}")

    # ── 4. Credit Spreads (daily) ─────────────────────────────
    log.info("")
    log.info("Fetching Credit Spreads (daily)...")
    try:
        hy = fetch_fred("BAMLH0A0HYM2", FRED_START, FRED_END)
        ig = fetch_fred("BAMLC0A0CM",   FRED_START, FRED_END)

        n = upsert_credit_spreads(conn, hy, ig)
        log.info(f"  ✅ stg_CreditSpreads: {n} rows upserted")
    except Exception as e:
        log.error(f"  ❌ Credit Spreads failed: {e}")

    # ── 5. Money Market / Yield Curve (daily) ─────────────────
    log.info("")
    log.info("Fetching Money Market data (daily)...")
    try:
        sofr   = fetch_fred("SOFR",   FRED_START, FRED_END)
        dff    = fetch_fred("DFF",    FRED_START, FRED_END)
        t10yff = fetch_fred("T10YFF", FRED_START, FRED_END)

        t10yff = apply_unit_conversion(t10yff, "pct_to_bps")

        n = upsert_money_market(conn, sofr, dff, t10yff)
        log.info(f"  ✅ stg_MoneyMarket: {n} rows upserted")
    except Exception as e:
        log.error(f"  ❌ Money Market failed: {e}")

# ── 6. SPX Price Data (daily — for forward return analysis) ──────
    log.info("")
    log.info("Fetching SPX price data (daily)...")
    try:
        spx = fetch_fred("SP500", FRED_START, FRED_END)
        spx = spx.dropna(subset=["value"])

        cursor = conn.cursor()
        upserted = 0
        for _, row in spx.iterrows():
            cursor.execute("""
                MERGE dbo.stg_SPX AS target
                USING (SELECT ? AS series_date) AS source
                    ON target.series_date = source.series_date
                WHEN MATCHED THEN
                    UPDATE SET spx_close = ?, loaded_at = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (series_date, spx_close)
                    VALUES (?, ?);
            """,
                str(row["series_date"]),
                round(float(row["value"]), 2),
                str(row["series_date"]),
                round(float(row["value"]), 2),
            )
            upserted += 1
        conn.commit()
        log.info(f"  ✅ stg_SPX: {upserted} rows upserted")
    except Exception as e:
        log.error(f"  ❌ SPX data failed: {e}")


    # ── 7. Verify ─────────────────────────────────────────────
    verify(conn)

    # ── 8. Done ───────────────────────────────────────────────
    conn.close()
    elapsed = (datetime.now() - run_start).seconds
    log.info("")
    log.info("=" * 60)
    log.info(f"LIQUIDITY ETL — COMPLETE ({elapsed}s)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
