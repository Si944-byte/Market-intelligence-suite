# =============================================================================
# sentiment_etl.py
# Sentiment Hub — Market Intelligence Suite
# Folder: C:\Users\TJs PC\OneDrive\Desktop\Projects\Sentiment Hub
#
# Data sources:
#   1. VIX daily close         — FRED: VIXCLS  (from 2006-01-01)
#   2. VIX 9-day close         — FRED: VXVCLS  (from 2006-01-01)
#   3. Equity Put/Call Ratio   — CBOE archive CSV (2006-Oct 2019)
#                              + CBOE daily stats page (Oct 2019-present)
#   4. Fear & Greed Index      — CNN dataviz endpoint (graceful fail)
#
# Output: SQL Server — SentimentRegime DB
#   raw_vix, raw_putcall, raw_fear_greed
#   sentiment_daily (master fact table)
#     - fg_score:     real CNN data (~253 days history)
#     - fg_synthetic: calculated from Z-scores, full history back to 2006
#
# Schedule: Task Scheduler — Saturday 5:30 AM
# =============================================================================

import requests
import pandas as pd
import numpy as np
import pyodbc
from io import StringIO
from datetime import datetime, date
import logging
import sys
import os

# =============================================================================
# CONFIG
# =============================================================================

SQL_SERVER   = "YOUR_SQL_SERVER"
SQL_DATABASE = "SentimentRegime"
SQL_USER     = "macro_user"
SQL_PASSWORD = "YOUR_SQL_PASSWORD"
DRIVER       = "ODBC Driver 17 for SQL Server"

FRED_API_KEY = "YOUR_FRED_API_KEY"
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"

# Archive CSV: 2006-11-01 through 2019-10-04 (CBOE discontinued free daily updates)
CBOE_ARCHIVE_URL     = "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/equitypc.csv"
# Daily stats page: current data from 2019 onward
CBOE_DAILY_STATS_URL = "https://www.cboe.com/us/options/market_statistics/daily/"

CNN_FG_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

# FRED history start — 2006 to capture all historical extremes
FRED_START_DATE = "2006-01-01"

# Rolling window for Z-score normalisation (252 trading days = ~1 year)
ZSCORE_WINDOW = 252

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sentiment_etl_log.txt")

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_conn():
    conn_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
    )
    return pyodbc.connect(conn_str)


# =============================================================================
# SCHEMA CREATION  (idempotent — safe to run every time)
# =============================================================================

def create_tables(conn):
    log.info("Ensuring schema exists...")
    cursor = conn.cursor()

    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='raw_vix' AND xtype='U')
        CREATE TABLE raw_vix (
            date        DATE  NOT NULL PRIMARY KEY,
            vix_close   FLOAT NOT NULL,
            vix9d_close FLOAT NULL
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='raw_putcall' AND xtype='U')
        CREATE TABLE raw_putcall (
            date            DATE  NOT NULL PRIMARY KEY,
            equity_pc_ratio FLOAT NOT NULL
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='raw_fear_greed' AND xtype='U')
        CREATE TABLE raw_fear_greed (
            date     DATE  NOT NULL PRIMARY KEY,
            fg_score FLOAT NOT NULL
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='sentiment_daily' AND xtype='U')
        CREATE TABLE sentiment_daily (
            date                DATE        NOT NULL PRIMARY KEY,
            vix_close           FLOAT       NULL,
            vix9d_close         FLOAT       NULL,
            vix_term_ratio      FLOAT       NULL,
            equity_pc_ratio     FLOAT       NULL,
            fg_score            FLOAT       NULL,
            fg_synthetic        FLOAT       NULL,
            vix_zscore          FLOAT       NULL,
            vix_term_zscore     FLOAT       NULL,
            pc_zscore           FLOAT       NULL,
            fg_zscore           FLOAT       NULL,
            composite_zscore    FLOAT       NULL,
            sentiment_label     VARCHAR(20) NULL
        )
    """)

    # Add fg_synthetic column if it doesn't exist (for existing installs)
    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'sentiment_daily'
            AND COLUMN_NAME = 'fg_synthetic'
        )
        ALTER TABLE sentiment_daily ADD fg_synthetic FLOAT NULL
    """)

    conn.commit()
    log.info("Schema check complete.")


# =============================================================================
# FETCH HELPERS
# =============================================================================

def fetch_fred(series_id, start_date=None):
    """Pull a FRED series and return a date-indexed Series."""
    if start_date is None:
        start_date = FRED_START_DATE
    log.info(f"Fetching FRED: {series_id} from {start_date}")
    params = {
        "series_id":         series_id,
        "api_key":           FRED_API_KEY,
        "file_type":         "json",
        "observation_start": start_date,
        "observation_end":   date.today().isoformat(),
    }
    r = requests.get(FRED_BASE, params=params, timeout=30)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    records = []
    for o in obs:
        try:
            val = float(o["value"])
            records.append({"date": pd.to_datetime(o["date"]).date(), "value": val})
        except (ValueError, KeyError):
            pass  # skip "." missing-value placeholders
    if not records:
        raise ValueError(f"No valid data returned for FRED series {series_id}")
    s = pd.DataFrame(records).set_index("date")["value"]
    log.info(f"  {series_id}: {len(s)} rows, latest {s.index.max()}")
    return s


def fetch_cboe_putcall():
    """
    Two-source approach to get full P/C history:
      Source 1 — Archive CSV:    2006-11-01 through 2019-10-04
      Source 2 — Daily stats page: 2019-10-07 through present
    Both are combined, deduplicated, and returned as a date-indexed Series.
    """
    log.info("Fetching CBOE equity put/call ratio (archive + current)...")

    # ------------------------------------------------------------------
    # Source 1: Archive CSV (2006-Oct 2019)
    # ------------------------------------------------------------------
    df_archive = pd.DataFrame(columns=["date", "pc_ratio"])
    try:
        r1 = requests.get(CBOE_ARCHIVE_URL, timeout=30)
        r1.raise_for_status()
        lines = r1.text.splitlines()
        header_idx = next(
            i for i, l in enumerate(lines) if l.strip().startswith("DATE")
        )
        csv_text = "\n".join(lines[header_idx:])
        df_archive = pd.read_csv(StringIO(csv_text))
        df_archive.columns = [c.strip() for c in df_archive.columns]
        df_archive = df_archive.rename(columns={"DATE": "date", "P/C Ratio": "pc_ratio"})
        df_archive["date"] = pd.to_datetime(
            df_archive["date"], format="%m/%d/%Y", errors="coerce"
        ).dt.date
        df_archive["pc_ratio"] = pd.to_numeric(df_archive["pc_ratio"], errors="coerce")
        df_archive = df_archive.dropna(subset=["date", "pc_ratio"])
        df_archive = df_archive[df_archive["pc_ratio"] > 0][["date", "pc_ratio"]]
        log.info(f"  Archive CSV: {len(df_archive)} rows, latest {df_archive['date'].max()}")
    except Exception as e:
        log.warning(f"  Archive CSV fetch failed: {e}")

    # ------------------------------------------------------------------
    # Source 2: CBOE daily stats page (2019-present)
    # ------------------------------------------------------------------
    df_current = pd.DataFrame(columns=["date", "pc_ratio"])
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }
        r2 = requests.get(CBOE_DAILY_STATS_URL, headers=headers, timeout=30)
        r2.raise_for_status()
        tables = pd.read_html(StringIO(r2.text), flavor="html5lib")
        log.info(f"  Daily stats page: found {len(tables)} table(s)")

        matched = None
        for i, t in enumerate(tables):
            col_lower = [str(c).lower().strip() for c in t.columns]
            has_date = any("date" in c for c in col_lower)
            has_pc   = any("p/c" in c or "put/call" in c or "put call" in c for c in col_lower)
            if has_date and has_pc:
                log.info(f"  Matched table index {i}, columns: {list(t.columns)}")
                matched = t
                break

        if matched is not None:
            matched.columns = [str(c).strip() for c in matched.columns]
            col_lower_map = {c.lower(): c for c in matched.columns}
            date_col = next(
                (col_lower_map[k] for k in col_lower_map if "date" in k), None
            )
            pc_col = next(
                (col_lower_map[k] for k in col_lower_map
                 if "p/c" in k or "put/call" in k or "put call" in k), None
            )
            if date_col and pc_col:
                df_current = matched[[date_col, pc_col]].rename(
                    columns={date_col: "date", pc_col: "pc_ratio"}
                )
                df_current["date"] = pd.to_datetime(
                    df_current["date"], errors="coerce"
                ).dt.date
                df_current["pc_ratio"] = pd.to_numeric(
                    df_current["pc_ratio"], errors="coerce"
                )
                df_current = df_current.dropna(subset=["date", "pc_ratio"])
                df_current = df_current[df_current["pc_ratio"] > 0][["date", "pc_ratio"]]
                log.info(
                    f"  Daily stats current: {len(df_current)} rows, "
                    f"latest {df_current['date'].max() if not df_current.empty else 'N/A'}"
                )
            else:
                log.warning(f"  Could not identify date/P/C columns. Available: {list(matched.columns)}")
        else:
            for i, t in enumerate(tables):
                log.warning(f"  Table {i} columns: {list(t.columns)}")
            log.warning("  No equity P/C table matched — archive data only for this run.")

    except Exception as e:
        log.warning(f"  Daily stats page scrape failed: {e}")

    # ------------------------------------------------------------------
    # Combine
    # ------------------------------------------------------------------
    df_combined = pd.concat([df_archive, df_current], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset="date", keep="last")
    df_combined = df_combined.sort_values("date").reset_index(drop=True)

    if df_combined.empty:
        raise ValueError("No put/call data retrieved from either source.")

    s = df_combined.set_index("date")["pc_ratio"]
    log.info(f"  Put/Call combined: {len(s)} rows, latest {s.index.max()}")
    return s


def fetch_fear_greed():
    """
    Fetch Fear & Greed from CNN's unofficial endpoint.
    Returns a date-indexed Series. On failure logs a warning and returns None.
    Note: only ~253 days of history available from this source.
    """
    log.info("Fetching CNN Fear & Greed Index...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer":    "https://edition.cnn.com/markets/fear-and-greed",
    }
    try:
        r = requests.get(CNN_FG_URL, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        historical = data.get("fear_and_greed_historical", {}).get("data", [])
        if not historical:
            raise ValueError("No historical data in CNN response")
        records = []
        for item in historical:
            try:
                ts    = int(item["x"])
                score = float(item["y"])
                d     = date.fromtimestamp(ts / 1000)
                records.append({"date": d, "fg_score": score})
            except (KeyError, ValueError, OSError):
                continue
        if not records:
            raise ValueError("Could not parse any Fear & Greed records")
        s = pd.DataFrame(records).set_index("date")["fg_score"]
        s = s[~s.index.duplicated(keep="last")].sort_index()
        log.info(f"  Fear & Greed: {len(s)} rows, latest {s.index.max()}")
        return s
    except Exception as e:
        log.warning(f"  Fear & Greed fetch failed — skipping. Reason: {e}")
        return None


# =============================================================================
# UPSERT HELPERS
# =============================================================================

def upsert_vix(conn, vix: pd.Series, vix9d: pd.Series):
    log.info("Upserting raw_vix...")
    cursor = conn.cursor()
    all_dates = vix.index.union(vix9d.index) if not vix9d.empty else vix.index
    count = 0
    for d in all_dates:
        v  = float(vix.get(d, np.nan))
        v9 = float(vix9d.get(d, np.nan)) if not vix9d.empty else np.nan
        if np.isnan(v):
            continue
        v9_val = None if np.isnan(v9) else v9
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM raw_vix WHERE date = ?)
                UPDATE raw_vix SET vix_close = ?, vix9d_close = ? WHERE date = ?
            ELSE
                INSERT INTO raw_vix (date, vix_close, vix9d_close) VALUES (?, ?, ?)
        """, d, v, v9_val, d, d, v, v9_val)
        count += 1
    conn.commit()
    log.info(f"  raw_vix upserted: {count} rows")


def upsert_putcall(conn, pc: pd.Series):
    log.info("Upserting raw_putcall...")
    cursor = conn.cursor()
    count = 0
    for d, v in pc.items():
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM raw_putcall WHERE date = ?)
                UPDATE raw_putcall SET equity_pc_ratio = ? WHERE date = ?
            ELSE
                INSERT INTO raw_putcall (date, equity_pc_ratio) VALUES (?, ?)
        """, d, float(v), d, d, float(v))
        count += 1
    conn.commit()
    log.info(f"  raw_putcall upserted: {count} rows")


def upsert_fear_greed(conn, fg: pd.Series):
    if fg is None:
        log.info("  Skipping raw_fear_greed upsert (no data).")
        return
    log.info("Upserting raw_fear_greed...")
    cursor = conn.cursor()
    count = 0
    for d, v in fg.items():
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM raw_fear_greed WHERE date = ?)
                UPDATE raw_fear_greed SET fg_score = ? WHERE date = ?
            ELSE
                INSERT INTO raw_fear_greed (date, fg_score) VALUES (?, ?)
        """, d, float(v), d, d, float(v))
        count += 1
    conn.commit()
    log.info(f"  raw_fear_greed upserted: {count} rows")


# =============================================================================
# SENTIMENT MASTER TABLE BUILD
# =============================================================================

def zscore_rolling(series: pd.Series, window: int) -> pd.Series:
    """Rolling Z-score: (value - rolling_mean) / rolling_std."""
    mean = series.rolling(window, min_periods=60).mean()
    std  = series.rolling(window, min_periods=60).std()
    return (series - mean) / std.replace(0, np.nan)


def classify_sentiment(z) -> str:
    if pd.isna(z):
        return "Unknown"
    if z < -1.5:
        return "Extreme Fear"
    if z < -0.5:
        return "Fear"
    if z <=  0.5:
        return "Neutral"
    if z <=  1.5:
        return "Greed"
    return "Extreme Greed"


def build_sentiment_master(conn):
    log.info("Building sentiment_daily master table...")

    vix_df = pd.read_sql(
        "SELECT date, vix_close, vix9d_close FROM raw_vix ORDER BY date",
        conn, parse_dates=["date"]
    ).set_index("date")

    pc_df = pd.read_sql(
        "SELECT date, equity_pc_ratio FROM raw_putcall ORDER BY date",
        conn, parse_dates=["date"]
    ).set_index("date")

    fg_df = pd.read_sql(
        "SELECT date, fg_score FROM raw_fear_greed ORDER BY date",
        conn, parse_dates=["date"]
    ).set_index("date")

    # Union of all dates across sources
    idx = vix_df.index.union(pc_df.index).union(fg_df.index)
    df  = pd.DataFrame(index=idx)
    df.index.name = "date"

    df["vix_close"]       = vix_df["vix_close"]
    df["vix9d_close"]     = vix_df["vix9d_close"]
    df["equity_pc_ratio"] = pc_df["equity_pc_ratio"]
    df["fg_score"]        = fg_df["fg_score"] if not fg_df.empty else np.nan
    
    # Force all numeric columns to float — prevents object dtype from pyodbc
    numeric_cols = [
        "vix_close", "vix9d_close", "equity_pc_ratio", "fg_score",
        ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # VIX term structure ratio
    df["vix_term_ratio"] = df["vix9d_close"] / df["vix_close"]

    # Z-scores (sign-adjusted: positive = greed, negative = fear)
    df["vix_zscore"]      = zscore_rolling(df["vix_close"],       ZSCORE_WINDOW) * -1
    df["vix_term_zscore"] = zscore_rolling(df["vix_term_ratio"],  ZSCORE_WINDOW) * -1
    df["pc_zscore"]       = zscore_rolling(df["equity_pc_ratio"], ZSCORE_WINDOW) * -1
    df["fg_zscore"]       = zscore_rolling(df["fg_score"],        ZSCORE_WINDOW)

    # Composite: equal-weight average of available Z-scores per row
    zscore_cols = ["vix_zscore", "vix_term_zscore", "pc_zscore", "fg_zscore"]
    df["composite_zscore"] = df[zscore_cols].mean(axis=1, skipna=True)

    # -------------------------------------------------------------------------
    # Synthetic Fear & Greed (fg_synthetic)
    # Maps composite Z-score to 0-100 scale (same as Gauge Value)
    # Clamped at ±3 standard deviations before mapping
    # 0 = maximum fear, 50 = neutral, 100 = maximum greed
    # Stored separately from real CNN fg_score — never overwrites it
    # -------------------------------------------------------------------------
    raw_z = df["composite_zscore"]
    clamped = raw_z.clip(-3, 3)
    df["fg_synthetic"] = ((clamped + 3) / 6 * 100).round(2)
    log.info(
        f"  fg_synthetic: calculated for {df['fg_synthetic'].notna().sum()} rows, "
        f"range {df['fg_synthetic'].min():.1f} – {df['fg_synthetic'].max():.1f}"
    )

    df["sentiment_label"] = df["composite_zscore"].apply(classify_sentiment)

    # -------------------------------------------------------------------------
    # Upsert into sentiment_daily
    # -------------------------------------------------------------------------
    cursor = conn.cursor()
    count  = 0

    def n(val):
        return None if pd.isna(val) else float(val)

    for idx_date, row in df.iterrows():
        d = idx_date.date() if hasattr(idx_date, "date") else idx_date

        cursor.execute("""
            IF EXISTS (SELECT 1 FROM sentiment_daily WHERE date = ?)
                UPDATE sentiment_daily SET
                    vix_close       = ?, vix9d_close     = ?,
                    vix_term_ratio  = ?, equity_pc_ratio = ?,
                    fg_score        = ?, fg_synthetic     = ?,
                    vix_zscore      = ?, vix_term_zscore  = ?,
                    pc_zscore       = ?, fg_zscore         = ?,
                    composite_zscore = ?, sentiment_label = ?
                WHERE date = ?
            ELSE
                INSERT INTO sentiment_daily (
                    date, vix_close, vix9d_close, vix_term_ratio, equity_pc_ratio,
                    fg_score, fg_synthetic, vix_zscore, vix_term_zscore, pc_zscore,
                    fg_zscore, composite_zscore, sentiment_label
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        # UPDATE (12 values + WHERE)
        d,
        n(row["vix_close"]),       n(row["vix9d_close"]),
        n(row["vix_term_ratio"]),  n(row["equity_pc_ratio"]),
        n(row["fg_score"]),        n(row["fg_synthetic"]),
        n(row["vix_zscore"]),      n(row["vix_term_zscore"]),
        n(row["pc_zscore"]),       n(row["fg_zscore"]),
        n(row["composite_zscore"]), row["sentiment_label"], d,
        # INSERT (13 values)
        d,
        n(row["vix_close"]),       n(row["vix9d_close"]),
        n(row["vix_term_ratio"]),  n(row["equity_pc_ratio"]),
        n(row["fg_score"]),        n(row["fg_synthetic"]),
        n(row["vix_zscore"]),      n(row["vix_term_zscore"]),
        n(row["pc_zscore"]),       n(row["fg_zscore"]),
        n(row["composite_zscore"]), row["sentiment_label"],
        )
        count += 1

    conn.commit()
    log.info(f"  sentiment_daily upserted: {count} rows")

    # Final summary
    latest_df = df.dropna(subset=["composite_zscore"])
    if not latest_df.empty:
        latest = latest_df.iloc[-1]
        log.info("--- Latest Sentiment Reading ---")
        log.info(f"  Date:          {latest_df.index[-1].date()}")
        log.info(f"  VIX:           {latest['vix_close']:.2f}"       if pd.notna(latest["vix_close"])        else "  VIX:           N/A")
        log.info(f"  VIX9D:         {latest['vix9d_close']:.2f}"     if pd.notna(latest["vix9d_close"])      else "  VIX9D:         N/A")
        log.info(f"  P/C Ratio:     {latest['equity_pc_ratio']:.2f}" if pd.notna(latest["equity_pc_ratio"])  else "  P/C Ratio:     N/A")
        log.info(f"  Fear & Greed:  {latest['fg_score']:.1f}"        if pd.notna(latest["fg_score"])         else "  Fear & Greed:  N/A (real)")
        log.info(f"  F&G Synthetic: {latest['fg_synthetic']:.1f}"    if pd.notna(latest["fg_synthetic"])     else "  F&G Synthetic: N/A")
        log.info(f"  Composite Z:   {latest['composite_zscore']:.3f}")
        log.info(f"  Sentiment:     {latest['sentiment_label']}")
        log.info("--------------------------------")


# =============================================================================
# MAIN
# =============================================================================

def main():
    log.info("=" * 60)
    log.info("Sentiment ETL — START")
    log.info(f"Run date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    errors = []

    # ------------------------------------------------------------------
    # 1. Fetch all sources independently
    # ------------------------------------------------------------------
    vix   = pd.Series(dtype=float)
    vix9d = pd.Series(dtype=float)

    try:
        vix = fetch_fred("VIXCLS")
    except Exception as e:
        log.error(f"VIX (VIXCLS) fetch failed: {e}")
        errors.append("VIXCLS fetch failed")

    try:
        vix9d = fetch_fred("VXVCLS")
    except Exception as e:
        log.warning(f"VIX9D (VXVCLS) fetch failed — term ratio will be skipped: {e}")

    try:
        pc = fetch_cboe_putcall()
    except Exception as e:
        log.error(f"CBOE put/call fetch failed: {e}")
        errors.append("Put/Call fetch failed")
        pc = pd.Series(dtype=float)

    fg = fetch_fear_greed()

    # ------------------------------------------------------------------
    # 2. Connect and ensure schema
    # ------------------------------------------------------------------
    try:
        conn = get_conn()
        log.info("SQL Server connection established.")
    except Exception as e:
        log.critical(f"Cannot connect to SQL Server: {e}")
        sys.exit(1)

    create_tables(conn)

    # ------------------------------------------------------------------
    # 3. Upsert raw staging tables
    # ------------------------------------------------------------------
    if not vix.empty:
        try:
            upsert_vix(conn, vix, vix9d)
        except Exception as e:
            log.error(f"raw_vix upsert error: {e}")
            errors.append("raw_vix upsert failed")

    if not pc.empty:
        try:
            upsert_putcall(conn, pc)
        except Exception as e:
            log.error(f"raw_putcall upsert error: {e}")
            errors.append("raw_putcall upsert failed")

    if fg is not None:
        try:
            upsert_fear_greed(conn, fg)
        except Exception as e:
            log.error(f"raw_fear_greed upsert error: {e}")
            errors.append("raw_fear_greed upsert failed")

    # ------------------------------------------------------------------
    # 4. Build master sentiment table
    # ------------------------------------------------------------------
    try:
        build_sentiment_master(conn)
    except Exception as e:
        log.error(f"sentiment_daily build error: {e}")
        errors.append("sentiment_daily build failed")

    conn.close()

    # ------------------------------------------------------------------
    # 5. Final status
    # ------------------------------------------------------------------
    log.info("=" * 60)
    if errors:
        log.warning(f"ETL completed WITH WARNINGS: {errors}")
    else:
        log.info("Sentiment ETL — COMPLETE (no errors)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
