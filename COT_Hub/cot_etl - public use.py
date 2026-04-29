"""
cot_etl.py — COT Positioning Dashboard ETL
Market Intelligence Suite | COT Hub

Pulls CFTC Commitment of Traders data for 12 futures markets,
calculates net positioning, Z-scores, and positioning labels,
and upserts into SQL Server (COTRegime database).

Schedule: Friday 6:00 PM (after CFTC releases at ~3:30 PM ET)
"""

import os
import io
import zipfile
import requests
import pandas as pd
import numpy as np
import pyodbc
import warnings
import logging
from datetime import datetime, date

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

SQL_SERVER   = "YOUR_SQL_SERVER"
SQL_DATABASE = "COTRegime"
SQL_USER     = "YOUR_SQL_USER"
SQL_PASSWORD = "YOUR_SQL_PASSWORD"
DRIVER       = "ODBC Driver 17 for SQL Server"

LOG_FILE     = "cot_etl_log.txt"
START_YEAR   = 2006
ZSCORE_WINDOW = 52   # weeks (1 year rolling)

# ─────────────────────────────────────────────
# INSTRUMENT MASTER
# Keyed by CFTC contract code.
# consolidated_id: post-May 2023 the CFTC merged e-mini + standard
# into a single consolidated row — we prefer that when available.
# ─────────────────────────────────────────────

INSTRUMENTS = {
    # ── Legacy report (equity indices, rates, FX) ──────────────────
    "13874A": {
        "symbol": "ES",  "name": "E-Mini S&P 500",
        "report": "legacy",  "group": "Equity Index",
        "consolidated_id": "13874+",
    },
    "209742": {
        "symbol": "NQ",  "name": "Nasdaq-100 Mini",
        "report": "legacy",  "group": "Equity Index",
        "consolidated_id": "20974+",
    },
    "12460A": {
        "symbol": "YM",  "name": "DJIA Mini",
        "report": "legacy",  "group": "Equity Index",
        "consolidated_id": "12460+",
    },
    "043602": {
        "symbol": "ZN",  "name": "10-Year T-Note",
        "report": "legacy",  "group": "Rates",
        "consolidated_id": None,
    },
    "020601": {
        "symbol": "ZB",  "name": "30-Year T-Bond",
        "report": "legacy",  "group": "Rates",
        "consolidated_id": None,
    },
    "099741": {
        "symbol": "6E",  "name": "Euro FX",
        "report": "legacy",  "group": "FX",
        "consolidated_id": None,
    },
    # ── Disaggregated report (energy, metals, ags) ─────────────────
    "067651": {
        "symbol": "CL",  "name": "Crude Oil",
        "report": "disagg",  "group": "Energy",
        "consolidated_id": None,
    },
    "088691": {
        "symbol": "GC",  "name": "Gold",
        "report": "disagg",  "group": "Metals",
        "consolidated_id": None,
    },
    "084691": {
        "symbol": "SI",  "name": "Silver",
        "report": "disagg",  "group": "Metals",
        "consolidated_id": None,
    },
    "005602": {
        "symbol": "ZS",  "name": "Soybeans",
        "report": "disagg",  "group": "Ags",
        "consolidated_id": None,
    },
    "002602": {
        "symbol": "ZC",  "name": "Corn",
        "report": "disagg",  "group": "Ags",
        "consolidated_id": None,
    },
    "023651": {
        "symbol": "NG",  "name": "Natural Gas",
        "report": "disagg",  "group": "Energy",
        "consolidated_id": None,
    },
}

# Build reverse lookup: consolidated_id → base CFTC code
# Used to map post-2023 consolidated rows back to the instrument entry
CONSOLIDATED_LOOKUP = {
    v["consolidated_id"]: k
    for k, v in INSTRUMENTS.items()
    if v["consolidated_id"]
}

# ─────────────────────────────────────────────
# CFTC DOWNLOAD URLS
# Pattern confirmed from CFTC HistoricalCompressed page.
# ─────────────────────────────────────────────

def legacy_url(year):
    return f"https://www.cftc.gov/files/dea/history/deacot{year}.zip"

def disagg_url(year):
    return f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# SQL CONNECTION
# ─────────────────────────────────────────────

def get_conn():
    conn_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

# ─────────────────────────────────────────────
# SCHEMA CREATION (idempotent)
# ─────────────────────────────────────────────

def create_tables(conn):
    cursor = conn.cursor()

    # Raw staging — one row per instrument per report date
    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables WHERE name = 'raw_cot'
        )
        CREATE TABLE raw_cot (
            report_date         DATE        NOT NULL,
            cftc_code           VARCHAR(10) NOT NULL,
            symbol              VARCHAR(10) NOT NULL,
            instrument_name     VARCHAR(100) NOT NULL,
            report_type         VARCHAR(10) NOT NULL,  -- 'legacy' or 'disagg'
            -- Legacy columns (Non-Commercial / Commercial / Non-Reportable)
            nc_long             BIGINT NULL,
            nc_short            BIGINT NULL,
            comm_long           BIGINT NULL,
            comm_short          BIGINT NULL,
            nonrept_long        BIGINT NULL,
            nonrept_short       BIGINT NULL,
            open_interest       BIGINT NULL,
            -- Disaggregated columns (Managed Money / Producer / Swap / Other)
            mm_long             BIGINT NULL,
            mm_short            BIGINT NULL,
            prod_long           BIGINT NULL,
            prod_short          BIGINT NULL,
            swap_long           BIGINT NULL,
            swap_short          BIGINT NULL,
            other_long          BIGINT NULL,
            other_short         BIGINT NULL,
            CONSTRAINT PK_raw_cot PRIMARY KEY (report_date, cftc_code)
        )
    """)

    # Master fact table — derived metrics + Z-scores
    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables WHERE name = 'cot_weekly'
        )
        CREATE TABLE cot_weekly (
            report_date             DATE        NOT NULL,
            cftc_code               VARCHAR(10) NOT NULL,
            symbol                  VARCHAR(10) NOT NULL,
            instrument_name         VARCHAR(100) NOT NULL,
            instrument_group        VARCHAR(20) NOT NULL,
            report_type             VARCHAR(10) NOT NULL,
            open_interest           BIGINT NULL,
            -- Legacy net positions
            net_noncomm             BIGINT NULL,   -- Large specs net
            net_comm                BIGINT NULL,   -- Commercials net
            net_nonrept             BIGINT NULL,   -- Small specs net
            -- Disaggregated net positions
            net_managed_money       BIGINT NULL,   -- Hedge funds / CTAs
            net_producer            BIGINT NULL,   -- Producers / hedgers
            net_swap                BIGINT NULL,   -- Swap dealers
            -- Normalized (% of open interest)
            noncomm_pct_oi          FLOAT NULL,
            comm_pct_oi             FLOAT NULL,
            mm_pct_oi               FLOAT NULL,
            prod_pct_oi             FLOAT NULL,
            -- 52-week rolling Z-scores
            noncomm_zscore          FLOAT NULL,
            comm_zscore             FLOAT NULL,
            mm_zscore               FLOAT NULL,
            prod_zscore             FLOAT NULL,
            -- Positioning label (5-tier, based on primary spec Z-score)
            positioning_label       VARCHAR(20) NULL,
            CONSTRAINT PK_cot_weekly PRIMARY KEY (report_date, cftc_code)
        )
    """)

    conn.commit()
    log.info("Schema verified (raw_cot, cot_weekly)")

# ─────────────────────────────────────────────
# DOWNLOAD + PARSE CFTC ZIP
# ─────────────────────────────────────────────

def download_zip(url):
    """Download a CFTC ZIP and return as BytesIO. Returns None on failure."""
    try:
        log.info(f"  Downloading: {url}")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return io.BytesIO(r.content)
    except Exception as e:
        log.warning(f"  Download failed: {url} — {e}")
        return None


def parse_legacy_zip(zip_bytes):
    """
    Parse a Legacy COT ZIP file.
    Returns a filtered DataFrame with only our target instruments.
    Handles both pre-2023 (e-mini IDs) and post-2023 (consolidated IDs).
    """
    with zipfile.ZipFile(zip_bytes) as zf:
        # The text file inside is usually 'annual.txt' or 'deacot{year}.txt'
        txt_name = [n for n in zf.namelist() if n.lower().endswith(".txt")]
        if not txt_name:
            log.warning("  No .txt file found in legacy ZIP")
            return pd.DataFrame()
        with zf.open(txt_name[0]) as f:
            df = pd.read_csv(f, low_memory=False)

    # Standardize column names (strip whitespace)
    df.columns = [c.strip() for c in df.columns]

    # Build set of all CFTC codes we want (base + consolidated)
    target_codes = set(INSTRUMENTS.keys()) | set(CONSOLIDATED_LOOKUP.keys())
    # Filter only legacy instruments
    legacy_codes = {k for k, v in INSTRUMENTS.items() if v["report"] == "legacy"}
    legacy_consolidated = {v["consolidated_id"] for v in INSTRUMENTS.values()
                           if v["report"] == "legacy" and v["consolidated_id"]}
    all_legacy_codes = legacy_codes | legacy_consolidated

    # CFTC code column name varies — try common names
    code_col = None
    for candidate in ["CFTC_Contract_Market_Code", "CFTC Contract Market Code",
                       "Contract_Market_Code", "cftc_contract_market_code"]:
        if candidate in df.columns:
            code_col = candidate
            break
    if code_col is None:
        log.warning("  Could not identify CFTC code column in legacy file")
        log.info(f"  Available columns: {list(df.columns[:10])}")
        return pd.DataFrame()

    df[code_col] = df[code_col].astype(str).str.strip()
    df = df[df[code_col].isin(all_legacy_codes)].copy()

    if df.empty:
        return pd.DataFrame()

    # Parse date — column is usually 'As_of_Date_in_Form_YYMMDD'
    date_col = None
    for candidate in ["As of Date in Form YYMMDD",
                       "As of Date in Form YYYY-MM-DD",
                       "As_of_Date_in_Form_YYMMDD",
                       "Report_Date_as_YYYY-MM-DD"]:
        if candidate in df.columns:
            date_col = candidate
            break
    if date_col is None:
        log.warning("  Could not identify date column in legacy file")
        return pd.DataFrame()

    # Parse YYMMDD format → date
    def parse_cot_date(val):
        val = str(val).strip()
        try:
            if len(val) == 6:   # YYMMDD
                return datetime.strptime(val, "%y%m%d").date()
            elif len(val) == 8:  # YYYYMMDD
                return datetime.strptime(val, "%Y%m%d").date()
            else:
                return pd.to_datetime(val).date()
        except Exception:
            return None

    df["report_date"] = df[date_col].apply(parse_cot_date)
    df = df[df["report_date"].notna()].copy()

    # Map consolidated IDs back to base instrument entry
    def resolve_cftc_code(code):
        if code in INSTRUMENTS:
            return code
        if code in CONSOLIDATED_LOOKUP:
            return CONSOLIDATED_LOOKUP[code]
        return code

    df["cftc_code"] = df[code_col].apply(resolve_cftc_code)

    # Column map for long/short positions (Legacy format)
    # Actual column names confirmed from CFTC annual.txt (space/hyphen format)
    col_map = {
        "Noncommercial Positions-Long (All)":  "nc_long",
        "Noncommercial Positions-Short (All)": "nc_short",
        "Commercial Positions-Long (All)":     "comm_long",
        "Commercial Positions-Short (All)":    "comm_short",
        "Nonreportable Positions-Long (All)":  "nonrept_long",
        "Nonreportable Positions-Short (All)": "nonrept_short",
        "Open Interest (All)":                 "open_interest",
    }

    result_rows = []
    for _, row in df.iterrows():
        cftc_code = row["cftc_code"]
        if cftc_code not in INSTRUMENTS:
            continue
        meta = INSTRUMENTS[cftc_code]
        r = {
            "report_date":    row["report_date"],
            "cftc_code":      cftc_code,
            "symbol":         meta["symbol"],
            "instrument_name": meta["name"],
            "report_type":    "legacy",
        }
        for src, dst in col_map.items():
            r[dst] = _safe_int(row.get(src))
        # Disaggregated columns null for legacy
        for col in ["mm_long", "mm_short", "prod_long", "prod_short",
                    "swap_long", "swap_short", "other_long", "other_short"]:
            r[col] = None
        result_rows.append(r)

    return pd.DataFrame(result_rows)


def parse_disagg_zip(zip_bytes):
    """
    Parse a Disaggregated COT ZIP file.
    Returns a filtered DataFrame with only our target instruments.
    """
    with zipfile.ZipFile(zip_bytes) as zf:
        txt_name = [n for n in zf.namelist() if n.lower().endswith(".txt")]
        if not txt_name:
            log.warning("  No .txt file found in disagg ZIP")
            return pd.DataFrame()
        with zf.open(txt_name[0]) as f:
            df = pd.read_csv(f, low_memory=False)

    df.columns = [c.strip() for c in df.columns]

    disagg_codes = {k for k, v in INSTRUMENTS.items() if v["report"] == "disagg"}

    code_col = None
    for candidate in ["CFTC_Contract_Market_Code",   # confirmed disagg format
                       "CFTC Contract Market Code",   # legacy format
                       "Contract_Market_Code"]:
        if candidate in df.columns:
            code_col = candidate
            break
    if code_col is None:
        log.warning(f"  Could not identify CFTC code column in disagg file. Cols: {list(df.columns[:8])}")
        return pd.DataFrame()

    df[code_col] = df[code_col].astype(str).str.strip()
    df = df[df[code_col].isin(disagg_codes)].copy()

    if df.empty:
        return pd.DataFrame()

    date_col = None
    for candidate in ["As_of_Date_In_Form_YYMMDD",    # confirmed disagg format
                       "As_of_Date_in_Form_YYMMDD",    # alternate casing
                       "Report_Date_as_YYYY-MM-DD",
                       "As of Date in Form YYMMDD"]:
        if candidate in df.columns:
            date_col = candidate
            break
    if date_col is None:
        log.warning(f"  Could not identify date column in disagg file. Cols: {list(df.columns[:5])}")
        return pd.DataFrame()

    def parse_cot_date(val):
        val = str(val).strip()
        try:
            if len(val) == 6:
                return datetime.strptime(val, "%y%m%d").date()
            elif len(val) == 8:
                return datetime.strptime(val, "%Y%m%d").date()
            else:
                return pd.to_datetime(val).date()
        except Exception:
            return None

    df["report_date"] = df[date_col].apply(parse_cot_date)
    df = df[df["report_date"].notna()].copy()

    # Disaggregated column map — exact names confirmed from CFTC f_year.txt
    col_map = {
        "Open_Interest_All":            "open_interest",
        "M_Money_Positions_Long_All":   "mm_long",
        "M_Money_Positions_Short_All":  "mm_short",
        "Prod_Merc_Positions_Long_All": "prod_long",
        "Prod_Merc_Positions_Short_All":"prod_short",
        "Swap_Positions_Long_All":      "swap_long",
        "Swap__Positions_Short_All":    "swap_short",   # double underscore confirmed
        "Other_Rept_Positions_Long_All":"other_long",
        "Other_Rept_Positions_Short_All":"other_short",
    }

    result_rows = []
    for _, row in df.iterrows():
        cftc_code = str(row[code_col]).strip()
        if cftc_code not in INSTRUMENTS:
            continue
        meta = INSTRUMENTS[cftc_code]
        r = {
            "report_date":    row["report_date"],
            "cftc_code":      cftc_code,
            "symbol":         meta["symbol"],
            "instrument_name": meta["name"],
            "report_type":    "disagg",
        }
        for src, dst in col_map.items():
            r[dst] = _safe_int(row.get(src))
        # Legacy columns null for disagg
        for col in ["nc_long", "nc_short", "comm_long", "comm_short",
                    "nonrept_long", "nonrept_short"]:
            r[col] = None
        result_rows.append(r)

    return pd.DataFrame(result_rows)


def _safe_int(val):
    """Convert to int, return None on failure."""
    if val is None:
        return None
    # Unwrap numpy scalars first
    if hasattr(val, "item"):
        val = val.item()
    # Check for float NaN/inf before any conversion
    if isinstance(val, float):
        if np.isnan(val) or np.isinf(val):
            return None
        return int(val)
    try:
        # Handle string representations
        cleaned = str(val).replace(",", "").strip()
        if cleaned.lower() in ("nan", "inf", "-inf", "none", ""):
            return None
        return int(float(cleaned))
    except Exception:
        return None

# ─────────────────────────────────────────────
# FETCH ALL YEARS
# ─────────────────────────────────────────────

def fetch_all_years():
    """
    Download and parse all CFTC annual ZIPs from START_YEAR to current year.
    Returns a single merged DataFrame of all instruments across all years.
    """
    current_year = datetime.now().year
    years = list(range(START_YEAR, current_year + 1))

    all_frames = []

    log.info("── Fetching Legacy COT ZIPs ──────────────────────────────")
    for year in years:
        url = legacy_url(year)
        zb = download_zip(url)
        if zb:
            df = parse_legacy_zip(zb)
            if not df.empty:
                log.info(f"  {year} legacy: {len(df)} rows for {df['symbol'].nunique()} instruments")
                all_frames.append(df)
            else:
                log.warning(f"  {year} legacy: parsed empty (no matching instruments)")

    log.info("── Fetching Disaggregated COT ZIPs ───────────────────────")
    for year in years:
        url = disagg_url(year)
        zb = download_zip(url)
        if zb:
            df = parse_disagg_zip(zb)
            if not df.empty:
                log.info(f"  {year} disagg: {len(df)} rows for {df['symbol'].nunique()} instruments")
                all_frames.append(df)
            else:
                log.warning(f"  {year} disagg: parsed empty (no matching instruments)")

    if not all_frames:
        log.error("No data frames collected — aborting")
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)

    # Deduplicate — keep latest version of any (date, cftc_code) pair
    combined = combined.sort_values("report_date")
    combined = combined.drop_duplicates(subset=["report_date", "cftc_code"], keep="last")

    log.info(f"Total raw rows after dedup: {len(combined)}")
    return combined

# ─────────────────────────────────────────────
# UPSERT raw_cot
# ─────────────────────────────────────────────

def upsert_raw_cot(conn, df):
    if df.empty:
        return
    cursor = conn.cursor()
    # Truncate first — ensures stale rows with null position data are wiped.
    # MERGE alone won't fix rows that were inserted with nulls; we need clean slate.
    cursor.execute("TRUNCATE TABLE raw_cot")
    conn.commit()
    log.info("raw_cot truncated — reinserting fresh data")
    sql = """
        MERGE raw_cot AS target
        USING (VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?,
            ?, ?, ?, ?, ?, ?, ?, ?
        )) AS source (
            report_date, cftc_code, symbol, instrument_name, report_type,
            nc_long, nc_short, comm_long, comm_short, nonrept_long, nonrept_short,
            open_interest,
            mm_long, mm_short, prod_long, prod_short, swap_long, swap_short,
            other_long, other_short
        )
        ON target.report_date = source.report_date
        AND target.cftc_code  = source.cftc_code
        WHEN MATCHED THEN UPDATE SET
            symbol           = source.symbol,
            instrument_name  = source.instrument_name,
            report_type      = source.report_type,
            nc_long          = source.nc_long,
            nc_short         = source.nc_short,
            comm_long        = source.comm_long,
            comm_short       = source.comm_short,
            nonrept_long     = source.nonrept_long,
            nonrept_short    = source.nonrept_short,
            open_interest    = source.open_interest,
            mm_long          = source.mm_long,
            mm_short         = source.mm_short,
            prod_long        = source.prod_long,
            prod_short       = source.prod_short,
            swap_long        = source.swap_long,
            swap_short       = source.swap_short,
            other_long       = source.other_long,
            other_short      = source.other_short
        WHEN NOT MATCHED THEN INSERT (
            report_date, cftc_code, symbol, instrument_name, report_type,
            nc_long, nc_short, comm_long, comm_short, nonrept_long, nonrept_short,
            open_interest,
            mm_long, mm_short, prod_long, prod_short, swap_long, swap_short,
            other_long, other_short
        ) VALUES (
            source.report_date, source.cftc_code, source.symbol,
            source.instrument_name, source.report_type,
            source.nc_long, source.nc_short, source.comm_long, source.comm_short,
            source.nonrept_long, source.nonrept_short,
            source.open_interest,
            source.mm_long, source.mm_short, source.prod_long, source.prod_short,
            source.swap_long, source.swap_short, source.other_long, source.other_short
        );
    """
    cols = [
        "report_date", "cftc_code", "symbol", "instrument_name", "report_type",
        "nc_long", "nc_short", "comm_long", "comm_short", "nonrept_long", "nonrept_short",
        "open_interest",
        "mm_long", "mm_short", "prod_long", "prod_short", "swap_long", "swap_short",
        "other_long", "other_short",
    ]
    rows = [tuple(row[c] for c in cols) for _, row in df[cols].iterrows()]
    cursor.executemany(sql, rows)
    conn.commit()
    log.info(f"raw_cot upserted: {len(rows)} rows")

# ─────────────────────────────────────────────
# BUILD cot_weekly MASTER TABLE
# ─────────────────────────────────────────────

def zscore_rolling(series, window=52):
    """52-week rolling Z-score. min_periods=10 to allow partial history."""
    mean = series.rolling(window, min_periods=10).mean()
    std  = series.rolling(window, min_periods=10).std()
    return (series - mean) / std.replace(0, np.nan)


def classify_positioning(z):
    """
    5-tier label based on primary spec Z-score.
    Sign convention: positive Z = net long (bullish crowd).
    """
    if pd.isna(z):        return "Unknown"
    if z >  1.5:          return "Extreme Long"
    if z >  0.5:          return "Long"
    if z >= -0.5:         return "Neutral"
    if z >= -1.5:         return "Short"
    return "Extreme Short"


def build_cot_master(conn):
    """
    Read raw_cot, compute all derived metrics per instrument,
    then upsert into cot_weekly.
    """
    log.info("Building cot_weekly master table...")

    # Truncate cot_weekly so stale rows don't persist after a raw_cot refresh
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE cot_weekly")
    conn.commit()
    log.info("cot_weekly truncated — rebuilding from raw_cot")

    df = pd.read_sql("SELECT * FROM raw_cot ORDER BY cftc_code, report_date", conn)

    if df.empty:
        log.error("raw_cot is empty — cannot build master")
        return

    output_rows = []

    for cftc_code, group in df.groupby("cftc_code"):
        group = group.sort_values("report_date").copy()
        meta  = INSTRUMENTS.get(cftc_code, {})
        rtype = meta.get("report", group["report_type"].iloc[0])

        oi = pd.to_numeric(group["open_interest"], errors="coerce")

        if rtype == "legacy":
            nc_l  = pd.to_numeric(group["nc_long"],     errors="coerce")
            nc_s  = pd.to_numeric(group["nc_short"],    errors="coerce")
            cm_l  = pd.to_numeric(group["comm_long"],   errors="coerce")
            cm_s  = pd.to_numeric(group["comm_short"],  errors="coerce")
            nr_l  = pd.to_numeric(group["nonrept_long"],  errors="coerce")
            nr_s  = pd.to_numeric(group["nonrept_short"], errors="coerce")

            net_nc   = nc_l  - nc_s
            net_comm = cm_l  - cm_s
            net_nr   = nr_l  - nr_s

            nc_pct  = (net_nc   / oi.replace(0, np.nan) * 100).round(2)
            cm_pct  = (net_comm / oi.replace(0, np.nan) * 100).round(2)

            nc_z  = zscore_rolling(net_nc)
            cm_z  = zscore_rolling(net_comm)

            # Primary spec signal = non-commercial Z
            primary_z = nc_z

            for i, (_, row) in enumerate(group.iterrows()):
                output_rows.append({
                    "report_date":       row["report_date"],
                    "cftc_code":         cftc_code,
                    "symbol":            meta.get("symbol", row["symbol"]),
                    "instrument_name":   meta.get("name",   row["instrument_name"]),
                    "instrument_group":  meta.get("group",  "Other"),
                    "report_type":       "legacy",
                    "open_interest":     _safe_int(row["open_interest"]),
                    "net_noncomm":       _safe_int(net_nc.iloc[i]),
                    "net_comm":          _safe_int(net_comm.iloc[i]),
                    "net_nonrept":       _safe_int(net_nr.iloc[i]),
                    "net_managed_money": None,
                    "net_producer":      None,
                    "net_swap":          None,
                    "noncomm_pct_oi":    _safe_float(nc_pct.iloc[i]),
                    "comm_pct_oi":       _safe_float(cm_pct.iloc[i]),
                    "mm_pct_oi":         None,
                    "prod_pct_oi":       None,
                    "noncomm_zscore":    _safe_float(nc_z.iloc[i]),
                    "comm_zscore":       _safe_float(cm_z.iloc[i]),
                    "mm_zscore":         None,
                    "prod_zscore":       None,
                    "positioning_label": classify_positioning(primary_z.iloc[i]),
                })

        else:  # disagg
            mm_l  = pd.to_numeric(group["mm_long"],    errors="coerce")
            mm_s  = pd.to_numeric(group["mm_short"],   errors="coerce")
            pr_l  = pd.to_numeric(group["prod_long"],  errors="coerce")
            pr_s  = pd.to_numeric(group["prod_short"], errors="coerce")
            sw_l  = pd.to_numeric(group["swap_long"],  errors="coerce")
            sw_s  = pd.to_numeric(group["swap_short"], errors="coerce")

            net_mm   = mm_l - mm_s
            net_prod = pr_l - pr_s
            net_swap = sw_l - sw_s

            mm_pct   = (net_mm   / oi.replace(0, np.nan) * 100).round(2)
            prod_pct = (net_prod / oi.replace(0, np.nan) * 100).round(2)

            mm_z   = zscore_rolling(net_mm)
            prod_z = zscore_rolling(net_prod)

            # Primary spec signal = managed money Z
            primary_z = mm_z

            for i, (_, row) in enumerate(group.iterrows()):
                output_rows.append({
                    "report_date":       row["report_date"],
                    "cftc_code":         cftc_code,
                    "symbol":            meta.get("symbol", row["symbol"]),
                    "instrument_name":   meta.get("name",   row["instrument_name"]),
                    "instrument_group":  meta.get("group",  "Other"),
                    "report_type":       "disagg",
                    "open_interest":     _safe_int(row["open_interest"]),
                    "net_noncomm":       None,
                    "net_comm":          None,
                    "net_nonrept":       None,
                    "net_managed_money": _safe_int(net_mm.iloc[i]),
                    "net_producer":      _safe_int(net_prod.iloc[i]),
                    "net_swap":          _safe_int(net_swap.iloc[i]),
                    "noncomm_pct_oi":    None,
                    "comm_pct_oi":       None,
                    "mm_pct_oi":         _safe_float(mm_pct.iloc[i]),
                    "prod_pct_oi":       _safe_float(prod_pct.iloc[i]),
                    "noncomm_zscore":    None,
                    "comm_zscore":       None,
                    "mm_zscore":         _safe_float(mm_z.iloc[i]),
                    "prod_zscore":       _safe_float(prod_z.iloc[i]),
                    "positioning_label": classify_positioning(primary_z.iloc[i]),
                })

    out_df = pd.DataFrame(output_rows)

    # NOTE: No DataFrame sanitization here — clean_val() in upsert_cot_weekly
    # handles all NaN/inf/numpy type conversion at the tuple level.
    # Any pre-upsert pandas sanitization destroys valid float values in
    # mixed None/float columns by coercing None→NaN→None chain.

    upsert_cot_weekly(conn, out_df)

    # Log current positioning snapshot
    log.info("── Current Positioning Snapshot ──────────────────────────")
    latest = (
        out_df.sort_values("report_date")
              .groupby("symbol")
              .last()
              .reset_index()
    )
    for _, row in latest.iterrows():
        # pandas NaN is truthy in Python — must check explicitly
        nc_z = row.get("noncomm_zscore")
        mm_z = row.get("mm_zscore")
        # Use noncomm for legacy, mm for disagg — pick whichever is a real number
        if nc_z is not None and not (isinstance(nc_z, float) and np.isnan(nc_z)):
            pz = float(nc_z)
        elif mm_z is not None and not (isinstance(mm_z, float) and np.isnan(mm_z)):
            pz = float(mm_z)
        else:
            pz = None
        pz_str = f"{pz:.2f}" if pz is not None else "nan"
        log.info(
            f"  {row['symbol']:<5} {row['instrument_name']:<22} "
            f"Z={pz_str:<7} {row['positioning_label']}"
        )




def _safe_float(val):
    try:
        f = float(val)
        if np.isnan(f) or np.isinf(f):
            return None
        return round(f, 4)
    except Exception:
        return None


def upsert_cot_weekly(conn, df):
    if df.empty:
        return
    cursor = conn.cursor()
    sql = """
        MERGE cot_weekly AS target
        USING (VALUES (
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?
        )) AS source (
            report_date, cftc_code, symbol, instrument_name,
            instrument_group, report_type,
            open_interest,
            net_noncomm, net_comm, net_nonrept,
            net_managed_money, net_producer, net_swap,
            noncomm_pct_oi, comm_pct_oi, mm_pct_oi, prod_pct_oi,
            noncomm_zscore, comm_zscore, mm_zscore, prod_zscore,
            positioning_label
        )
        ON target.report_date = source.report_date
        AND target.cftc_code  = source.cftc_code
        WHEN MATCHED THEN UPDATE SET
            symbol             = source.symbol,
            instrument_name    = source.instrument_name,
            instrument_group   = source.instrument_group,
            report_type        = source.report_type,
            open_interest      = source.open_interest,
            net_noncomm        = source.net_noncomm,
            net_comm           = source.net_comm,
            net_nonrept        = source.net_nonrept,
            net_managed_money  = source.net_managed_money,
            net_producer       = source.net_producer,
            net_swap           = source.net_swap,
            noncomm_pct_oi     = source.noncomm_pct_oi,
            comm_pct_oi        = source.comm_pct_oi,
            mm_pct_oi          = source.mm_pct_oi,
            prod_pct_oi        = source.prod_pct_oi,
            noncomm_zscore     = source.noncomm_zscore,
            comm_zscore        = source.comm_zscore,
            mm_zscore          = source.mm_zscore,
            prod_zscore        = source.prod_zscore,
            positioning_label  = source.positioning_label
        WHEN NOT MATCHED THEN INSERT (
            report_date, cftc_code, symbol, instrument_name,
            instrument_group, report_type,
            open_interest,
            net_noncomm, net_comm, net_nonrept,
            net_managed_money, net_producer, net_swap,
            noncomm_pct_oi, comm_pct_oi, mm_pct_oi, prod_pct_oi,
            noncomm_zscore, comm_zscore, mm_zscore, prod_zscore,
            positioning_label
        ) VALUES (
            source.report_date, source.cftc_code, source.symbol,
            source.instrument_name, source.instrument_group, source.report_type,
            source.open_interest,
            source.net_noncomm, source.net_comm, source.net_nonrept,
            source.net_managed_money, source.net_producer, source.net_swap,
            source.noncomm_pct_oi, source.comm_pct_oi, source.mm_pct_oi,
            source.prod_pct_oi,
            source.noncomm_zscore, source.comm_zscore, source.mm_zscore,
            source.prod_zscore,
            source.positioning_label
        );
    """
    float_cols = {
        "noncomm_pct_oi", "comm_pct_oi", "mm_pct_oi", "prod_pct_oi",
        "noncomm_zscore", "comm_zscore", "mm_zscore", "prod_zscore",
    }
    int_cols = {
        "open_interest", "net_noncomm", "net_comm", "net_nonrept",
        "net_managed_money", "net_producer", "net_swap",
    }
    cols = [
        "report_date", "cftc_code", "symbol", "instrument_name",
        "instrument_group", "report_type",
        "open_interest",
        "net_noncomm", "net_comm", "net_nonrept",
        "net_managed_money", "net_producer", "net_swap",
        "noncomm_pct_oi", "comm_pct_oi", "mm_pct_oi", "prod_pct_oi",
        "noncomm_zscore", "comm_zscore", "mm_zscore", "prod_zscore",
        "positioning_label",
    ]

    def clean_val(col, val):
        """Convert every value to a pyodbc-safe Python scalar."""
        # numpy scalar → Python native first
        if hasattr(val, "item"):
            val = val.item()
        # None / NaN / inf all become None
        if val is None:
            return None
        if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
            return None
        if col in float_cols:
            try:
                f = float(val)
                return None if (np.isnan(f) or np.isinf(f)) else round(f, 4)
            except Exception:
                return None
        if col in int_cols:
            try:
                f = float(val)
                return None if (np.isnan(f) or np.isinf(f)) else int(f)
            except Exception:
                return None
        return val

    rows = [
        tuple(clean_val(c, row[c]) for c in cols)
        for _, row in df[cols].iterrows()
    ]
    cursor.executemany(sql, rows)
    conn.commit()
    log.info(f"cot_weekly upserted: {len(rows)} rows")

# ─────────────────────────────────────────────
# SQL VIEWS (idempotent CREATE OR ALTER)
# ─────────────────────────────────────────────

def create_views(conn):
    cursor = conn.cursor()

    # vw_cot_latest — most recent row per instrument (for KPI cards)
    cursor.execute("""
        IF OBJECT_ID('vw_cot_latest', 'V') IS NOT NULL
            DROP VIEW vw_cot_latest
    """)
    cursor.execute("""
        CREATE VIEW vw_cot_latest AS
        SELECT w.*
        FROM cot_weekly w
        INNER JOIN (
            SELECT cftc_code, MAX(report_date) AS max_date
            FROM cot_weekly
            WHERE positioning_label IS NOT NULL
            GROUP BY cftc_code
        ) latest ON w.cftc_code = latest.cftc_code
                 AND w.report_date = latest.max_date
    """)

    # vw_cot_history — full history with 13-week moving average on primary Z
    cursor.execute("""
        IF OBJECT_ID('vw_cot_history', 'V') IS NOT NULL
            DROP VIEW vw_cot_history
    """)
    cursor.execute("""
        CREATE VIEW vw_cot_history AS
        SELECT
            report_date,
            cftc_code,
            symbol,
            instrument_name,
            instrument_group,
            report_type,
            open_interest,
            net_noncomm,
            net_comm,
            net_managed_money,
            net_producer,
            noncomm_pct_oi,
            comm_pct_oi,
            mm_pct_oi,
            prod_pct_oi,
            noncomm_zscore,
            comm_zscore,
            mm_zscore,
            prod_zscore,
            positioning_label,
            -- 13-week smoothed primary Z (for trend chart)
            AVG(COALESCE(noncomm_zscore, mm_zscore)) OVER (
                PARTITION BY cftc_code
                ORDER BY report_date
                ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
            ) AS primary_zscore_13w,
            -- Raw primary Z (single column regardless of report type)
            COALESCE(noncomm_zscore, mm_zscore) AS primary_zscore
        FROM cot_weekly
        WHERE COALESCE(noncomm_zscore, mm_zscore) IS NOT NULL
    """)

    # vw_cot_extremes — top 5 most extreme long + short per instrument
    cursor.execute("""
        IF OBJECT_ID('vw_cot_extremes', 'V') IS NOT NULL
            DROP VIEW vw_cot_extremes
    """)
    cursor.execute("""
        CREATE VIEW vw_cot_extremes AS
        WITH primary_z AS (
            SELECT
                report_date,
                cftc_code,
                symbol,
                instrument_name,
                instrument_group,
                open_interest,
                positioning_label,
                COALESCE(noncomm_zscore, mm_zscore) AS primary_zscore,
                COALESCE(net_noncomm, net_managed_money) AS primary_net,
                CASE
                    WHEN COALESCE(noncomm_zscore, mm_zscore) >= 0 THEN 'Long'
                    ELSE 'Short'
                END AS side
            FROM cot_weekly
            WHERE COALESCE(noncomm_zscore, mm_zscore) IS NOT NULL
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY cftc_code, side
                    ORDER BY ABS(primary_zscore) DESC
                ) AS rn
            FROM primary_z
        )
        SELECT
            report_date,
            cftc_code,
            symbol,
            instrument_name,
            instrument_group,
            open_interest,
            positioning_label,
            CAST(primary_zscore AS FLOAT) AS primary_zscore,
            primary_net,
            side
        FROM ranked
        WHERE rn <= 5
    """)

    conn.commit()
    log.info("Views created: vw_cot_latest, vw_cot_history, vw_cot_extremes")

# ─────────────────────────────────────────────
# SCHEMA HELPERS
# ─────────────────────────────────────────────

def add_primary_zscore_column(conn):
    """
    Add primary_zscore and commercial_zscore as stored computed columns
    so Power BI can use them directly without DAX calculated columns.
    These are idempotent — safe to run every ETL cycle.
    """
    cursor = conn.cursor()

    # Add primary_zscore column if not exists
    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID('cot_weekly')
            AND name = 'primary_zscore'
        )
        ALTER TABLE cot_weekly ADD primary_zscore FLOAT NULL
    """)

    # Add commercial_zscore column if not exists
    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID('cot_weekly')
            AND name = 'commercial_zscore'
        )
        ALTER TABLE cot_weekly ADD commercial_zscore FLOAT NULL
    """)

    # Add net_position_primary column if not exists
    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID('cot_weekly')
            AND name = 'net_position_primary'
        )
        ALTER TABLE cot_weekly ADD net_position_primary BIGINT NULL
    """)

    conn.commit()

    # Populate the computed columns from existing data
    cursor.execute("""
        UPDATE cot_weekly SET
            primary_zscore = COALESCE(noncomm_zscore, mm_zscore),
            commercial_zscore = COALESCE(comm_zscore, prod_zscore),
            net_position_primary = COALESCE(net_noncomm, net_managed_money)
    """)
    conn.commit()

    affected = cursor.rowcount
    log.info(f"Computed columns updated: primary_zscore, commercial_zscore, net_position_primary ({affected:,} rows)")


# ─────────────────────────────────────────────
# POST-UPSERT VALIDATION
# ─────────────────────────────────────────────

def validate_cot_weekly(conn):
    """
    Run null checks on cot_weekly after every upsert.
    Logs warnings for any critical columns with unexpected nulls.
    Catches data quality issues before they reach Power BI.
    """
    log.info("── Data Quality Validation ───────────────────────────────")
    cursor = conn.cursor()

    # 1. Row count per instrument
    cursor.execute("""
        SELECT symbol, report_type, COUNT(*) as rows,
               MAX(report_date) as latest_date
        FROM cot_weekly
        GROUP BY symbol, report_type
        ORDER BY symbol
    """)
    rows = cursor.fetchall()
    log.info("  Instrument coverage:")
    for r in rows:
        log.info(f"    {r[0]:<5} ({r[1]:<6}): {r[2]:>4} rows | latest: {r[3]}")

    # 2. Latest date null checks — critical columns
    cursor.execute("""
        SELECT
            w.symbol,
            w.report_type,
            w.report_date,
            COALESCE(w.noncomm_zscore, w.mm_zscore) AS primary_zscore,
            w.net_noncomm,
            w.net_managed_money,
            w.open_interest,
            w.positioning_label
        FROM cot_weekly w
        INNER JOIN (
            SELECT cftc_code, MAX(report_date) AS max_date
            FROM cot_weekly
            GROUP BY cftc_code
        ) latest ON w.cftc_code = latest.cftc_code
                 AND w.report_date = latest.max_date
        ORDER BY w.symbol
    """)
    latest_rows = cursor.fetchall()

    log.info("  Latest row null check (critical columns):")
    issues = 0
    for r in latest_rows:
        symbol, rtype, date, pz, nc, mm, oi, label = r
        primary_net = nc if rtype == "legacy" else mm
        problems = []
        if pz is None:      problems.append("primary_zscore=NULL")
        if primary_net is None: problems.append(f"net_pos=NULL ({rtype})")
        if oi is None:      problems.append("open_interest=NULL")
        if label is None:   problems.append("positioning_label=NULL")
        if problems:
            log.warning(f"    ❌ {symbol}: {', '.join(problems)}")
            issues += 1
        else:
            log.info(f"    ✅ {symbol:<5} ({rtype:<6}): Z={pz:.2f} | net={primary_net:>10,} | OI={oi:>12,} | {label}")

    if issues == 0:
        log.info("  All instruments passed null checks ✅")
    else:
        log.warning(f"  {issues} instrument(s) have data quality issues ❌")

    # 3. Overall null rates for key columns
    cursor.execute("""
        SELECT
            SUM(CASE WHEN primary_zscore IS NULL THEN 1 ELSE 0 END) as z_nulls,
            SUM(CASE WHEN net_noncomm IS NULL AND net_managed_money IS NULL THEN 1 ELSE 0 END) as net_nulls,
            SUM(CASE WHEN open_interest IS NULL THEN 1 ELSE 0 END) as oi_nulls,
            COUNT(*) as total_rows
        FROM cot_weekly
    """)
    r = cursor.fetchone()
    z_pct   = r[0] / r[3] * 100
    net_pct = r[1] / r[3] * 100
    oi_pct  = r[2] / r[3] * 100
    log.info(f"  Overall null rates — Z: {z_pct:.1f}% | Net pos: {net_pct:.1f}% | OI: {oi_pct:.1f}% | Total rows: {r[3]:,}")
    if z_pct > 15:
        log.warning(f"  ⚠ Z-score null rate {z_pct:.1f}% is high (expected <15% for warm-up period)")
    if net_pct > 5:
        log.warning(f"  ⚠ Net position null rate {net_pct:.1f}% is high — check ETL column mapping")
    log.info("─────────────────────────────────────────────────────────")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="COT ETL Pipeline")
    parser.add_argument(
        "--rebuild-master-only",
        action="store_true",
        help="Skip download/raw upsert — rebuild cot_weekly from existing raw_cot only"
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info(f"COT ETL START  —  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.rebuild_master_only:
        log.info("Mode: REBUILD MASTER ONLY (skipping download)")
    log.info("=" * 60)

    # 1. Connect
    log.info("Connecting to SQL Server...")
    conn = get_conn()
    log.info(f"Connected: {SQL_SERVER} / {SQL_DATABASE}")

    # 2. Schema
    create_tables(conn)

    if not args.rebuild_master_only:
        # 3. Download + parse all years
        raw_df = fetch_all_years()
        if raw_df.empty:
            log.error("No data retrieved — ETL aborted")
            conn.close()
            return

        # 4. Upsert raw staging
        upsert_raw_cot(conn, raw_df)
    else:
        log.info("Skipping download — using existing raw_cot data")

    # 5. Build master fact table
    build_cot_master(conn)

    # 6. Create / refresh SQL views
    create_views(conn)

    # 7. Add primary_zscore computed column if not exists
    add_primary_zscore_column(conn)

    # 8. Validate data quality
    validate_cot_weekly(conn)

    conn.close()

    log.info("=" * 60)
    log.info(f"COT ETL COMPLETE  —  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
