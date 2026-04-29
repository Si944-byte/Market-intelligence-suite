import os
import time
import urllib
import pandas as pd
from sqlalchemy import create_engine, text
from fredapi import Fred
import yfinance as yf
import requests
from datetime import datetime

# =============================================================
# CONFIG
# =============================================================

SQL_SERVER   = "YOUR_SQL_SERVER"
SQL_DATABASE = "MacroRegime"
SQL_USER     = "macro_user"
SQL_PASSWORD = "YOUR_SQL_PASSWORD"
FRED_API_KEY = "YOUR_FRED_API_KEY"

RAPIDAPI_KEY = "YOUR_RAPIDAPI_KEY"

START_DATE = "2010-01-01"
END_DATE   = datetime.today().strftime("%Y-%m-%d")

FRED_SERIES = {
    "CPIAUCSL":        ("raw_cpi", "CPI Level (Headline)"),
    "CPILFESL":        ("raw_cpi", "CPI Less Food & Energy"),
    "CPIHOSSL":        ("raw_cpi", "CPI Housing"),
    "CPIFABSL":        ("raw_cpi", "CPI Food & Beverages"),
    "CPIENGSL":        ("raw_cpi", "CPI Energy"),
    "CPITRNSL":        ("raw_cpi", "CPI Transportation"),
    "FEDFUNDS":        ("raw_ffr",          "Fed Funds Rate"),
    "UNRATE":          ("raw_unemployment", "Unemployment Rate"),
    "A191RL1Q225SBEA": ("raw_gdp",          "Real GDP Growth QoQ Annualized"),
    "T10Y2Y":          ("raw_yield_curve",  "10Y-2Y Treasury Spread"),
    "IPMAN":           ("raw_pmi",          "Industrial Production Manufacturing Index"),
}

# =============================================================
# CONNECTION
# =============================================================

def get_master_engine():
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE=master;"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate=yes;"
    )
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        isolation_level="AUTOCOMMIT"
    )


def get_engine():
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


# =============================================================
# DATABASE SETUP
# =============================================================

def create_database():
    engine = get_master_engine()
    with engine.connect() as conn:
        exists = conn.execute(
            text(f"SELECT COUNT(*) FROM sys.databases WHERE name = '{SQL_DATABASE}'")
        ).scalar()
        if not exists:
            conn.execute(text(f"CREATE DATABASE [{SQL_DATABASE}]"))
            print(f"  Database '{SQL_DATABASE}' created.")
        else:
            print(f"  Database '{SQL_DATABASE}' already exists.")


def run_schema(engine):
    with open("schema.sql", "r") as f:
        raw = f.read()
    statements = [s.strip() for s in raw.split(";") if s.strip() and not s.strip().startswith("--")]
    with engine.begin() as conn:
        for stmt in statements:
            if stmt:
                try:
                    conn.execute(text(stmt))
                except Exception as e:
                    print(f"  Schema note: {e}")
    print("  Schema applied.")


# =============================================================
# HELPERS
# =============================================================

def to_monthly_first(df):
    df.index = pd.to_datetime(df.index)
    df = df.resample("MS").mean()
    return df


def pct_change_yoy(series):
    return series.pct_change(12) * 100


def pct_change_mom(series):
    return series.pct_change(1) * 100


# =============================================================
# EXTRACT
# =============================================================

def extract_fred(fred, series_id):
    print(f"  Pulling {series_id}...")
    max_attempts = 3
    retry_wait   = 30
    for attempt in range(max_attempts):
        try:
            s = fred.get_series(series_id, observation_start=START_DATE, observation_end=END_DATE)
            df = pd.DataFrame(s, columns=["value"])
            df.index.name = "date"
            return to_monthly_first(df)
        except Exception as e:
            if attempt < max_attempts - 1:
                print(f"  FRED error on {series_id} (attempt {attempt + 1}/{max_attempts}): {e}")
                print(f"  Retrying in {retry_wait}s...")
                time.sleep(retry_wait)
            else:
                print(f"  FRED failed on {series_id} after {max_attempts} attempts: {e}")
                raise


def extract_spx():
    print("  Pulling SPX via RapidAPI YFinance...")
    url = "https://yahoo-finance166.p.rapidapi.com/api/stock/get-chart"
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "yahoo-finance166.p.rapidapi.com"
    }
    params = {
        "symbol": "^GSPC",
        "region": "US",
        "interval": "1mo",
        "range": "16y"
    }
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    timestamps = data["chart"]["result"][0]["timestamp"]
    closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
    df = pd.DataFrame({
        "date": pd.to_datetime(timestamps, unit="s"),
        "spx_close": closes
    })
    df["date"] = df["date"].dt.to_period("M").dt.to_timestamp()
    df = df.set_index("date").resample("MS").last()
    return df


# =============================================================
# LOAD RAW TABLES
# =============================================================

def load_raw_cpi(engine, cpi_frames):
    rows = []
    for series_id, (_, series_name) in FRED_SERIES.items():
        if series_id not in cpi_frames:
            continue
        df = cpi_frames[series_id]
        for date, row in df.iterrows():
            rows.append({
                "date": date.date(),
                "series_id": series_id,
                "series_name": series_name,
                "value": row["value"]
            })
    df_out = pd.DataFrame(rows).dropna()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM raw_cpi"))
    df_out.to_sql("raw_cpi", engine, if_exists="append", index=False)
    print(f"  raw_cpi loaded: {len(df_out)} rows")


def load_raw_single(engine, table, df):
    df_clean = df[["value"]].dropna().copy()
    df_clean.index = pd.to_datetime(df_clean.index).date
    df_clean = df_clean.reset_index()
    df_clean.columns = ["date", "value"]
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table}"))
    df_clean.to_sql(table, engine, if_exists="append", index=False)
    print(f"  {table} loaded: {len(df_clean)} rows")


def load_raw_spx(engine, df):
    df_clean = df[["spx_close"]].dropna().copy()
    df_clean.index = pd.to_datetime(df_clean.index).date
    df_clean = df_clean.reset_index()
    df_clean.columns = ["date", "spx_close"]
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM raw_spx"))
    df_clean.to_sql("raw_spx", engine, if_exists="append", index=False)
    print(f"  raw_spx loaded: {len(df_clean)} rows")


# =============================================================
# BUILD MASTER TABLE
# =============================================================

def build_master(engine):
    print("\nBuilding macro_monthly master table...")

    def read(query):
        return pd.read_sql(query, engine, index_col="date", parse_dates=["date"])

    cpi_hl = read("SELECT date, value as cpi           FROM raw_cpi WHERE series_id='CPIAUCSL'")
    cpi_co = read("SELECT date, value as cpi_core      FROM raw_cpi WHERE series_id='CPILFESL'")
    cpi_hs = read("SELECT date, value as cpi_housing   FROM raw_cpi WHERE series_id='CPIHOSSL'")
    cpi_fd = read("SELECT date, value as cpi_food      FROM raw_cpi WHERE series_id='CPIFABSL'")
    cpi_en = read("SELECT date, value as cpi_energy    FROM raw_cpi WHERE series_id='CPIENGSL'")
    cpi_tr = read("SELECT date, value as cpi_transport FROM raw_cpi WHERE series_id='CPITRNSL'")
    ffr    = read("SELECT date, value as ffr                 FROM raw_ffr")
    unemp  = read("SELECT date, value as unemployment_rate   FROM raw_unemployment")
    gdp    = read("SELECT date, value as gdp_real_growth     FROM raw_gdp")
    yc     = read("SELECT date, value as yield_spread_10y2y  FROM raw_yield_curve")
    spx    = read("SELECT date, spx_close                    FROM raw_spx")
    pmi    = read("SELECT date, value as pmi                 FROM raw_pmi")

    all_dates = pd.date_range(start=START_DATE, end=END_DATE, freq="MS")
    df = pd.DataFrame(index=all_dates)
    df.index.name = "date"

    for frame in [cpi_hl, cpi_co, cpi_hs, cpi_fd, cpi_en, cpi_tr, ffr, unemp, yc, spx, pmi]:
        df = df.join(frame, how="left")

    gdp.index = pd.to_datetime(gdp.index)
    df = df.join(gdp, how="left")
    df["gdp_real_growth"] = df["gdp_real_growth"].ffill()

    df["cpi_mom_pct"]          = pct_change_mom(df["cpi"])
    df["cpi_yoy_pct"]          = pct_change_yoy(df["cpi"])
    df["cpi_core_yoy_pct"]     = pct_change_yoy(df["cpi_core"])
    df["real_interest_rate"]   = df["ffr"] - df["cpi_yoy_pct"]
    df["yield_curve_inverted"] = (df["yield_spread_10y2y"] < 0).astype(int)
    df["spx_return_1m"]        = pct_change_mom(df["spx_close"])
    df["spx_return_12m"]       = pct_change_yoy(df["spx_close"])
    df["pmi_expanding"]        = (df["pmi"] >= 100).astype(int)
    df["gdp_smoothed"]         = df["gdp_real_growth"].rolling(window=6, min_periods=1).mean()
    df["cpi_smoothed"]         = df["cpi_yoy_pct"].rolling(window=3, min_periods=1).mean()

    df = df.rename(columns={"cpi": "cpi_level", "cpi_core": "cpi_core_level"})

    def classify_regime(row):
        cpi = row.get("cpi_smoothed")
        gdp = row.get("gdp_smoothed")
        if pd.isna(cpi) or pd.isna(gdp):
            return None, None
        if   cpi < 3.0  and gdp >= 2.0: return "Goldilocks",  1
        elif cpi >= 3.0 and gdp >= 2.0: return "Inflation",   2
        elif cpi >= 3.0 and gdp < 2.0:  return "Stagflation", 3
        else:                            return "Recession",   4

    df[["regime_label", "regime_code"]] = df.apply(
        lambda r: pd.Series(classify_regime(r)), axis=1
    )

    cols = [
        "cpi_level", "cpi_core_level", "cpi_mom_pct", "cpi_yoy_pct", "cpi_core_yoy_pct",
        "cpi_smoothed",
        "cpi_housing", "cpi_food", "cpi_energy", "cpi_transport",
        "ffr", "real_interest_rate",
        "unemployment_rate",
        "gdp_real_growth", "gdp_smoothed",
        "yield_spread_10y2y", "yield_curve_inverted",
        "spx_close", "spx_return_1m", "spx_return_12m",
        "pmi", "pmi_expanding",
        "regime_label", "regime_code"
    ]
    df = df[[c for c in cols if c in df.columns]]
    df.index = pd.to_datetime(df.index).date
    df.index.name = "date"
    df = df.reset_index()

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM macro_monthly"))
    df.to_sql("macro_monthly", engine, if_exists="append", index=False)

    print(f"  macro_monthly built: {len(df)} rows")
    print(f"  Regime distribution:\n{df['regime_label'].value_counts().to_string()}")


# =============================================================
# MAIN
# =============================================================

def run():
    print("=" * 55)
    print("Macro Regime ETL — SQL Server 2019")
    print("=" * 55)

    print("\nInitializing database...")
    create_database()
    engine = get_engine()
    run_schema(engine)

    fred = Fred(api_key=FRED_API_KEY)
    print("\nExtracting FRED series...")

    cpi_frames = {}
    ffr_df = unemp_df = gdp_df = yc_df = pmi_df = None

    for series_id, (table, name) in FRED_SERIES.items():
        df = extract_fred(fred, series_id)
        if table == "raw_cpi":
            cpi_frames[series_id] = df
        elif table == "raw_ffr":
            ffr_df = df
        elif table == "raw_unemployment":
            unemp_df = df
        elif table == "raw_gdp":
            gdp_df = df
        elif table == "raw_yield_curve":
            yc_df = df
        elif table == "raw_pmi":
            pmi_df = df

    time.sleep(5)
    spx_df = extract_spx()

    print("\nLoading raw tables...")
    load_raw_cpi(engine, cpi_frames)
    load_raw_single(engine, "raw_ffr", ffr_df)
    load_raw_single(engine, "raw_unemployment", unemp_df)
    load_raw_single(engine, "raw_gdp", gdp_df)
    load_raw_single(engine, "raw_yield_curve", yc_df)
    load_raw_single(engine, "raw_pmi", pmi_df)
    load_raw_spx(engine, spx_df)

    build_master(engine)

    print("\nDone.")
    print(f"Connect Power BI to: Server={SQL_SERVER}, Database={SQL_DATABASE}")
    print("=" * 55)


if __name__ == "__main__":
    run()
