"""
fetch_fundamentals_rapidapi.py
───────────────────────────────
Fetches fundamental data for all S&P 500 stocks using the
Yahoo Finance API via RapidAPI (yahoo-finance166).

Response structure confirmed from live API test:
  quoteSummary → result → [0] → financialData → {field: {raw: value}}

Uses 2 calls per ticker:
  1. get-financial-data  — price, FCF, margins, debt, revenue
  2. get-price           — 52wk high/low

Usage:
    python fetch_fundamentals_rapidapi.py

Requirements:
    pip install requests pandas
"""

import sqlite3
import requests
import pandas as pd
from datetime import datetime
import os
import time

# ── CONFIG ────────────────────────────────────────────────────────────────────
RAPIDAPI_KEY = "YOUR_RAPIDAPI_KEY"
DB_PATH      = r"C:\Users\TJs PC\OneDrive\Desktop\Projects\DCF Models\sp500_prices.db"
TICKERS_PATH = r"C:\Users\TJs PC\OneDrive\Desktop\Projects\DCF Models\sp500_tickers.csv"

FINANCIAL_SECTORS = {'Financials'}
PAUSE_PER_TICKER  = 0.75  # Seconds between tickers

BASE_URL = "https://yahoo-finance166.p.rapidapi.com/api/stock"
HEADERS  = {
    "Content-Type":    "application/json",
    "x-rapidapi-host": "yahoo-finance166.p.rapidapi.com",
    "x-rapidapi-key":  RAPIDAPI_KEY,
}

# ── DATABASE SETUP ─────────────────────────────────────────────────────────────
def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_date           TEXT NOT NULL,
            ticker               TEXT NOT NULL,
            company              TEXT,
            sector               TEXT,
            current_price        REAL,
            market_cap           REAL,
            revenue              REAL,
            fcf                  REAL,
            operating_cash_flow  REAL,
            capital_expenditure  REAL,
            net_income           REAL,
            total_debt           REAL,
            debt_to_equity       REAL,
            profit_margin        REAL,
            operating_margin     REAL,
            week52_high          REAL,
            week52_low           REAL,
            dcf_method           TEXT,
            created_at           TEXT NOT NULL,
            UNIQUE(fetch_date, ticker)
        )
    """)
    conn.commit()


# ── HELPERS ───────────────────────────────────────────────────────────────────
def api_get(endpoint, params):
    """Make one RapidAPI call. Returns JSON or raises."""
    url  = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def raw(data, key):
    """
    Extract raw numeric value from Yahoo Finance's {raw, fmt} structure.
    Handles both nested {raw: x} and plain values.
    """
    val = data.get(key)
    if val is None:
        return None
    if isinstance(val, dict):
        return val.get('raw')
    if isinstance(val, (int, float)):
        return val
    return None


def safe_float(val):
    try:
        return float(val) if val is not None else None
    except:
        return None


# ── FETCH ONE TICKER ──────────────────────────────────────────────────────────
def fetch_one(ticker, sector):
    """
    Fetch all fundamentals for one ticker.
    Uses confirmed JSON path:
      quoteSummary → result → [0] → financialData
    """
    is_financial = sector in FINANCIAL_SECTORS

    # ── Call 1: get-financial-data ────────────────────────────────────────────
    fd_resp = api_get("get-financial-data", {"region": "US", "symbol": ticker})

    # Navigate confirmed path
    try:
        fin = fd_resp['quoteSummary']['result'][0]['financialData']
    except (KeyError, IndexError, TypeError):
        raise ValueError(f"Unexpected response structure: {str(fd_resp)[:200]}")

    price            = raw(fin, 'currentPrice')
    total_revenue    = raw(fin, 'totalRevenue')
    total_debt       = raw(fin, 'totalDebt')
    operating_cf     = raw(fin, 'operatingCashflow')
    free_cf          = raw(fin, 'freeCashflow')
    profit_margin    = raw(fin, 'profitMargins')
    operating_margin = raw(fin, 'operatingMargins')
    debt_to_equity   = raw(fin, 'debtToEquity')
    net_income       = raw(fin, 'netIncomeToCommon') or raw(fin, 'netIncome')
    ebitda           = raw(fin, 'ebitda')
    total_cash       = raw(fin, 'totalCash')

    if not price:
        raise ValueError("No price in financialData")

    # debtToEquity comes back as percentage (102.63 = 102.63%)
    # Convert to ratio (1.0263)
    de = safe_float(debt_to_equity)
    if de and de > 20:
        de = de / 100

    # Market cap not in financialData — estimate from price later
    # We'll get it from get-price endpoint
    market_cap = None

    time.sleep(0.3)

    # ── Call 2: get-price (52wk range + market cap) ───────────────────────────
    week52_high = None
    week52_low  = None
    try:
        pr_resp = api_get("get-price", {"region": "US", "symbol": ticker})
        try:
            price_data = pr_resp['quoteSummary']['result'][0]['price']
            week52_high = raw(price_data, 'fiftyTwoWeekHigh')
            week52_low  = raw(price_data, 'fiftyTwoWeekLow')
            market_cap  = raw(price_data, 'marketCap')
            # Fallback price if financialData didn't have it
            if not price:
                price = raw(price_data, 'regularMarketPrice')
        except (KeyError, IndexError, TypeError):
            pass
    except Exception:
        pass  # 52wk data is nice to have, not critical

    # ── Determine FCF base ────────────────────────────────────────────────────
    if is_financial:
        fcf        = safe_float(net_income)
        dcf_method = 'Net Income (Financial)'
    else:
        fcf_val = safe_float(free_cf)
        ocf_val = safe_float(operating_cf)
        if fcf_val and fcf_val != 0:
            fcf = fcf_val
        elif ocf_val:
            fcf = ocf_val
        else:
            fcf = None
        dcf_method = 'FCF (TTM)'

    price_f = safe_float(price)
    if not price_f:
        raise ValueError("No valid price")

    return {
        'current_price':       round(price_f, 4),
        'market_cap':          safe_float(market_cap),
        'revenue':             safe_float(total_revenue),
        'fcf':                 fcf,
        'operating_cash_flow': safe_float(operating_cf),
        'capital_expenditure': None,
        'net_income':          safe_float(net_income),
        'total_debt':          safe_float(total_debt),
        'debt_to_equity':      round(de, 4) if de else None,
        'profit_margin':       round(safe_float(profit_margin), 6) if safe_float(profit_margin) else None,
        'operating_margin':    round(safe_float(operating_margin), 6) if safe_float(operating_margin) else None,
        'week52_high':         safe_float(week52_high),
        'week52_low':          safe_float(week52_low),
        'dcf_method':          dcf_method,
    }


# ── SAVE ROW ──────────────────────────────────────────────────────────────────
def save_row(conn, row):
    conn.execute("""
        INSERT OR IGNORE INTO fundamentals (
            fetch_date, ticker, company, sector,
            current_price, market_cap, revenue, fcf,
            operating_cash_flow, capital_expenditure, net_income,
            total_debt, debt_to_equity, profit_margin, operating_margin,
            week52_high, week52_low, dcf_method, created_at
        ) VALUES (
            :fetch_date, :ticker, :company, :sector,
            :current_price, :market_cap, :revenue, :fcf,
            :operating_cash_flow, :capital_expenditure, :net_income,
            :total_debt, :debt_to_equity, :profit_margin, :operating_margin,
            :week52_high, :week52_low, :dcf_method, :created_at
        )
    """, row)
    conn.commit()


# ── RESUME SUPPORT ────────────────────────────────────────────────────────────
def get_already_fetched(conn, today):
    rows = conn.execute(
        "SELECT ticker FROM fundamentals WHERE fetch_date = ?", (today,)
    ).fetchall()
    return {r[0] for r in rows}


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  S&P 500 Fundamentals Fetcher  —  RapidAPI Yahoo Finance")
    print("=" * 60)
    print(f"  Host:    yahoo-finance166.p.rapidapi.com")
    print(f"  Calls:   2 per ticker (financial-data + price)")
    print(f"  Pause:   {PAUSE_PER_TICKER}s per ticker")
    est = 503 * (PAUSE_PER_TICKER + 0.6) / 60
    print(f"  Est:     ~{est:.0f} minutes for all 503 tickers")
    print("=" * 60)

    if not os.path.exists(TICKERS_PATH):
        print("\n  ERROR: Run fetch_sp500_list.py first.")
        return

    df_tickers  = pd.read_csv(TICKERS_PATH)
    all_tickers = df_tickers['Ticker'].tolist()
    ticker_map  = df_tickers.set_index('Ticker')[['Company', 'Sector']].to_dict('index')

    today = datetime.now().strftime("%Y-%m-%d")
    now   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    already_done = get_already_fetched(conn, today)
    remaining    = [t for t in all_tickers if t not in already_done]

    if already_done:
        print(f"\n  Resuming — {len(already_done)}/{len(all_tickers)} already done today.")
    print(f"  Remaining: {len(remaining)} tickers\n")

    success = 0
    errors  = []

    for i, ticker in enumerate(remaining, 1):
        info    = ticker_map.get(ticker, {})
        company = info.get('Company', ticker)
        sector  = info.get('Sector', 'Unknown')

        try:
            data = fetch_one(ticker, sector)
            data.update({
                'fetch_date': today,
                'ticker':     ticker,
                'company':    company,
                'sector':     sector,
                'created_at': now,
            })
            save_row(conn, data)
            success += 1

            total_done = len(already_done) + success
            price_str  = f"${data['current_price']:.2f}"
            fcf_val    = data['fcf']
            fcf_str    = f"FCF: ${fcf_val/1e9:.1f}B" if fcf_val else "FCF: N/A"
            flag       = " [NI]" if data['dcf_method'] == 'Net Income (Financial)' else ""
            print(f"  [{total_done:>3}/{len(all_tickers)}] {ticker:<6} "
                  f"{price_str:<10} {fcf_str}{flag}")

        except Exception as e:
            errors.append(f"{ticker}: {e}")
            total_done = len(already_done) + success
            print(f"  [{total_done:>3}/{len(all_tickers)}] {ticker:<6} ERROR — {e}")

        time.sleep(PAUSE_PER_TICKER)

    conn.close()

    total_done      = len(already_done) + success
    still_remaining = len(all_tickers) - total_done

    print("\n" + "=" * 60)
    print(f"  COMPLETE")
    print(f"  Fetched this run:  {success}")
    print(f"  Total complete:    {total_done}/{len(all_tickers)}")
    print(f"  Still remaining:   {still_remaining}")
    print(f"  Errors:            {len(errors)}")

    if still_remaining > 0:
        print(f"\n  Run again to fetch remaining {still_remaining} tickers.")
    else:
        print(f"\n  All tickers complete!")
        print(f"  Run calculate_dcf.py next.")

    if errors:
        print(f"\n  ERRORS ({len(errors)}):")
        for e in errors[:15]:
            print(f"    {e}")
        if len(errors) > 15:
            print(f"    ... and {len(errors)-15} more")
    print("=" * 60)


if __name__ == "__main__":
    main()
