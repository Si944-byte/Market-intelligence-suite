"""
calculate_dcf.py
────────────────
Reads the latest fundamentals from SQLite, runs the full DCF
calculation for all S&P 500 stocks, assigns Quality Tiers and
Signals, then writes to SQL Server (DCFRegime) AND exports
Stock_Data_Current.csv as a backup.

DCF Assumptions:
  - Sector-specific growth rates and discount rates (Base Case)
  - Conservative: 5% growth, 10% discount (all sectors)
  - Aggressive:   15% growth, 7% discount (all sectors)
  - Terminal growth: 2.5% (all scenarios, all sectors)
  - Projection period: 5 years

Usage:
    python calculate_dcf.py

Requirements:
    pip install pandas pyodbc
"""

import sqlite3
import pandas as pd
import numpy as np
import pyodbc
import os
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH     = r"C:\Users\TJs PC\OneDrive\Desktop\Projects\DCF Models\sp500_prices.db"
OUTPUT_PATH = r"C:\Users\TJs PC\OneDrive\Desktop\Projects\DCF Models\Stock_Data_Current.csv"

SQL_SERVER   = "YOUR_SQL_SERVER"
SQL_DATABASE = "DCFRegime"
SQL_USER     = "dcf_user"
SQL_PASSWORD = "YOUR_SQL_PASSWORD"

TERMINAL_GROWTH = 0.025
DCF_YEARS       = 5

# ── SECTOR-SPECIFIC BASE CASE ASSUMPTIONS ────────────────────────────────────
# Format: 'Sector': (growth_rate, discount_rate)
SECTOR_ASSUMPTIONS = {
    'Information Technology':  (0.12, 0.09),
    'Consumer Discretionary':  (0.09, 0.09),
    'Communication Services':  (0.10, 0.09),
    'Health Care':             (0.09, 0.08),
    'Industrials':             (0.08, 0.08),
    'Materials':               (0.07, 0.08),
    'Energy':                  (0.07, 0.08),
    'Consumer Staples':        (0.06, 0.07),
    'Utilities':               (0.05, 0.07),
    'Real Estate':             (0.06, 0.07),
    'Financials':              (0.08, 0.08),
}

# Default for any sector not in the map
DEFAULT_ASSUMPTIONS = (0.08, 0.08)

# ── SCENARIO ASSUMPTIONS (fixed, all sectors) ─────────────────────────────────
SCENARIOS = {
    'Conservative': (0.05, 0.10),
    'Aggressive':   (0.15, 0.07),
}

# ── QUALITY TIER LOGIC ────────────────────────────────────────────────────────
def assign_quality_tier(profit_margin, debt_to_equity, sector):
    """
    Assign quality tier based on profitability and leverage.
    Financials and Utilities get adjusted thresholds.
    """
    if profit_margin is None or np.isnan(profit_margin):
        return 'Low'

    # Sector adjustments
    if sector == 'Financials':
        # Financials have lower margins but that's structural
        if profit_margin > 0.15 and (debt_to_equity is None or debt_to_equity < 2.0):
            return 'High'
        elif profit_margin > 0.05:
            return 'Medium'
        else:
            return 'Low'
    elif sector == 'Utilities':
        # Utilities carry structural debt — use looser D/E threshold
        if profit_margin > 0.10 and (debt_to_equity is None or debt_to_equity < 1.5):
            return 'High'
        elif profit_margin > 0.05:
            return 'Medium'
        else:
            return 'Low'
    elif sector == 'Real Estate':
        if profit_margin > 0.10:
            return 'High'
        elif profit_margin > 0.03:
            return 'Medium'
        else:
            return 'Low'
    else:
        # Standard logic
        de = debt_to_equity if debt_to_equity is not None else 0
        if profit_margin > 0.15 and de < 0.5:
            return 'High'
        elif profit_margin > 0.05 and de < 1.0:
            return 'Medium'
        else:
            return 'Low'


# ── DCF CALCULATION ───────────────────────────────────────────────────────────
def calculate_dcf(fcf, growth, discount, terminal=TERMINAL_GROWTH, years=DCF_YEARS):
    """
    Calculate total DCF value (not per share).
    Returns None if FCF is missing or zero.
    """
    if fcf is None or fcf == 0 or np.isnan(fcf):
        return None
    if discount <= terminal:
        return None

    # Sum of discounted cash flows for projection period
    pv_sum = sum(
        (fcf * (1 + growth) ** t) / (1 + discount) ** t
        for t in range(1, years + 1)
    )

    # Terminal value
    fcf_terminal = fcf * (1 + growth) ** years
    terminal_value = (fcf_terminal * (1 + terminal)) / (discount - terminal)
    pv_terminal = terminal_value / (1 + discount) ** years

    return pv_sum + pv_terminal


def intrinsic_per_share(total_value, market_cap, current_price):
    """Convert total DCF value to per-share intrinsic value."""
    if total_value is None or current_price is None or current_price == 0:
        return None
    if market_cap and market_cap > 0:
        shares = market_cap / current_price
    else:
        return None
    if shares == 0:
        return None
    return total_value / shares


def valuation_gap(intrinsic, current_price):
    """Calculate valuation gap percentage."""
    if intrinsic is None or current_price is None or current_price == 0:
        return None
    return (intrinsic - current_price) / current_price


def assign_signal(gap):
    """Assign BUY/HOLD/SELL based on valuation gap."""
    if gap is None or np.isnan(gap):
        return 'INSUFFICIENT DATA'
    elif gap > 0.10:
        return 'BUY'
    elif gap < -0.10:
        return 'SELL'
    else:
        return 'HOLD'


# ── LOAD DATA ─────────────────────────────────────────────────────────────────
def load_latest_fundamentals():
    """Load the most recent fundamentals snapshot from SQLite."""
    conn = sqlite3.connect(DB_PATH)

    latest_date = conn.execute(
        "SELECT MAX(fetch_date) FROM fundamentals"
    ).fetchone()[0]

    if not latest_date:
        conn.close()
        raise ValueError("No fundamentals data found. Run fetch_fundamentals.py first.")

    print(f"  Loading fundamentals — fetch date: {latest_date}")

    df = pd.read_sql_query("""
        SELECT
            ticker, company, sector,
            current_price, market_cap, revenue,
            fcf, operating_cash_flow, capital_expenditure,
            net_income, total_debt, debt_to_equity,
            profit_margin, operating_margin,
            week52_high, week52_low, dcf_method,
            fetch_date
        FROM fundamentals
        WHERE fetch_date = ?
        ORDER BY ticker
    """, conn, params=(latest_date,))

    conn.close()
    print(f"  {len(df)} stocks loaded.")
    return df, latest_date


# ── MAIN CALCULATION ──────────────────────────────────────────────────────────
def run_calculations(df):
    """Run all DCF calculations and assign tiers/signals."""
    print(f"\n  Running DCF calculations for {len(df)} stocks...")

    results = []

    for _, row in df.iterrows():
        ticker  = row['ticker']
        sector  = row['sector'] if row['sector'] else 'Unknown'
        fcf     = row['fcf']
        price   = row['current_price']
        mkt_cap = row['market_cap']

        # Get sector assumptions
        growth_base, discount_base = SECTOR_ASSUMPTIONS.get(sector, DEFAULT_ASSUMPTIONS)

        # ── Base Case DCF ────────────────────────────────────────────────────
        base_total      = calculate_dcf(fcf, growth_base, discount_base)
        base_iv_share   = intrinsic_per_share(base_total, mkt_cap, price)
        base_gap        = valuation_gap(base_iv_share, price)
        base_iv_total   = base_total

        # ── Conservative DCF ─────────────────────────────────────────────────
        g_cons, d_cons  = SCENARIOS['Conservative']
        cons_total      = calculate_dcf(fcf, g_cons, d_cons)
        cons_iv_share   = intrinsic_per_share(cons_total, mkt_cap, price)
        cons_gap        = valuation_gap(cons_iv_share, price)

        # ── Aggressive DCF ───────────────────────────────────────────────────
        g_aggr, d_aggr  = SCENARIOS['Aggressive']
        aggr_total      = calculate_dcf(fcf, g_aggr, d_aggr)
        aggr_iv_share   = intrinsic_per_share(aggr_total, mkt_cap, price)
        aggr_gap        = valuation_gap(aggr_iv_share, price)

        # ── Derived fields ───────────────────────────────────────────────────
        fcf_yield       = (fcf / mkt_cap) if (fcf and mkt_cap and mkt_cap > 0) else None
        gap_dollars     = ((base_iv_share - price) * (mkt_cap / price)) if (
                            base_iv_share and price and mkt_cap and price > 0) else None

        quality_tier    = assign_quality_tier(
                            row['profit_margin'],
                            row['debt_to_equity'],
                            sector
                          )
        signal          = assign_signal(base_gap)

        results.append({
            'Ticker':                    ticker,
            'Company':                   row['company'],
            'Sector':                    sector,
            'Current_Price':             round(price, 4)          if price          else None,
            'Intrinsic_Value_Per_Share': round(base_iv_share, 4)  if base_iv_share  else None,
            'Intrinsic_Value_Total':     round(base_iv_total, 2)  if base_iv_total  else None,
            'Valuation_Gap_Pct':         round(base_gap, 6)       if base_gap is not None else None,
            'Valuation_Gap_Dollars':     round(gap_dollars, 2)    if gap_dollars    else None,
            'Market_Cap':                mkt_cap,
            'FCF':                       fcf,
            'FCF_Yield_Pct':             round(fcf_yield, 6)      if fcf_yield      else None,
            'Revenue':                   row['revenue'],
            'Total_Debt':                row['total_debt'],
            'Debt_to_Equity':            row['debt_to_equity'],
            'Operating_Cash_Flow':       row['operating_cash_flow'],
            'Capital_Expenditure':       row['capital_expenditure'],
            'Profit_Margin':             row['profit_margin'],
            'Operating_Margin':          row['operating_margin'],
            'Week52_Low':                row['week52_low'],
            'Week52_High':               row['week52_high'],
            'Quality_Tier':              quality_tier,
            'Signal':                    signal,
            'DCF_Method':                row['dcf_method'],
            'Sector_Growth_Rate':        growth_base,
            'Sector_Discount_Rate':      discount_base,
            'Conservative_IV':           round(cons_iv_share, 4)  if cons_iv_share  else None,
            'Conservative_Gap':          round(cons_gap, 6)       if cons_gap is not None else None,
            'Aggressive_IV':             round(aggr_iv_share, 4)  if aggr_iv_share  else None,
            'Aggressive_Gap':            round(aggr_gap, 6)       if aggr_gap is not None else None,
            'Data_Date':                 row['fetch_date'],
        })

    return pd.DataFrame(results)


# ── SQL SERVER CONNECTION ─────────────────────────────────────────────────────
def get_sql_connection():
    """Connect to SQL Server DCFRegime database."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
    )
    return pyodbc.connect(conn_str)


# ── WRITE TO SQL SERVER ───────────────────────────────────────────────────────
def write_to_sql(df):
    """
    Write DCF results to SQL Server dcf_results table.
    Skips rows that already exist for the same Data_Date + Ticker
    (handled by the UNIQUE constraint in the table).
    Inserts in batches of 100 for performance.
    """
    print(f"\n  Writing {len(df)} rows to SQL Server DCFRegime...")

    conn   = get_sql_connection()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO dbo.dcf_results (
            Data_Date, Ticker, Company, Sector,
            Current_Price, Intrinsic_Value_Per_Share, Intrinsic_Value_Total,
            Valuation_Gap_Pct, Valuation_Gap_Dollars,
            Market_Cap, FCF, FCF_Yield_Pct, Revenue, Total_Debt,
            Debt_to_Equity, Operating_Cash_Flow, Capital_Expenditure,
            Profit_Margin, Operating_Margin, Week52_Low, Week52_High,
            Quality_Tier, Signal, DCF_Method,
            Sector_Growth_Rate, Sector_Discount_Rate,
            Conservative_IV, Conservative_Gap,
            Aggressive_IV, Aggressive_Gap
        )
        SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        WHERE NOT EXISTS (
            SELECT 1 FROM dbo.dcf_results
            WHERE Data_Date = ? AND Ticker = ?
        )
    """

    inserted = 0
    skipped  = 0
    errors   = []

    for _, row in df.iterrows():
        try:
            def v(col):
                val = row.get(col)
                if val is None:
                    return None
                try:
                    if np.isnan(val):
                        return None
                except:
                    pass
                return val

            params = (
                v('Data_Date'), v('Ticker'), v('Company'), v('Sector'),
                v('Current_Price'), v('Intrinsic_Value_Per_Share'), v('Intrinsic_Value_Total'),
                v('Valuation_Gap_Pct'), v('Valuation_Gap_Dollars'),
                v('Market_Cap'), v('FCF'), v('FCF_Yield_Pct'), v('Revenue'), v('Total_Debt'),
                v('Debt_to_Equity'), v('Operating_Cash_Flow'), v('Capital_Expenditure'),
                v('Profit_Margin'), v('Operating_Margin'), v('Week52_Low'), v('Week52_High'),
                v('Quality_Tier'), v('Signal'), v('DCF_Method'),
                v('Sector_Growth_Rate'), v('Sector_Discount_Rate'),
                v('Conservative_IV'), v('Conservative_Gap'),
                v('Aggressive_IV'), v('Aggressive_Gap'),
                # WHERE NOT EXISTS params
                v('Data_Date'), v('Ticker')
            )

            cursor.execute(insert_sql, params)

            if cursor.rowcount > 0:
                inserted += 1
            else:
                skipped += 1

        except Exception as e:
            errors.append(f"{row.get('Ticker', '?')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"  Inserted: {inserted} new rows")
    print(f"  Skipped:  {skipped} already existed for this date")

    if errors:
        print(f"  Errors:   {len(errors)}")
        for e in errors[:10]:
            print(f"    {e}")

    return inserted, skipped


# ── SUMMARY ───────────────────────────────────────────────────────────────────
def print_summary(df):
    total    = len(df)
    buy      = (df['Signal'] == 'BUY').sum()
    hold     = (df['Signal'] == 'HOLD').sum()
    sell     = (df['Signal'] == 'SELL').sum()
    no_data  = (df['Signal'] == 'INSUFFICIENT DATA').sum()
    robust   = ((df['Valuation_Gap_Pct'] > 0.10) & (df['Conservative_Gap'] > 0.10)).sum()
    downside = ((df['Valuation_Gap_Pct'] > 0.10) & (df['Conservative_Gap'] < 0)).sum()

    print(f"\n  {'-'*40}")
    print(f"  PORTFOLIO SUMMARY ({total} stocks)")
    print(f"  {'-'*40}")
    print(f"  BUY signals:          {buy}")
    print(f"  HOLD signals:         {hold}")
    print(f"  SELL signals:         {sell}")
    print(f"  Insufficient data:    {no_data}")
    print(f"  Robust BUY (all scenarios): {robust}")
    print(f"  Downside Risk:        {downside}")
    print(f"\n  Avg Valuation Gap:    {df['Valuation_Gap_Pct'].mean()*100:.1f}%")
    print(f"\n  By Sector:")
    sector_summary = df.groupby('Sector')['Valuation_Gap_Pct'].mean().sort_values(ascending=False)
    for sector, gap in sector_summary.items():
        sign = "+" if gap > 0 else ""
        print(f"    {sector:<35} {sign}{gap*100:.1f}%")


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  S&P 500 DCF Calculator & SQL Server Writer")
    print("=" * 60)

    if not os.path.exists(DB_PATH):
        print(f"\n  ERROR: Database not found.")
        print(f"  Run fetch_fundamentals.py first.")
        return

    df_raw, latest_date = load_latest_fundamentals()
    df_out = run_calculations(df_raw)

    # Sort by Valuation Gap descending (best opportunities first)
    df_out = df_out.sort_values('Valuation_Gap_Pct', ascending=False, na_position='last')

    # ── Write to SQL Server ───────────────────────────────────────────────────
    try:
        inserted, skipped = write_to_sql(df_out)
    except Exception as e:
        print(f"\n  SQL Server write failed: {e}")
        print(f"  Falling back to CSV export only.")

    # ── CSV backup export (unchanged) ────────────────────────────────────────
    df_out.to_csv(OUTPUT_PATH, index=False)

    print_summary(df_out)

    print(f"\n{'='*60}")
    print(f"  COMPLETE")
    print(f"  Data as of:    {latest_date}")
    print(f"  Stocks:        {len(df_out)}")
    print(f"  SQL Server:    DCFRegime.dbo.dcf_results")
    print(f"  CSV backup:    {OUTPUT_PATH}")
    print(f"{'='*60}")
    print(f"\n  Open Power BI and hit Refresh.")


if __name__ == "__main__":
    main()
