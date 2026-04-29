@echo off
:: ============================================================
:: run_liquidity_etl.bat
:: Liquidity Dashboard — Task Scheduler launcher
:: Schedule: Saturday 8:00 PM
:: ============================================================

cd /d "C:\Users\TJs PC\OneDrive\Desktop\Projects\Liquidity Hub"

echo [%date% %time%] Starting Liquidity ETL... >> liquidity_etl_log.txt

python liquidity_etl.py

if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] ETL FAILED with error code %ERRORLEVEL% >> liquidity_etl_log.txt
    exit /b %ERRORLEVEL%
)

echo [%date% %time%] ETL completed successfully >> liquidity_etl_log.txt
