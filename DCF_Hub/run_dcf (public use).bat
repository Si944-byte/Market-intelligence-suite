@echo off
REM ============================================================
REM  S&P 500 DCF Dashboard — Automated Refresh
REM  Step 1: Monthly fundamentals fetch (first Sunday only)
REM  Step 2: DCF calculation + CSV export (every Sunday)
REM  Runs every Sunday at 5:00 AM via Task Scheduler
REM  Power BI Service refreshes at 6:00 AM after this completes
REM ============================================================

SET PROJECT_DIR=C:\Users\TJs PC\OneDrive\Desktop\Projects\DCF Models
SET LOG_FILE=%PROJECT_DIR%\dcf_log.txt
SET PYTHON=python

REM Get current timestamp
FOR /F "tokens=1-4 delims=/ " %%a IN ('date /t') DO SET DATE_STR=%%a %%b %%c %%d
FOR /F "tokens=1-2 delims=: " %%a IN ('time /t') DO SET TIME_STR=%%a:%%b

ECHO ============================================================ >> "%LOG_FILE%"
ECHO  DCF Dashboard Refresh >> "%LOG_FILE%"
ECHO  Started: %DATE_STR% %TIME_STR% >> "%LOG_FILE%"
ECHO ============================================================ >> "%LOG_FILE%"

REM Navigate to project folder
CD /D "%PROJECT_DIR%"

REM ── STEP 1: Check if today is the 1st Sunday of the month ─────
REM  If so, run full fundamentals refresh (monthly)
REM  Otherwise skip to save API calls
FOR /F %%a IN ('powershell -command "(Get-Date).Day"') DO SET DAY=%%a
IF %DAY% LEQ 7 (
    ECHO. >> "%LOG_FILE%"
    ECHO [STEP 1] First week of month - running fundamentals refresh... >> "%LOG_FILE%"
    ECHO [STEP 1] Running monthly fundamentals refresh...
    %PYTHON% fetch_fundamentals_rapidapi.py >> "%LOG_FILE%" 2>&1
    IF %ERRORLEVEL% NEQ 0 (
        ECHO [WARNING] fetch_fundamentals_rapidapi.py failed - using existing data >> "%LOG_FILE%"
        ECHO [WARNING] Fundamentals fetch failed - continuing with existing data
    ) ELSE (
        ECHO [STEP 1] Fundamentals refresh complete >> "%LOG_FILE%"
    )
) ELSE (
    ECHO. >> "%LOG_FILE%"
    ECHO [STEP 1] Mid-month - skipping fundamentals refresh >> "%LOG_FILE%"
    ECHO [STEP 1] Skipping fundamentals refresh (mid-month)
)

REM ── STEP 2: Run DCF calculation and export CSV ────────────────
ECHO. >> "%LOG_FILE%"
ECHO [STEP 2] Running DCF calculations and exporting CSV... >> "%LOG_FILE%"
ECHO [STEP 2] Running DCF calculations...
%PYTHON% calculate_dcf.py >> "%LOG_FILE%" 2>&1
IF %ERRORLEVEL% NEQ 0 (
    ECHO [ERROR] calculate_dcf.py failed >> "%LOG_FILE%"
    ECHO [ERROR] calculate_dcf.py failed
    GOTO :ERROR
)
ECHO [STEP 2] Complete >> "%LOG_FILE%"

REM ── SUCCESS ───────────────────────────────────────────────────
ECHO. >> "%LOG_FILE%"
ECHO [SUCCESS] All steps complete. Stock_Data_Current.csv updated. >> "%LOG_FILE%"
ECHO [SUCCESS] Power BI Service will refresh at 6:00 AM. >> "%LOG_FILE%"
ECHO. >> "%LOG_FILE%"
ECHO [SUCCESS] DCF refresh complete - Power BI will refresh at 6:00 AM
GOTO :END

REM ── ERROR HANDLER ─────────────────────────────────────────────
:ERROR
ECHO. >> "%LOG_FILE%"
ECHO [FAILED] DCF refresh encountered an error. Check log for details. >> "%LOG_FILE%"
ECHO. >> "%LOG_FILE%"
ECHO [FAILED] Check dcf_log.txt for details
EXIT /B 1

:END
ECHO ============================================================ >> "%LOG_FILE%"
ECHO  Finished: %DATE_STR% %TIME_STR% >> "%LOG_FILE%"
ECHO ============================================================ >> "%LOG_FILE%"
EXIT /B 0
