@echo off
cd "C:\Users\TJs PC\OneDrive\Desktop\Projects\Macro Inflation Watch"
echo ============================================ >> etl_log.txt
echo %date% %time% - ETL started >> etl_log.txt
python etl.py >> etl_log.txt 2>&1
echo %date% %time% - ETL completed >> etl_log.txt
echo ============================================ >> etl_log.txt