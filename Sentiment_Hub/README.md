# Sentiment Hub

ETL pipeline for the Market Sentiment Dashboard.

**Database:** SentimentRegime | **Script:** sentiment_etl.py
**Refresh:** Saturday 5:30 AM ETL | 6:30 AM Power BI
**Data sources:** CBOE, CNN Fear and Greed, AAII

## Indicators
- CBOE Put/Call Ratio
- CNN Fear and Greed Index
- AAII Bull/Bear spread

## Output
Composite sentiment score, regime classification,
historical percentile context.

## Setup
pip install -r requirements.txt
Configure SQL credentials.
