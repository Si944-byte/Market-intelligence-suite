# Security Policy

## Credential Handling

This repository contains no real API keys, passwords, or connection strings.
All credentials in ETL scripts have been replaced with placeholder values:

- SQL_PASSWORD = "YOUR_SQL_PASSWORD"
- FRED_API_KEY = "YOUR_FRED_API_KEY"
- RAPIDAPI_KEY = "YOUR_RAPIDAPI_KEY"

Never commit real credentials to this repository.
Use environment variables or a local .env file (already in .gitignore).

## Reporting Issues

If you discover a security concern, open an issue or contact via GitHub.