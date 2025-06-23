# AI-Enabled Trading Workflow

A unified codebase for an agent-based, AI-driven trading system that collects market data, evaluates signals with LLMs, and backtests portfolio strategies.

## Overview

This repository supports the end-to-end implementation of a modular investment workflow:

1. **Data Aggregation**  
   - Python package (`financial_data_aggregator/`) fetches price series, technical indicators, fundamental metrics, and sentiment signals via Alpha Vantage and news APIs.  
   - Optional Dropbox integration for centralized data storage.  

2. **Backtesting & Analysis**  
   - `backtesting_main.py` performs historical simulations, computes performance metrics (cumulative/annualized returns, volatility, drawdowns, Sharpe, alpha/beta), and generates comparative plots.  

3. **LLM-Orchestrated Workflows**  
   - `n8n_project/` contains JSON exports of n8n workflows that:  
     - Summarize and sentiment-score news articles  
     - Run specialized LLM agents for technical, fundamental, bull-case, and bear-case analysis  
     - Synthesize per-stock insights into weekly return forecasts  
     - Coordinate portfolio construction and generate human-readable reports  

## Getting Started

1. **Install Python dependencies**
     pip install -r financial_data_aggregator/requirements.txt
   
2. **Configure Credentials**
    Copy financial_data_aggregator/env/.env.example to .env and populate ALPHA_VANTAGE_API_KEY and DROPBOX_TOKEN.

3. **Run Data Aggregator**
    python financial_data_aggregator/main.py --tickers NVDA MSFT AAPL

4. **Generate Backtesting Report**
    python backtesting_main.py

5. **Import n8n Workflows**
    In n8n, import the JSON files from n8n_project/.
    




