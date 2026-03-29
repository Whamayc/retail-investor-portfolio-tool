# Retail Investor Portfolio Tool

A Streamlit app for analyzing a Canadian retail investment portfolio in CAD.

This project helps you import brokerage holdings, classify positions, inspect allocation exposure, monitor market sentiment, review performance and risk metrics, and compare rebalancing scenarios side by side.

## Features

- Import portfolio exports from `.csv` or `.xlsx`
- Support Canadian brokerage-style files with `Ticker` and `Market Value(CAD)` columns
- Normalize holdings into a CAD-weighted portfolio view
- Classify positions into equity, fixed income, REITs, commodities, money market, and cash
- Analyze allocation by holding, sector, region, and asset class
- Track market sentiment with VIX and Fear & Greed data
- Calculate return, drawdown, volatility, and correlation metrics across multiple time periods
- Compare the current portfolio with a custom rebalanced version
- Optionally upload per-ETF holdings CSVs for better look-through exposure analysis

## Pages

### Import

Upload a holdings file and build the working portfolio. The app:

- validates required columns
- aggregates duplicate tickers across accounts
- preserves cash as a separate synthetic position
- detects ETFs and money market funds
- lets you upload optional ETF constituent CSVs before running analysis

### Allocation

View the portfolio through interactive Plotly charts:

- holdings treemap
- sector breakdown
- regional allocation
- asset class allocation

### Market Sentiment

Monitor:

- CBOE VIX
- CNN Fear & Greed Index

### Performance & Risk

Review per-ticker metrics and correlation heatmaps across multiple periods.

### Comparison Lab

Adjust portfolio weights, keep cash and money market positions fixed, and compare the current portfolio against a simulated rebalance.

## Tech Stack

- Python
- Streamlit
- Pandas
- Plotly
- yfinance
- requests / BeautifulSoup
- openpyxl

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/Whamayc/retail-investor-portfolio-tool.git
cd retail-investor-portfolio-tool
```

### 2. Create and activate a virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

macOS / Linux:

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Copy `.env.example` to `.env` and add your API keys:

```env
FMP_API_KEY=your_fmp_api_key_here
ALPHAVANTAGE_API_KEY=your_alphavantage_api_key_here
```

## Running the App

From the project root:

```bash
streamlit run app.py
```

Then open the local URL shown by Streamlit, usually `http://localhost:8501`.

## Input File Format

Required columns:

- `Ticker`
- `Market Value(CAD)`

Optional columns:

- `Account`
- `Account Type`

Notes:

- rows with zero or negative market value are ignored
- duplicate tickers are aggregated
- `CASH` is handled as a dedicated cash position
- tickers without an exchange suffix are normalized with `.TO` by default

## Optional ETF Holdings CSV Format

For more accurate look-through analysis, you can upload an ETF holdings CSV on the Import page.

Required columns:

- `Ticker` or `Symbol`
- `Weight`, `Weight%`, `Pct`, or `Allocation`

Optional columns:

- `Name`
- `Sector`
- `Region` or `Country`

## Project Structure

```text
.
├── app.py
├── pages/
│   ├── 1_import.py
│   ├── 2_allocation.py
│   ├── 3_sentiment.py
│   ├── 4_performance.py
│   └── 5_comparison.py
├── core/
│   ├── loader.py
│   ├── metrics.py
│   ├── fx.py
│   ├── fmp.py
│   ├── alphavantage.py
│   ├── money_market.py
│   └── ...
└── requirements.txt
```

## Notes

- This tool is intended for personal portfolio analysis and education.
- It depends on external market data providers, so results can vary with API availability and ticker coverage.
- It is not financial advice.

## Roadmap Ideas

- Add sample input files
- Add screenshots or a demo GIF
- Export analysis results to CSV or PDF
- Add stronger validation for broker-specific file formats
- Add automated tests for loaders and metric calculations

## License

No license has been added yet. If you plan to share or open-source this project, consider adding one.
