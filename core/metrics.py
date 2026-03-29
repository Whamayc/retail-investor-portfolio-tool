"""
core/metrics.py — Historical performance and risk metrics.

Computes per-ticker metrics (total return, max drawdown, annualised std dev)
and a portfolio-level correlation matrix across configurable time periods.

Cash and Money Market positions are excluded from price-based calculations.
Money Market positions use the ^IRX (13-week T-bill) proxy for return,
with std dev hardcoded to 0.
"""

import logging
from datetime import date, timedelta
from math import sqrt

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Excluded asset classes from price-based analysis
# ---------------------------------------------------------------------------

EXCLUDED_ASSET_CLASSES = {"Cash", "Money Market"}

# ---------------------------------------------------------------------------
# Period definitions
# ---------------------------------------------------------------------------

PERIODS = ["1M", "3M", "6M", "QTD", "YTD", "1Y", "2Y", "3Y", "5Y", "10Y", "15Y", "20Y"]


def get_period_bounds(period: str) -> tuple[str, str]:
    """
    Return (start_date_str, end_date_str) for the requested period.
    Dates are ISO-format strings: "YYYY-MM-DD".
    """
    today = date.today()
    end = today.strftime("%Y-%m-%d")

    if period == "1M":
        start = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    elif period == "3M":
        start = (today - timedelta(days=91)).strftime("%Y-%m-%d")
    elif period == "6M":
        start = (today - timedelta(days=182)).strftime("%Y-%m-%d")
    elif period == "QTD":
        q_start_month = ((today.month - 1) // 3) * 3 + 1
        start = date(today.year, q_start_month, 1).strftime("%Y-%m-%d")
    elif period == "YTD":
        start = date(today.year, 1, 1).strftime("%Y-%m-%d")
    elif period == "1Y":
        start = date(today.year - 1, today.month, today.day).strftime("%Y-%m-%d")
    elif period == "2Y":
        start = date(today.year - 2, today.month, today.day).strftime("%Y-%m-%d")
    elif period == "3Y":
        start = date(today.year - 3, today.month, today.day).strftime("%Y-%m-%d")
    elif period == "5Y":
        start = date(today.year - 5, today.month, today.day).strftime("%Y-%m-%d")
    elif period == "10Y":
        start = date(today.year - 10, today.month, today.day).strftime("%Y-%m-%d")
    elif period == "15Y":
        start = date(today.year - 15, today.month, today.day).strftime("%Y-%m-%d")
    elif period == "20Y":
        start = date(today.year - 20, today.month, today.day).strftime("%Y-%m-%d")
    else:
        raise ValueError(f"Unknown period: {period}")

    return start, end


# ---------------------------------------------------------------------------
# yfinance download helper (normalises single vs multi-ticker output)
# ---------------------------------------------------------------------------

def _download_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """
    Batch-download adjusted close prices.

    Handles the yfinance gotcha where a single-ticker download returns a flat
    DataFrame instead of a MultiIndex DataFrame.
    """
    if not tickers:
        return pd.DataFrame()

    raw = yf.download(tickers, start=start, end=end, auto_adjust=True, progress=False)

    if raw.empty:
        return pd.DataFrame()

    if len(tickers) == 1:
        # Single ticker: raw has columns like Close, Open, High, Low, Volume
        if "Close" in raw.columns:
            return raw[["Close"]].rename(columns={"Close": tickers[0]})
        return pd.DataFrame()

    # Multi-ticker: raw has MultiIndex columns — extract "Close" level
    if "Close" in raw.columns.get_level_values(0):
        prices = raw["Close"]
        # Ensure column order matches requested tickers
        return prices[[t for t in tickers if t in prices.columns]]

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Individual metric calculations
# ---------------------------------------------------------------------------

def _total_return(prices: pd.Series) -> float:
    """Total return as a percentage over the series."""
    prices = prices.dropna()
    if len(prices) < 2:
        return float("nan")
    return (prices.iloc[-1] / prices.iloc[0] - 1) * 100


def _max_drawdown(prices: pd.Series) -> float:
    """Maximum drawdown as a negative percentage."""
    prices = prices.dropna()
    if len(prices) < 2:
        return float("nan")
    roll_max = prices.cummax()
    drawdown = (prices - roll_max) / roll_max
    return float(drawdown.min() * 100)


def _annualised_std(prices: pd.Series) -> float:
    """Annualised standard deviation of daily returns (%)."""
    prices = prices.dropna()
    if len(prices) < 3:
        return float("nan")
    daily_returns = prices.pct_change().dropna()
    return float(daily_returns.std() * sqrt(252) * 100)


# ---------------------------------------------------------------------------
# Money market proxy (^IRX)
# ---------------------------------------------------------------------------

def _get_tbill_return(start: str, end: str) -> float:
    """
    Approximate return for a money market position using the 13-week T-bill yield.
    ^IRX is quoted as an annualised percentage — convert to period return.
    """
    try:
        raw = yf.download("^IRX", start=start, end=end, auto_adjust=True, progress=False)
        if raw.empty:
            return float("nan")
        # ^IRX is in percent; take average annualised rate over period
        if isinstance(raw.columns, pd.MultiIndex):
            close = raw["Close"]["^IRX"]
        else:
            close = raw["Close"]
        avg_annual_rate = float(close.mean()) / 100
        # Approximate period days
        days = (pd.Timestamp(end) - pd.Timestamp(start)).days
        return ((1 + avg_annual_rate) ** (days / 365) - 1) * 100
    except Exception as e:
        logger.warning("T-bill proxy fetch failed: %s", e)
        return float("nan")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_metrics(
    portfolio_df: pd.DataFrame,
    period: str,
) -> dict:
    """
    Compute performance and risk metrics for all positions in portfolio_df.

    Parameters
    ----------
    portfolio_df : DataFrame with columns ticker, weight_pct, asset_class
    period       : one of PERIODS

    Returns
    -------
    dict with keys:
        metrics_df         : pd.DataFrame — per-ticker metrics table
        correlation_matrix : pd.DataFrame — Pearson correlation of daily returns
        actual_start       : str — actual earliest data date (may be > requested start)
        period             : str — echoed back
    """
    start, end = get_period_bounds(period)

    # Separate tradeable from excluded
    tradeable = portfolio_df[
        ~portfolio_df["asset_class"].isin(EXCLUDED_ASSET_CLASSES)
    ].copy()
    mm_positions = portfolio_df[
        portfolio_df["asset_class"] == "Money Market"
    ].copy()

    tradeable_tickers = tradeable["ticker"].tolist()

    # Download prices for tradeable positions
    prices = _download_prices(tradeable_tickers, start, end)

    actual_start = start
    if not prices.empty:
        actual_start = prices.index.min().strftime("%Y-%m-%d")

    # Build metrics rows
    rows = []

    for _, row in tradeable.iterrows():
        ticker = row["ticker"]
        if ticker not in prices.columns:
            rows.append({
                "ticker": ticker,
                "asset_class": row["asset_class"],
                "return_pct": float("nan"),
                "max_drawdown_pct": float("nan"),
                "annualised_std_pct": float("nan"),
            })
            continue

        series = prices[ticker].dropna()
        rows.append({
            "ticker": ticker,
            "asset_class": row["asset_class"],
            "return_pct": _total_return(series),
            "max_drawdown_pct": _max_drawdown(series),
            "annualised_std_pct": _annualised_std(series),
        })

    # Money market rows
    mm_return = _get_tbill_return(start, end)
    for _, row in mm_positions.iterrows():
        rows.append({
            "ticker": row["ticker"],
            "asset_class": "Money Market",
            "return_pct": mm_return,
            "max_drawdown_pct": 0.0,
            "annualised_std_pct": 0.0,
        })

    metrics_df = pd.DataFrame(rows)

    # Correlation matrix — tradeable tickers only, minimum 20 observations
    corr_matrix = pd.DataFrame()
    if not prices.empty and len(prices) >= 20:
        daily_returns = prices.pct_change().dropna(how="all")
        # Drop columns with too many NaN values (short history)
        daily_returns = daily_returns.dropna(axis=1, thresh=int(len(daily_returns) * 0.5))
        if len(daily_returns.columns) >= 2:
            corr_matrix = daily_returns.corr(method="pearson")

    return {
        "metrics_df": metrics_df,
        "correlation_matrix": corr_matrix,
        "actual_start": actual_start,
        "period": period,
    }
