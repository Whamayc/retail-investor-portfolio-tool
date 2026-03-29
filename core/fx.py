"""
core/fx.py — CAD/USD FX rate fetching and price adjustment.

NOTE: The current portfolio is all TSX-listed (.TO) tickers priced in CAD,
so this module is not called in the main pipeline. It is retained for
forward-compatibility when USD-listed positions are added.

Usage in pages: cache the rate in st.session_state to avoid re-fetching
on every render. See get_usdcad_rate() docstring.
"""

import time
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

# Tickers with these suffixes are already CAD-priced — no FX adjustment needed
_CAD_SUFFIXES = (".TO", ".V", ".CN")


def identify_usd_tickers(tickers: list[str]) -> list[str]:
    """Return the subset of tickers that are USD-priced (no CAD exchange suffix)."""
    return [t for t in tickers if not any(t.upper().endswith(s) for s in _CAD_SUFFIXES)]


def get_usdcad_rate() -> float:
    """
    Fetch the latest USD/CAD exchange rate from yfinance.

    Intended to be cached in st.session_state with a 15-minute TTL:

        now = time.time()
        cached = st.session_state.get("usdcad_rate")
        cached_at = st.session_state.get("usdcad_rate_ts", 0)
        if cached is None or (now - cached_at) > 900:
            rate = get_usdcad_rate()
            st.session_state["usdcad_rate"] = rate
            st.session_state["usdcad_rate_ts"] = now
    """
    hist = yf.Ticker("USDCAD=X").history(period="2d")
    if hist.empty:
        return 1.36  # reasonable fallback
    return float(hist["Close"].iloc[-1])


def adjust_to_cad(prices_df: pd.DataFrame, usd_tickers: list[str]) -> pd.DataFrame:
    """
    Multiply USD-priced columns in prices_df by the current USDCAD rate.

    Parameters
    ----------
    prices_df  : DataFrame with date index and ticker columns (daily close prices)
    usd_tickers: list of column names to convert (must be a subset of prices_df.columns)

    Returns
    -------
    DataFrame with USD columns multiplied by USDCAD rate (in place copy).

    IMPORTANT: Apply this BEFORE computing returns, not after.
    """
    if not usd_tickers:
        return prices_df

    rate = get_usdcad_rate()
    result = prices_df.copy()
    valid = [t for t in usd_tickers if t in result.columns]
    result[valid] = result[valid] * rate
    return result
