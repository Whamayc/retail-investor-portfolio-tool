"""
core/money_market.py — Money market ETF detection.

Provides a hardcoded allowlist of known Canadian money market / high-interest
savings ETFs, plus a runtime check via yfinance category metadata.

These positions are classified as asset_class = "Money Market" and are never
sent through ETF holdings piercing.
"""

import yfinance as yf

MONEY_MARKET_TICKERS: set[str] = {
    "ZMMK.TO",   # BMO Money Market ETF
    "CMR.TO",    # iShares Premium Money Market ETF
    "PSA.TO",    # Purpose High Interest Savings ETF
    "CASH.TO",   # Horizons High Interest Savings ETF
    "HSAV.TO",   # Horizons Cash Maximizer ETF
    "CSAV.TO",   # CI High Interest Savings ETF
}

# Keywords in yfinance "category" field that identify money market / HISA funds
_MM_KEYWORDS = ("money market", "high interest savings", "canadian money market")


def is_money_market(ticker: str) -> bool:
    """
    Return True if the ticker should be treated as Money Market.

    Checks the hardcoded list first (fast path), then falls back to a
    yfinance `.info["category"]` lookup.

    NOTE: The yfinance fallback adds ~200-400ms latency. Callers that check
    multiple tickers should parallelise using ThreadPoolExecutor.
    """
    if ticker in MONEY_MARKET_TICKERS:
        return True
    try:
        info = yf.Ticker(ticker).info
        category = (info.get("category") or "").lower()
        return any(kw in category for kw in _MM_KEYWORDS)
    except Exception:
        return False
