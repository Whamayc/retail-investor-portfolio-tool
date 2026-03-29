"""
core/fmp.py — Financial Modeling Prep ETF holdings fetcher.

Uses the FMP ETF Holder endpoint as the primary holdings source before
falling back to provider-specific web scraping.

API key is read from the FMP_API_KEY environment variable.

Endpoint: GET https://financialmodelingprep.com/api/v3/etf-holder/{symbol}
Response: [{"asset": str, "name": str, "weightPercentage": float, ...}, ...]

weightPercentage is already a percentage (e.g. 7.5 means 7.5%).
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

_FMP_BASE = "https://financialmodelingprep.com/api/v3"
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def _get_api_key() -> str | None:
    return os.environ.get("FMP_API_KEY", "").strip() or None


def fetch_fmp_holdings(ticker: str) -> list[dict]:
    """
    Fetch ETF holdings from FMP for a given ticker.

    Parameters
    ----------
    ticker : normalised ticker, e.g. "XIC.TO"

    Returns
    -------
    List of holding dicts in the standard internal format:
        {"ticker", "name", "holding_weight_pct", "sector", "region", "asset_class"}

    Returns [] if the API key is missing, the request fails, or no data is returned.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.debug("FMP_API_KEY not set — skipping FMP lookup for %s", ticker)
        return []

    # FMP accepts both "XIC.TO" and "XIC" — try with suffix first
    for symbol in [ticker, ticker.split(".")[0]]:
        holdings = _fetch_for_symbol(symbol, api_key)
        if holdings:
            return holdings

    logger.warning("FMP returned no holdings for %s", ticker)
    return []


def _fetch_for_symbol(symbol: str, api_key: str) -> list[dict]:
    url = f"{_FMP_BASE}/etf-holder/{symbol}"
    try:
        resp = _SESSION.get(url, params={"apikey": api_key}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.warning("FMP request failed for %s: %s", symbol, e)
        return []
    except ValueError as e:
        logger.warning("FMP JSON parse error for %s: %s", symbol, e)
        return []

    if not isinstance(data, list) or not data:
        return []

    # FMP returns error dicts like {"Error Message": "..."} as a list sometimes
    if isinstance(data[0], dict) and "Error Message" in data[0]:
        logger.warning("FMP error for %s: %s", symbol, data[0]["Error Message"])
        return []

    holdings = []
    for item in data:
        if not isinstance(item, dict):
            continue

        weight_raw = item.get("weightPercentage") or item.get("weight") or 0
        try:
            weight = float(weight_raw)
        except (TypeError, ValueError):
            continue

        if weight <= 0:
            continue

        holdings.append({
            "ticker": str(item.get("asset") or item.get("ticker") or "").strip(),
            "name": str(item.get("name") or item.get("asset") or "").strip(),
            "holding_weight_pct": weight,
            "sector": str(item.get("sector") or item.get("industry") or "Unknown").strip(),
            "region": str(item.get("country") or "Unknown").strip(),
            "asset_class": str(item.get("assetClass") or "Equity").strip(),
        })

    return holdings
