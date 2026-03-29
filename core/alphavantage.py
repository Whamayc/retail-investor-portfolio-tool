"""
core/alphavantage.py — Alpha Vantage ETF_PROFILE holdings fetcher.

Endpoint: GET https://www.alphavantage.co/query
Params:   function=ETF_PROFILE, symbol={ticker}, apikey={key}

Response holdings field:
    [{"symbol": "AAPL", "description": "APPLE INC", "weight": "0.0747"}, ...]

weight is a decimal string (0.0747 = 7.47%).

Free tier: 25 calls/day, 5 calls/minute.
API key stored in ALPHAVANTAGE_API_KEY environment variable.
"""

import logging
import os
import time

import requests

# Minimum seconds between AV API calls — free tier allows 5/min (1 per 12s to be safe)
_MIN_CALL_INTERVAL = 12.0
_last_call_ts: float = 0.0

logger = logging.getLogger(__name__)

_AV_URL = "https://www.alphavantage.co/query"
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def _get_api_key() -> str | None:
    return os.environ.get("ALPHAVANTAGE_API_KEY", "").strip() or None


def fetch_av_holdings(ticker: str) -> list[dict]:
    """
    Fetch ETF top holdings from Alpha Vantage ETF_PROFILE endpoint.

    Parameters
    ----------
    ticker : normalised ticker, e.g. "XIC.TO" or "QQQ"

    Returns
    -------
    List of holding dicts in standard internal format:
        {"ticker", "name", "holding_weight_pct", "sector", "region", "asset_class"}

    weight is converted from decimal (0.0747) to percentage (7.47).

    Returns [] if API key missing, rate-limited, or no data returned.
    """
    api_key = _get_api_key()
    if not api_key or api_key == "your_alphavantage_api_key_here":
        logger.debug("ALPHAVANTAGE_API_KEY not set — skipping AV lookup for %s", ticker)
        return []

    # Alpha Vantage ETF_PROFILE only covers US-listed ETFs — skip Canadian exchange suffixes
    _CA_SUFFIXES = (".TO", ".V", ".CN", ".NE", ".TSX")
    if any(ticker.upper().endswith(s) for s in _CA_SUFFIXES):
        logger.debug("AV ETF_PROFILE does not cover Canadian ETFs — skipping %s", ticker)
        return []

    # Try with full ticker first (e.g. XIC.TO), then bare symbol (e.g. XIC)
    for symbol in _ticker_variants(ticker):
        holdings = _fetch_for_symbol(symbol, api_key)
        if holdings:
            return holdings

    logger.warning("Alpha Vantage returned no holdings for %s", ticker)
    return []


def _ticker_variants(ticker: str) -> list[str]:
    """Return ticker variants to try: full ticker first, then bare symbol."""
    variants = [ticker]
    if "." in ticker:
        variants.append(ticker.split(".")[0])
    return variants


def _rate_limit_wait() -> None:
    """Enforce minimum interval between API calls."""
    global _last_call_ts
    elapsed = time.time() - _last_call_ts
    if elapsed < _MIN_CALL_INTERVAL:
        time.sleep(_MIN_CALL_INTERVAL - elapsed)
    _last_call_ts = time.time()


def _fetch_for_symbol(symbol: str, api_key: str) -> list[dict]:
    _rate_limit_wait()
    try:
        resp = _SESSION.get(
            _AV_URL,
            params={"function": "ETF_PROFILE", "symbol": symbol, "apikey": api_key},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.warning("Alpha Vantage request failed for %s: %s", symbol, e)
        return []
    except ValueError as e:
        logger.warning("Alpha Vantage JSON parse error for %s: %s", symbol, e)
        return []

    # Rate limit or error responses come back as {"Information": "..."} or {"Note": "..."}
    if "Information" in data or "Note" in data:
        msg = data.get("Information") or data.get("Note", "")
        logger.warning("Alpha Vantage API message for %s: %s", symbol, msg[:120])
        return []

    raw_holdings = data.get("holdings")
    if not raw_holdings or not isinstance(raw_holdings, list):
        return []

    holdings = []
    for item in raw_holdings:
        if not isinstance(item, dict):
            continue

        weight_raw = item.get("weight") or "0"
        try:
            # AV returns decimal (0.0747); convert to percentage (7.47)
            weight_pct = float(weight_raw) * 100
        except (TypeError, ValueError):
            continue

        if weight_pct <= 0:
            continue

        holdings.append({
            "ticker": str(item.get("symbol") or "").strip(),
            "name": str(item.get("description") or item.get("symbol") or "").strip(),
            "holding_weight_pct": round(weight_pct, 4),
            "sector": "Unknown",   # AV ETF_PROFILE doesn't include per-holding sector
            "region": "Unknown",
            "asset_class": "Equity",
        })

    return holdings


def fetch_av_sector_weights(ticker: str) -> dict[str, float]:
    """
    Return sector weights from Alpha Vantage ETF_PROFILE as {sector_name: weight_pct}.
    Used to enrich the allocation page sector chart when per-holding sectors are unavailable.
    Returns {} on any failure.
    """
    api_key = _get_api_key()
    if not api_key or api_key == "your_alphavantage_api_key_here":
        return {}

    for symbol in _ticker_variants(ticker):
        try:
            resp = _SESSION.get(
                _AV_URL,
                params={"function": "ETF_PROFILE", "symbol": symbol, "apikey": api_key},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            continue

        if "Information" in data or "Note" in data:
            return {}

        sectors = data.get("sectors")
        if sectors and isinstance(sectors, list):
            return {
                s["sector"].title(): round(float(s.get("weight", 0)) * 100, 2)
                for s in sectors
                if isinstance(s, dict) and s.get("sector")
            }

    return {}
