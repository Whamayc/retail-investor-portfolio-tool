"""
core/etf_piercing.py — ETF holdings piercing orchestrator.

For each ETF position, the lookup order is:
  1. Daily cache       (cache/<ticker>.json, 24h TTL)
  2. Alpha Vantage     (ETF_PROFILE endpoint — requires ALPHAVANTAGE_API_KEY env var, 25 calls/day free)
  3. Provider scraping (iShares CSV / Vanguard JSON / BMO GAM page — comprehensive holdings)
  4. yfinance funds_data.top_holdings  (free, no key, top ~10 holdings for any ETF)
  5. Single-position fallback  (yfinance metadata, with UI warning)

All holdings are scaled to portfolio-level exposure:
    weight_in_portfolio_pct = (holding_weight_pct / 100) * investor_weight_pct
"""

import json
import logging
import os
import pathlib
import time

import yfinance as yf

from .alphavantage import fetch_av_holdings
from .money_market import is_money_market
from .provider_urls import PROVIDER_MAP, get_holdings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fund-of-fund wrappers — Canadian ETFs that hold a single US ETF 100%
# Pierce through to the underlying US ticker for real holdings.
# ---------------------------------------------------------------------------

FUND_OF_FUNDS_MAP: dict[str, str] = {
    "VFV.TO":  "VOO",    # Vanguard S&P 500 ETF (Canada) → wraps VOO
    "QQC.TO":  "QQQM",   # Invesco QQQ (Canada) → wraps QQQM
}

# ---------------------------------------------------------------------------
# Cache directory — resolved relative to this file, not Streamlit's cwd
# ---------------------------------------------------------------------------

_CACHE_DIR = pathlib.Path(__file__).parent.parent / "cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_CACHE_TTL_SECONDS = 86_400  # 24 hours


def _cache_path(ticker: str) -> pathlib.Path:
    safe = ticker.replace(".", "_")
    return _CACHE_DIR / f"{safe}.json"


def _load_cache(ticker: str) -> list[dict] | None:
    """Return cached holdings if they exist and are younger than TTL."""
    p = _cache_path(ticker)
    if not p.exists():
        return None
    age = time.time() - p.stat().st_mtime
    if age > _CACHE_TTL_SECONDS:
        return None
    try:
        with p.open() as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(ticker: str, holdings: list[dict]) -> None:
    try:
        with _cache_path(ticker).open("w") as f:
            json.dump(holdings, f)
    except OSError as e:
        logger.warning("Cache write failed for %s: %s", ticker, e)


# ---------------------------------------------------------------------------
# Single-position fallback helpers
# ---------------------------------------------------------------------------

def _single_position_fallback(ticker: str, weight_pct: float) -> dict:
    """
    When scraping fails, treat the ETF as a single holding.
    Attempts to enrich sector/asset_class via yfinance.
    """
    info = {}
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception:
        pass

    sector = info.get("sector") or info.get("category") or "Unknown"
    asset_class = _infer_asset_class(info)

    return {
        "holdings": [{
            "ticker": ticker,
            "name": info.get("longName") or info.get("shortName") or ticker,
            "holding_weight_pct": 100.0,
            "weight_in_portfolio_pct": weight_pct,
            "sector": sector,
            "region": "Unknown",
            "asset_class": asset_class,
        }],
        "warning": (
            f"Holdings data unavailable for {ticker} — shown as a single position. "
            "You can upload a manual holdings CSV on the Import page."
        ),
        "source": "fallback",
    }


def _infer_asset_class(info: dict) -> str:
    category = (info.get("category") or "").lower()
    if any(k in category for k in ("bond", "fixed income", "income")):
        return "Fixed Income"
    if "reit" in category or "real estate" in category:
        return "REITs"
    if any(k in category for k in ("commodity", "gold", "silver", "oil")):
        return "Commodity"
    return "Equity"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def pierce_etf(ticker: str, weight_pct: float) -> dict:
    """
    Return pierced holdings for an ETF position.

    Parameters
    ----------
    ticker     : normalised ticker (e.g. "XIC.TO")
    weight_pct : investor's portfolio weight as a percentage (e.g. 15.3)

    Returns
    -------
    dict with keys:
        holdings  : list of holding dicts, each with weight_in_portfolio_pct
        warning   : str or None
        source    : "cache" | "alphavantage" | "scraped" | "yfinance" | "fallback" | "money_market"
    """
    # --- Money market / HISA — classify immediately, no piercing ---
    if is_money_market(ticker):
        return {
            "holdings": [{
                "ticker": ticker,
                "name": ticker,
                "holding_weight_pct": 100.0,
                "weight_in_portfolio_pct": weight_pct,
                "sector": "Money Market",
                "region": "Canada",
                "asset_class": "Money Market",
            }],
            "warning": None,
            "source": "money_market",
        }

    # --- Try cache ---
    cached = _load_cache(ticker)
    if cached is not None:
        return _scale_and_wrap(ticker, cached, weight_pct, source="cache")

    # --- Fund-of-fund: pierce through to underlying US ETF ---
    if ticker in FUND_OF_FUNDS_MAP:
        underlying = FUND_OF_FUNDS_MAP[ticker]
        # Check if underlying is already cached
        underlying_cached = _load_cache(underlying)
        if underlying_cached:
            _write_cache(ticker, underlying_cached)
            return _scale_and_wrap(ticker, underlying_cached, weight_pct, source="cache")
        # Fetch underlying via Alpha Vantage (works for US tickers)
        raw_holdings = fetch_av_holdings(underlying)
        if raw_holdings:
            _write_cache(ticker, raw_holdings)
            _write_cache(underlying, raw_holdings)
            return _scale_and_wrap(ticker, raw_holdings, weight_pct, source="alphavantage")
        # AV failed (rate limit?): try yfinance on the underlying US ticker directly
        raw_holdings = _fetch_yfinance_holdings(underlying)
        if raw_holdings:
            _write_cache(ticker, raw_holdings)
            _write_cache(underlying, raw_holdings)
            return _scale_and_wrap(ticker, raw_holdings, weight_pct, source="yfinance")
        # All sources failed — fall through to single-position fallback below

    # --- Try Alpha Vantage (primary API source, top ~10 holdings) ---
    raw_holdings = fetch_av_holdings(ticker)
    if raw_holdings:
        _write_cache(ticker, raw_holdings)
        return _scale_and_wrap(ticker, raw_holdings, weight_pct, source="alphavantage")

    # --- Try provider web scraping (comprehensive holdings for known providers) ---
    raw_holdings = get_holdings(ticker)
    if raw_holdings:
        _write_cache(ticker, raw_holdings)
        return _scale_and_wrap(ticker, raw_holdings, weight_pct, source="scraped")

    # --- Try yfinance funds_data (free, no key, top ~10 holdings for any ETF) ---
    raw_holdings = _fetch_yfinance_holdings(ticker)
    if raw_holdings:
        _write_cache(ticker, raw_holdings)
        return _scale_and_wrap(ticker, raw_holdings, weight_pct, source="yfinance")

    # --- Single-position fallback ---
    return _single_position_fallback(ticker, weight_pct)


def _fetch_yfinance_holdings(ticker: str) -> list[dict]:
    """
    Fetch top ~10 ETF holdings via yfinance funds_data.top_holdings.
    Free, no API key required, works for Canadian ETFs.
    Returns [] on any failure.
    """
    try:
        fd = yf.Ticker(ticker).funds_data
        df = fd.top_holdings
        if df is None or df.empty:
            return []
    except Exception as e:
        logger.debug("yfinance funds_data failed for %s: %s", ticker, e)
        return []

    holdings = []
    for symbol, row in df.iterrows():
        weight_decimal = row.get("Holding Percent", 0)
        try:
            weight_pct = float(weight_decimal) * 100
        except (TypeError, ValueError):
            continue
        if weight_pct <= 0:
            continue

        holdings.append({
            "ticker": str(symbol).strip(),
            "name": str(row.get("Name") or symbol).strip(),
            "holding_weight_pct": round(weight_pct, 4),
            "sector": "Unknown",
            "region": "Unknown",
            "asset_class": "Equity",
        })

    return holdings


def _scale_and_wrap(
    ticker: str,
    raw_holdings: list[dict],
    investor_weight_pct: float,
    source: str,
) -> dict:
    """Scale holding_weight_pct to portfolio-level exposure and wrap result."""
    scaled = []
    for h in raw_holdings:
        entry = dict(h)
        entry["weight_in_portfolio_pct"] = (
            (entry.get("holding_weight_pct", 0) / 100) * investor_weight_pct
        )
        scaled.append(entry)
    return {"holdings": scaled, "warning": None, "source": source}
