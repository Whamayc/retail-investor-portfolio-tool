"""
core/provider_urls.py — ETF holdings scraping by provider.

Each scraper returns a list of dicts:
    [{"ticker": str, "name": str, "holding_weight_pct": float,
      "sector": str, "region": str, "asset_class": str}, ...]

Returns [] on any failure so etf_piercing.py can apply the single-position fallback.

LAST_VERIFIED dates indicate when each scraper was last confirmed working.
"""

import io
import json
import logging

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Provider mapping — ticker → provider key
# ---------------------------------------------------------------------------

PROVIDER_MAP: dict[str, str] = {
    "XIC.TO":  "ishares_ca",
    "XDIV.TO": "ishares_ca",
    "XEF.TO":  "ishares_ca",
    # VDY.TO: Vanguard CA page migrated to Angular SPA — scraper dead, use yfinance fallback
    "VDY.TO":  "fallback",
    # VFV.TO: handled by FUND_OF_FUNDS_MAP in etf_piercing.py (pierces through to VOO)
    "VFV.TO":  "fallback",
    "ZLB.TO":  "bmo_gam",
    "QQC.TO":  "fallback",
}

# ---------------------------------------------------------------------------
# Shared HTTP session
# ---------------------------------------------------------------------------

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-CA,en;q=0.9",
})

# ---------------------------------------------------------------------------
# iShares Canada
# LAST_VERIFIED: 2026-03-28
# ---------------------------------------------------------------------------

# BlackRock CA fund page IDs — extract from the URL of each ETF's product page
ISHARES_FUND_IDS: dict[str, str] = {
    "XIC.TO":  "239837",
    "XDIV.TO": "287823",   # corrected: iShares Core MSCI Canadian Quality Dividend
    "XEF.TO":  "251421",   # corrected: iShares Core MSCI EAFE IMI Index ETF
}

ISHARES_BASE = (
    "https://www.blackrock.com/ca/investors/en/products/{fund_id}/"
    "ishares-{slug}/1464253357814.ajax?tab=holdings&fileType=csv"
)

# Simpler direct CSV URL pattern that has been more reliable
ISHARES_CSV_URL = (
    "https://www.blackrock.com/ca/individual/en/products/{fund_id}/"
    "?tab=holdings&fileType=csv"
)

_ISHARES_SLUGS: dict[str, str] = {
    "XIC.TO":  "core-sp-tsx-capped-composite-index-etf",
    "XDIV.TO": "canadian-select-dividend-index-etf",
    "XEF.TO":  "core-msci-eafe-imi-index-etf",
}


def _find_header_row(lines: list[str]) -> int:
    """Return the index of the line containing both 'Name' and 'Weight'."""
    for i, line in enumerate(lines):
        if "Name" in line and "Weight" in line:
            return i
    return 0


def scrape_ishares_ca(ticker: str) -> list[dict]:
    """Fetch holdings CSV from BlackRock Canada and parse it."""
    # LAST_VERIFIED: 2026-03-28
    fund_id = ISHARES_FUND_IDS.get(ticker)
    if not fund_id:
        logger.warning("No iShares fund ID for %s", ticker)
        return []

    url = ISHARES_CSV_URL.format(fund_id=fund_id)
    try:
        resp = _SESSION.get(url, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("iShares CA request failed for %s: %s", ticker, e)
        return []

    content = resp.text
    lines = content.splitlines()
    header_idx = _find_header_row(lines)

    try:
        df = pd.read_csv(
            io.StringIO(content),
            skiprows=header_idx,
            on_bad_lines="skip",
        )
    except Exception as e:
        logger.warning("iShares CA CSV parse failed for %s: %s", ticker, e)
        return []

    # Normalise column names
    df.columns = df.columns.str.strip()

    # Find weight column (may be "Weight (%)" or "Gewichtung (%)" etc.)
    weight_col = next(
        (c for c in df.columns if "weight" in c.lower() or "%" in c.lower()),
        None,
    )
    name_col = next((c for c in df.columns if c.strip().lower() == "name"), None)
    ticker_col = next(
        (c for c in df.columns if "ticker" in c.lower() or "symbol" in c.lower()),
        None,
    )
    sector_col = next((c for c in df.columns if "sector" in c.lower()), None)
    asset_col = next(
        (c for c in df.columns if "asset" in c.lower() and "class" in c.lower()),
        None,
    )

    if weight_col is None or name_col is None:
        logger.warning("iShares CA: could not find Name/Weight columns for %s. Cols: %s", ticker, df.columns.tolist())
        return []

    holdings = []
    for _, row in df.iterrows():
        weight_raw = str(row.get(weight_col, "")).replace(",", "").replace("%", "").strip()
        try:
            weight = float(weight_raw)
        except ValueError:
            continue
        if weight <= 0:
            continue

        holdings.append({
            "ticker": str(row.get(ticker_col, "")).strip() if ticker_col else "",
            "name": str(row.get(name_col, "")).strip(),
            "holding_weight_pct": weight,
            "sector": str(row.get(sector_col, "")).strip() if sector_col else "Unknown",
            "region": "Unknown",  # enriched later via COUNTRY_TO_REGION
            "asset_class": str(row.get(asset_col, "Equity")).strip() if asset_col else "Equity",
        })

    return holdings


# ---------------------------------------------------------------------------
# Vanguard Canada
# LAST_VERIFIED: 2026-03-28
# ---------------------------------------------------------------------------

VANGUARD_CA_URLS: dict[str, str] = {
    "VDY.TO": (
        "https://www.vanguard.ca/en/product/etf/equity/9563/"
        "vanguard-ftse-canadian-high-dividend-yield-index-etf"
    ),
    "VFV.TO": (
        "https://www.vanguard.ca/en/product/etf/equity/9554/"
        "vanguard-sp-500-index-etf"
    ),
}


def _extract_vanguard_json(html: str) -> dict | None:
    """Extract the largest <script type='application/json'> block from Vanguard page."""
    soup = BeautifulSoup(html, "html.parser")
    candidates = soup.find_all("script", {"type": "application/json"})
    # Pick the largest block — it's most likely the fund data payload
    best = max(candidates, key=lambda t: len(t.string or ""), default=None)
    if best and best.string:
        try:
            return json.loads(best.string)
        except json.JSONDecodeError:
            return None
    return None


def scrape_vanguard_ca(ticker: str) -> list[dict]:
    """Fetch Vanguard CA fund page and parse embedded JSON holdings."""
    # LAST_VERIFIED: 2026-03-28
    url = VANGUARD_CA_URLS.get(ticker)
    if not url:
        return []

    try:
        resp = _SESSION.get(url, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("Vanguard CA request failed for %s: %s", ticker, e)
        return []

    data = _extract_vanguard_json(resp.text)
    if data is None:
        logger.warning("Vanguard CA: no JSON found for %s", ticker)
        return []

    # Navigate the JSON tree — wrap every key access defensively
    try:
        holdings_raw = (
            data["props"]["pageProps"]["fundData"]["holdings"]
        )
    except (KeyError, TypeError):
        # Try alternate path
        try:
            holdings_raw = data["props"]["pageProps"]["holdings"]
        except (KeyError, TypeError):
            logger.warning("Vanguard CA: holdings key not found for %s", ticker)
            return []

    holdings = []
    for h in holdings_raw:
        try:
            weight = float(str(h.get("weight") or h.get("portfolioWeight") or 0).replace("%", ""))
        except (ValueError, AttributeError):
            continue
        if weight <= 0:
            continue

        holdings.append({
            "ticker": str(h.get("ticker") or h.get("symbol") or "").strip(),
            "name": str(h.get("name") or h.get("holdingName") or "").strip(),
            "holding_weight_pct": weight,
            "sector": str(h.get("sector") or h.get("sectorName") or "Unknown").strip(),
            "region": str(h.get("country") or h.get("region") or "Unknown").strip(),
            "asset_class": str(h.get("assetClass") or "Equity").strip(),
        })

    return holdings


# ---------------------------------------------------------------------------
# BMO GAM
# LAST_VERIFIED: 2026-03-28
# ---------------------------------------------------------------------------

BMO_CA_URLS: dict[str, str] = {
    "ZLB.TO": (
        "https://www.bmo.com/gam/ca/advisor/products/etfs"
        "?fundUrl=bmo-low-volatility-canadian-equity-etf"
        "#fundUrl=bmo-low-volatility-canadian-equity-etf"
    ),
}

# BMO also exposes a direct holdings CSV endpoint
BMO_HOLDINGS_API = (
    "https://www.bmo.com/gam/ca/advisor/products/etfs/holdings-csv"
    "?fundUrl={fund_slug}"
)

_BMO_FUND_SLUGS: dict[str, str] = {
    "ZLB.TO": "bmo-low-volatility-canadian-equity-etf",
}


def scrape_bmo_gam(ticker: str) -> list[dict]:
    """Fetch BMO GAM fund page, locate CSV download link, and parse holdings."""
    # LAST_VERIFIED: 2026-03-28
    fund_slug = _BMO_FUND_SLUGS.get(ticker)
    if not fund_slug:
        return []

    # Try direct CSV API first (faster, more reliable)
    csv_url = BMO_HOLDINGS_API.format(fund_slug=fund_slug)
    try:
        resp = _SESSION.get(csv_url, timeout=20)
        if resp.status_code == 200 and "text/csv" in resp.headers.get("content-type", ""):
            return _parse_bmo_csv(resp.text, ticker)
    except requests.RequestException:
        pass

    # Fallback: scrape the fund page for a CSV link
    page_url = BMO_CA_URLS.get(ticker)
    if not page_url:
        return []

    try:
        resp = _SESSION.get(page_url, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("BMO GAM request failed for %s: %s", ticker, e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    # Find <a> tags whose href contains ".csv" and "holdings"
    csv_link = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".csv" in href.lower() and "holding" in href.lower():
            csv_link = href
            break

    if not csv_link:
        logger.warning("BMO GAM: no holdings CSV link found for %s", ticker)
        return []

    if not csv_link.startswith("http"):
        csv_link = "https://www.bmo.com" + csv_link

    try:
        csv_resp = _SESSION.get(csv_link, timeout=20)
        csv_resp.raise_for_status()
        return _parse_bmo_csv(csv_resp.text, ticker)
    except requests.RequestException as e:
        logger.warning("BMO GAM CSV download failed for %s: %s", ticker, e)
        return []


def _parse_bmo_csv(content: str, ticker: str) -> list[dict]:
    """Parse a BMO GAM holdings CSV string."""
    lines = content.splitlines()
    header_idx = _find_header_row(lines)

    try:
        df = pd.read_csv(io.StringIO(content), skiprows=header_idx, on_bad_lines="skip")
    except Exception as e:
        logger.warning("BMO GAM CSV parse failed for %s: %s", ticker, e)
        return []

    df.columns = df.columns.str.strip()

    weight_col = next(
        (c for c in df.columns if "weight" in c.lower() or "%" in c.lower()), None
    )
    name_col = next(
        (c for c in df.columns if c.strip().lower() in ("name", "security name", "holding")),
        None,
    )
    ticker_col = next(
        (c for c in df.columns if "ticker" in c.lower() or "symbol" in c.lower()), None
    )
    sector_col = next((c for c in df.columns if "sector" in c.lower()), None)

    if weight_col is None or name_col is None:
        logger.warning("BMO GAM: could not identify Name/Weight columns for %s", ticker)
        return []

    holdings = []
    for _, row in df.iterrows():
        weight_raw = str(row.get(weight_col, "")).replace(",", "").replace("%", "").strip()
        try:
            weight = float(weight_raw)
        except ValueError:
            continue
        if weight <= 0:
            continue

        holdings.append({
            "ticker": str(row.get(ticker_col, "")).strip() if ticker_col else "",
            "name": str(row.get(name_col, "")).strip(),
            "holding_weight_pct": weight,
            "sector": str(row.get(sector_col, "Unknown")).strip() if sector_col else "Unknown",
            "region": "Unknown",
            "asset_class": "Equity",
        })

    return holdings


# ---------------------------------------------------------------------------
# Public dispatch function
# ---------------------------------------------------------------------------

def get_holdings(ticker: str) -> list[dict]:
    """
    Dispatch to the correct provider scraper for a given ticker.
    Returns [] on any failure (triggers single-position fallback in etf_piercing.py).
    """
    provider = PROVIDER_MAP.get(ticker, "fallback")

    if provider == "ishares_ca":
        return scrape_ishares_ca(ticker)
    elif provider == "vanguard_ca":
        return scrape_vanguard_ca(ticker)
    elif provider == "bmo_gam":
        return scrape_bmo_gam(ticker)
    else:
        # fallback — no scraping; etf_piercing.py treats [] as single-position
        return []
