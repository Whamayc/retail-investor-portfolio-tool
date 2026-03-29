"""
pages/1_import.py — Portfolio Import

Upload a .csv or .xlsx holdings file, validate it, run ETF piercing,
and populate st.session_state["portfolio"] and st.session_state["pierced_holdings"].

ETF holdings lookup order:
  1. User-uploaded CSV (per ETF, on this page)
  2. ETF itself as a single holding (no external fetching)
"""

import hashlib
import sys
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import streamlit as st
import yfinance as yf

# Ensure the portfolio_tool package is importable
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from core.loader import load_portfolio
from core.money_market import is_money_market

st.set_page_config(page_title="Import Portfolio", layout="wide")
st.title("Import Portfolio")
st.markdown("Upload your holdings export from Questrade, Wealthsimple, or any CAD brokerage.")

_ETF_QUOTE_TYPES = {"ETF", "MUTUALFUND"}


def _is_etf(ticker: str, info: dict) -> bool:
    return (info.get("quoteType") or "").upper() in _ETF_QUOTE_TYPES


def _fetch_info(ticker: str) -> tuple[str, dict]:
    try:
        return ticker, yf.Ticker(ticker).info or {}
    except Exception:
        return ticker, {}


def _assign_asset_class(ticker: str, mm_flags: dict[str, bool], info_map: dict[str, dict]) -> str:
    if ticker == "CASH":
        return "Cash"
    if mm_flags.get(ticker):
        return "Money Market"

    info = info_map.get(ticker, {})
    if _is_etf(ticker, info):
        cat = (info.get("category") or "").lower()
        if "bond" in cat or "fixed" in cat:
            return "Fixed Income"
        if "reit" in cat or "real estate" in cat:
            return "REITs"
        if "commodity" in cat or "gold" in cat:
            return "Commodity"

    return "Equity"


def _parse_etf_csv(file) -> list[dict] | None:
    """
    Parse a user-uploaded ETF holdings CSV into a holdings list.

    Required columns: Ticker (or Symbol), Weight (or Weight%, Pct, Allocation).
    Optional columns: Name, Sector, Region (or Country).

    Returns None if the file cannot be parsed or required columns are missing.
    """
    try:
        df = pd.read_csv(file)
        # Normalise column names: lowercase, strip, collapse spaces/punctuation
        df.columns = [
            c.strip().lower()
             .replace(" ", "_").replace("%", "pct")
             .replace("(", "").replace(")", "")
            for c in df.columns
        ]

        ticker_col = next(
            (c for c in df.columns if c in ("ticker", "symbol", "holding_ticker")), None
        )
        weight_col = next(
            (c for c in df.columns if c in (
                "weight", "weight_pct", "weightpct", "pct",
                "percent", "allocation", "allocation_pct",
            )),
            None,
        )
        name_col   = next((c for c in df.columns if c in ("name", "security_name", "holding_name", "description")), None)
        sector_col = next((c for c in df.columns if c in ("sector", "industry")), None)
        region_col = next((c for c in df.columns if c in ("region", "country", "geography")), None)

        if not ticker_col or not weight_col:
            return None

        holdings = []
        for _, row in df.iterrows():
            try:
                w = float(row[weight_col])
            except (TypeError, ValueError):
                continue
            holdings.append({
                "ticker":             str(row[ticker_col]).strip(),
                "name":               str(row[name_col] if name_col else row[ticker_col]).strip(),
                "holding_weight_pct": w,
                "sector":             str(row[sector_col] if sector_col else "Unknown"),
                "region":             str(row[region_col] if region_col else "Unknown"),
                "asset_class":        "Equity",
            })
        return holdings if holdings else None
    except Exception:
        return None


def _display_portfolio_summary(portfolio_df: pd.DataFrame) -> None:
    st.subheader("Portfolio Summary")
    display_cols = ["ticker", "account_type", "market_value_cad", "weight_pct", "asset_class", "sector"]
    display_df = portfolio_df[[c for c in display_cols if c in portfolio_df.columns]].copy()
    display_df["market_value_cad"] = display_df["market_value_cad"].map("${:,.2f}".format)
    display_df["weight_pct"]       = display_df["weight_pct"].map("{:.2f}%".format)
    st.dataframe(display_df.rename(columns={
        "ticker":           "Ticker",
        "account_type":     "Account Type",
        "market_value_cad": "Market Value (CAD)",
        "weight_pct":       "Weight",
        "asset_class":      "Asset Class",
        "sector":           "Sector",
    }), width="stretch")
    total = portfolio_df["market_value_cad"].sum()
    st.metric("Total Portfolio Value", f"${total:,.2f} CAD")
    st.info("Portfolio loaded. Use the sidebar to navigate to Allocation, Sentiment, Performance, or Comparison.")


# ── Step 1: Portfolio file upload ──────────────────────────────────────────────

uploaded = st.file_uploader(
    "Choose a file",
    type=["csv", "xlsx"],
    help="Required columns: Ticker, Market Value(CAD). Optional: Account, Account Type.",
)

if uploaded is None:
    if st.session_state.get("portfolio") is not None:
        st.info("A portfolio is already loaded. Upload a new file to replace it.")
        _display_portfolio_summary(st.session_state["portfolio"])
    st.stop()

# ── Detect file change and (re-)classify only when necessary ──────────────────

file_hash = hashlib.md5(uploaded.getvalue()).hexdigest()

if st.session_state.get("_import_file_hash") != file_hash:
    # New file — purge stale state
    for key in [k for k in st.session_state if k.startswith("_import_")]:
        del st.session_state[key]
    for key in ("portfolio", "pierced_holdings", "metrics_cache"):
        st.session_state.pop(key, None)
    st.session_state["_import_file_hash"] = file_hash

    with st.spinner("Reading file..."):
        try:
            raw_df = load_portfolio(uploaded, uploaded.name)
        except ValueError as e:
            st.error(f"File error: {e}")
            st.stop()

    non_cash = raw_df[raw_df["ticker"] != "CASH"].copy()
    tickers  = non_cash["ticker"].tolist()

    with st.spinner("Classifying positions..."):
        mm_flags: dict[str, bool] = {}
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(is_money_market, t): t for t in tickers}
            for fut in as_completed(futures):
                mm_flags[futures[fut]] = fut.result()

    non_mm_tickers = [t for t in tickers if not mm_flags.get(t)]

    with st.spinner("Fetching ticker metadata..."):
        info_map: dict[str, dict] = {}
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(_fetch_info, t): t for t in non_mm_tickers}
            for fut in as_completed(futures):
                t, info = fut.result()
                info_map[t] = info

    etf_tickers     = [t for t in non_mm_tickers if _is_etf(t, info_map.get(t, {}))]
    non_etf_tickers = [t for t in non_mm_tickers if t not in etf_tickers]

    st.session_state.update({
        "_import_raw_df":          raw_df,
        "_import_non_cash":        non_cash,
        "_import_tickers":         tickers,
        "_import_mm_flags":        mm_flags,
        "_import_info_map":        info_map,
        "_import_etf_tickers":     etf_tickers,
        "_import_non_etf_tickers": non_etf_tickers,
    })

# Retrieve cached classification from session state
raw_df          = st.session_state["_import_raw_df"]
non_cash        = st.session_state["_import_non_cash"]
tickers         = st.session_state["_import_tickers"]
mm_flags        = st.session_state["_import_mm_flags"]
info_map        = st.session_state["_import_info_map"]
etf_tickers     = st.session_state["_import_etf_tickers"]
non_etf_tickers = st.session_state["_import_non_etf_tickers"]

st.success(f"File loaded — {len(raw_df)} unique positions found.")

with st.expander("Raw positions (before enrichment)", expanded=False):
    st.dataframe(raw_df, width="stretch")

# ── Step 2: Optional per-ETF holdings CSVs ────────────────────────────────────

user_etf_holdings: dict[str, list[dict]] = {}

if etf_tickers:
    st.subheader("ETF Holdings (Optional)")
    st.markdown(
        "Upload a CSV for any ETF to provide your own holdings data — "
        "this takes **first priority** over all external sources.  \n"
        "**Required columns:** `Ticker`, `Weight` (as %).  \n"
        "**Optional columns:** `Name`, `Sector`, `Region`."
    )

    for ticker in etf_tickers:
        etf_file = st.file_uploader(
            f"{ticker}",
            type=["csv"],
            key=f"etf_upload_{ticker}",
            help=f"Holdings CSV for {ticker}",
        )
        if etf_file is not None:
            holdings = _parse_etf_csv(etf_file)
            if holdings:
                user_etf_holdings[ticker] = holdings
                st.success(f"{ticker}: {len(holdings)} holdings loaded from your CSV.")
            else:
                st.warning(
                    f"{ticker}: Could not parse CSV — ensure it has `Ticker` and `Weight` columns."
                )

# ── Step 3: Run Analysis ───────────────────────────────────────────────────────

run_analysis = st.button("Run Analysis", type="primary")

if not run_analysis and st.session_state.get("portfolio") is None:
    st.info("Click **Run Analysis** to fetch ETF holdings and build your portfolio summary.")
    st.stop()

if run_analysis:
    for key in ("portfolio", "pierced_holdings", "metrics_cache"):
        st.session_state.pop(key, None)

    pierced_holdings: list[dict] = []

    # --- ETF holdings ---
    for ticker in etf_tickers:
        weight = float(non_cash.loc[non_cash["ticker"] == ticker, "weight_pct"].iloc[0])

        if ticker in user_etf_holdings:
            # Use user-provided CSV
            for h in user_etf_holdings[ticker]:
                entry = dict(h)
                entry["weight_in_portfolio_pct"] = (h["holding_weight_pct"] / 100) * weight
                pierced_holdings.append(entry)
        else:
            # No CSV uploaded — use the ETF itself as a single holding
            info = info_map.get(ticker, {})
            pierced_holdings.append({
                "ticker":                  ticker,
                "name":                    info.get("longName") or info.get("shortName") or ticker,
                "holding_weight_pct":      100.0,
                "weight_in_portfolio_pct": weight,
                "sector":                  info.get("sector") or info.get("category") or "Unknown",
                "region":                  info.get("country") or "Unknown",
                "asset_class":             _assign_asset_class(ticker, mm_flags, info_map),
            })

    # --- Enrich individual stocks ---
    for ticker in non_etf_tickers:
        info   = info_map.get(ticker, {})
        weight = float(non_cash.loc[non_cash["ticker"] == ticker, "weight_pct"].iloc[0])
        pierced_holdings.append({
            "ticker":               ticker,
            "name":                 info.get("longName") or info.get("shortName") or ticker,
            "holding_weight_pct":   100.0,
            "weight_in_portfolio_pct": weight,
            "sector":               info.get("sector") or "Unknown",
            "region":               info.get("country") or "Unknown",
            "asset_class":          "Equity",
        })

    portfolio_df = raw_df.copy()
    portfolio_df["asset_class"] = portfolio_df["ticker"].apply(
        lambda ticker: _assign_asset_class(ticker, mm_flags, info_map)
    )

    def _assign_sector(row) -> str:
        if row["ticker"] == "CASH":
            return "Cash"
        if mm_flags.get(row["ticker"]):
            return "Money Market"
        info = info_map.get(row["ticker"], {})
        return info.get("sector") or info.get("category") or "Unknown"

    portfolio_df["sector"] = portfolio_df.apply(_assign_sector, axis=1)

    # --- Money market pierced holdings ---
    for ticker in tickers:
        if mm_flags.get(ticker):
            weight = float(non_cash.loc[non_cash["ticker"] == ticker, "weight_pct"].iloc[0])
            pierced_holdings.append({
                "ticker":               ticker,
                "name":                 ticker,
                "holding_weight_pct":   100.0,
                "weight_in_portfolio_pct": weight,
                "sector":               "Money Market",
                "region":               "Canada",
                "asset_class":          "Money Market",
            })

    # --- Cash ---
    cash_rows = raw_df[raw_df["ticker"] == "CASH"]
    if not cash_rows.empty:
        cash_weight = float(cash_rows["weight_pct"].sum())
        pierced_holdings.append({
            "ticker":               "CASH",
            "name":                 "Cash",
            "holding_weight_pct":   100.0,
            "weight_in_portfolio_pct": cash_weight,
            "sector":               "Cash",
            "region":               "Canada",
            "asset_class":          "Cash",
        })

    st.session_state["portfolio"]        = portfolio_df
    st.session_state["pierced_holdings"] = pierced_holdings

    _display_portfolio_summary(portfolio_df)

elif st.session_state.get("portfolio") is not None:
    _display_portfolio_summary(st.session_state["portfolio"])
