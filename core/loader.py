"""
core/loader.py — Portfolio file ingestion and normalisation.

Accepts .csv or .xlsx exports from Canadian brokerages (Questrade, Wealthsimple, etc.).
Returns a clean, validated DataFrame ready for the rest of the pipeline.
"""

import io
import pandas as pd


# Tickers that should never have .TO appended (indices, FX pairs, etc.)
_NO_SUFFIX_PREFIXES = {"^", "="}


def load_portfolio(file_obj: io.IOBase, filename: str) -> pd.DataFrame:
    """
    Parse and validate a portfolio export file.

    Parameters
    ----------
    file_obj : file-like object (from st.file_uploader)
    filename : original filename, used to detect .csv vs .xlsx

    Returns
    -------
    pd.DataFrame with columns:
        ticker, account, account_type, market_value_cad, weight_pct
    Plus a synthetic 'asset_class' column for Cash rows.

    Raises
    ------
    ValueError if required columns are missing or the file is malformed.
    """
    # --- 1. Read file ---
    if filename.lower().endswith(".xlsx"):
        df = pd.read_excel(file_obj, engine="openpyxl")
    elif filename.lower().endswith(".csv"):
        df = pd.read_csv(file_obj)
    else:
        raise ValueError(f"Unsupported file type: '{filename}'. Please upload a .csv or .xlsx file.")

    # --- 2. Strip column name whitespace ---
    df.columns = df.columns.str.strip()

    # --- 3. Validate required columns ---
    required = {"Ticker", "Market Value(CAD)"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required column(s): {', '.join(sorted(missing))}. "
            f"Found columns: {', '.join(df.columns.tolist())}"
        )

    # --- 4. Pre-clean market value (handle "$1,234.56" formats) ---
    mv_col = "Market Value(CAD)"
    df[mv_col] = (
        df[mv_col]
        .astype(str)
        .str.replace(r"[\$,\s]", "", regex=True)
        .replace("", "0")
        .astype(float)
    )

    # Drop rows with zero or negative market value (often header/footer artefacts)
    df = df[df[mv_col] > 0].copy()

    if df.empty:
        raise ValueError("No valid positions found after filtering zero-value rows.")

    # --- 5. Normalise Ticker column ---
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()

    # --- 6. Separate Cash rows ---
    cash_mask = df["Ticker"] == "CASH"
    cash_total = df.loc[cash_mask, mv_col].sum()
    df = df[~cash_mask].copy()

    # --- 7. Optional columns ---
    account_col = next((c for c in df.columns if c.strip().lower() == "account"), None)
    acct_type_col = next(
        (c for c in df.columns if c.strip().lower() in ("account type", "accounttype")), None
    )

    df["account"] = df[account_col].astype(str).str.strip() if account_col else "Unknown"
    df["account_type"] = (
        df[acct_type_col].astype(str).str.strip() if acct_type_col else "Unknown"
    )

    # --- 8. Append .TO suffix to Canadian tickers that have no exchange suffix ---
    def _normalise_ticker(t: str) -> str:
        if any(t.startswith(p) for p in _NO_SUFFIX_PREFIXES):
            return t
        if "." not in t:
            return t + ".TO"
        return t

    df["ticker"] = df["Ticker"].apply(_normalise_ticker)

    # --- 9. Aggregate duplicate tickers (same ticker across multiple account rows) ---
    agg_cols = {"market_value_cad": (mv_col, "sum")}
    # For account/account_type, concatenate unique values
    group = df.groupby("ticker", sort=False)

    mv_agg = group[mv_col].sum().reset_index()
    mv_agg.columns = ["ticker", "market_value_cad"]

    account_agg = (
        group["account"]
        .apply(lambda x: " / ".join(sorted(set(x))))
        .reset_index()
    )
    acct_type_agg = (
        group["account_type"]
        .apply(lambda x: " / ".join(sorted(set(x))))
        .reset_index()
    )

    result = mv_agg.merge(account_agg, on="ticker").merge(acct_type_agg, on="ticker")

    # --- 10. Recompute weight_pct from market values ---
    total_mv = result["market_value_cad"].sum()
    if cash_total > 0:
        total_mv += cash_total
    result["weight_pct"] = result["market_value_cad"] / total_mv * 100

    # --- 11. Add Cash synthetic row ---
    if cash_total > 0:
        cash_row = pd.DataFrame([{
            "ticker": "CASH",
            "account": "—",
            "account_type": "—",
            "market_value_cad": cash_total,
            "weight_pct": cash_total / total_mv * 100,
            "asset_class": "Cash",
        }])
        result = pd.concat([result, cash_row], ignore_index=True)

    # --- 12. Ensure asset_class column exists (non-Cash rows populated later by pipeline) ---
    if "asset_class" not in result.columns:
        result["asset_class"] = None

    return result[["ticker", "account", "account_type", "market_value_cad", "weight_pct", "asset_class"]]
