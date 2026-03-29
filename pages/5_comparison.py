"""
pages/5_comparison.py — Comparison Lab

Side-by-side view of current portfolio vs a user-adjusted rebalancing scenario.
Cash and Money Market positions are shown as fixed (excluded from rebalancing sum).
"""

import sys
import pathlib

import pandas as pd
import streamlit as st

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from core.metrics import compute_metrics, PERIODS

st.set_page_config(page_title="Comparison Lab", layout="wide")

# ---------------------------------------------------------------------------
# Guard
# ---------------------------------------------------------------------------

if "portfolio" not in st.session_state:
    st.warning("No portfolio loaded. Please import your holdings first.")
    st.stop()

portfolio_df: pd.DataFrame = st.session_state["portfolio"]

st.title("Comparison Lab")
st.markdown(
    "Simulate a rebalancing scenario and compare metrics side-by-side. "
    "Cash and Money Market positions are fixed — adjust the remaining allocations to total 100%."
)

# ---------------------------------------------------------------------------
# Separate adjustable from fixed positions
# ---------------------------------------------------------------------------

FIXED_CLASSES = {"Cash", "Money Market"}

fixed_df = portfolio_df[portfolio_df["asset_class"].isin(FIXED_CLASSES)].copy()
adjustable_df = portfolio_df[~portfolio_df["asset_class"].isin(FIXED_CLASSES)].copy()

fixed_weight_total = fixed_df["weight_pct"].sum()
adjustable_weight_budget = 100.0 - fixed_weight_total  # headroom for adjustable tickers

# ---------------------------------------------------------------------------
# Default adjusted weights — preserve current relative weights scaled to budget
# ---------------------------------------------------------------------------

if "adjusted_weights" not in st.session_state:
    current_adjustable_total = adjustable_df["weight_pct"].sum()
    if current_adjustable_total > 0:
        scaled = adjustable_df["weight_pct"] / current_adjustable_total * adjustable_weight_budget
    else:
        scaled = adjustable_df["weight_pct"]
    st.session_state["adjusted_weights"] = dict(
        zip(adjustable_df["ticker"], scaled.round(2))
    )

# ---------------------------------------------------------------------------
# Two-column layout
# ---------------------------------------------------------------------------

col_current, col_adjusted = st.columns(2)

# ---- Left: Current portfolio ----
with col_current:
    st.subheader("Current Portfolio")
    current_display = portfolio_df[["ticker", "asset_class", "weight_pct"]].copy()
    current_display["weight_pct"] = current_display["weight_pct"].map("{:.2f}%".format)
    current_display = current_display.rename(columns={
        "ticker": "Ticker",
        "asset_class": "Asset Class",
        "weight_pct": "Weight",
    })
    st.dataframe(current_display, width="stretch", hide_index=True)

    if not fixed_df.empty:
        st.caption(f"Fixed positions (not adjustable): {', '.join(fixed_df['ticker'].tolist())} — {fixed_weight_total:.1f}%")

# ---- Right: Adjusted portfolio ----
with col_adjusted:
    st.subheader("Adjusted Portfolio")

    adj_weights = st.session_state["adjusted_weights"]

    # Render number inputs
    new_weights: dict[str, float] = {}
    for _, row in adjustable_df.iterrows():
        ticker = row["ticker"]
        current_val = adj_weights.get(ticker, row["weight_pct"])
        new_val = st.number_input(
            label=f"{ticker}  ({row['asset_class']})",
            min_value=0.0,
            max_value=100.0,
            value=float(round(current_val, 2)),
            step=0.5,
            format="%.2f",
            key=f"adj_{ticker}",
        )
        new_weights[ticker] = new_val

    # Update session state
    st.session_state["adjusted_weights"] = new_weights

    # Running total
    adjustable_total = sum(new_weights.values())
    grand_total = adjustable_total + fixed_weight_total
    delta_str = f"{grand_total - 100:.2f}%"

    st.metric(
        "Total Allocated (adjustable)",
        f"{adjustable_total:.1f}%",
        delta=f"vs budget of {adjustable_weight_budget:.1f}%",
    )
    st.metric("Grand Total (incl. fixed)", f"{grand_total:.1f}%")

    at_100 = abs(grand_total - 100.0) <= 0.01

    calculate_btn = st.button(
        "Calculate Comparison",
        disabled=not at_100,
        type="primary",
        help="Grand total must equal 100% to calculate." if not at_100 else "",
    )

# ---------------------------------------------------------------------------
# Period selector
# ---------------------------------------------------------------------------

st.divider()
selected_period = st.selectbox("Comparison period", PERIODS, index=PERIODS.index("1Y"))

# ---------------------------------------------------------------------------
# Comparison calculation
# ---------------------------------------------------------------------------

if calculate_btn or st.session_state.get("comparison_result"):

    # Build adjusted portfolio DataFrame
    adj_rows = []
    for _, row in adjustable_df.iterrows():
        adj_rows.append({
            "ticker": row["ticker"],
            "weight_pct": new_weights.get(row["ticker"], 0.0),
            "asset_class": row["asset_class"],
        })
    for _, row in fixed_df.iterrows():
        adj_rows.append({
            "ticker": row["ticker"],
            "weight_pct": row["weight_pct"],
            "asset_class": row["asset_class"],
        })

    adj_portfolio_df = pd.DataFrame(adj_rows)

    if calculate_btn:
        with st.spinner("Computing comparison metrics..."):
            try:
                current_result = compute_metrics(portfolio_df, selected_period)
                adj_result = compute_metrics(adj_portfolio_df, selected_period)
                st.session_state["comparison_result"] = {
                    "current": current_result,
                    "adjusted": adj_result,
                    "period": selected_period,
                }
            except Exception as e:
                st.error(f"Calculation failed: {e}")
                st.stop()

    result = st.session_state.get("comparison_result")
    if result is None or result["period"] != selected_period:
        st.info("Click **Calculate Comparison** to see results.")
        st.stop()

    current_metrics = result["current"]["metrics_df"]
    adj_metrics = result["adjusted"]["metrics_df"]

    # ---------------------------------------------------------------------------
    # Render comparison table
    # ---------------------------------------------------------------------------

    st.subheader(f"Metrics Comparison — {result['period']}")

    # Merge on ticker
    merged = current_metrics.merge(
        adj_metrics,
        on=["ticker", "asset_class"],
        suffixes=("_current", "_adjusted"),
    )

    METRIC_COLS = ["return_pct", "max_drawdown_pct", "annualised_std_pct"]
    DISPLAY_NAMES = {
        "return_pct": "Return (%)",
        "max_drawdown_pct": "Max Drawdown (%)",
        "annualised_std_pct": "Ann. Std Dev (%)",
    }

    for metric in METRIC_COLS:
        cur_col = f"{metric}_current"
        adj_col = f"{metric}_adjusted"
        delta_col = f"{metric}_delta"
        if cur_col in merged.columns and adj_col in merged.columns:
            merged[delta_col] = merged[adj_col] - merged[cur_col]

    # Build display table
    display_rows = []
    for _, row in merged.iterrows():
        display_row = {
            "Ticker": row["ticker"],
            "Asset Class": row["asset_class"],
        }
        for metric in METRIC_COLS:
            cur_col = f"{metric}_current"
            adj_col = f"{metric}_adjusted"
            delta_col = f"{metric}_delta"
            label = DISPLAY_NAMES[metric]
            display_row[f"{label} (Current)"] = row.get(cur_col)
            display_row[f"{label} (Adjusted)"] = row.get(adj_col)
            display_row[f"{label} (Δ)"] = row.get(delta_col)
        display_rows.append(display_row)

    display_df = pd.DataFrame(display_rows)

    # Styler for delta columns
    def _colour_delta(val):
        if pd.isna(val) or val == 0:
            return ""
        return "color: green" if val > 0 else "color: red"

    delta_columns = [c for c in display_df.columns if "(Δ)" in c]

    styled = display_df.style.applymap(_colour_delta, subset=delta_columns)

    pct_cols = [c for c in display_df.columns if "%" in c]
    for col in pct_cols:
        styled = styled.format("{:.2f}%", subset=[col], na_rep="N/A")

    st.dataframe(styled, width="stretch", hide_index=True)

    # ---------------------------------------------------------------------------
    # Portfolio-level summary (weighted average return)
    # ---------------------------------------------------------------------------

    def _weighted_avg(df: pd.DataFrame, metric: str) -> float:
        weights = portfolio_df.set_index("ticker")["weight_pct"]
        m = df.set_index("ticker")[metric].dropna()
        common = m.index.intersection(weights.index)
        if common.empty:
            return float("nan")
        w = weights[common]
        return float((m[common] * w).sum() / w.sum())

    st.subheader("Portfolio-Level Summary")
    summary_data = {
        "Metric": ["Weighted Avg Return (%)", "Weighted Avg Drawdown (%)", "Weighted Avg Std Dev (%)"],
        "Current": [
            _weighted_avg(current_metrics, "return_pct"),
            _weighted_avg(current_metrics, "max_drawdown_pct"),
            _weighted_avg(current_metrics, "annualised_std_pct"),
        ],
        "Adjusted": [
            _weighted_avg(adj_metrics, "return_pct"),
            _weighted_avg(adj_metrics, "max_drawdown_pct"),
            _weighted_avg(adj_metrics, "annualised_std_pct"),
        ],
    }
    summary_df = pd.DataFrame(summary_data)
    summary_df["Delta"] = summary_df["Adjusted"] - summary_df["Current"]

    summary_styled = summary_df.style.applymap(
        _colour_delta, subset=["Delta"]
    ).format("{:.2f}%", subset=["Current", "Adjusted", "Delta"], na_rep="N/A")

    st.dataframe(summary_styled, width="stretch", hide_index=True)
