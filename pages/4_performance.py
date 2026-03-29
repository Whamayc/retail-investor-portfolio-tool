"""
pages/4_performance.py — Performance & Risk Metrics

Per-ticker metrics table and correlation heatmap for 12 configurable time periods.
Metrics are computed lazily per tab and cached in st.session_state["metrics_cache"].
"""

import sys
import pathlib

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from core.metrics import compute_metrics, PERIODS

st.set_page_config(page_title="Performance & Risk", layout="wide")

# ---------------------------------------------------------------------------
# Guard
# ---------------------------------------------------------------------------

if "portfolio" not in st.session_state:
    st.warning("No portfolio loaded. Please import your holdings first.")
    st.stop()

portfolio_df: pd.DataFrame = st.session_state["portfolio"]

st.title("Performance & Risk")
st.markdown("Returns and risk metrics across 12 time periods. Cash and Money Market excluded from correlation.")

# ---------------------------------------------------------------------------
# Initialise metrics cache
# ---------------------------------------------------------------------------

if "metrics_cache" not in st.session_state:
    st.session_state["metrics_cache"] = {}

# ---------------------------------------------------------------------------
# Helper: render metrics tab
# ---------------------------------------------------------------------------

def _render_metrics_table(metrics_df: pd.DataFrame, actual_start: str, period: str, key_prefix: str = "") -> None:
    if metrics_df.empty:
        st.info("No data available for this period.")
        return

    st.caption(f"Period: **{period}** | Data from: {actual_start}")

    display = metrics_df.copy()
    display = display.rename(columns={
        "ticker": "Ticker",
        "asset_class": "Asset Class",
        "return_pct": "Return (%)",
        "max_drawdown_pct": "Max Drawdown (%)",
        "annualised_std_pct": "Ann. Std Dev (%)",
    })

    st.dataframe(
        display,
        width="stretch",
        column_config={
            "Return (%)": st.column_config.NumberColumn(format="%.2f%%"),
            "Max Drawdown (%)": st.column_config.NumberColumn(format="%.2f%%"),
            "Ann. Std Dev (%)": st.column_config.NumberColumn(format="%.2f%%"),
        },
        hide_index=True,
    )


def _render_correlation_heatmap(corr_matrix: pd.DataFrame, key_prefix: str = "") -> None:
    if corr_matrix.empty or len(corr_matrix) < 2:
        st.info("Insufficient data for correlation matrix (need at least 2 tickers with price history).")
        return

    st.subheader("Correlation Matrix (Daily Returns)")

    fig = go.Figure(go.Heatmap(
        z=corr_matrix.values,
        x=corr_matrix.columns.tolist(),
        y=corr_matrix.index.tolist(),
        colorscale="RdBu",
        zmid=0,
        zmin=-1,
        zmax=1,
        text=corr_matrix.round(2).values,
        texttemplate="%{text}",
        textfont={"size": 11},
        hovertemplate="<b>%{y} × %{x}</b><br>Correlation: %{z:.3f}<extra></extra>",
        colorbar=dict(title="ρ"),
    ))
    fig.update_layout(
        margin=dict(t=10, b=10, l=10, r=10),
        height=max(300, len(corr_matrix) * 45 + 80),
        xaxis=dict(side="bottom"),
    )
    st.plotly_chart(fig, width="stretch", key=f"corr_{key_prefix}")


# ---------------------------------------------------------------------------
# Tab strip
# ---------------------------------------------------------------------------

tabs = st.tabs(PERIODS)

for tab, period in zip(tabs, PERIODS):
    with tab:
        cache = st.session_state["metrics_cache"]

        if period not in cache:
            with st.spinner(f"Computing metrics for {period}..."):
                try:
                    result = compute_metrics(portfolio_df, period)
                    cache[period] = result
                except Exception as e:
                    st.error(f"Failed to compute metrics: {e}")
                    continue

        result = cache[period]
        _render_metrics_table(result["metrics_df"], result["actual_start"], period, key_prefix=period)
        _render_correlation_heatmap(result["correlation_matrix"], key_prefix=period)
