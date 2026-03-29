"""
pages/3_sentiment.py — Market Sentiment

Displays:
  - VIX: real-time value + 1-year chart
  - Fear & Greed Index: current gauge + history line chart
  - Optional auto-refresh toggle
"""

import sys
import pathlib
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

st.set_page_config(page_title="Market Sentiment", layout="wide")
st.title("Market Sentiment")

# ---------------------------------------------------------------------------
# Auto-refresh toggle
# ---------------------------------------------------------------------------

col_title, col_refresh = st.columns([4, 1])
with col_refresh:
    auto_refresh = st.toggle("Auto-refresh (5 min)", value=False)

# ---------------------------------------------------------------------------
# VIX
# ---------------------------------------------------------------------------

st.subheader("CBOE Volatility Index (VIX)")

@st.cache_data(ttl=300)
def _load_vix():
    ticker = yf.Ticker("^VIX")
    hist = ticker.history(period="1y")
    # Extract scalar from fast_info before returning — FastInfo is not pickle-serializable
    try:
        last_price = float(ticker.fast_info["last_price"])
    except Exception:
        last_price = float(hist["Close"].iloc[-1]) if not hist.empty else None
    return hist, last_price

@st.cache_data(ttl=300)
def _load_fear_greed():
    try:
        import fear_greed
        from fear_greed import HistoricalPoint

        raw = fear_greed.fetch()
        current = {
            "score": float(raw["fear_and_greed"]["score"]),
            "rating": str(raw["fear_and_greed"]["rating"]),
        }
        history = [
            HistoricalPoint.from_api(point).to_dict()
            for point in raw["fear_and_greed_historical"]["data"]
        ]
        return current, history, None
    except ImportError:
        return None, None, "fear-greed library not installed. Run: pip install fear-greed"
    except Exception as e:
        return None, None, str(e)

with st.spinner("Loading sentiment data..."):
    with ThreadPoolExecutor(max_workers=2) as executor:
        vix_future = executor.submit(_load_vix)
        fg_future = executor.submit(_load_fear_greed)

        try:
            vix_hist, vix_current = vix_future.result()
            if vix_current is not None:
                vix_current = round(vix_current, 2)
        except Exception as e:
            st.error(f"Failed to load VIX data: {e}")
            vix_hist = None
            vix_current = None

        fg_data, fg_history, fg_error = fg_future.result()

if vix_current is not None:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("VIX (Current)", f"{vix_current:.2f}")
    with col2:
        st.metric("Threshold — Elevated", "20")
    with col3:
        st.metric("Threshold — High Fear", "30")

if vix_hist is not None and not vix_hist.empty:
    fig_vix = go.Figure()
    fig_vix.add_trace(go.Scatter(
        x=vix_hist.index,
        y=vix_hist["Close"],
        mode="lines",
        name="VIX",
        line=dict(color="#3b82f6", width=1.5),
        fill="tozeroy",
        fillcolor="rgba(59,130,246,0.08)",
    ))
    fig_vix.add_hline(y=20, line_dash="dash", line_color="#f59e0b",
                      annotation_text="Elevated (20)", annotation_position="right")
    fig_vix.add_hline(y=30, line_dash="dash", line_color="#ef4444",
                      annotation_text="High Fear (30)", annotation_position="right")
    fig_vix.update_layout(
        yaxis_title="VIX",
        xaxis_title=None,
        margin=dict(t=10, b=30),
        height=320,
        showlegend=False,
    )
    st.plotly_chart(fig_vix, width="stretch")

st.divider()

st.subheader("CNN Fear & Greed Index")

if fg_error:
    st.warning(f"Fear & Greed Index unavailable: {fg_error}")
elif fg_data is not None:
    try:
        fg_value = float(fg_data.get("score") or fg_data.get("value") or 0)
        fg_label = str(fg_data.get("rating") or fg_data.get("description") or "Unknown").title()
    except Exception:
        fg_value = None
        fg_label = "Unknown"

    if fg_value is not None:
        col_gauge, col_info = st.columns([2, 1])

        with col_gauge:
            fig_fg = go.Figure(go.Indicator(
                mode="gauge+number",
                value=fg_value,
                title={"text": "Fear & Greed"},
                gauge={
                    "axis": {"range": [0, 100], "tickwidth": 1},
                    "bar": {"color": "#3b82f6"},
                    "steps": [
                        {"range": [0, 25], "color": "#ef4444"},
                        {"range": [25, 45], "color": "#f59e0b"},
                        {"range": [45, 55], "color": "#6b7280"},
                        {"range": [55, 75], "color": "#22c55e"},
                        {"range": [75, 100], "color": "#15803d"},
                    ],
                    "threshold": {
                        "line": {"color": "white", "width": 3},
                        "thickness": 0.8,
                        "value": fg_value,
                    },
                },
            ))
            fig_fg.update_layout(height=300, margin=dict(t=30, b=10))
            st.plotly_chart(fig_fg, width="stretch")

        with col_info:
            st.metric("Score", f"{fg_value:.0f}/100")
            st.metric("Sentiment", fg_label)
            st.markdown("""
**Score guide:**
- 0-25: Extreme Fear
- 25-45: Fear
- 45-55: Neutral
- 55-75: Greed
- 75-100: Extreme Greed
""")

        if fg_history:
            fg_hist_df = pd.DataFrame(fg_history)
            fg_hist_df["date"] = pd.to_datetime(fg_hist_df["date"])
            fg_hist_df = fg_hist_df.sort_values("date")

            fig_fg_hist = go.Figure()
            fig_fg_hist.add_trace(go.Scatter(
                x=fg_hist_df["date"],
                y=fg_hist_df["score"],
                mode="lines",
                name="Fear & Greed",
                line=dict(color="#3b82f6", width=2),
                fill="tozeroy",
                fillcolor="rgba(59,130,246,0.08)",
            ))
            fig_fg_hist.add_hline(y=25, line_dash="dash", line_color="#ef4444")
            fig_fg_hist.add_hline(y=45, line_dash="dash", line_color="#f59e0b")
            fig_fg_hist.add_hline(y=55, line_dash="dash", line_color="#6b7280")
            fig_fg_hist.add_hline(y=75, line_dash="dash", line_color="#22c55e")
            fig_fg_hist.update_layout(
                title="Fear & Greed History",
                yaxis_title="Score",
                xaxis_title=None,
                yaxis=dict(range=[0, 100]),
                margin=dict(t=50, b=30),
                height=320,
                showlegend=False,
            )
            st.plotly_chart(fig_fg_hist, width="stretch")

# ---------------------------------------------------------------------------
# Auto-refresh logic
# ---------------------------------------------------------------------------

if auto_refresh:
    st.caption("Auto-refreshing every 5 minutes...")
    time.sleep(300)
    st.rerun()
