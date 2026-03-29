"""
pages/2_allocation.py — Allocation Visualisation

Four Plotly charts:
  1. Treemap by individual holding (top 20 + Other)
  2. Horizontal bar by sector
  3. Donut by region
  4. Donut by asset class
"""

import sys
import pathlib

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

st.set_page_config(page_title="Allocation", layout="wide")

# ---------------------------------------------------------------------------
# Guard
# ---------------------------------------------------------------------------

if "portfolio" not in st.session_state or "pierced_holdings" not in st.session_state:
    st.warning("No portfolio loaded. Please import your holdings first.")
    st.stop()

portfolio_df: pd.DataFrame = st.session_state["portfolio"]
pierced: list[dict] = st.session_state["pierced_holdings"]
holdings_df = pd.DataFrame(pierced)

st.title("Allocation")
st.markdown("All weights are % of total CAD portfolio value.")

# ---------------------------------------------------------------------------
# Country → Region mapping
# ---------------------------------------------------------------------------

COUNTRY_TO_REGION: dict[str, str] = {
    # North America
    "United States": "North America",
    "USA": "North America",
    "US": "North America",
    "Canada": "North America",
    "CA": "North America",
    "Mexico": "North America",
    # Europe
    "United Kingdom": "Europe",
    "UK": "Europe",
    "Germany": "Europe",
    "France": "Europe",
    "Switzerland": "Europe",
    "Netherlands": "Europe",
    "Sweden": "Europe",
    "Denmark": "Europe",
    "Norway": "Europe",
    "Finland": "Europe",
    "Italy": "Europe",
    "Spain": "Europe",
    "Belgium": "Europe",
    "Austria": "Europe",
    "Ireland": "Europe",
    # Asia-Pacific
    "Japan": "Asia-Pacific",
    "Australia": "Asia-Pacific",
    "South Korea": "Asia-Pacific",
    "Hong Kong": "Asia-Pacific",
    "Singapore": "Asia-Pacific",
    "New Zealand": "Asia-Pacific",
    "Taiwan": "Asia-Pacific",
    # Emerging Markets
    "China": "Emerging Markets",
    "India": "Emerging Markets",
    "Brazil": "Emerging Markets",
    "South Africa": "Emerging Markets",
    "Mexico": "Emerging Markets",
    "Indonesia": "Emerging Markets",
    "Thailand": "Emerging Markets",
    "Malaysia": "Emerging Markets",
    "Chile": "Emerging Markets",
    "Colombia": "Emerging Markets",
}


def _get_region(country: str) -> str:
    return COUNTRY_TO_REGION.get(country, "Other")


# ---------------------------------------------------------------------------
# Chart 1 — Treemap by holding
# ---------------------------------------------------------------------------

st.subheader("Holdings (Look-Through)")

if holdings_df.empty:
    st.info("No holding data available.")
else:
    sorted_h = holdings_df.sort_values("weight_in_portfolio_pct", ascending=False)
    top20 = sorted_h.head(20)
    other_weight = sorted_h.iloc[20:]["weight_in_portfolio_pct"].sum() if len(sorted_h) > 20 else 0

    labels = top20["name"].fillna(top20["ticker"]).tolist()
    values = top20["weight_in_portfolio_pct"].tolist()
    parents = ["Portfolio"] * len(labels)

    if other_weight > 0:
        labels.append("Other")
        values.append(other_weight)
        parents.append("Portfolio")

    # Root node
    labels = ["Portfolio"] + labels
    values = [sum(values)] + values
    parents = [""] + parents

    fig1 = go.Figure(go.Treemap(
        labels=labels,
        parents=parents,
        values=values,
        branchvalues="total",
        textinfo="label+percent root",
        hovertemplate="<b>%{label}</b><br>%{value:.2f}%<extra></extra>",
        root_color="#1f2937",
    ))
    fig1.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=450)
    st.plotly_chart(fig1, width="stretch")

# ---------------------------------------------------------------------------
# Charts 2 & 3 in columns
# ---------------------------------------------------------------------------

col_left, col_right = st.columns(2)

# Chart 2 — Sector horizontal bar
with col_left:
    st.subheader("By Sector")

    sector_data = holdings_df.groupby("sector")["weight_in_portfolio_pct"].sum().reset_index()
    sector_data = sector_data.sort_values("weight_in_portfolio_pct", ascending=True)

    fig2 = go.Figure(go.Bar(
        x=sector_data["weight_in_portfolio_pct"],
        y=sector_data["sector"],
        orientation="h",
        text=sector_data["weight_in_portfolio_pct"].map("{:.1f}%".format),
        textposition="outside",
        marker_color="#3b82f6",
    ))
    fig2.update_layout(
        xaxis_title="Weight (%)",
        xaxis=dict(range=[0, sector_data["weight_in_portfolio_pct"].max() * 1.15]),
        margin=dict(t=10, b=30, l=10, r=40),
        height=max(300, len(sector_data) * 32),
    )
    st.plotly_chart(fig2, width="stretch")

# Chart 3 — Region donut
with col_right:
    st.subheader("By Region")

    holdings_df["region_mapped"] = holdings_df["region"].apply(_get_region)
    region_data = holdings_df.groupby("region_mapped")["weight_in_portfolio_pct"].sum().reset_index()

    fig3 = go.Figure(go.Pie(
        labels=region_data["region_mapped"],
        values=region_data["weight_in_portfolio_pct"],
        hole=0.45,
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>%{value:.2f}%<extra></extra>",
    ))
    fig3.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=380)
    st.plotly_chart(fig3, width="stretch")

# ---------------------------------------------------------------------------
# Chart 4 — Asset class donut
# ---------------------------------------------------------------------------

st.subheader("By Asset Class")

ac_data = portfolio_df.groupby("asset_class")["weight_pct"].sum().reset_index()
ac_data.columns = ["asset_class", "weight_pct"]

# Colour scheme — distinct colours per asset class
AC_COLOURS = {
    "Equity":        "#3b82f6",
    "Fixed Income":  "#22c55e",
    "REITs":         "#f59e0b",
    "Commodity":     "#ef4444",
    "Money Market":  "#8b5cf6",
    "Cash":          "#6b7280",
}
colours = [AC_COLOURS.get(ac, "#94a3b8") for ac in ac_data["asset_class"]]

fig4 = go.Figure(go.Pie(
    labels=ac_data["asset_class"],
    values=ac_data["weight_pct"],
    hole=0.45,
    marker_colors=colours,
    textinfo="label+percent",
    hovertemplate="<b>%{label}</b><br>%{value:.2f}%<extra></extra>",
))
fig4.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=380)
st.plotly_chart(fig4, width="stretch")
