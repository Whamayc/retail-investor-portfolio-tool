import pathlib

from dotenv import load_dotenv

# Load .env from the same directory as app.py
load_dotenv(pathlib.Path(__file__).parent / ".env")

import streamlit as st

st.set_page_config(
    page_title="CAD Portfolio Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("CAD ETF & Stock Portfolio Analyzer")
st.markdown("A personal investment tool for Canadian CAD-based investors.")

st.divider()

portfolio = st.session_state.get("portfolio")
if portfolio is not None:
    n = len(portfolio)
    total_cad = portfolio["market_value_cad"].sum()
    st.success(f"Portfolio loaded — {n} positions | Total: ${total_cad:,.2f} CAD")
else:
    st.warning("No portfolio loaded. Navigate to **Import** to upload your holdings.")

st.markdown("""
### Getting started
1. **Import** — Upload your `.csv` or `.xlsx` holdings file
2. **Allocation** — View breakdowns by holding, sector, region, and asset class
3. **Sentiment** — Check VIX and the Fear & Greed Index
4. **Performance** — Analyse returns, drawdown, and risk metrics across time periods
5. **Comparison** — Simulate portfolio rebalancing and compare metrics side by side
""")
