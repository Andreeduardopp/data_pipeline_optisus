"""
Optisus Data Pipeline — main entry point.

Multipage Streamlit app with two modules:
  - Module 1: ML Data Preparation (Bronze → Silver → Gold)
  - Module 2: GTFS Data Maturity Pipeline (Silver → DB → GTFS)

Run with:
    uv run streamlit run app.py
"""

import streamlit as st

from ui_theme import inject_custom_css, render_logo_header

# ─── Page config (must be the first Streamlit call) ──────────────────────
st.set_page_config(
    page_title="Optisus Data Pipeline",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_custom_css()

# ─── Sidebar branding ───────────────────────────────────────────────────
with st.sidebar:
    render_logo_header()
    st.markdown("---")

# ─── Navigation ─────────────────────────────────────────────────────────
ml_page = st.Page(
    "pages/1_ml_data_pipeline.py",
    title="ML Data Preparation",
    icon="🧠",
    default=True,
)
gtfs_page = st.Page(
    "pages/2_gtfs_pipeline.py",
    title="GTFS Data Maturity",
    icon="🚌",
)

nav = st.navigation(
    {
        "Modules": [ml_page, gtfs_page],
    }
)
nav.run()
