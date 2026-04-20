"""
Optisus Streamlit app — navigation + global theming.

The root ``app.py`` at the repository root is a thin bootstrap that
delegates to :func:`main`.  Streamlit page paths remain relative to that
root script so that ``st.Page(...)`` continues to resolve correctly.
"""

from __future__ import annotations

import streamlit as st

from optisus.ui.theme import inject_custom_css, render_logo_header

# Page paths are resolved relative to the main Streamlit script
# (repo-root ``app.py``).  Using literal paths keeps Streamlit's page
# hashing behaviour stable across reruns.
_ML_PAGE_PATH = "src/optisus/ui/pages/ml_pipeline.py"
_GTFS_PAGE_PATH = "src/optisus/ui/pages/gtfs_pipeline.py"
_DB_OVERVIEW_PAGE_PATH = "src/optisus/ui/pages/db_overview.py"


def main() -> None:
    """Entry point invoked from the repo-root ``app.py``."""
    st.set_page_config(
        page_title="Optisus Data Pipeline",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_custom_css()

    with st.sidebar:
        render_logo_header()
        st.markdown("---")

    ml_page = st.Page(
        _ML_PAGE_PATH,
        title="ML Data Preparation",
        icon="🧠",
        default=True,
    )
    gtfs_page = st.Page(
        _GTFS_PAGE_PATH,
        title="GTFS Data Maturity",
        icon="🚌",
    )
    db_overview_page = st.Page(
        _DB_OVERVIEW_PAGE_PATH,
        title="Database Overview",
        icon="🗄️",
    )

    nav = st.navigation({"Modules": [ml_page, gtfs_page, db_overview_page]})
    nav.run()
