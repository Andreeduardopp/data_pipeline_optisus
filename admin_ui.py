"""
Legacy bootstrap kept for backwards compatibility.

``uv run streamlit run admin_ui.py`` still works — it simply forwards
to the package entry point.  Prefer ``uv run streamlit run app.py``.
"""

from optisus.ui.app import main

main()
