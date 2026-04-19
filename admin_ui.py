"""
Backwards-compatible entry point.

The application has moved to a multipage structure.  The preferred way
to start the app is now::

    uv run streamlit run app.py

This file is kept so that the legacy command still works::

    uv run streamlit run admin_ui.py
"""

import runpy
import sys
from pathlib import Path

# Ensure the project root is on sys.path so that app.py can resolve
# its page references correctly.
_project_root = str(Path(__file__).parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

runpy.run_path(str(Path(__file__).parent / "app.py"), run_name="__main__")
