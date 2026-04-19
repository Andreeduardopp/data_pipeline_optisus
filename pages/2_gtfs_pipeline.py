"""
Module 2 — GTFS Data Maturity Pipeline.

Sections:
  0. Project selector
  1. Database Status Bar
  2. Maturity Dashboard
  3. Feed Completeness Gauge
  4. GTFS Table Browser
  5. Direct GTFS Upload
  6. Silver → GTFS Mapping Wizard
  7. Integrity Report
  8. Export & Validate
"""

import io
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from gtfs_database import (
    check_integrity,
    clear_table,
    create_gtfs_database,
    database_exists,
    delete_records,
    get_database_summary,
    get_table_columns,
    get_table_count,
    get_table_records,
    upsert_records,
)
from gtfs_exporter import (
    compute_feed_completeness,
    export_gtfs_feed,
    list_exports,
    validate_before_export,
)
from gtfs_mapper import (
    _SCHEMA_TO_MAPPER,
    _ALL_GTFS_TABLES,
    map_project_to_gtfs,
)
from storage_layers import (
    PROJECTS_ROOT,
    get_project_silver_datasets,
    list_project_runs,
    list_projects,
)
from ui_theme import (
    BORDER_DEFAULT,
    ERROR,
    LIGHT_ACCENT,
    SUCCESS,
    SURFACE_RAISED,
    TEXT_MUTED,
    TEXT_PRIMARY,
    TEXT_SECONDARY,
    TEAL_1,
    TEAL_2,
    TEAL_3,
    BLUE_ACCENT,
    WARNING,
    inject_custom_css,
)

inject_custom_css()

# ─── All public GTFS tables (exclude internal _gtfs_meta) ─────────────────
_GTFS_TABLES = [
    "agency", "stops", "routes", "trips", "stop_times",
    "calendar", "calendar_dates", "shapes", "frequencies",
    "transfers", "feed_info",
    "board_alight", "ridership", "ride_feed_info", "trip_capacity",
]

# Silver schema → GTFS target tables (for wizard display)
_SILVER_TO_GTFS: dict = {
    "Stop Spatial Features":      ["stops"],
    "Stop Connections":           ["transfers"],
    "Calendar Events":            ["calendar_dates"],
    "Transported Passengers":     ["board_alight"],
    "Operations and Circulation": ["routes", "trips", "stop_times"],
    "Fleet Identification":       ["agency", "trip_capacity"],
}

_PAGE_SIZE = 50


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 ** 2:
        return f"{n/1024:.1f} KB"
    return f"{n/1024**2:.1f} MB"


def _fmt_ts(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return iso


def _status_dot(ok: bool) -> str:
    return f"<span style='color:{SUCCESS};'>●</span>" if ok else f"<span style='color:{ERROR};'>●</span>"


# ═══════════════════════════════════════════════════════════════════════════
# Maturity level logic
# ═══════════════════════════════════════════════════════════════════════════

_LEVEL_DESCRIPTIONS = {
    1: ("Spreadsheets",
        "Your data is stored as raw uploaded files. "
        "Next step: validate them against schemas to move to Level 2."),
    2: ("Validated Tables",
        "Your data has been validated and persisted as Silver Parquet. "
        "Next step: load it into the relational GTFS database."),
    3: ("Relational Database",
        "Your data is stored in a relational database with referential integrity. "
        "Next step: export to GTFS."),
    4: ("GTFS Feed",
        "You have a standards-compliant GTFS feed ready to publish. "
        "Keep iterating to raise your completeness score."),
}


def _has_bronze(project_slug: str) -> bool:
    """True if the project has any raw uploaded Bronze files."""
    runs_dir = PROJECTS_ROOT / project_slug / "runs"
    if not runs_dir.exists():
        return False
    for bronze in runs_dir.glob("*/bronze"):
        if bronze.is_dir() and any(bronze.iterdir()):
            return True
    return False


def _has_silver(project_slug: str) -> bool:
    return bool(get_project_silver_datasets(project_slug))


def _has_populated_db(project_slug: str) -> bool:
    if not database_exists(project_slug):
        return False
    summary = get_database_summary(project_slug)
    return summary.get("total_records", 0) > 0


def _has_exports(project_slug: str) -> bool:
    return len(list_exports(project_slug)) > 0


def _compute_maturity_level(project_slug: str) -> int:
    """Determine the current maturity level (1–4) based on project state.

    Returns the highest level reached.  Returns 0 when nothing has been
    uploaded yet.
    """
    if _has_exports(project_slug):
        return 4
    if _has_populated_db(project_slug):
        return 3
    if _has_silver(project_slug):
        return 2
    if _has_bronze(project_slug):
        return 1
    return 0


# ═══════════════════════════════════════════════════════════════════════════
# Maturity Dashboard
# ═══════════════════════════════════════════════════════════════════════════

def _level_card(
    level: int,
    label: str,
    metric: str,
    current: int,
) -> str:
    """Render one level card as HTML."""
    if level < current:
        bg = f"linear-gradient(135deg, {TEAL_1}88, {TEAL_2}aa)"
        border = TEAL_1
        icon = "✅"
        label_color = LIGHT_ACCENT
    elif level == current:
        bg = f"linear-gradient(135deg, {BLUE_ACCENT}44, {TEAL_1}44)"
        border = BLUE_ACCENT
        icon = "🔵"
        label_color = LIGHT_ACCENT
    else:
        bg = TEAL_2 + "22"
        border = BORDER_DEFAULT
        icon = "🔲"
        label_color = TEXT_MUTED

    return (
        f"<div style='background:{bg};border:1px solid {border};"
        f"border-radius:10px;padding:1rem;text-align:center;flex:1;'>"
        f"<div style='font-size:1.3rem;margin-bottom:0.3rem;'>{icon}</div>"
        f"<div style='color:{TEXT_PRIMARY};font-weight:600;font-size:0.85rem;'>Level {level}</div>"
        f"<div style='color:{label_color};font-weight:700;font-size:0.95rem;margin:0.25rem 0;'>{label}</div>"
        f"<div style='color:{TEXT_MUTED};font-size:0.75rem;'>{metric}</div>"
        f"</div>"
    )


def _render_maturity_dashboard(project_slug: str, level: int) -> None:
    """Render the visual maturity progression with level cards."""

    # ── Gather metrics for each level ────────────────────────────────────
    runs = list_project_runs(project_slug)
    silver = get_project_silver_datasets(project_slug)
    summary = get_database_summary(project_slug) if database_exists(project_slug) else {}
    exports = list_exports(project_slug)

    l1_metric = f"{len(runs)} upload run(s)" if runs else "no uploads"
    l2_metric = f"{len(silver)} dataset(s) validated" if silver else "not yet"
    if summary.get("exists"):
        pop_tables = len(summary.get("populated_tables", []))
        l3_metric = f"{summary.get('total_records', 0)} rows · {pop_tables} tables"
    else:
        l3_metric = "not yet"
    l4_metric = f"{len(exports)} feed(s) exported" if exports else "not yet"

    # ── Progress indicator dots ───────────────────────────────────────────
    def _dot(lvl: int) -> str:
        if lvl <= level and level > 0:
            color = TEAL_1 if lvl < level else BLUE_ACCENT
            return f"<span style='color:{color};font-size:1.2rem;'>●</span>"
        return f"<span style='color:{BORDER_DEFAULT};font-size:1.2rem;'>○</span>"

    def _bar(lvl: int) -> str:
        filled = lvl < level
        color = TEAL_1 if filled else BORDER_DEFAULT
        return (
            f"<span style='flex:1;height:3px;background:{color};"
            f"margin:0 0.4rem;border-radius:2px;'></span>"
        )

    dots_bar = (
        f"<div style='display:flex;align-items:center;margin:0.5rem 0 1rem 0;'>"
        f"{_dot(1)}{_bar(2)}{_dot(2)}{_bar(3)}{_dot(3)}{_bar(4)}{_dot(4)}"
        f"</div>"
    )

    # ── Current level text ────────────────────────────────────────────────
    if level == 0:
        level_title = "Not Started"
        level_blurb = "Upload raw data in Module 1 to begin the journey."
    else:
        label, blurb = _LEVEL_DESCRIPTIONS[level]
        level_title = f"Level {level} — {label}"
        level_blurb = blurb

    # ── Level cards ───────────────────────────────────────────────────────
    cards = (
        f"<div style='display:flex;gap:0.75rem;margin-top:0.8rem;'>"
        + _level_card(1, "Raw Data", l1_metric, level)
        + _level_card(2, "Validated", l2_metric, level)
        + _level_card(3, "Database", l3_metric, level)
        + _level_card(4, "GTFS Feed", l4_metric, level)
        + f"</div>"
    )

    st.markdown(
        f"""
        <div style="background:{SURFACE_RAISED};border:1px solid {BORDER_DEFAULT};
             border-radius:14px;padding:1.4rem 1.8rem;margin-bottom:1.2rem;">
            <h3 style="color:{LIGHT_ACCENT};margin-top:0;font-weight:700;">
                Data Maturity Progress
            </h3>
            {dots_bar}
            <div style="color:{TEXT_PRIMARY};font-weight:600;font-size:1rem;margin-top:0.4rem;">
                Current: {level_title}
            </div>
            <div style="color:{TEXT_SECONDARY};font-size:0.9rem;margin:0.3rem 0 0.6rem 0;">
                {level_blurb}
            </div>
            {cards}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Feed completeness gauge
# ═══════════════════════════════════════════════════════════════════════════

def _render_completeness_gauge(project_slug: str) -> None:
    """Render the feed completeness breakdown with progress bars."""
    fc = compute_feed_completeness(project_slug)

    st.markdown(
        f"<div style='color:{TEXT_PRIMARY};font-weight:600;font-size:1.05rem;'>"
        f"Feed Completeness: <span style='color:{LIGHT_ACCENT};'>{fc.score:.0f}%</span>"
        f"</div>",
        unsafe_allow_html=True,
    )
    st.progress(min(fc.score / 100.0, 1.0))

    # Group the breakdown
    groups = {"required": [], "recommended": [], "optional": []}
    for tbl, info in fc.breakdown.items():
        groups.get(info.get("group", "optional"), groups["optional"]).append((tbl, info))

    g_labels = {
        "required": "Required",
        "recommended": "Recommended",
        "optional": "Optional",
    }

    for g_key, g_label in g_labels.items():
        items = groups[g_key]
        if not items:
            continue
        populated = sum(1 for _, info in items if info["populated"])
        total = len(items)
        pct = populated / total if total else 0

        st.markdown(
            f"<div style='display:flex;align-items:center;gap:0.8rem;margin-top:0.6rem;'>"
            f"<span style='color:{TEXT_SECONDARY};min-width:9rem;font-size:0.88rem;'>"
            f"{g_label} ({populated}/{total})</span>"
            f"<span style='flex:1;height:8px;background:{BORDER_DEFAULT};border-radius:4px;"
            f"position:relative;overflow:hidden;'>"
            f"<span style='display:block;width:{pct*100:.0f}%;height:100%;"
            f"background:linear-gradient(90deg, {TEAL_1}, {LIGHT_ACCENT});border-radius:4px;'></span>"
            f"</span>"
            f"<span style='color:{LIGHT_ACCENT};font-weight:600;min-width:2.5rem;text-align:right;'>"
            f"{pct*100:.0f}%</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

    # Missing recommended
    missing_rec = [t for t, info in groups["recommended"] if not info["populated"]]
    if missing_rec:
        missing_html = "".join(
            f"<div style='color:{WARNING};font-size:0.85rem;margin-top:0.2rem;'>"
            f"⚠ <code>{t}.txt</code> — recommended</div>"
            for t in missing_rec
        )
        st.markdown(
            f"<div style='margin-top:0.8rem;'>"
            f"<div style='color:{TEXT_MUTED};font-size:0.85rem;font-weight:500;'>"
            f"Missing recommended:</div>"
            f"{missing_html}</div>",
            unsafe_allow_html=True,
        )


# ═══════════════════════════════════════════════════════════════════════════
# Export section
# ═══════════════════════════════════════════════════════════════════════════

def _render_export_section(project_slug: str) -> None:
    """Render the export UI with pre-checks, button, and results."""
    include_ride = st.checkbox(
        "Include GTFS-ride extension files",
        value=True,
        key="export_include_ride",
    )

    # Pre-export validation (always runs)
    vr = validate_before_export(project_slug)
    summary = get_database_summary(project_slug)
    counts = summary.get("table_counts", {}) if summary.get("exists") else {}

    required_tables = ["agency", "stops", "routes", "trips", "stop_times"]
    calendar_ok = counts.get("calendar", 0) > 0 or counts.get("calendar_dates", 0) > 0

    st.markdown(
        f"<div style='color:{TEXT_SECONDARY};font-size:0.9rem;margin-top:0.4rem;'>"
        "Pre-export checks:</div>",
        unsafe_allow_html=True,
    )
    check_rows_html = ""
    for tbl in required_tables:
        cnt = counts.get(tbl, 0)
        if cnt > 0:
            icon = f"<span style='color:{SUCCESS};'>✅</span>"
            detail = f"{cnt} records"
        else:
            icon = f"<span style='color:{ERROR};'>❌</span>"
            detail = "empty (required)"
        check_rows_html += (
            f"<div style='padding:0.2rem 0;color:{TEXT_PRIMARY};font-size:0.88rem;'>"
            f"{icon} <code>{tbl}.txt</code> — {detail}</div>"
        )

    # Calendar group
    if calendar_ok:
        total_cal = counts.get("calendar", 0) + counts.get("calendar_dates", 0)
        check_rows_html += (
            f"<div style='padding:0.2rem 0;color:{TEXT_PRIMARY};font-size:0.88rem;'>"
            f"<span style='color:{SUCCESS};'>✅</span> "
            f"<code>calendar(_dates).txt</code> — {total_cal} records</div>"
        )
    else:
        check_rows_html += (
            f"<div style='padding:0.2rem 0;color:{TEXT_PRIMARY};font-size:0.88rem;'>"
            f"<span style='color:{ERROR};'>❌</span> "
            f"<code>calendar(_dates).txt</code> — both empty (required)</div>"
        )

    # Optional/recommended info rows
    for tbl in ["feed_info", "shapes", "transfers", "frequencies"]:
        cnt = counts.get(tbl, 0)
        if cnt > 0:
            icon_html = f"<span style='color:{SUCCESS};'>✅</span>"
            detail = f"{cnt} records"
        else:
            icon_html = "⬜"
            detail = "empty (optional — will be excluded)"
        check_rows_html += (
            f"<div style='padding:0.2rem 0;color:{TEXT_MUTED};font-size:0.85rem;'>"
            f"{icon_html} <code>{tbl}.txt</code> — {detail}</div>"
        )

    st.markdown(
        f"<div style='background:{SURFACE_RAISED};border:1px solid {BORDER_DEFAULT};"
        f"border-radius:10px;padding:0.8rem 1.1rem;margin:0.5rem 0 1rem 0;'>"
        f"{check_rows_html}</div>",
        unsafe_allow_html=True,
    )

    # Errors/warnings from validator
    if vr.errors:
        for err in vr.errors:
            st.error(err)

    # Export button
    export_clicked = st.button(
        "🚀 Export GTFS Feed",
        type="primary",
        key="btn_export",
        disabled=not vr.can_export,
        use_container_width=False,
    )

    if export_clicked:
        with st.spinner("Building GTFS .zip archive…"):
            er = export_gtfs_feed(project_slug, include_ride=include_ride)

        if er.success:
            st.success(
                f"Exported {len(er.files_included)} files · "
                f"{er.total_records} records · "
                f"{er.completeness_score:.0f}% complete"
            )
            if er.warnings:
                with st.expander(f"{len(er.warnings)} warning(s)", expanded=False):
                    for w in er.warnings:
                        st.caption(f"• {w}")
        else:
            st.error("Export failed.")
            for e in er.errors:
                st.caption(f"• {e}")

    # Export history
    exports = list_exports(project_slug)
    if exports:
        st.markdown(
            f"<div style='margin-top:1.2rem;color:{LIGHT_ACCENT};"
            f"font-weight:600;font-size:0.95rem;'>Export History</div>",
            unsafe_allow_html=True,
        )
        for i, exp in enumerate(exports[:10]):
            c_info, c_btn = st.columns([4, 1])
            with c_info:
                st.markdown(
                    f"<div style='color:{TEXT_PRIMARY};font-size:0.88rem;'>"
                    f"<code>{exp['filename']}</code> · "
                    f"<span style='color:{TEXT_MUTED};'>"
                    f"{_fmt_bytes(exp['size_bytes'])} · "
                    f"{_fmt_ts(exp['created_at'])}</span></div>",
                    unsafe_allow_html=True,
                )
            with c_btn:
                try:
                    with open(exp["path"], "rb") as f:
                        st.download_button(
                            "⬇ Download",
                            data=f.read(),
                            file_name=exp["filename"],
                            mime="application/zip",
                            key=f"dl_export_{i}",
                        )
                except OSError:
                    st.caption("File missing")


# ═══════════════════════════════════════════════════════════════════════════
# Section 0: Page header & project selector
# ═══════════════════════════════════════════════════════════════════════════

st.title("GTFS Data Maturity Pipeline")
st.caption("Transform your transit data from spreadsheets to a standards-compliant GTFS feed.")

projects = list_projects()
if not projects:
    st.info("No projects found. Create a project in Module 1 first.")
    st.stop()

project_names = [p["name"] for p in projects]
project_slugs = {p["name"]: p["slug"] for p in projects}

# Sync with Module 1 session state when possible
_default_idx = 0
if "current_project" in st.session_state and st.session_state["current_project"]:
    _cur = st.session_state["current_project"].get("name", "")
    if _cur in project_names:
        _default_idx = project_names.index(_cur)

selected_name = st.selectbox(
    "Project",
    project_names,
    index=_default_idx,
    key="gtfs_selected_project",
)
project_slug = project_slugs[selected_name]

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# Section 1: Database Status Bar
# ═══════════════════════════════════════════════════════════════════════════

st.subheader("Database Status")

summary = get_database_summary(project_slug)

if not summary.get("exists"):
    st.markdown(
        f"""
        <div style="background:{SURFACE_RAISED};border:1px solid {BORDER_DEFAULT};
             border-radius:12px;padding:1.2rem 1.6rem;margin-bottom:1rem;
             display:flex;align-items:center;gap:1rem;">
            <span style="font-size:1.5rem;">🗄️</span>
            <div>
                <div style="color:{TEXT_PRIMARY};font-weight:600;">No GTFS database yet</div>
                <div style="color:{TEXT_MUTED};font-size:0.85rem;">
                    Create the database to start storing GTFS data for this project.
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button("Create GTFS Database", type="primary", key="btn_create_db"):
        with st.spinner("Creating database…"):
            create_gtfs_database(project_slug)
        st.success("GTFS database created.")
        st.rerun()
else:
    integrity_icon = "✅" if summary["integrity_clean"] else "⚠️"
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Records", summary["total_records"])
    c2.metric("Populated Tables", len(summary["populated_tables"]))
    c3.metric("DB Size", _fmt_bytes(summary["size_bytes"]))
    c4.metric("Integrity", f"{integrity_icon} {'Clean' if summary['integrity_clean'] else str(summary['violation_count'])+' issues'}")
    st.caption(
        f"Schema {summary['schema_version']} · "
        f"Created {_fmt_ts(summary['created_at'])} · "
        f"Last modified {_fmt_ts(summary['last_modified'])}"
    )

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# Section 1b: Maturity Dashboard
# ═══════════════════════════════════════════════════════════════════════════

_current_level = _compute_maturity_level(project_slug)
_render_maturity_dashboard(project_slug, _current_level)

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# Guard: database must exist for remaining sections
# ═══════════════════════════════════════════════════════════════════════════

if not database_exists(project_slug):
    st.info("Create the GTFS database above to enable the table browser, upload, and mapping features.")
    st.stop()


# ═══════════════════════════════════════════════════════════════════════════
# Section 1c: Feed Completeness Gauge
# ═══════════════════════════════════════════════════════════════════════════

st.subheader("Feed Completeness")
_render_completeness_gauge(project_slug)

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# Section 2: GTFS Table Browser
# ═══════════════════════════════════════════════════════════════════════════

st.subheader("Table Browser")

browser_table = st.selectbox(
    "Select GTFS table",
    _GTFS_TABLES,
    key="browser_table",
)

total_rows = get_table_count(project_slug, browser_table)

col_hdr, col_dl = st.columns([4, 1])
with col_hdr:
    st.markdown(
        f"<span style='color:{LIGHT_ACCENT};font-weight:700;font-size:1.05rem;'>"
        f"{browser_table}</span> "
        f"<span style='color:{TEXT_MUTED};'>({total_rows} records)</span>",
        unsafe_allow_html=True,
    )

# Pagination
total_pages = max(1, -(-total_rows // _PAGE_SIZE))  # ceiling division
page = st.number_input(
    f"Page (1–{total_pages})", min_value=1, max_value=total_pages,
    value=1, step=1, key="browser_page",
)
offset = (page - 1) * _PAGE_SIZE

rows = get_table_records(project_slug, browser_table, limit=_PAGE_SIZE, offset=offset)

if rows:
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Download
    with col_dl:
        csv_bytes = df.to_csv(index=False).encode()
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name=f"{browser_table}.csv",
            mime="text/csv",
            key="dl_table_csv",
        )

    # Delete / Clear actions
    with st.expander("Danger Zone", expanded=False):
        st.warning("These actions are irreversible.")
        d_col1, d_col2 = st.columns(2)

        with d_col1:
            pk_cols = get_table_columns(browser_table)
            if pk_cols:
                ids_input = st.text_area(
                    "Delete by PK (one per line; composite PKs use `|` separator)",
                    key="del_ids_input",
                    height=80,
                )
                if st.button("Delete Selected", key="btn_delete_rows"):
                    ids = [line.strip() for line in ids_input.splitlines() if line.strip()]
                    if ids:
                        deleted = delete_records(project_slug, browser_table, ids)
                        st.success(f"Deleted {deleted} record(s).")
                        st.rerun()
                    else:
                        st.warning("Enter at least one PK value.")

        with d_col2:
            if st.button("Clear Entire Table", key="btn_clear_table"):
                st.session_state["confirm_clear"] = True

            if st.session_state.get("confirm_clear"):
                st.error(f"Are you sure you want to delete ALL records from **{browser_table}**?")
                c_yes, c_no = st.columns(2)
                if c_yes.button("Yes, clear it", key="btn_confirm_clear"):
                    deleted = clear_table(project_slug, browser_table)
                    st.success(f"Cleared {deleted} records.")
                    st.session_state.pop("confirm_clear", None)
                    st.rerun()
                if c_no.button("Cancel", key="btn_cancel_clear"):
                    st.session_state.pop("confirm_clear", None)
                    st.rerun()
else:
    st.info(f"No records in **{browser_table}** yet.")
    with col_dl:
        st.download_button(
            "Download CSV",
            data=b"",
            file_name=f"{browser_table}.csv",
            mime="text/csv",
            disabled=True,
            key="dl_table_csv_empty",
        )

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# Section 3: Direct GTFS Upload
# ═══════════════════════════════════════════════════════════════════════════

st.subheader("Upload GTFS Data")

upload_table = st.selectbox(
    "Target GTFS table",
    _GTFS_TABLES,
    key="upload_table",
)

uploaded_file = st.file_uploader(
    f"Upload CSV for `{upload_table}`",
    type=["csv"],
    key="gtfs_upload_file",
)

if uploaded_file:
    try:
        upload_df = pd.read_csv(uploaded_file)
    except Exception as exc:
        st.error(f"Could not parse CSV: {exc}")
        upload_df = None

    if upload_df is not None and not upload_df.empty:
        st.markdown(
            f"**Preview** — {len(upload_df)} rows, {len(upload_df.columns)} columns",
        )
        st.dataframe(upload_df.head(5), use_container_width=True, hide_index=True)

        expected_cols = get_table_columns(upload_table)
        missing = [c for c in expected_cols if c not in upload_df.columns]
        extra = [c for c in upload_df.columns if c not in expected_cols]

        if missing:
            st.warning(f"Missing expected columns: `{'`, `'.join(missing)}`")
        if extra:
            st.caption(f"Extra columns (will be ignored): `{'`, `'.join(extra)}`")

        if st.button("Validate & Insert into Database", type="primary", key="btn_upload_insert"):
            records = upload_df.where(upload_df.notna(), other=None).to_dict(orient="records")
            with st.spinner(f"Inserting {len(records)} records into `{upload_table}`…"):
                result = upsert_records(project_slug, upload_table, records)

            c_ok, c_fail = st.columns(2)
            c_ok.metric("Inserted", result.inserted)
            c_fail.metric("Failed", result.failed)

            if result.errors:
                with st.expander(f"{len(result.errors)} validation error(s)", expanded=False):
                    for err in result.errors[:50]:
                        st.caption(f"• {err}")

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# Section 4: Silver → GTFS Mapping Wizard
# ═══════════════════════════════════════════════════════════════════════════

st.subheader("Map Silver Data → GTFS")

silver_datasets = get_project_silver_datasets(project_slug)

if not silver_datasets:
    st.info("No Silver datasets found for this project. Upload and validate data in Module 1 first.")
else:
    st.markdown(
        f"<div style='color:{TEXT_SECONDARY};font-size:0.9rem;margin-bottom:0.8rem;'>"
        "Available Silver datasets and their GTFS targets:</div>",
        unsafe_allow_html=True,
    )

    rows_html = ""
    for schema_name, gtfs_targets in _SILVER_TO_GTFS.items():
        if schema_name in silver_datasets:
            icon = f"<span style='color:{SUCCESS};'>✅</span>"
            targets_str = ", ".join(f"<code>{t}</code>" for t in gtfs_targets)
            status = f"→ {targets_str}"
        else:
            icon = "⬜"
            status = f"<span style='color:{TEXT_MUTED};'>(not uploaded)</span>"

        rows_html += (
            f"<div style='display:flex;align-items:center;gap:0.7rem;"
            f"padding:0.45rem 0;border-bottom:1px solid {BORDER_DEFAULT}22;'>"
            f"<span style='min-width:1.4rem;text-align:center;'>{icon}</span>"
            f"<span style='color:{TEXT_PRIMARY};min-width:11rem;font-size:0.9rem;'>{schema_name}</span>"
            f"<span style='color:{TEXT_SECONDARY};font-size:0.85rem;'>{status}</span>"
            f"</div>"
        )

    st.markdown(
        f"<div style='background:{SURFACE_RAISED};border:1px solid {BORDER_DEFAULT};"
        f"border-radius:10px;padding:0.8rem 1.2rem;margin-bottom:1rem;'>"
        f"{rows_html}</div>",
        unsafe_allow_html=True,
    )

    # Available schemas that have Silver data
    mappable = [s for s in _SILVER_TO_GTFS if s in silver_datasets]

    map_col1, map_col2 = st.columns([1, 2])
    with map_col1:
        if st.button("Map All Available", type="primary", key="btn_map_all"):
            with st.spinner("Running all mappers…"):
                report = map_project_to_gtfs(project_slug)
            st.session_state["last_mapping_report"] = report

    with map_col2:
        if mappable:
            selected_schema = st.selectbox(
                "Map individual dataset",
                mappable,
                key="map_single_schema",
                label_visibility="collapsed",
            )
            if st.button("Map Selected", key="btn_map_single"):
                subset = {selected_schema: silver_datasets[selected_schema]}
                with st.spinner(f"Mapping {selected_schema}…"):
                    report = map_project_to_gtfs(project_slug, available_datasets=subset)
                st.session_state["last_mapping_report"] = report

    # Show mapping results
    if "last_mapping_report" in st.session_state:
        rpt = st.session_state["last_mapping_report"]
        st.markdown(f"**Mapping Results** — {rpt.total_mapped} records mapped, {rpt.total_failed} failed")

        for mr in rpt.results:
            warn_tag = f" · ⚠️ {len(mr.warnings)} warnings" if mr.warnings else ""
            result_color = SUCCESS if mr.records_failed == 0 else WARNING
            st.markdown(
                f"<span style='color:{result_color};'>●</span> "
                f"<code>{mr.gtfs_table}</code>: "
                f"**{mr.records_mapped}** mapped, **{mr.records_failed}** failed{warn_tag}",
                unsafe_allow_html=True,
            )
            if mr.warnings:
                with st.expander(f"Warnings for {mr.gtfs_table}", expanded=False):
                    for w in mr.warnings:
                        st.caption(f"• {w}")

        if rpt.unmapped_tables:
            st.caption(
                f"Tables still needing manual data: "
                + ", ".join(f"`{t}`" for t in sorted(rpt.unmapped_tables))
            )

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# Section 5: Integrity Report
# ═══════════════════════════════════════════════════════════════════════════

st.subheader("Database Integrity")

if "integrity_report" not in st.session_state:
    st.session_state["integrity_report"] = None

if st.button("Run Full Integrity Check", key="btn_integrity"):
    with st.spinner("Checking integrity…"):
        st.session_state["integrity_report"] = check_integrity(project_slug)

irpt = st.session_state.get("integrity_report")

if irpt is None:
    st.info("Click the button above to run an integrity check.")
else:
    if irpt.is_clean:
        st.success("No integrity violations found.")
    else:
        st.error(f"{len(irpt.violations)} violation(s) found.")

    # Summary table counts (non-empty tables only)
    populated = {t: c for t, c in irpt.table_counts.items() if c > 0}
    if populated:
        count_df = pd.DataFrame(
            [(t, c) for t, c in sorted(populated.items())],
            columns=["Table", "Records"],
        )
        st.dataframe(count_df, use_container_width=True, hide_index=True)

    if irpt.violations:
        with st.expander(f"Violation Details ({len(irpt.violations)})", expanded=True):
            for v in irpt.violations:
                st.markdown(
                    f"⚠️ <code>{v.table}</code> · "
                    f"<span style='color:{TEXT_MUTED};'>{v.violation_type}</span> — "
                    f"{v.detail}",
                    unsafe_allow_html=True,
                )

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# Section 6: Export & Validate
# ═══════════════════════════════════════════════════════════════════════════

st.subheader("Export GTFS Feed")
_render_export_section(project_slug)
