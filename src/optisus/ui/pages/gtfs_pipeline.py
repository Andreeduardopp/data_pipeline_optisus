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

from optisus.core.gtfs.database import (
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
from optisus.core.gtfs.exporter import (
    compute_feed_completeness,
    export_gtfs_feed,
    export_gtfs_subset,
    latest_export_path,
    list_exports,
    validate_before_export,
    validate_latest_export,
)
from optisus.core.gtfs.analytics import (
    GTFS_KIT_AVAILABLE,
    build_routes_map,
    compute_analytics,
    db_signature,
    feed_from_db,
)
from optisus.core.gtfs.importer import (
    GtfsImportError,
    ImportMode,
    import_gtfs_zip,
    preview_gtfs_zip,
)
from optisus.core.gtfs.mapper import (
    _SCHEMA_TO_MAPPER,
    _ALL_GTFS_TABLES,
    map_project_to_gtfs,
)
from optisus.core.storage.layers import (
    PROJECTS_ROOT,
    get_project_silver_datasets,
    list_project_runs,
    list_projects,
)
from optisus.ui.theme import (
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
    badge: tuple[str, str] | None = None,
) -> str:
    """Render one level card as HTML.

    ``badge`` is an optional ``(text, color)`` pair shown as a pill below
    the metric line — used by Level 4 to indicate feed-quality status.
    """
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

    badge_html = ""
    if badge is not None:
        badge_text, badge_color = badge
        badge_html = (
            f"<div style='display:inline-block;margin-top:0.4rem;padding:0.15rem 0.55rem;"
            f"border-radius:10px;background:{badge_color}22;border:1px solid {badge_color};"
            f"color:{badge_color};font-weight:600;font-size:0.72rem;'>"
            f"{badge_text}</div>"
        )

    return (
        f"<div style='background:{bg};border:1px solid {border};"
        f"border-radius:10px;padding:1rem;text-align:center;flex:1;'>"
        f"<div style='font-size:1.3rem;margin-bottom:0.3rem;'>{icon}</div>"
        f"<div style='color:{TEXT_PRIMARY};font-weight:600;font-size:0.85rem;'>Level {level}</div>"
        f"<div style='color:{label_color};font-weight:700;font-size:0.95rem;margin:0.25rem 0;'>{label}</div>"
        f"<div style='color:{TEXT_MUTED};font-size:0.75rem;'>{metric}</div>"
        f"{badge_html}"
        f"</div>"
    )


# ─── Quality tag for Level 4 ──────────────────────────────────────────────
# Tags: "certified" | "warnings" | "draft" | "none"
_QUALITY_BADGE: dict[str, tuple[str, str]] = {
    "certified": ("✓ Certified",       SUCCESS),
    "warnings":  ("⚠ Warnings",        WARNING),
    "draft":     ("✗ Draft",           ERROR),
}


@st.cache_data(show_spinner=False)
def _cached_latest_validation(slug: str, sig: tuple):
    """Cache the validator report keyed on (zip path, mtime)."""
    del sig  # only used for cache invalidation
    return validate_latest_export(slug)


def _compute_quality_tag(slug: str) -> tuple[str, int, int]:
    """Return ``(tag, error_count, warning_count)`` for the latest export."""
    zp = latest_export_path(slug)
    if zp is None:
        return ("none", 0, 0)
    sig = (str(zp), zp.stat().st_mtime)
    report = _cached_latest_validation(slug, sig)
    if report is None:
        return ("none", 0, 0)
    if report.error_count > 0:
        return ("draft", report.error_count, report.warning_count)
    if report.warning_count > 0:
        return ("warnings", 0, report.warning_count)
    return ("certified", 0, 0)


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

    # Quality badge for Level 4 (validates latest export; cached on mtime)
    q_tag, q_errs, q_warns = _compute_quality_tag(project_slug)
    q_badge: tuple[str, str] | None = None
    if q_tag in _QUALITY_BADGE:
        text, color = _QUALITY_BADGE[q_tag]
        if q_tag == "draft":
            text = f"✗ Draft · {q_errs} err"
        elif q_tag == "warnings":
            text = f"⚠ {q_warns} warn"
        q_badge = (text, color)

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
        + _level_card(4, "GTFS Feed", l4_metric, level, badge=q_badge)
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

def _render_subset_export(slug: str) -> None:
    """Expander with date + route pickers → gtfs-kit subset export."""
    if not GTFS_KIT_AVAILABLE:
        return

    sig = db_signature(slug)
    if sig is None:
        return

    feed = _cached_feed(slug, sig)
    if feed is None:
        return

    try:
        active_dates = feed.get_dates()
    except Exception:
        active_dates = []

    with st.expander("Export subset (by date or route)", expanded=False):
        if not active_dates:
            st.info(
                "No active service dates found in calendar / calendar_dates — "
                "subset export needs at least one populated service pattern."
            )
            return

        st.caption(
            f"Service active on {len(active_dates)} date(s): "
            f"**{active_dates[0]} → {active_dates[-1]}**. "
            "Subset exports skip GTFS-ride tables."
        )

        # ── Date range picker ────────────────────────────────────────────
        import datetime as _dt
        d_min = _dt.datetime.strptime(active_dates[0], "%Y%m%d").date()
        d_max = _dt.datetime.strptime(active_dates[-1], "%Y%m%d").date()
        d_default_end = min(d_min + _dt.timedelta(days=6), d_max)

        date_range = st.date_input(
            "Date range",
            value=(d_min, d_default_end),
            min_value=d_min,
            max_value=d_max,
            key="subset_date_range",
        )

        # Streamlit returns a single date or a tuple depending on interaction
        if isinstance(date_range, tuple) and len(date_range) == 2:
            d_start, d_end = date_range
        else:
            d_start = d_end = date_range

        # ── Route picker ─────────────────────────────────────────────────
        route_options: list[str] = []
        route_labels: dict[str, str] = {}
        try:
            rdf = feed.routes
            if rdf is not None and not rdf.empty:
                for _, row in rdf.iterrows():
                    rid = row["route_id"]
                    short = row.get("route_short_name") or ""
                    long = row.get("route_long_name") or ""
                    label = f"{rid} — {short} {long}".strip()
                    route_options.append(rid)
                    route_labels[rid] = label
        except Exception:
            pass

        selected_routes = st.multiselect(
            "Routes (optional — leave empty for all)",
            options=route_options,
            format_func=lambda rid: route_labels.get(rid, rid),
            key="subset_routes",
        )

        # ── Go button ────────────────────────────────────────────────────
        if st.button("Export subset", key="btn_export_subset"):
            # Build the date list (inclusive), intersected with active_dates
            active_set = set(active_dates)
            days = (d_end - d_start).days + 1
            wanted = [
                (d_start + _dt.timedelta(days=i)).strftime("%Y%m%d")
                for i in range(max(days, 0))
            ]
            dates = [d for d in wanted if d in active_set]

            if not dates:
                st.warning(
                    "No active service dates fall inside the selected range."
                )
                return

            with st.spinner(
                f"Building subset for {len(dates)} day(s)"
                + (f", {len(selected_routes)} route(s)" if selected_routes else "")
                + "…"
            ):
                er = export_gtfs_subset(
                    slug,
                    dates=dates,
                    route_ids=selected_routes or None,
                )

            if er.success:
                st.success(
                    f"Subset exported — {len(er.files_included)} files · "
                    f"{er.total_records} records."
                )
                for w in er.warnings:
                    st.caption(f"• {w}")
            else:
                st.error("Subset export failed.")
                for e in er.errors:
                    st.caption(f"• {e}")


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

    # ── Subset export (date / route slice via gtfs-kit) ──────────────────
    _render_subset_export(project_slug)

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
                subset_tag = (
                    f" <span style='background:{BLUE_ACCENT}33;color:{BLUE_ACCENT};"
                    f"border:1px solid {BLUE_ACCENT};padding:0.05rem 0.4rem;"
                    f"border-radius:8px;font-size:0.7rem;font-weight:600;'>subset</span>"
                    if exp.get("is_subset") else ""
                )
                st.markdown(
                    f"<div style='color:{TEXT_PRIMARY};font-size:0.88rem;'>"
                    f"<code>{exp['filename']}</code>{subset_tag} · "
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
# Section 1d: Feed Analytics & Network Map (powered by gtfs-kit)
# ═══════════════════════════════════════════════════════════════════════════

@st.cache_resource(show_spinner=False)
def _cached_feed(slug: str, sig: tuple):
    """Cache the gtfs-kit Feed, keyed on (db path, mtime)."""
    del sig  # used only to invalidate the cache when the DB changes
    return feed_from_db(slug)


def _render_analytics_and_map(slug: str) -> None:
    st.subheader("Feed Analytics & Network Map")

    if not GTFS_KIT_AVAILABLE:
        st.info("`gtfs-kit` is not installed — run `uv sync` to enable feed analytics.")
        return

    sig = db_signature(slug)
    if sig is None:
        st.info("Create the GTFS database above to enable analytics.")
        return

    feed = _cached_feed(slug, sig)
    analytics = compute_analytics(feed)

    if not analytics.available:
        st.info(
            "Feed analytics need at least agency, stops, routes, and trips "
            "to have records. Populate them via mapping or direct upload."
        )
        return

    # ── Summary metrics ──────────────────────────────────────────────────
    ind = analytics.indicators
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Routes", int(ind.get("num_routes", 0)))
    m2.metric("Trips", int(ind.get("num_trips", 0)))
    m3.metric("Stops", int(ind.get("num_stops", 0)))
    m4.metric("Shapes", int(ind.get("num_shapes", 0)))

    if analytics.service_start and analytics.service_end:
        st.caption(
            f"Service span: **{analytics.service_start} → {analytics.service_end}** "
            f"· {analytics.num_active_dates} active date(s)"
            + (
                f" · Busiest date: **{analytics.busiest_date}** "
                f"({analytics.busiest_date_trips} trips)"
                if analytics.busiest_date else ""
            )
        )

    # ── Health indicators ────────────────────────────────────────────────
    health_rows = []
    for key, label in [
        ("num_trips_missing_shapes",          "Trips missing shapes"),
        ("num_stops_without_trips",           "Stops with no trips"),
        ("num_routes_without_trips",          "Routes with no trips"),
        ("num_stop_times_with_no_departure", "Stop times missing departure"),
        ("num_stop_times_with_no_arrival",   "Stop times missing arrival"),
    ]:
        if key in ind:
            val = int(ind[key])
            if val > 0:
                health_rows.append((label, val))

    if health_rows:
        with st.expander(f"{len(health_rows)} health indicator(s) to review", expanded=False):
            for label, val in health_rows:
                st.markdown(
                    f"<div style='color:{WARNING};font-size:0.88rem;'>⚠ {label}: "
                    f"<strong>{val}</strong></div>",
                    unsafe_allow_html=True,
                )

    # ── Route stats ──────────────────────────────────────────────────────
    if analytics.route_stats is not None and not analytics.route_stats.empty:
        with st.expander("Per-route stats (first service day)", expanded=False):
            rs = analytics.route_stats
            display_cols = [
                c for c in [
                    "route_id", "route_short_name", "num_trips",
                    "mean_headway", "service_duration", "service_distance",
                    "service_speed",
                ]
                if c in rs.columns
            ]
            st.dataframe(rs[display_cols], use_container_width=True, hide_index=True)

    # ── Network map ──────────────────────────────────────────────────────
    st.markdown(
        f"<div style='color:{TEXT_SECONDARY};font-size:0.9rem;margin-top:0.6rem;'>"
        "Network map — routes are drawn from <code>shapes.txt</code> when available, "
        "otherwise from straight lines between consecutive stops.</div>",
        unsafe_allow_html=True,
    )

    map_col1, map_col2 = st.columns([3, 1])
    with map_col2:
        show_stops = st.checkbox("Overlay stops", value=False, key="map_show_stops")
        show_map = st.checkbox("Render map", value=False, key="map_render_toggle")

    if show_map:
        with st.spinner("Building map…"):
            fmap = build_routes_map(feed, show_stops=show_stops)
        if fmap is None:
            st.warning("Could not build the map (feed likely missing shapes/coordinates).")
        else:
            try:
                from streamlit_folium import st_folium
                st_folium(fmap, height=500, use_container_width=True, returned_objects=[])
            except ImportError:
                st.components.v1.html(fmap._repr_html_(), height=500, scrolling=False)


_render_analytics_and_map(project_slug)
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
# Section 3a: Import complete GTFS feed (.zip)
# ═══════════════════════════════════════════════════════════════════════════

st.subheader("Import complete GTFS feed (.zip)")
st.caption(
    "Upload an existing GTFS archive and populate this project's database "
    "directly. Use this when you already have a third-party feed and want to "
    "skip the Silver → GTFS mapping step."
)

zip_file = st.file_uploader(
    "Upload a GTFS .zip",
    type=["zip"],
    key="gtfs_zip_upload_file",
)

if zip_file is not None:
    zip_bytes = zip_file.getvalue()
    preview = preview_gtfs_zip(io.BytesIO(zip_bytes))

    if preview.errors:
        for err in preview.errors:
            st.error(err)

    if preview.missing_required:
        missing_display = [
            "calendar.txt or calendar_dates.txt"
            if m == "calendar_or_calendar_dates"
            else f"{m}.txt"
            for m in preview.missing_required
        ]
        st.error(
            "Archive is missing required GTFS files: "
            + ", ".join(f"`{m}`" for m in missing_display)
        )

    if preview.recognised_tables:
        st.markdown("**Recognised tables**")
        preview_df = pd.DataFrame(
            [
                {"table": t, "rows": n}
                for t, n in sorted(preview.recognised_tables.items())
            ]
        )
        st.dataframe(preview_df, use_container_width=True, hide_index=True)

    if preview.unknown_files:
        with st.expander(
            f"{len(preview.unknown_files)} unknown file(s) will be ignored"
        ):
            for name in preview.unknown_files:
                st.caption(f"• `{name}`")

    # Decide the default import mode based on DB state
    summary = get_database_summary(project_slug)
    db_has_records = bool(summary.get("exists")) and int(
        summary.get("total_records", 0)
    ) > 0

    if db_has_records:
        st.info(
            f"This project's database already has "
            f"**{summary.get('total_records', 0):,}** record(s). "
            "Choose how to merge the uploaded feed."
        )
        mode_label = st.radio(
            "Import mode",
            options=[
                "Replace (clear all tables, then insert)",
                "Merge (upsert — existing PKs are overwritten)",
                "Abort if not empty (safest)",
            ],
            index=0,
            key="gtfs_zip_mode",
        )
        if mode_label.startswith("Replace"):
            selected_mode = ImportMode.REPLACE
        elif mode_label.startswith("Merge"):
            selected_mode = ImportMode.MERGE
        else:
            selected_mode = ImportMode.ABORT_IF_NOT_EMPTY
    else:
        selected_mode = ImportMode.REPLACE

    import_disabled = not preview.is_valid
    if st.button(
        "Import feed into database",
        type="primary",
        disabled=import_disabled,
        key="btn_gtfs_zip_import",
    ):
        with st.spinner("Importing GTFS feed…"):
            try:
                result = import_gtfs_zip(
                    project_slug,
                    io.BytesIO(zip_bytes),
                    mode=selected_mode,
                )
            except GtfsImportError as exc:
                st.error(str(exc))
                result = None

        if result is not None:
            c_ok, c_fail, c_time = st.columns(3)
            c_ok.metric("Rows inserted", f"{result.total_inserted:,}")
            c_fail.metric("Rows failed", f"{result.total_failed:,}")
            c_time.metric("Duration (s)", result.duration_seconds)

            rows = []
            for tbl in sorted(
                set(result.inserted_by_table) | set(result.failed_by_table)
            ):
                rows.append(
                    {
                        "table": tbl,
                        "inserted": result.inserted_by_table.get(tbl, 0),
                        "failed": result.failed_by_table.get(tbl, 0),
                    }
                )
            if rows:
                st.dataframe(
                    pd.DataFrame(rows),
                    use_container_width=True,
                    hide_index=True,
                )

            if result.cleared_tables:
                st.caption(
                    "Cleared before insert: "
                    + ", ".join(f"`{t}`" for t in result.cleared_tables)
                )

            if result.errors_by_table:
                total = sum(len(v) for v in result.errors_by_table.values())
                with st.expander(f"{total} validation error(s)", expanded=False):
                    for tbl, errs in result.errors_by_table.items():
                        st.markdown(f"**{tbl}** ({len(errs)} shown)")
                        for err in errs:
                            st.caption(f"• {err}")

            if result.total_failed == 0 and result.total_inserted > 0:
                st.success("Import complete.")
                st.rerun()

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
