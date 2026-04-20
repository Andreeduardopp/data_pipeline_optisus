"""
Module 3 — Database Overview.

A read-only dashboard over the project's GTFS SQLite database.  Gives
users a visual summary of database health, table-level metrics, and
column-level profiling in one place — without scrolling through the
full GTFS pipeline page.

Sections:
  0. Project selector
  1. Database header card
  2. Table heatmap grid
  3. Table deep-dive (lazy column profiling)
  4. ER diagram (from _FK_CHECKS)
  5. Storage & history
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import streamlit as st

from optisus.core.gtfs.database import create_gtfs_database, database_exists
from optisus.core.gtfs.database_profiler import (
    DatabaseProfile,
    TableProfile,
    profile_database,
    profile_table_columns,
)
from optisus.core.storage.layers import PROJECTS_ROOT, list_projects
from optisus.ui.theme import (
    BLUE_ACCENT,
    BORDER_DEFAULT,
    ERROR,
    LIGHT_ACCENT,
    SUCCESS,
    SURFACE_RAISED,
    TEAL_1,
    TEAL_2,
    TEAL_3,
    TEXT_MUTED,
    TEXT_PRIMARY,
    TEXT_SECONDARY,
    WARNING,
    inject_custom_css,
)

inject_custom_css()


# Grouped for the heatmap — keeps the visual layout sane
_TABLE_GROUPS: dict[str, list[str]] = {
    "Core":      ["agency", "routes", "trips", "stops", "stop_times"],
    "Service":   ["calendar", "calendar_dates", "frequencies"],
    "Spatial":   ["shapes", "transfers"],
    "Metadata":  ["feed_info"],
    "GTFS-ride": ["board_alight", "ridership", "ride_feed_info", "trip_capacity"],
}


# ═══════════════════════════════════════════════════════════════════════════
# Small helpers
# ═══════════════════════════════════════════════════════════════════════════

def _fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 ** 2:
        return f"{n/1024:.1f} KB"
    if n < 1024 ** 3:
        return f"{n/1024**2:.1f} MB"
    return f"{n/1024**3:.2f} GB"


def _fmt_ts(iso: str) -> str:
    try:
        return datetime.fromisoformat(iso).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return iso or "—"


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except OSError:
                continue
    return total


def _heatmap_color(row_count: int) -> tuple[str, str]:
    """Return (background, border) for a table card based on row count."""
    if row_count == 0:
        return (f"{SURFACE_RAISED}", BORDER_DEFAULT)
    if row_count <= 100:
        return (f"{TEAL_2}88", TEAL_2)
    if row_count <= 1000:
        return (f"{TEAL_1}66", TEAL_1)
    return (f"{BLUE_ACCENT}44", BLUE_ACCENT)


# ═══════════════════════════════════════════════════════════════════════════
# Section renderers
# ═══════════════════════════════════════════════════════════════════════════

def _render_header_card(profile: DatabaseProfile) -> None:
    integrity_icon = "✅" if profile.integrity_clean else "⚠️"
    integrity_text = (
        "Clean" if profile.integrity_clean
        else f"{profile.violation_count} issues"
    )
    integrity_color = SUCCESS if profile.integrity_clean else WARNING

    st.markdown(
        f"""
        <div style="background:{SURFACE_RAISED};border:1px solid {BORDER_DEFAULT};
             border-radius:14px;padding:1.4rem 1.8rem;margin-bottom:1.2rem;">
            <div style="display:flex;align-items:center;justify-content:space-between;
                        margin-bottom:1rem;">
                <div>
                    <div style="color:{TEXT_MUTED};font-size:0.78rem;
                                letter-spacing:0.08em;text-transform:uppercase;">
                        Database
                    </div>
                    <div style="color:{LIGHT_ACCENT};font-weight:700;font-size:1.35rem;
                                margin-top:0.2rem;">
                        {profile.project_name}
                    </div>
                    <div style="color:{TEXT_MUTED};font-size:0.82rem;font-family:monospace;">
                        {profile.db_path}
                    </div>
                </div>
                <div style="text-align:right;">
                    <div style="display:inline-block;padding:0.3rem 0.8rem;
                                border-radius:10px;background:{integrity_color}22;
                                border:1px solid {integrity_color};
                                color:{integrity_color};font-weight:600;font-size:0.85rem;">
                        {integrity_icon} Integrity · {integrity_text}
                    </div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Records", f"{profile.total_records:,}")
    c2.metric(
        "Populated Tables",
        f"{profile.populated_tables} / {profile.total_tables}",
    )
    c3.metric("DB Size", _fmt_bytes(profile.db_size_bytes))
    c4.metric("Completeness", f"{profile.completeness_pct:.0f}%")

    st.caption(
        f"Schema {profile.schema_version} · "
        f"Created {_fmt_ts(profile.created_at)} · "
        f"Last modified {_fmt_ts(profile.last_modified)}"
    )


def _render_heatmap(profile: DatabaseProfile) -> None:
    st.subheader("Tables at a Glance")
    st.caption(
        "Colour indicates row count: "
        f"<span style='color:{TEXT_MUTED};'>dim=empty</span> · "
        f"<span style='color:{TEAL_3};'>teal=1–1K</span> · "
        f"<span style='color:{BLUE_ACCENT};'>blue=1K+</span>",
        unsafe_allow_html=True,
    )

    by_name: dict[str, TableProfile] = {t.table_name: t for t in profile.tables}

    for group_label, tables in _TABLE_GROUPS.items():
        st.markdown(
            f"<div style='color:{TEXT_SECONDARY};font-weight:600;"
            f"font-size:0.9rem;margin-top:1rem;margin-bottom:0.4rem;'>"
            f"{group_label}</div>",
            unsafe_allow_html=True,
        )
        cards_html = "<div style='display:flex;flex-wrap:wrap;gap:0.6rem;'>"
        for tbl in tables:
            tp = by_name.get(tbl)
            if tp is None:
                continue
            bg, border = _heatmap_color(tp.row_count)
            row_text = f"{tp.row_count:,} rows" if tp.row_count else "empty"
            cards_html += (
                f"<div style='flex:1 1 180px;min-width:160px;"
                f"background:{bg};border:1px solid {border};"
                f"border-radius:10px;padding:0.8rem 1rem;'>"
                f"<div style='color:{TEXT_PRIMARY};font-weight:600;"
                f"font-family:monospace;font-size:0.9rem;'>{tp.table_name}</div>"
                f"<div style='color:{LIGHT_ACCENT};font-weight:700;"
                f"font-size:1.1rem;margin-top:0.2rem;'>{row_text}</div>"
                f"<div style='color:{TEXT_MUTED};font-size:0.75rem;margin-top:0.2rem;'>"
                f"{tp.column_count} cols"
                + (f" · → {', '.join(tp.fk_references)}" if tp.fk_references else "")
                + "</div></div>"
            )
        cards_html += "</div>"
        st.markdown(cards_html, unsafe_allow_html=True)


def _render_table_deep_dive(profile: DatabaseProfile) -> None:
    st.subheader("Table Deep-Dive")
    st.caption(
        "Column profiling runs only for the table you pick — "
        "expensive stats (distinct counts, null %) are computed on demand."
    )

    by_name = {t.table_name: t for t in profile.tables}
    all_tables = [t.table_name for t in profile.tables]

    # Prefer a populated table for the default selection
    populated = [n for n in all_tables if by_name[n].row_count > 0]
    default = populated[0] if populated else all_tables[0]

    table_name = st.selectbox(
        "Table",
        all_tables,
        index=all_tables.index(default) if default in all_tables else 0,
        key="db_overview_table_select",
    )
    tp = by_name[table_name]

    if tp.row_count == 0:
        st.info(f"`{table_name}` is empty — no column stats to show.")
        return

    with st.spinner(f"Profiling columns of {table_name}…"):
        cols = profile_table_columns(profile.project_slug, table_name)

    if not cols:
        st.warning("Could not read column metadata.")
        return

    # Render as an HTML table so we can colour-code null %
    rows_html = ""
    for c in cols:
        null_color = (
            ERROR if c.null_pct > 50
            else WARNING if c.null_pct > 10
            else SUCCESS if c.null_pct == 0
            else TEXT_SECONDARY
        )
        pk_badge = (
            f"<span style='background:{BLUE_ACCENT}33;color:{BLUE_ACCENT};"
            f"padding:0.05rem 0.4rem;border-radius:6px;font-size:0.7rem;"
            f"margin-left:0.3rem;'>PK</span>"
            if c.is_primary_key else ""
        )
        nn_badge = (
            f"<span style='background:{TEAL_2};color:{LIGHT_ACCENT};"
            f"padding:0.05rem 0.4rem;border-radius:6px;font-size:0.7rem;"
            f"margin-left:0.3rem;'>NOT NULL</span>"
            if c.not_null and not c.is_primary_key else ""
        )
        samples = ", ".join(f"<code>{s}</code>" for s in c.sample_values) or "—"
        rows_html += (
            f"<tr>"
            f"<td style='padding:0.4rem 0.7rem;font-family:monospace;"
            f"color:{TEXT_PRIMARY};'>{c.name}{pk_badge}{nn_badge}</td>"
            f"<td style='padding:0.4rem 0.7rem;color:{TEXT_SECONDARY};"
            f"font-family:monospace;font-size:0.85rem;'>{c.dtype or '—'}</td>"
            f"<td style='padding:0.4rem 0.7rem;color:{null_color};"
            f"text-align:right;'>{c.null_pct:.1f}%</td>"
            f"<td style='padding:0.4rem 0.7rem;color:{TEXT_PRIMARY};"
            f"text-align:right;'>{c.distinct_count:,}</td>"
            f"<td style='padding:0.4rem 0.7rem;color:{TEXT_MUTED};"
            f"font-size:0.82rem;'>{samples}</td>"
            f"</tr>"
        )

    st.markdown(
        f"""
        <div style="background:{SURFACE_RAISED};border:1px solid {BORDER_DEFAULT};
             border-radius:10px;overflow:hidden;">
            <table style="width:100%;border-collapse:collapse;">
                <thead>
                    <tr style="background:{BORDER_DEFAULT}55;">
                        <th style="padding:0.6rem 0.7rem;text-align:left;
                                   color:{TEXT_SECONDARY};font-weight:600;
                                   font-size:0.8rem;letter-spacing:0.04em;
                                   text-transform:uppercase;">Column</th>
                        <th style="padding:0.6rem 0.7rem;text-align:left;
                                   color:{TEXT_SECONDARY};font-weight:600;
                                   font-size:0.8rem;letter-spacing:0.04em;
                                   text-transform:uppercase;">Type</th>
                        <th style="padding:0.6rem 0.7rem;text-align:right;
                                   color:{TEXT_SECONDARY};font-weight:600;
                                   font-size:0.8rem;letter-spacing:0.04em;
                                   text-transform:uppercase;">Null %</th>
                        <th style="padding:0.6rem 0.7rem;text-align:right;
                                   color:{TEXT_SECONDARY};font-weight:600;
                                   font-size:0.8rem;letter-spacing:0.04em;
                                   text-transform:uppercase;">Distinct</th>
                        <th style="padding:0.6rem 0.7rem;text-align:left;
                                   color:{TEXT_SECONDARY};font-weight:600;
                                   font-size:0.8rem;letter-spacing:0.04em;
                                   text-transform:uppercase;">Samples</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_er_diagram(profile: DatabaseProfile) -> None:
    """Render a Mermaid erDiagram derived from _FK_CHECKS.

    Streamlit renders mermaid code fences natively (v1.22+).  If the
    install doesn't support it, the block shows as a plain code snippet
    which is still readable.
    """
    st.subheader("Relationships")
    st.caption(
        "Entity-relationship view generated from the database's "
        "foreign-key constraints."
    )

    from optisus.core.gtfs.database import _FK_CHECKS

    by_name = {t.table_name: t for t in profile.tables}

    # Unique edges keyed by (parent, child) so multi-column FKs show once
    edges: dict[tuple[str, str], list[str]] = {}
    for fk in _FK_CHECKS:
        key = (fk["ref_table"], fk["table"])
        edges.setdefault(key, []).append(fk["column"])

    lines = ["erDiagram"]
    for (parent, child), cols in edges.items():
        p_rows = by_name.get(parent).row_count if by_name.get(parent) else 0
        c_rows = by_name.get(child).row_count if by_name.get(child) else 0
        label = ",".join(cols)
        lines.append(
            f'    {parent} ||--o{{ {child} : "{label} '
            f'({p_rows}->{c_rows})"'
        )

    st.markdown("```mermaid\n" + "\n".join(lines) + "\n```")


def _render_storage(profile: DatabaseProfile) -> None:
    st.subheader("Storage Footprint")

    project_dir = PROJECTS_ROOT / profile.project_slug
    db_size = profile.db_size_bytes
    exports_size = _dir_size(project_dir / "exports")
    runs_size = _dir_size(project_dir / "runs")
    total = db_size + exports_size + runs_size

    if total == 0:
        st.info("No files yet under this project.")
        return

    def _bar(label: str, size: int) -> None:
        pct = (size / total * 100.0) if total else 0
        st.markdown(
            f"<div style='display:flex;align-items:center;gap:0.8rem;"
            f"margin-top:0.5rem;'>"
            f"<span style='color:{TEXT_SECONDARY};min-width:7rem;"
            f"font-size:0.88rem;'>{label}</span>"
            f"<span style='flex:1;height:8px;background:{BORDER_DEFAULT};"
            f"border-radius:4px;overflow:hidden;'>"
            f"<span style='display:block;width:{pct:.1f}%;height:100%;"
            f"background:linear-gradient(90deg, {TEAL_1}, {LIGHT_ACCENT});"
            f"border-radius:4px;'></span></span>"
            f"<span style='color:{LIGHT_ACCENT};font-weight:600;"
            f"min-width:5.5rem;text-align:right;'>{_fmt_bytes(size)}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

    _bar("GTFS DB", db_size)
    _bar("Exports", exports_size)
    _bar("Runs", runs_size)

    st.markdown(
        f"<div style='margin-top:0.8rem;color:{TEXT_MUTED};font-size:0.85rem;'>"
        f"Total project footprint: <strong style='color:{LIGHT_ACCENT};'>"
        f"{_fmt_bytes(total)}</strong></div>",
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Page flow
# ═══════════════════════════════════════════════════════════════════════════

st.title("Database Overview")
st.caption(
    "Inspect the shape, completeness and health of a project's GTFS database "
    "at a glance.  Each project has exactly one GTFS database."
)

projects = list_projects()
if not projects:
    st.info("No projects found. Create a project in Module 1 first.")
    st.stop()

project_names = [p["name"] for p in projects]
project_slugs = {p["name"]: p["slug"] for p in projects}

_default_idx = 0
if "current_project" in st.session_state and st.session_state["current_project"]:
    _cur = st.session_state["current_project"].get("name", "")
    if _cur in project_names:
        _default_idx = project_names.index(_cur)

selected_name = st.selectbox(
    "Project",
    project_names,
    index=_default_idx,
    key="db_overview_selected_project",
)
project_slug = project_slugs[selected_name]

st.markdown("---")

# ── Empty-state: DB not yet created ───────────────────────────────────────
if not database_exists(project_slug):
    st.markdown(
        f"""
        <div style="background:{SURFACE_RAISED};border:1px solid {BORDER_DEFAULT};
             border-radius:12px;padding:1.4rem 1.8rem;margin-bottom:1rem;
             display:flex;align-items:center;gap:1rem;">
            <span style="font-size:1.8rem;">🗄️</span>
            <div>
                <div style="color:{TEXT_PRIMARY};font-weight:600;font-size:1.05rem;">
                    No GTFS database for this project yet
                </div>
                <div style="color:{TEXT_MUTED};font-size:0.88rem;">
                    Create it here or jump to Module 2 to populate it.
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button("Create GTFS Database", type="primary", key="btn_db_overview_create"):
        with st.spinner("Creating database…"):
            create_gtfs_database(project_slug)
        st.rerun()
    st.stop()

# ── Populated state ───────────────────────────────────────────────────────
profile = profile_database(project_slug)

_render_header_card(profile)
st.markdown("---")
_render_heatmap(profile)
st.markdown("---")
_render_table_deep_dive(profile)
st.markdown("---")
_render_er_diagram(profile)
st.markdown("---")
_render_storage(profile)
