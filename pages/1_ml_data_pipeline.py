"""
Module 1 — ML Data Preparation page.

Manages projects, uploads tabular and spatial data, validates against schemas,
and builds Mode A / Mode B ML-ready artifacts through Bronze → Silver → Gold layers.

This page is loaded by app.py via st.navigation().
"""

import tempfile
from pathlib import Path

import streamlit as st

from ui_validation import (
    TABULAR_SCHEMAS,
    get_default_required_fields,
    get_all_field_names,
    generate_template_csv,
    MODE_A,
    MODE_B,
    MODE_DESCRIPTIONS,
    get_mode_dataset_checklist,
)
from ingestion_tabular import read_tabular_for_preview, validate_tabular_for_ui
from ingestion_geo import read_spatial_for_preview, validate_spatial_data
from storage_layers import (
    create_project,
    list_projects,
    list_project_runs,
    create_project_layered_run,
    get_project_silver_datasets,
    save_bronze_bytes,
    save_silver_tabular,
    save_silver_spatial,
    save_silver_validation_report,
    build_gold_metrics,
    build_gold_spatial_metrics,
    save_gold_metrics,
    write_layer_lineage,
)
from mode_builders import (
    evaluate_quality_gate,
    build_mode_a_artifacts,
    build_mode_b_artifacts,
)


# ═══════════════════════════════════════════════════════════════════════════
# Navigation helpers
# ═══════════════════════════════════════════════════════════════════════════

def _open_project(project: dict) -> None:
    st.session_state["current_project"] = project


def _go_home() -> None:
    st.session_state["current_project"] = None


# ═══════════════════════════════════════════════════════════════════════════
# Page: Home — project list + creation form
# ═══════════════════════════════════════════════════════════════════════════

def _render_home() -> None:
    st.title("ML Data Preparation")
    st.caption("Upload and validate transit data, then build ML-ready artifacts.")

    st.subheader("Create a new project")
    col_input, col_btn = st.columns([3, 1])
    with col_input:
        new_project_name = st.text_input(
            "Project name", key="new_project_name", label_visibility="collapsed",
            placeholder="Enter project name…",
        )
    with col_btn:
        create_clicked = st.button("Create project", key="btn_create_project", use_container_width=True)
    if create_clicked:
        if new_project_name.strip():
            try:
                create_project(new_project_name)
                st.rerun()
            except ValueError as exc:
                st.error(str(exc))
        else:
            st.warning("Please enter a project name.")

    st.divider()

    projects = list_projects()
    if not projects:
        st.info("No projects yet. Create one above to get started.")
        return

    st.subheader("Your projects")
    cols_per_row = 3
    for row_start in range(0, len(projects), cols_per_row):
        row_projects = projects[row_start : row_start + cols_per_row]
        cols = st.columns(cols_per_row)
        for col, proj in zip(cols, row_projects):
            with col:
                run_count = len(list_project_runs(proj["slug"]))
                with st.container(border=True):
                    st.markdown(f"### {proj['name']}")
                    st.caption(f"Created: {proj.get('created_at', 'N/A')}")
                    st.caption(f"Runs: {run_count}")
                    st.button(
                        "Open",
                        key=f"open_{proj['slug']}",
                        on_click=_open_project,
                        args=(proj,),
                        use_container_width=True,
                    )


# ═══════════════════════════════════════════════════════════════════════════
# Mode panel — requirements checklist, quality gate, build trigger
# ═══════════════════════════════════════════════════════════════════════════

def _render_mode_panel(project_slug: str) -> None:
    """Show forecasting-mode requirements, quality gate, and build trigger."""
    st.subheader("Forecasting Modes")

    available = get_project_silver_datasets(project_slug)

    # --- Two-column requirements overview ---
    col_a, col_b = st.columns(2)
    for col, mode in ((col_a, MODE_A), (col_b, MODE_B)):
        with col:
            info = MODE_DESCRIPTIONS[mode]
            with st.container(border=True):
                st.markdown(f"**{info['title']}**")
                st.caption(info["description"])
                st.caption(f"Output: `{info['output_artifact']}`")
                checklist = get_mode_dataset_checklist(mode, available)
                for item in checklist:
                    if item["available"]:
                        icon = "✅"
                    elif item["kind"] == "optional":
                        icon = "⬜"
                    else:
                        icon = "❌"
                    st.markdown(f"{icon} {item['dataset']} *({item['kind']})*")

    # --- Mode selector + build action ---
    selected_mode = st.radio(
        "Select mode to build",
        options=[MODE_A, MODE_B],
        horizontal=True,
        key="mode_selector",
    )

    gate_passed, missing = evaluate_quality_gate(selected_mode, available)

    if gate_passed:
        if st.button(
            f"Build {selected_mode} Artifacts",
            key="btn_build_mode",
            type="primary",
            use_container_width=True,
        ):
            with st.spinner(f"Building {selected_mode} artifacts..."):
                if selected_mode == MODE_A:
                    run_path, warnings = build_mode_a_artifacts(
                        project_slug, available
                    )
                else:
                    run_path, warnings = build_mode_b_artifacts(
                        project_slug, available
                    )

            if run_path:
                st.success(f"Build complete — {run_path}")
                if warnings:
                    with st.expander(f"{len(warnings)} warning(s)"):
                        for w in warnings[:50]:
                            st.text(w)
                        if len(warnings) > 50:
                            st.caption(f"... and {len(warnings) - 50} more.")
            else:
                st.error("Build failed.")
                for w in (warnings or [])[:20]:
                    st.text(w)
    else:
        st.warning(
            f"**Quality Gate — {selected_mode}:** upload the missing required "
            f"datasets before building: {', '.join(missing)}"
        )
        st.button(
            f"Build {selected_mode} Artifacts",
            key="btn_build_mode",
            disabled=True,
            use_container_width=True,
        )


# ═══════════════════════════════════════════════════════════════════════════
# Page: Project detail — run history + upload tabs
# ═══════════════════════════════════════════════════════════════════════════

def _render_project(project: dict) -> None:
    project_slug = project["slug"]

    st.button("← Back to projects", key="btn_back", on_click=_go_home)
    st.title(project["name"])

    # --- Run history ---
    with st.expander("Run history", expanded=False):
        runs = list_project_runs(project_slug)
        if runs:
            for run in runs:
                bronze_count = len(run.get("bronze", []))
                silver_count = len(run.get("silver", []))
                gold_count = len(run.get("gold", []))
                st.markdown(
                    f"**{run['run_id']}** — *{run.get('context', '')}* "
                    f"&nbsp; Bronze: {bronze_count} &nbsp; Silver: {silver_count} &nbsp; Gold: {gold_count} "
                    f"&nbsp; _{run.get('timestamp', '')}_"
                )
        else:
            st.caption("No runs recorded for this project yet.")

    st.divider()

    # --- Forecasting mode panel ---
    _render_mode_panel(project_slug)

    st.divider()

    # --- Upload tabs ---
    schema_options = [label for label, _ in TABULAR_SCHEMAS]
    tab_tabular, tab_spatial = st.tabs(["Tabular Data", "Spatial Data"])

    # ───────────────────────────────────────────────────────────────────────
    # Tab 1 — Tabular
    # ───────────────────────────────────────────────────────────────────────
    with tab_tabular:
        st.subheader("Tabular file")
        upload_tabular = st.file_uploader(
            "Upload CSV or Excel (.csv, .xlsx, .xls)",
            type=["csv", "xlsx", "xls"],
            key="tabular_upload",
        )

        schema_choice = st.selectbox("Schema model", options=schema_options, key="tabular_schema")
        model_class = next(m for label, m in TABULAR_SCHEMAS if label == schema_choice)

        template_csv = generate_template_csv(model_class)
        safe_name = schema_choice.lower().replace(" ", "_").replace("&", "and")
        st.download_button(
            f"Download {schema_choice} template (.csv)",
            data=template_csv,
            file_name=f"{safe_name}_template.csv",
            mime="text/csv",
            key="btn_download_template",
        )

        all_fields = get_all_field_names(model_class)
        default_required = get_default_required_fields(model_class)
        mandatory_fields = st.multiselect(
            "Mandatory fields for your context",
            options=all_fields,
            default=default_required,
            key="tabular_mandatory",
        )

        if upload_tabular and mandatory_fields:
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(upload_tabular.name).suffix) as tmp:
                tmp.write(upload_tabular.getvalue())
                tmp_path = tmp.name

            df_preview, read_err = read_tabular_for_preview(tmp_path)
            if read_err:
                st.error(f"Could not read file: {read_err}")
            else:
                st.caption("Column preview (normalized)")
                st.dataframe(df_preview.head(10), use_container_width=True)

            if st.button("Validate & Save", key="btn_validate_tabular"):
                layers = create_project_layered_run(project_slug, schema_choice)
                bronze_artifacts = []
                silver_artifacts = []
                gold_artifacts = []
                metrics = None

                bronze_path = save_bronze_bytes(
                    upload_tabular.getvalue(),
                    layers["bronze"],
                    upload_tabular.name,
                )
                bronze_artifacts.append(str(bronze_path))

                clean_df, read_err, missing_cols, row_errors = validate_tabular_for_ui(
                    tmp_path, model_class, mandatory_fields
                )
                total_rows = len(df_preview) if df_preview is not None else 0

                if read_err:
                    st.error(f"Read error: {read_err}")
                elif missing_cols:
                    st.error(f"Missing mandatory columns: {missing_cols}")
                    report_path = save_silver_validation_report(
                        layers["silver"],
                        schema_choice,
                        total_rows=total_rows,
                        valid_rows=0,
                        invalid_rows=total_rows,
                        missing_mandatory_columns=missing_cols,
                    )
                    silver_artifacts.append(str(report_path))
                else:
                    valid_count = len(clean_df) if clean_df is not None else 0
                    invalid_count = len(row_errors)

                    if row_errors:
                        st.warning(f"Row-level validation failed for {invalid_count} row(s).")
                        with st.expander("Row errors"):
                            for idx, msg in row_errors[:50]:
                                st.text(f"Row {idx}: {msg}")
                            if len(row_errors) > 50:
                                st.caption(f"... and {len(row_errors) - 50} more.")

                    report_path = save_silver_validation_report(
                        layers["silver"],
                        schema_choice,
                        total_rows=total_rows,
                        valid_rows=valid_count,
                        invalid_rows=invalid_count,
                    )
                    silver_artifacts.append(str(report_path))

                    if clean_df is not None and not clean_df.empty:
                        silver_path = save_silver_tabular(clean_df, layers["silver"], schema_choice)
                        silver_artifacts.append(str(silver_path))

                        st.success(f"Valid rows: {valid_count}")
                        st.dataframe(clean_df, use_container_width=True)

                        metrics = build_gold_metrics(clean_df, schema_choice)
                        gold_path = save_gold_metrics(metrics, layers["gold"], schema_choice)
                        gold_artifacts.append(str(gold_path))
                    else:
                        st.info("No valid rows after validation.")

                write_layer_lineage(
                    layers["root"],
                    run_id=layers["run_id"],
                    context=schema_choice,
                    bronze_artifacts=bronze_artifacts,
                    silver_artifacts=silver_artifacts,
                    gold_artifacts=gold_artifacts,
                )

                st.divider()
                st.subheader("Run Summary")
                st.code(f"Run ID: {layers['run_id']}")
                col_b, col_s, col_g = st.columns(3)
                with col_b:
                    st.markdown("**Bronze**")
                    for a in bronze_artifacts:
                        st.text(Path(a).name)
                with col_s:
                    st.markdown("**Silver**")
                    for a in silver_artifacts:
                        st.text(Path(a).name)
                with col_g:
                    st.markdown("**Gold**")
                    for a in gold_artifacts:
                        st.text(Path(a).name)

                if gold_artifacts and metrics:
                    with st.expander("Gold metrics preview"):
                        st.json(metrics)

            try:
                Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass

    # ───────────────────────────────────────────────────────────────────────
    # Tab 2 — Spatial Data
    # ───────────────────────────────────────────────────────────────────────
    with tab_spatial:
        st.subheader("Spatial file")
        st.caption("Upload .geojson or .shp (for .shp ensure sidecar files .dbf, .shx, .prj are alongside).")
        upload_spatial = st.file_uploader(
            "Upload spatial file (.geojson, .shp)",
            type=["geojson", "shp"],
            key="spatial_upload",
        )

        if upload_spatial is not None:
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(upload_spatial.name).suffix) as tmp:
                tmp.write(upload_spatial.getvalue())
                tmp_spatial_path = tmp.name

            gdf_preview, preview_err = read_spatial_for_preview(tmp_spatial_path)

            if preview_err:
                st.error(f"Could not read spatial file: {preview_err}")
            else:
                st.caption("Column preview")
                st.dataframe(
                    gdf_preview.drop(columns=["geometry"], errors="ignore").head(10),
                    use_container_width=True,
                )

                available_cols = [c for c in gdf_preview.columns if c != gdf_preview.geometry.name]
                default_sel = [c for c in ["stop_id"] if c in available_cols]
                spatial_required = st.multiselect(
                    "Required columns (geometry is always validated)",
                    options=available_cols,
                    default=default_sel,
                    key="spatial_required",
                )

                if st.button("Validate & Save", key="btn_validate_spatial"):
                    layers = create_project_layered_run(project_slug, "spatial_data")
                    bronze_artifacts = []
                    silver_artifacts = []
                    gold_artifacts = []
                    spatial_metrics = None

                    bp = save_bronze_bytes(
                        upload_spatial.getvalue(),
                        layers["bronze"],
                        upload_spatial.name,
                    )
                    bronze_artifacts.append(str(bp))

                    full_required = spatial_required + ["geometry"]
                    result = validate_spatial_data(tmp_spatial_path, full_required)

                    if result["error"]:
                        st.error(result["error"])
                        rp = save_silver_validation_report(
                            layers["silver"],
                            "spatial_data",
                            total_rows=result["total_rows"],
                            valid_rows=result["valid_rows"],
                            invalid_rows=result["invalid_rows"],
                            missing_mandatory_columns=result["missing_columns"],
                        )
                        silver_artifacts.append(str(rp))
                    else:
                        gdf_clean = result["gdf"]

                        if result["invalid_rows"] > 0:
                            st.warning(
                                f"Dropped {result['invalid_rows']} feature(s) with invalid geometry."
                            )

                        sp = save_silver_spatial(gdf_clean, layers["silver"], "spatial_data")
                        silver_artifacts.append(str(sp))

                        rp = save_silver_validation_report(
                            layers["silver"],
                            "spatial_data",
                            total_rows=result["total_rows"],
                            valid_rows=result["valid_rows"],
                            invalid_rows=result["invalid_rows"],
                        )
                        silver_artifacts.append(str(rp))

                        st.success(f"Valid features: {result['valid_rows']}")
                        st.dataframe(
                            gdf_clean.drop(columns=["geometry"], errors="ignore").head(20),
                            use_container_width=True,
                        )

                        spatial_metrics = build_gold_spatial_metrics(gdf_clean, "spatial_data")
                        gp = save_gold_metrics(spatial_metrics, layers["gold"], "spatial_data")
                        gold_artifacts.append(str(gp))

                    write_layer_lineage(
                        layers["root"],
                        run_id=layers["run_id"],
                        context="spatial_data",
                        bronze_artifacts=bronze_artifacts,
                        silver_artifacts=silver_artifacts,
                        gold_artifacts=gold_artifacts,
                    )

                    st.divider()
                    st.subheader("Run Summary")
                    st.code(f"Run ID: {layers['run_id']}")
                    col_b, col_s, col_g = st.columns(3)
                    with col_b:
                        st.markdown("**Bronze**")
                        for a in bronze_artifacts:
                            st.text(Path(a).name)
                    with col_s:
                        st.markdown("**Silver**")
                        for a in silver_artifacts:
                            st.text(Path(a).name)
                    with col_g:
                        st.markdown("**Gold**")
                        for a in gold_artifacts:
                            st.text(Path(a).name)

                    if spatial_metrics:
                        with st.expander("Gold spatial metrics preview"):
                            st.json(spatial_metrics)

            try:
                Path(tmp_spatial_path).unlink(missing_ok=True)
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════════
# Router
# ═══════════════════════════════════════════════════════════════════════════

if st.session_state.get("current_project") is None:
    _render_home()
else:
    _render_project(st.session_state["current_project"])
