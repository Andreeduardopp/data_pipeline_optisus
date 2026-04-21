# Feature Plan — Export Project GTFS Database as NeTEx (PT-EPIP, zipped multi-file)

## Goal

Allow a user to export a project's GTFS SQLite database as a **PT-EPIP-conformant NeTEx dataset** packaged as a `.zip` of multiple XML files, produced under `data_lake_outputs/projects/<slug>/exports/netex/<timestamp>/`, ready for submission to the Portuguese National Access Point (PNDT).

## User story

> As a Portuguese transit operator, after producing a validated GTFS feed with this tool, I click **Export NeTEx (PT-EPIP)**, fill in my codespace (NIF / IMT short name), authority, and operator, and download a zipped multi-file NeTEx package I can submit to the PNDT.

## Non-goals

- No NeTEx **import**. Export only.
- No fare frames, alternative routing, real-time (SIRI) coupling, or accessibility assessments beyond what PT-EPIP mandates.
- No GTFS-ride (board_alight, ridership) passthrough — NeTEx has no equivalent in PT-EPIP.
- No modification of existing GTFS Pydantic models. The NeTEx layer reads `gtfs.db` directly.

## PT-EPIP package layout (zipped, multi-file)

Zip name follows the PNDT convention:

```
NETEX_PT_{OPERATOR_SHORT_NAME}_{VERSION}_{YYYYMMDDHHMM}.zip
```

Internal layout (frame-typed names):

```
SiteFrame_Stops.xml                           # StopPlaces + Quays (SiteFrame) + shared ResourceFrame/ServiceCalendarFrame
ServiceFrame_Lines.xml                        # Lines, Routes, ScheduledStopPoints, JourneyPatterns
TimetableFrame_Schedules_Line_<route_id>.xml  # One per GTFS route: ServiceJourneys + TimetabledPassingTimes
```

Each XML file is a standalone `PublicationDelivery`. The framework file (`SiteFrame_Stops.xml`) carries the shared `ResourceFrame` (codespace, Authority + Operator organisations, ResponsibilityRoleAssignment) and `ServiceCalendarFrame` (DayTypes, OperatingPeriods, DayTypeAssignments).

## Architecture

### New module: `src/optisus/core/netex/`

| File | Responsibility |
|---|---|
| `__init__.py` | Public API re-exports |
| `config.py` | `NetexExportConfig` Pydantic model — codespace (regex `^[A-Z0-9]+$`), authority block, operator block, default_lang, version_strategy. Persisted per-project at `projects/<slug>/netex_config.json`. |
| `urn.py` | `build_urn(codespace, object_type, local_id)` → `"PT:{codespace}:{type}:{id}"`. Sanitiser for non-conformant chars. |
| `mappings.py` | Static dicts: `ROUTE_TYPE_TO_VEHICLE_MODE` (GTFS int → NeTEx string), `LOCATION_TYPE_TO_STOP_PLACE_TYPE`, `WHEELCHAIR_TO_LIMITATION`. |
| `schemas.py` | Pydantic models for NeTEx domain objects: `Codespace`, `Authority`, `Operator`, `StopPlace`, `Quay`, `Line`, `Route`, `RoutePoint`, `ScheduledStopPoint`, `JourneyPattern`, `StopPointInJourneyPattern`, `DayType`, `OperatingPeriod`, `DayTypeAssignment`, `ServiceJourney`, `TimetabledPassingTime`. Internal representation — XML serialisation is separate. |
| `translator.py` | Reads `gtfs.db` and builds a `NetexDataset` object graph. One function per frame: `translate_resource_frame`, `translate_site_frame`, `translate_service_calendar_frame`, `translate_service_frame`, `translate_timetable_frame_for_line`. |
| `calendar.py` | GTFS `calendar` + `calendar_dates` → NeTEx `DayType` + `OperatingPeriod` + `DayTypeAssignment`. Handles bitmask fold and exception overlay. |
| `xml_builder.py` | `lxml.etree.ElementMaker`-based serializer. One function per entity. Handles namespaces, child ordering, XSD-required wrappers. |
| `frames.py` | Assembles frames into `PublicationDelivery` root elements (one per output XML file). |
| `exporter.py` | Public entry point: `export_netex(project_slug, config) → NetexExportResult`. Orchestrates validate → translate → serialise → zip → write → return result. |
| `validator.py` (phase 2) | `validate_netex(xml_path)` using bundled XSDs. Phase 1 ships without in-tool XSD validation. |

### Reused existing code

- `core.gtfs.database.get_connection` / `get_table_count` — read source rows.
- `core.gtfs.exporter.validate_before_export` — pre-flight GTFS completeness check.
- `core.storage.layers.PROJECTS_ROOT` — output path resolution.

### No changes to `core/schemas/gtfs.py`

The GTFS models stay spec-pure. NeTEx-specific metadata (codespace, versioning, participant ref) lives in `NetexExportConfig` or is derived by the translator.

## PT-Portugal specific rules

### 1. Codespace — mandatory, NIF-preferred

Every ID is a URN: `PT:{codespace}:{type}:{local_id}` (e.g. `PT:980123456:StopPlace:001`).

- `codespace` should be the operator's NIF (Tax ID) or official IMT-assigned short name.
- Config UI has a **mandatory** `codespace` field. Placeholder value `FIXME` is allowed in storage but triggers a **Critical Validation Error** at pre-flight and disables the export button.
- PNDT validator rejects any file without a registered codespace.

### 2. Authority + Operator — dual entity, always two Organisation blocks

Portuguese law rigidly separates:

- **Authority** (Autoridade de Transportes) — usually a CIM (e.g. CIM do Ave) or metropolitan entity (e.g. Transportes Metropolitanos do Porto).
- **Operator** (Operador) — the company running the service (e.g. Guimabus, STCP).

The `ResourceFrame` emits **two `Organisation` blocks** plus a `ResponsibilityRoleAssignment` linking data-ownership → Authority and service-operation → Operator. Even when they are the same entity (some municipal services), they must be declared as two separate roles.

### 3. Dataset naming — PNDT convention

- Zip name: `NETEX_PT_{OPERATOR_SHORT_NAME}_{VERSION}_{YYYYMMDDHHMM}.zip`
- Internal XML files use frame-typed names (see "package layout" above).

### 4. Version strategy — date-based snapshot (phase 1)

- Phase 1: every NeTEx object emits `version="{YYYYMMDD}"` of the export date. Simple, ensures PNDT recognises each upload as newer than the previous.
- Phase 2: per-object change detection for true incremental deliveries.
- Config offers `version_strategy: Literal["timestamp", "manual"]`; phase 1 only implements `"timestamp"`.

## Translation rules (GTFS → NeTEx under PT-EPIP)

| GTFS source | NeTEx target | Key mapping rule |
|---|---|---|
| `agency` row | `Operator` (and shared `Authority` from config) | Multi-agency feeds → each agency becomes an `Operator`; single `Authority` always comes from config. |
| `stops` where `location_type=1` | `StopPlace` | `Centroid/Location` from lat/lon. |
| `stops` where `location_type=0` AND `parent_station IS NOT NULL` | `Quay` under its parent `StopPlace` | |
| `stops` where `location_type=0` AND `parent_station IS NULL` | Synthetic `StopPlace` containing a single `Quay` | PT-EPIP requires every stop to sit in a StopPlace hierarchy. |
| `stops` where `location_type IN (2,3,4)` | `StopPlaceEntrance` / `PathJunction` / `BoardingPosition` | |
| `routes` row | `Line` | `TransportMode` from `ROUTE_TYPE_TO_VEHICLE_MODE`. `PublicCode` ← `route_short_name`. `Name` ← `route_long_name`. |
| `shapes` + distinct direction patterns from `stop_times` | `Route` + `RoutePoint`s | Deduped per direction pattern. |
| Distinct `(route_id, direction_id, stop pattern)` | `JourneyPattern` + `StopPointInJourneyPattern`s | References `ScheduledStopPoint`s. |
| `calendar` row | `DayType` + `OperatingPeriod` + `DayTypeAssignment`s | Bitmask folded to explicit assignments for the window. |
| `calendar_dates` row | `DayTypeAssignment` overlay with `isAvailable=true/false` | |
| `trips` row | `ServiceJourney` | References `Line`, `JourneyPattern`, `DayType`. |
| `stop_times` row | `TimetabledPassingTime` inside its `ServiceJourney` | `>24:00:00` → `DayOffset=1`. |
| `transfers` | `ServiceJourneyInterchange` | Phase 2. |
| `frequencies` | `HeadwayJourneyGroup` | Phase 2. |
| `feed_info` | `PublicationDelivery/@PublicationTimestamp` + `Description` | |

## UX — new section in `src/optisus/ui/pages/gtfs_pipeline.py`

Positioned as **Section 9 — Export NeTEx (PT-EPIP)**, below the existing GTFS export section.

1. **Config form** (first-run blank; subsequent runs pre-fill from `netex_config.json`):
   - `codespace` (required, regex-validated) with helper text pointing to NIF / IMT short name.
   - Authority block: `authority_id`, `authority_name`, contact.
   - Operator block: `operator_id` (default derived from `agency_id`), `operator_name` (default from `agency_name`), `operator_short_name` (used in zip filename).
   - `participant_ref` (NAP-assigned).
   - `default_lang` (default `"pt"`).
   - `version_strategy` (phase 1: only `"timestamp"`).
2. **Pre-flight**:
   - Reuses `validate_before_export` (required GTFS tables present).
   - NeTEx-specific checks: codespace ≠ `FIXME`, every `route_type` maps to a known VehicleMode, every `location_type=0` stop resolves into a StopPlace.
   - Codespace == `FIXME` → red "Critical Validation Error" banner, export button disabled.
3. **Export button** → progress (`Translating frames → Serialising XML → Zipping`) → summary: counts of StopPlaces / Lines / ServiceJourneys / output files, total XML size, zip path, download button.
4. **Validation notice**: phase 1 ships without bundled XSDs; link to official PT-EPIP XSDs and show manual validation steps. Phase 2 adds in-tool XSD validation.

## Testing

New file `tests/test_netex_export.py` (and `tests/test_netex_scaffold.py` for phase-1 scaffolding units). Use the existing `isolated_gtfs` fixture.

| Test | Covers |
|---|---|
| `test_config_rejects_placeholder_codespace` | `FIXME` fails pre-flight |
| `test_config_codespace_regex` | Accepts `980123456`, `STCP`; rejects `pt-test`, spaces |
| `test_urn_builder` | URN format + sanitisation |
| `test_route_type_mapping_covers_all_enums` | Every `RouteType` enum has a NeTEx mode |
| `test_translate_site_frame_quay_hierarchy` | Stops with `parent_station` → Quays; orphan stops → synthetic StopPlace |
| `test_translate_calendar_bitmask_to_daytypes` | Bitmask → DayType props; exception overlay |
| `test_translate_calendar_dates_only` | Feed with only `calendar_dates` |
| `test_translate_timetable_overnight_trips` | `>24:00:00` → `DayOffset=1` |
| `test_translate_multi_agency_feed` | Each agency → Operator; shared Authority from config |
| `test_resource_frame_emits_two_organisations_and_role_assignment` | Authority + Operator both present even when identical |
| `test_xml_builder_namespaces` | `xmlns`, `xmlns:gml`, `xmlns:siri`, `xmlns:xsi` on root |
| `test_xml_builder_child_order` | `PublicationDelivery` children in PT-EPIP-prescribed order |
| `test_zip_naming_follows_pndt_convention` | `NETEX_PT_{OP}_{VERSION}_{YYYYMMDDHHMM}.zip` |
| `test_export_produces_expected_zip_layout` | Zip contains `SiteFrame_Stops.xml`, `ServiceFrame_Lines.xml`, one `TimetableFrame_Schedules_Line_<route>.xml` per route |
| `test_export_roundtrip_with_sample_feed` | End-to-end: sample GTFS → import → export → parse XMLs with lxml, XPath-check counts |
| `test_xsd_validation_if_schemas_present` | Skipped unless `PT_EPIP_XSD_ROOT` env var set |

## Dependencies

- `lxml` — XML builder, XPath, XSD validation (new hard dependency).
- `python-dateutil` — expanding calendar windows (already transitively available via pandas).

No other new deps. XSD files not bundled (licensing / size) — documented env var for users who want local XSD validation.

## Phasing

1. **Phase 1 — Core export** (this plan): module scaffolding, translator, XML builder, exporter, UI section, tests with XPath assertions. Output validates manually against the official PT-EPIP XSDs.
2. **Phase 2** (follow-up): in-tool XSD validation, `transfers` → `ServiceJourneyInterchange`, `frequencies` → `HeadwayJourneyGroup`, FareFrame if mandated by PT-EPIP updates, optional `accessibilityAssessment` expansion, per-object change-detection for incremental partial deliveries.
