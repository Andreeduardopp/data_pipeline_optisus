"""
GTFS (SQLite) → NeTEx dataset translator.

Reads the project's ``gtfs.db`` and builds an in-memory
:class:`NetexDataset`. No XML is produced here — serialisation lives in
``xml_builder.py``. Keeping the two layers apart lets us unit-test the
translation logic against plain object graphs.
"""

import logging
import sqlite3
from typing import Dict, Iterable, List, Optional, Tuple

from optisus.core.gtfs.database import get_connection
from optisus.core.netex.calendar import translate_calendar
from optisus.core.netex.config import NetexExportConfig
from optisus.core.netex.mappings import vehicle_mode_for_route_type
from optisus.core.netex.schemas import (
    Codespace,
    DayType,
    DayTypeAssignment,
    JourneyPattern,
    Line,
    Location,
    NetexDataset,
    OperatingPeriod,
    Organisation,
    PassengerStopAssignment,
    Quay,
    ResponsibilityRoleAssignment,
    ScheduledStopPoint,
    ServiceJourney,
    StopPlace,
    StopPointInJourneyPattern,
    TimetabledPassingTime,
)
from optisus.core.netex.urn import build_urn

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _rows(conn: sqlite3.Connection, table: str) -> List[sqlite3.Row]:
    cur = conn.execute(f"SELECT * FROM {table}")
    return cur.fetchall()


def _row_to_dict(row: sqlite3.Row) -> Dict:
    return {k: row[k] for k in row.keys()}


def _parse_gtfs_time(value: Optional[str]) -> Tuple[Optional[str], int]:
    """Parse a GTFS time (``HH:MM:SS`` allowing ≥24h) → (normalised HH:MM:SS, day_offset)."""
    if value is None or value == "":
        return None, 0
    h, m, s = value.split(":")
    h_int = int(h)
    offset, h_mod = divmod(h_int, 24)
    return f"{h_mod:02d}:{m}:{s}", offset


# ═══════════════════════════════════════════════════════════════════════════
# Frame translators
# ═══════════════════════════════════════════════════════════════════════════

def translate_resource_frame(
    agencies: Iterable[Dict], config: NetexExportConfig, version: str
) -> Tuple[Codespace, List[Organisation], List[ResponsibilityRoleAssignment]]:
    """Codespace + two Organisation blocks (Authority + Operator(s)) + role assignments.

    PT-EPIP requires both Authority and Operator to be present even when
    they are the same entity. Every GTFS agency becomes an additional
    Operator; the Authority is always taken from config.
    """
    codespace = Codespace(
        id=build_urn(config.codespace, "Codespace", config.codespace),
        xmlns=config.codespace,
        xmlns_url=f"http://www.pndt.pt/netex/{config.codespace}",
        description=f"PT-EPIP codespace for {config.operator.name}",
    )

    organisations: List[Organisation] = []

    authority_urn = build_urn(config.codespace, "Authority", config.authority.id)
    organisations.append(Organisation(
        id=authority_urn,
        version=version,
        kind="Authority",
        name=config.authority.name,
        short_name=config.authority.short_name,
        contact_email=config.authority.contact_email,
    ))

    # Primary operator (from config)
    primary_operator_urn = build_urn(config.codespace, "Operator", config.operator.id)
    organisations.append(Organisation(
        id=primary_operator_urn,
        version=version,
        kind="Operator",
        name=config.operator.name,
        short_name=config.operator.short_name,
        contact_email=config.operator.contact_email,
    ))

    # Additional operators — one per GTFS agency (deduped against primary)
    seen = {config.operator.id}
    for a in agencies:
        agency_id = a.get("agency_id") or config.operator.id
        if agency_id in seen:
            continue
        seen.add(agency_id)
        organisations.append(Organisation(
            id=build_urn(config.codespace, "Operator", agency_id),
            version=version,
            kind="Operator",
            name=a.get("agency_name") or agency_id,
            contact_email=a.get("agency_email"),
        ))

    role_assignments = [
        ResponsibilityRoleAssignment(
            id=build_urn(config.codespace, "ResponsibilityRoleAssignment", "ownership"),
            version=version,
            role="ownership",
            organisation_ref=authority_urn,
        ),
        ResponsibilityRoleAssignment(
            id=build_urn(config.codespace, "ResponsibilityRoleAssignment", "operation"),
            version=version,
            role="operation",
            organisation_ref=primary_operator_urn,
        ),
    ]

    return codespace, organisations, role_assignments


def translate_site_frame(
    stops: Iterable[Dict], codespace: str, version: str,
    stop_modes: Optional[Dict[str, str]] = None,
) -> List[StopPlace]:
    """Stops → StopPlaces + Quays, with synthetic StopPlaces for orphan stops.

    * ``location_type=1`` → StopPlace (no parent).
    * ``location_type=0`` with ``parent_station`` set → Quay under that parent.
    * ``location_type=0`` without parent → synthetic StopPlace containing one Quay.
    * ``location_type`` 2/3/4 are ignored in phase 1 (entrances/nodes/boarding areas).

    ``stop_modes`` maps ``stop_id`` → NeTEx VehicleMode derived from the routes
    serving each stop. Stops not in the map (or with no serving routes) fall
    back to ``"other"``, which is a valid NeTEx enum value — unlike
    ``"unknown"`` which the PT-EPIP validator rejects.
    """
    stops_list = [dict(s) for s in stops]
    modes = stop_modes or {}

    def _mode_for(stop_id: str) -> str:
        return modes.get(stop_id, "other")

    def _parent_mode(child_ids: List[str]) -> str:
        """Modal mode across child stops; falls back to 'other'."""
        counts: Dict[str, int] = {}
        for cid in child_ids:
            m = modes.get(cid)
            if m:
                counts[m] = counts.get(m, 0) + 1
        if not counts:
            return "other"
        return max(counts, key=counts.get)

    # Index by stop_id for quick lookup
    by_id: Dict[str, Dict] = {s["stop_id"]: s for s in stops_list}

    # Gather children per parent_station so we can derive parent modes.
    children_of: Dict[str, List[str]] = {}
    for s in stops_list:
        if (s.get("location_type") or 0) == 0 and s.get("parent_station"):
            children_of.setdefault(s["parent_station"], []).append(s["stop_id"])

    # Pass 1 — build StopPlace objects for location_type=1
    stop_places: Dict[str, StopPlace] = {}
    for s in stops_list:
        if (s.get("location_type") or 0) == 1:
            stop_places[s["stop_id"]] = StopPlace(
                id=build_urn(codespace, "StopPlace", s["stop_id"]),
                version=version,
                name=s.get("stop_name") or s["stop_id"],
                centroid=_centroid(s),
                transport_mode=_parent_mode(children_of.get(s["stop_id"], [])),
                stop_place_type=None,
                quays=[],
            )

    # Pass 2 — child quays + synthetic StopPlaces for orphans
    for s in stops_list:
        lt = s.get("location_type") or 0
        if lt != 0:
            continue

        quay = Quay(
            id=build_urn(codespace, "Quay", s["stop_id"]),
            version=version,
            name=s.get("stop_name"),
            public_code=s.get("stop_code") or s.get("platform_code"),
            centroid=_centroid(s),
        )

        parent = s.get("parent_station")
        if parent and parent in stop_places:
            stop_places[parent].quays.append(quay)
        elif parent and parent in by_id:
            # Parent exists but wasn't a station — synthesise a StopPlace for it
            parent_row = by_id[parent]
            sp = stop_places.setdefault(parent, StopPlace(
                id=build_urn(codespace, "StopPlace", parent),
                version=version,
                name=parent_row.get("stop_name") or parent,
                centroid=_centroid(parent_row),
                transport_mode=_parent_mode(children_of.get(parent, [])),
                quays=[],
            ))
            sp.quays.append(quay)
        else:
            # Orphan — synthesise a StopPlace with this single Quay
            synth_id = f"{s['stop_id']}_place"
            stop_places[synth_id] = StopPlace(
                id=build_urn(codespace, "StopPlace", synth_id),
                version=version,
                name=s.get("stop_name") or s["stop_id"],
                centroid=_centroid(s),
                transport_mode=_mode_for(s["stop_id"]),
                quays=[quay],
            )

    return list(stop_places.values())


def _derive_stop_modes(
    routes: Iterable[Dict], trips: Iterable[Dict], stop_times: Iterable[Dict],
) -> Dict[str, str]:
    """Pick the modal vehicle mode across routes that serve each stop."""
    route_mode: Dict[str, str] = {}
    for r in routes:
        try:
            route_mode[r["route_id"]] = vehicle_mode_for_route_type(r["route_type"])
        except KeyError:
            continue
    trip_mode: Dict[str, str] = {
        t["trip_id"]: route_mode.get(t["route_id"])
        for t in trips if route_mode.get(t["route_id"])
    }
    counts: Dict[str, Dict[str, int]] = {}
    for st in stop_times:
        mode = trip_mode.get(st["trip_id"])
        if not mode:
            continue
        counts.setdefault(st["stop_id"], {}).setdefault(mode, 0)
        counts[st["stop_id"]][mode] += 1
    return {
        sid: max(by_mode, key=by_mode.get)
        for sid, by_mode in counts.items()
    }


def _centroid(stop_row: Dict) -> Optional[Location]:
    lat, lon = stop_row.get("stop_lat"), stop_row.get("stop_lon")
    if lat is None or lon is None:
        return None
    return Location(longitude=float(lon), latitude=float(lat))


def translate_service_calendar_frame(
    calendar_rows: Iterable[Dict],
    calendar_dates_rows: Iterable[Dict],
    codespace: str,
    version: str,
) -> Tuple[List[DayType], List[OperatingPeriod], List[DayTypeAssignment]]:
    """Thin wrapper over ``calendar.translate_calendar``."""
    return translate_calendar(calendar_rows, calendar_dates_rows, codespace, version)


def translate_service_frame(
    routes: Iterable[Dict],
    trips: Iterable[Dict],
    stop_times: Iterable[Dict],
    stops: Iterable[Dict],
    config: NetexExportConfig,
    version: str,
) -> Tuple[
    List[Line],
    List[ScheduledStopPoint],
    List[PassengerStopAssignment],
    List[JourneyPattern],
    Dict[str, str],  # trip_id → journey_pattern_ref
    Dict[str, Dict[int, str]],  # journey_pattern_id → {stop_sequence: spjp_id}
]:
    """Lines, ScheduledStopPoints, PassengerStopAssignments, JourneyPatterns.

    JourneyPatterns are deduplicated across trips by the tuple
    ``(route_id, direction_id, (stop_id_1, stop_id_2, …))``.
    """
    codespace = config.codespace

    # ── Lines ─────────────────────────────────────────────────────────────
    lines: List[Line] = []
    for r in routes:
        lines.append(Line(
            id=build_urn(codespace, "Line", r["route_id"]),
            version=version,
            name=r.get("route_long_name") or r.get("route_short_name") or r["route_id"],
            public_code=r.get("route_short_name"),
            transport_mode=vehicle_mode_for_route_type(r["route_type"]),
            operator_ref=build_urn(
                codespace, "Operator",
                r.get("agency_id") or config.operator.id,
            ),
            presentation_colour=r.get("route_color"),
            presentation_text_colour=r.get("route_text_color"),
        ))

    # ── ScheduledStopPoints (one per GTFS stop_id) ────────────────────────
    ssp_list: List[ScheduledStopPoint] = []
    psa_list: List[PassengerStopAssignment] = []
    order = 1
    for s in stops:
        if (s.get("location_type") or 0) != 0:
            continue
        ssp_id = build_urn(codespace, "ScheduledStopPoint", s["stop_id"])
        ssp_list.append(ScheduledStopPoint(
            id=ssp_id, version=version, name=s.get("stop_name"),
        ))
        psa_list.append(PassengerStopAssignment(
            id=build_urn(codespace, "PassengerStopAssignment", s["stop_id"]),
            version=version,
            order=order,
            scheduled_stop_point_ref=ssp_id,
            quay_ref=build_urn(codespace, "Quay", s["stop_id"]),
        ))
        order += 1

    # ── Group stop_times by trip_id (sorted by stop_sequence) ─────────────
    times_by_trip: Dict[str, List[Dict]] = {}
    for st in stop_times:
        times_by_trip.setdefault(st["trip_id"], []).append(st)
    for trip_id in times_by_trip:
        times_by_trip[trip_id].sort(key=lambda r: r["stop_sequence"])

    # ── JourneyPattern dedup by (route, direction, stop tuple) ────────────
    trips_list = list(trips)
    patterns: Dict[Tuple, JourneyPattern] = {}
    trip_to_jp: Dict[str, str] = {}
    jp_stop_map: Dict[str, Dict[int, str]] = {}

    for trip in trips_list:
        stops_for_trip = times_by_trip.get(trip["trip_id"], [])
        if not stops_for_trip:
            continue
        key = (
            trip["route_id"],
            trip.get("direction_id"),
            tuple(st["stop_id"] for st in stops_for_trip),
        )
        if key not in patterns:
            jp_local_id = f"{trip['route_id']}_{trip.get('direction_id') or 0}_{len(patterns) + 1}"
            jp_id = build_urn(codespace, "JourneyPattern", jp_local_id)
            spjps: List[StopPointInJourneyPattern] = []
            stop_seq_to_spjp: Dict[int, str] = {}
            for idx, st in enumerate(stops_for_trip, start=1):
                spjp_id = build_urn(codespace, "StopPointInJourneyPattern", f"{jp_local_id}_{idx}")
                spjps.append(StopPointInJourneyPattern(
                    id=spjp_id,
                    version=version,
                    order=idx,
                    scheduled_stop_point_ref=build_urn(
                        codespace, "ScheduledStopPoint", st["stop_id"]
                    ),
                ))
                stop_seq_to_spjp[st["stop_sequence"]] = spjp_id
            direction = "outbound" if (trip.get("direction_id") or 0) == 0 else "inbound"
            patterns[key] = JourneyPattern(
                id=jp_id,
                version=version,
                name=jp_local_id,
                line_ref=build_urn(codespace, "Line", trip["route_id"]),
                direction_type=direction,
                stop_points=spjps,
            )
            jp_stop_map[jp_id] = stop_seq_to_spjp
        trip_to_jp[trip["trip_id"]] = patterns[key].id

    return (
        lines, ssp_list, psa_list,
        list(patterns.values()), trip_to_jp, jp_stop_map,
    )


def translate_timetable_frame(
    trips: Iterable[Dict],
    stop_times: Iterable[Dict],
    trip_to_jp: Dict[str, str],
    jp_stop_map: Dict[str, Dict[int, str]],
    config: NetexExportConfig,
    version: str,
) -> List[ServiceJourney]:
    """trips + stop_times → ServiceJourney + TimetabledPassingTime list."""
    codespace = config.codespace

    times_by_trip: Dict[str, List[Dict]] = {}
    for st in stop_times:
        times_by_trip.setdefault(st["trip_id"], []).append(st)
    for trip_id in times_by_trip:
        times_by_trip[trip_id].sort(key=lambda r: r["stop_sequence"])

    journeys: List[ServiceJourney] = []
    for trip in trips:
        jp_ref = trip_to_jp.get(trip["trip_id"])
        if jp_ref is None:
            continue
        seq_map = jp_stop_map[jp_ref]

        passing_times: List[TimetabledPassingTime] = []
        for idx, st in enumerate(times_by_trip.get(trip["trip_id"], []), start=1):
            arr_norm, arr_off = _parse_gtfs_time(st.get("arrival_time"))
            dep_norm, dep_off = _parse_gtfs_time(st.get("departure_time"))
            passing_times.append(TimetabledPassingTime(
                id=build_urn(codespace, "TimetabledPassingTime", f"{trip['trip_id']}_{idx}"),
                stop_point_in_journey_pattern_ref=seq_map[st["stop_sequence"]],
                arrival_time=arr_norm,
                departure_time=dep_norm,
                arrival_day_offset=arr_off,
                departure_day_offset=dep_off,
            ))

        journeys.append(ServiceJourney(
            id=build_urn(codespace, "ServiceJourney", trip["trip_id"]),
            version=version,
            name=trip.get("trip_short_name") or trip.get("trip_headsign"),
            line_ref=build_urn(codespace, "Line", trip["route_id"]),
            journey_pattern_ref=jp_ref,
            day_type_refs=[build_urn(codespace, "DayType", trip["service_id"])],
            operator_ref=build_urn(codespace, "Operator", config.operator.id),
            passing_times=passing_times,
        ))

    return journeys


# ═══════════════════════════════════════════════════════════════════════════
# Top-level entry point
# ═══════════════════════════════════════════════════════════════════════════

def translate_project(
    project_slug: str, config: NetexExportConfig, version: str,
) -> NetexDataset:
    """Read ``projects/<slug>/gtfs.db`` and produce a fully-wired NetexDataset."""
    conn = get_connection(project_slug)
    conn.row_factory = sqlite3.Row
    try:
        agencies = [_row_to_dict(r) for r in _rows(conn, "agency")]
        stops = [_row_to_dict(r) for r in _rows(conn, "stops")]
        routes = [_row_to_dict(r) for r in _rows(conn, "routes")]
        trips = [_row_to_dict(r) for r in _rows(conn, "trips")]
        stop_times = [_row_to_dict(r) for r in _rows(conn, "stop_times")]
        calendar_rows = [_row_to_dict(r) for r in _rows(conn, "calendar")]
        calendar_dates_rows = [_row_to_dict(r) for r in _rows(conn, "calendar_dates")]
    finally:
        conn.close()

    codespace_obj, organisations, role_assignments = translate_resource_frame(
        agencies, config, version,
    )
    stop_modes = _derive_stop_modes(routes, trips, stop_times)
    stop_places = translate_site_frame(
        stops, config.codespace, version, stop_modes=stop_modes,
    )
    day_types, operating_periods, assignments = translate_service_calendar_frame(
        calendar_rows, calendar_dates_rows, config.codespace, version,
    )
    lines, ssps, psas, jps, trip_to_jp, jp_stop_map = translate_service_frame(
        routes, trips, stop_times, stops, config, version,
    )
    service_journeys = translate_timetable_frame(
        trips, stop_times, trip_to_jp, jp_stop_map, config, version,
    )

    return NetexDataset(
        codespace=codespace_obj,
        organisations=organisations,
        role_assignments=role_assignments,
        stop_places=stop_places,
        day_types=day_types,
        operating_periods=operating_periods,
        day_type_assignments=assignments,
        lines=lines,
        scheduled_stop_points=ssps,
        passenger_stop_assignments=psas,
        journey_patterns=jps,
        service_journeys=service_journeys,
    )
