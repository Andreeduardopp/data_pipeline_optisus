"""
PublicationDelivery wrappers — assemble NeTEx frames into the three
output XML files prescribed by the PT-EPIP multi-file layout:

  * ``SiteFrame_Stops.xml``         — CompositeFrame(ResourceFrame+SiteFrame+ServiceCalendarFrame)
  * ``ServiceFrame_Lines.xml``      — CompositeFrame(ResourceFrame+ServiceFrame)
  * ``TimetableFrame_Schedules_Line_<route>.xml`` — CompositeFrame(ResourceFrame+TimetableFrame)

Each file is a standalone ``<PublicationDelivery>`` containing one
``<CompositeFrame>`` whose ``TypeOfFrameRef`` declares the relevant
EPIP profile (``EU_PI_STOP`` / ``EU_PI_LINE_OFFER`` /
``EU_PI_TIMETABLE``) so the PNDT validator can route it to the correct
profile checks.
"""

from datetime import datetime, timezone
from typing import List, Optional, Tuple

from lxml import etree

from optisus.core.netex.config import NetexExportConfig
from optisus.core.netex.schemas import NetexDataset, ServiceJourney
from optisus.core.netex.urn import build_urn
from optisus.core.netex.xml_builder import (
    NETEX_NS,
    NSMAP,
    XSI_NS,
    build_codespace,
    build_data_source,
    build_day_type,
    build_day_type_assignment,
    build_journey_pattern,
    build_line,
    build_operating_period,
    build_organisation,
    build_passenger_stop_assignment,
    build_responsibility_role_assignment,
    build_scheduled_stop_point,
    build_service_journey,
    build_stop_place,
    build_type_of_frame_ref,
    build_valid_between,
)

_PUBLICATION_VERSION = "1.15:PT-EPIP-1.0"

# EPIP profile TypeOfFrame refs — what the PNDT validator routes on.
TYPE_OF_FRAME_COMPOSITE = "epip:EU_PI_METADATA"
TYPE_OF_FRAME_STOP = "epip:EU_PI_STOP"
TYPE_OF_FRAME_LINE_OFFER = "epip:EU_PI_LINE_OFFER"
TYPE_OF_FRAME_TIMETABLE = "epip:EU_PI_TIMETABLE"


# ═══════════════════════════════════════════════════════════════════════════
# Frame builders
# ═══════════════════════════════════════════════════════════════════════════

def _new(tag: str, **attrs) -> etree._Element:
    return etree.Element(f"{{{NETEX_NS}}}{tag}", **attrs)


def _dataset_validity(dataset: NetexDataset) -> Tuple[str, str]:
    """Derive (from_date, to_date) for ValidBetween.

    Uses the union of all OperatingPeriod ranges when present; falls back to
    ``today .. today+1y`` so the attribute is never omitted.
    """
    if dataset.operating_periods:
        froms = sorted(op.from_date for op in dataset.operating_periods)
        tos = sorted(op.to_date for op in dataset.operating_periods)
        return froms[0], tos[-1]
    today = datetime.now(timezone.utc).date()
    next_year = today.replace(year=today.year + 1)
    return today.isoformat(), next_year.isoformat()


def _build_resource_frame(dataset: NetexDataset, frame_version: str) -> etree._Element:
    frame = _new(
        "ResourceFrame",
        id=f"{dataset.codespace.xmlns}:ResourceFrame:ResourceFrame_1",
        version=frame_version,
    )
    codespaces_wrap = _new("codespaces")
    codespaces_wrap.append(build_codespace(dataset.codespace))
    frame.append(codespaces_wrap)

    # dataSources — lineage pointer PT-EPIP expects
    ds_wrap = _new("dataSources")
    ds_id = build_urn(dataset.codespace.xmlns, "DataSource", "default")
    ds_name = (
        dataset.organisations[0].name
        if dataset.organisations else dataset.codespace.xmlns
    )
    ds_wrap.append(build_data_source(ds_id, frame_version, ds_name))
    frame.append(ds_wrap)

    organisations_wrap = _new("organisations")
    for org in dataset.organisations:
        organisations_wrap.append(build_organisation(org))
    frame.append(organisations_wrap)

    if dataset.role_assignments:
        responsibility_sets = _new("responsibilitySets")
        rs = _new(
            "ResponsibilitySet",
            id=f"{dataset.codespace.xmlns}:ResponsibilitySet:default",
            version=frame_version,
        )
        roles_wrap = _new("roles")
        for rra in dataset.role_assignments:
            roles_wrap.append(build_responsibility_role_assignment(rra))
        rs.append(roles_wrap)
        responsibility_sets.append(rs)
        frame.append(responsibility_sets)

    return frame


def _build_site_frame(dataset: NetexDataset, frame_version: str) -> etree._Element:
    frame = _new(
        "SiteFrame",
        id=f"{dataset.codespace.xmlns}:SiteFrame:SiteFrame_1",
        version=frame_version,
    )
    stop_places_wrap = _new("stopPlaces")
    for sp in dataset.stop_places:
        stop_places_wrap.append(build_stop_place(sp))
    frame.append(stop_places_wrap)
    return frame


def _build_service_calendar_frame(
    dataset: NetexDataset, frame_version: str,
) -> etree._Element:
    frame = _new(
        "ServiceCalendarFrame",
        id=f"{dataset.codespace.xmlns}:ServiceCalendarFrame:ServiceCalendarFrame_1",
        version=frame_version,
    )
    if dataset.day_types:
        day_types_wrap = _new("dayTypes")
        for dt in dataset.day_types:
            day_types_wrap.append(build_day_type(dt))
        frame.append(day_types_wrap)
    if dataset.operating_periods:
        op_wrap = _new("operatingPeriods")
        for op in dataset.operating_periods:
            op_wrap.append(build_operating_period(op))
        frame.append(op_wrap)
    if dataset.day_type_assignments:
        ass_wrap = _new("dayTypeAssignments")
        for a in dataset.day_type_assignments:
            ass_wrap.append(build_day_type_assignment(a))
        frame.append(ass_wrap)
    return frame


def _build_service_frame(
    dataset: NetexDataset, frame_version: str,
) -> etree._Element:
    frame = _new(
        "ServiceFrame",
        id=f"{dataset.codespace.xmlns}:ServiceFrame:ServiceFrame_1",
        version=frame_version,
    )
    if dataset.lines:
        lines_wrap = _new("lines")
        for ln in dataset.lines:
            lines_wrap.append(build_line(ln))
        frame.append(lines_wrap)
    if dataset.scheduled_stop_points:
        ssp_wrap = _new("scheduledStopPoints")
        for ssp in dataset.scheduled_stop_points:
            ssp_wrap.append(build_scheduled_stop_point(ssp))
        frame.append(ssp_wrap)
    if dataset.passenger_stop_assignments:
        psa_wrap = _new("stopAssignments")
        for psa in dataset.passenger_stop_assignments:
            psa_wrap.append(build_passenger_stop_assignment(psa))
        frame.append(psa_wrap)
    if dataset.journey_patterns:
        jp_wrap = _new("journeyPatterns")
        for jp in dataset.journey_patterns:
            jp_wrap.append(build_journey_pattern(jp))
        frame.append(jp_wrap)
    return frame


def _build_timetable_frame(
    dataset: NetexDataset,
    line_ref: str,
    journeys: List[ServiceJourney],
    frame_version: str,
) -> etree._Element:
    local = line_ref.rsplit(":", 1)[-1]
    frame = _new(
        "TimetableFrame",
        id=f"{dataset.codespace.xmlns}:TimetableFrame:Line_{local}",
        version=frame_version,
    )
    vj_wrap = _new("vehicleJourneys")
    for sj in journeys:
        vj_wrap.append(build_service_journey(sj))
    frame.append(vj_wrap)
    return frame


# ═══════════════════════════════════════════════════════════════════════════
# CompositeFrame wrapper
# ═══════════════════════════════════════════════════════════════════════════

def _wrap_composite(
    dataset: NetexDataset,
    frame_version: str,
    local_id: str,
    type_of_frame_ref: str,
    inner_frames: List[etree._Element],
) -> etree._Element:
    """Wrap inner frames in a PT-EPIP CompositeFrame.

    Emits TypeOfFrameRef (identifies the EPIP profile) and a ValidBetween
    covering the feed's operating range — both required for a submission
    to be routed through the correct profile checks.
    """
    composite = _new(
        "CompositeFrame",
        id=f"{dataset.codespace.xmlns}:CompositeFrame:{local_id}",
        version=frame_version,
    )
    from_date, to_date = _dataset_validity(dataset)
    composite.append(build_valid_between(from_date, to_date))

    type_wrap = _new("TypesOfFrameRef")
    type_wrap.append(build_type_of_frame_ref(type_of_frame_ref))
    composite.append(type_wrap)

    frames_wrap = _new("frames")
    for f in inner_frames:
        frames_wrap.append(f)
    composite.append(frames_wrap)
    return composite


# ═══════════════════════════════════════════════════════════════════════════
# PublicationDelivery wrappers
# ═══════════════════════════════════════════════════════════════════════════

def _publication_delivery(
    config: NetexExportConfig,
    data_object_children: List[etree._Element],
    description: str,
) -> etree._Element:
    pd = etree.Element(
        f"{{{NETEX_NS}}}PublicationDelivery",
        nsmap=NSMAP,
        version=_PUBLICATION_VERSION,
    )
    pd.set(
        f"{{{XSI_NS}}}schemaLocation",
        f"{NETEX_NS} http://netex.uk/netex/schema/1.15/xsd/NeTEx_publication.xsd",
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pd_ts = etree.SubElement(pd, f"{{{NETEX_NS}}}PublicationTimestamp")
    pd_ts.text = timestamp
    pd_part = etree.SubElement(pd, f"{{{NETEX_NS}}}ParticipantRef")
    pd_part.text = config.participant_ref or config.operator.short_name
    pd_desc = etree.SubElement(pd, f"{{{NETEX_NS}}}Description")
    pd_desc.text = description
    pd_desc.set("lang", config.default_lang)

    data_objects = etree.SubElement(pd, f"{{{NETEX_NS}}}dataObjects")
    for child in data_object_children:
        data_objects.append(child)
    return pd


def build_stops_publication(
    dataset: NetexDataset, config: NetexExportConfig, frame_version: str,
) -> etree._Element:
    """``SiteFrame_Stops.xml`` — EPIP EU_PI_STOP composite."""
    composite = _wrap_composite(
        dataset, frame_version,
        local_id="Stops",
        type_of_frame_ref=TYPE_OF_FRAME_STOP,
        inner_frames=[
            _build_resource_frame(dataset, frame_version),
            _build_site_frame(dataset, frame_version),
            _build_service_calendar_frame(dataset, frame_version),
        ],
    )
    return _publication_delivery(
        config,
        [composite],
        description=f"NeTEx stops & calendar for {config.operator.name}",
    )


def build_lines_publication(
    dataset: NetexDataset, config: NetexExportConfig, frame_version: str,
) -> etree._Element:
    """``ServiceFrame_Lines.xml`` — EPIP EU_PI_LINE_OFFER composite."""
    composite = _wrap_composite(
        dataset, frame_version,
        local_id="Lines",
        type_of_frame_ref=TYPE_OF_FRAME_LINE_OFFER,
        inner_frames=[
            _build_resource_frame(dataset, frame_version),
            _build_service_frame(dataset, frame_version),
        ],
    )
    return _publication_delivery(
        config,
        [composite],
        description=f"NeTEx lines for {config.operator.name}",
    )


def build_timetable_publication_for_line(
    dataset: NetexDataset,
    config: NetexExportConfig,
    line_ref: str,
    journeys: List[ServiceJourney],
    frame_version: str,
) -> etree._Element:
    """One ``TimetableFrame_Schedules_Line_<route>.xml`` per GTFS route — EPIP EU_PI_TIMETABLE."""
    local = line_ref.rsplit(":", 1)[-1]
    composite = _wrap_composite(
        dataset, frame_version,
        local_id=f"Timetable_Line_{local}",
        type_of_frame_ref=TYPE_OF_FRAME_TIMETABLE,
        inner_frames=[
            _build_resource_frame(dataset, frame_version),
            _build_timetable_frame(dataset, line_ref, journeys, frame_version),
        ],
    )
    return _publication_delivery(
        config,
        [composite],
        description=f"NeTEx schedules for line {line_ref}",
    )
