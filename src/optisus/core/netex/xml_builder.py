"""
lxml serializers for NeTEx entities.

One ``build_*`` function per domain object. Each returns an
``lxml.etree._Element`` in the NeTEx namespace. Child ordering follows
what the PT-EPIP XSDs expect — deviate and the PNDT validator rejects
the file.
"""

from typing import List, Optional

from lxml import etree
from lxml.builder import ElementMaker

from optisus.core.netex.schemas import (
    Codespace,
    DayType,
    DayTypeAssignment,
    JourneyPattern,
    Line,
    Location,
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

NETEX_NS = "http://www.netex.org.uk/netex"
GML_NS = "http://www.opengis.net/gml/3.2"
SIRI_NS = "http://www.siri.org.uk/siri"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"

NSMAP = {
    None: NETEX_NS,
    "gml": GML_NS,
    "siri": SIRI_NS,
    "xsi": XSI_NS,
}

_E = ElementMaker(namespace=NETEX_NS, nsmap=NSMAP)
_GML = ElementMaker(namespace=GML_NS, nsmap=NSMAP)


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _ref(tag: str, ref: str, version: str = "any") -> etree._Element:
    """A ``<FooRef ref="..." version="..."/>`` element."""
    el = etree.SubElement(etree.Element("placeholder"), f"{{{NETEX_NS}}}{tag}")
    el.set("ref", ref)
    el.set("version", version)
    # Clean up parent
    parent = el.getparent()
    parent.remove(el)
    return el


def _text(tag: str, value) -> etree._Element:
    el = etree.Element(f"{{{NETEX_NS}}}{tag}")
    el.text = str(value)
    return el


def _multilingual(tag: str, value: str, lang: str = "pt") -> etree._Element:
    """MultilingualString — Name, Description etc."""
    el = etree.Element(f"{{{NETEX_NS}}}{tag}")
    el.text = value
    el.set("lang", lang)
    return el


# ═══════════════════════════════════════════════════════════════════════════
# Resource frame objects
# ═══════════════════════════════════════════════════════════════════════════

def build_codespace(cs: Codespace) -> etree._Element:
    el = _E.Codespace(
        _text("Xmlns", cs.xmlns),
        _text("XmlnsUrl", cs.xmlns_url),
        id=cs.id,
    )
    if cs.description:
        el.append(_text("Description", cs.description))
    return el


def build_data_source(
    id_urn: str, version: str, name: str, email: Optional[str] = None,
    lang: str = "pt",
) -> etree._Element:
    """A ResourceFrame/DataSource — PT-EPIP wants this present for lineage."""
    children: List[etree._Element] = [_multilingual("Name", name, lang=lang)]
    if email:
        children.append(_E.Email(email))
    el = etree.Element(
        f"{{{NETEX_NS}}}DataSource", id=id_urn, version=version,
    )
    for c in children:
        el.append(c)
    return el


def build_type_of_frame_ref(ref: str, version_ref: str = "1.0") -> etree._Element:
    """``<TypeOfFrameRef ref="epip:EU_PI_STOP" versionRef="1.0"/>``."""
    el = etree.Element(f"{{{NETEX_NS}}}TypeOfFrameRef")
    el.set("ref", ref)
    el.set("versionRef", version_ref)
    return el


def build_valid_between(from_date: str, to_date: str) -> etree._Element:
    """``<ValidBetween><FromDate/><ToDate/></ValidBetween>`` (ISO 8601 dates)."""
    return _E.ValidBetween(
        _text("FromDate", f"{from_date}T00:00:00"),
        _text("ToDate", f"{to_date}T00:00:00"),
    )


def build_organisation(org: Organisation, lang: str = "pt") -> etree._Element:
    """Emit an Authority or Operator element (same structure, different tag)."""
    children = [_multilingual("Name", org.name, lang=lang)]
    if org.short_name:
        children.append(_multilingual("ShortName", org.short_name, lang=lang))
    if org.contact_email:
        children.append(_E.ContactDetails(_text("Email", org.contact_email)))

    tag = f"{{{NETEX_NS}}}{org.kind}"
    el = etree.Element(tag, id=org.id, version=org.version)
    for c in children:
        el.append(c)
    return el


def build_responsibility_role_assignment(
    rra: ResponsibilityRoleAssignment,
) -> etree._Element:
    org_ref = etree.Element(f"{{{NETEX_NS}}}ResponsibleOrganisationRef")
    org_ref.set("ref", rra.organisation_ref)
    org_ref.set("version", "any")

    role_el = _text("ResponsibilityRoleType", rra.role)

    return _E.ResponsibilityRoleAssignment(
        org_ref,
        role_el,
        id=rra.id, version=rra.version,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Site frame objects
# ═══════════════════════════════════════════════════════════════════════════

def build_location(loc: Location) -> etree._Element:
    """<Centroid><Location><Longitude/><Latitude/></Location></Centroid>"""
    return _E.Centroid(
        _E.Location(
            _text("Longitude", loc.longitude),
            _text("Latitude", loc.latitude),
        )
    )


def build_quay(q: Quay, lang: str = "pt") -> etree._Element:
    children: List[etree._Element] = []
    if q.name:
        children.append(_multilingual("Name", q.name, lang=lang))
    if q.centroid:
        children.append(build_location(q.centroid))
    if q.public_code:
        children.append(_text("PublicCode", q.public_code))
    el = etree.Element(f"{{{NETEX_NS}}}Quay", id=q.id, version=q.version)
    for c in children:
        el.append(c)
    return el


def build_stop_place(sp: StopPlace, lang: str = "pt") -> etree._Element:
    children: List[etree._Element] = [_multilingual("Name", sp.name, lang=lang)]
    if sp.centroid:
        children.append(build_location(sp.centroid))
    children.append(_text("TransportMode", sp.transport_mode))
    if sp.stop_place_type:
        children.append(_text("StopPlaceType", sp.stop_place_type))
    if sp.quays:
        quays_wrap = etree.Element(f"{{{NETEX_NS}}}quays")
        for q in sp.quays:
            quays_wrap.append(build_quay(q, lang=lang))
        children.append(quays_wrap)

    el = etree.Element(f"{{{NETEX_NS}}}StopPlace", id=sp.id, version=sp.version)
    for c in children:
        el.append(c)
    return el


# ═══════════════════════════════════════════════════════════════════════════
# Service calendar frame objects
# ═══════════════════════════════════════════════════════════════════════════

def build_day_type(dt: DayType, lang: str = "pt") -> etree._Element:
    children: List[etree._Element] = []
    if dt.name:
        children.append(_multilingual("Name", dt.name, lang=lang))
    if dt.days_of_week:
        prop = _E.properties(
            _E.PropertyOfDay(
                _text("DaysOfWeek", " ".join(dt.days_of_week)),
            )
        )
        children.append(prop)
    el = etree.Element(f"{{{NETEX_NS}}}DayType", id=dt.id, version=dt.version)
    for c in children:
        el.append(c)
    return el


def build_operating_period(op: OperatingPeriod) -> etree._Element:
    return _E.OperatingPeriod(
        _text("FromDate", f"{op.from_date}T00:00:00"),
        _text("ToDate", f"{op.to_date}T00:00:00"),
        id=op.id, version=op.version,
    )


def build_day_type_assignment(a: DayTypeAssignment) -> etree._Element:
    children: List[etree._Element] = [
        _text("Order", a.order),
    ]
    if a.operating_period_ref:
        op_ref = etree.Element(f"{{{NETEX_NS}}}OperatingPeriodRef")
        op_ref.set("ref", a.operating_period_ref)
        op_ref.set("version", "any")
        children.append(op_ref)
    elif a.date:
        children.append(_text("Date", a.date))

    dt_ref = etree.Element(f"{{{NETEX_NS}}}DayTypeRef")
    dt_ref.set("ref", a.day_type_ref)
    dt_ref.set("version", "any")
    children.append(dt_ref)
    children.append(_text("isAvailable", "true" if a.is_available else "false"))

    el = etree.Element(
        f"{{{NETEX_NS}}}DayTypeAssignment", id=a.id, version=a.version,
    )
    for c in children:
        el.append(c)
    return el


# ═══════════════════════════════════════════════════════════════════════════
# Service frame objects
# ═══════════════════════════════════════════════════════════════════════════

def build_line(ln: Line, lang: str = "pt") -> etree._Element:
    children: List[etree._Element] = [_multilingual("Name", ln.name, lang=lang)]
    if ln.public_code:
        children.append(_text("PublicCode", ln.public_code))
    children.append(_text("TransportMode", ln.transport_mode))
    if ln.operator_ref:
        op_ref = etree.Element(f"{{{NETEX_NS}}}OperatorRef")
        op_ref.set("ref", ln.operator_ref)
        op_ref.set("version", "any")
        children.append(op_ref)
    if ln.presentation_colour or ln.presentation_text_colour:
        presentation = _E.Presentation()
        if ln.presentation_colour:
            presentation.append(_text("Colour", ln.presentation_colour))
        if ln.presentation_text_colour:
            presentation.append(_text("TextColour", ln.presentation_text_colour))
        children.append(presentation)

    el = etree.Element(f"{{{NETEX_NS}}}Line", id=ln.id, version=ln.version)
    for c in children:
        el.append(c)
    return el


def build_scheduled_stop_point(ssp: ScheduledStopPoint, lang: str = "pt") -> etree._Element:
    children: List[etree._Element] = []
    if ssp.name:
        children.append(_multilingual("Name", ssp.name, lang=lang))
    el = etree.Element(
        f"{{{NETEX_NS}}}ScheduledStopPoint", id=ssp.id, version=ssp.version,
    )
    for c in children:
        el.append(c)
    return el


def build_passenger_stop_assignment(psa: PassengerStopAssignment) -> etree._Element:
    ssp_ref = etree.Element(f"{{{NETEX_NS}}}ScheduledStopPointRef")
    ssp_ref.set("ref", psa.scheduled_stop_point_ref)
    ssp_ref.set("version", "any")

    children: List[etree._Element] = [ssp_ref]
    if psa.quay_ref:
        q_ref = etree.Element(f"{{{NETEX_NS}}}QuayRef")
        q_ref.set("ref", psa.quay_ref)
        q_ref.set("version", "any")
        children.append(q_ref)
    elif psa.stop_place_ref:
        sp_ref = etree.Element(f"{{{NETEX_NS}}}StopPlaceRef")
        sp_ref.set("ref", psa.stop_place_ref)
        sp_ref.set("version", "any")
        children.append(sp_ref)

    el = etree.Element(
        f"{{{NETEX_NS}}}PassengerStopAssignment",
        id=psa.id, version=psa.version, order=str(psa.order),
    )
    for c in children:
        el.append(c)
    return el


def build_stop_point_in_journey_pattern(
    spjp: StopPointInJourneyPattern,
) -> etree._Element:
    ssp_ref = etree.Element(f"{{{NETEX_NS}}}ScheduledStopPointRef")
    ssp_ref.set("ref", spjp.scheduled_stop_point_ref)
    ssp_ref.set("version", "any")

    el = etree.Element(
        f"{{{NETEX_NS}}}StopPointInJourneyPattern",
        id=spjp.id, version=spjp.version, order=str(spjp.order),
    )
    el.append(ssp_ref)
    el.append(_text("ForAlighting", "true" if spjp.for_alighting else "false"))
    el.append(_text("ForBoarding", "true" if spjp.for_boarding else "false"))
    return el


def build_journey_pattern(jp: JourneyPattern, lang: str = "pt") -> etree._Element:
    children: List[etree._Element] = []
    if jp.name:
        children.append(_multilingual("Name", jp.name, lang=lang))
    if jp.line_ref:
        l_ref = etree.Element(f"{{{NETEX_NS}}}LineRef")
        l_ref.set("ref", jp.line_ref)
        l_ref.set("version", "any")
        children.append(l_ref)
    if jp.direction_type:
        children.append(_text("DirectionType", jp.direction_type))

    pts_in_seq = etree.Element(f"{{{NETEX_NS}}}pointsInSequence")
    for spjp in jp.stop_points:
        pts_in_seq.append(build_stop_point_in_journey_pattern(spjp))
    children.append(pts_in_seq)

    el = etree.Element(
        f"{{{NETEX_NS}}}ServiceJourneyPattern", id=jp.id, version=jp.version,
    )
    for c in children:
        el.append(c)
    return el


# ═══════════════════════════════════════════════════════════════════════════
# Timetable frame objects
# ═══════════════════════════════════════════════════════════════════════════

def build_timetabled_passing_time(
    tpt: TimetabledPassingTime,
) -> etree._Element:
    spjp_ref = etree.Element(f"{{{NETEX_NS}}}StopPointInJourneyPatternRef")
    spjp_ref.set("ref", tpt.stop_point_in_journey_pattern_ref)
    spjp_ref.set("version", "any")

    attrs = {}
    if tpt.id:
        attrs["id"] = tpt.id
    el = etree.Element(f"{{{NETEX_NS}}}TimetabledPassingTime", **attrs)
    el.append(spjp_ref)
    if tpt.arrival_time:
        el.append(_text("ArrivalTime", tpt.arrival_time))
        if tpt.arrival_day_offset:
            el.append(_text("ArrivalDayOffset", tpt.arrival_day_offset))
    if tpt.departure_time:
        el.append(_text("DepartureTime", tpt.departure_time))
        if tpt.departure_day_offset:
            el.append(_text("DepartureDayOffset", tpt.departure_day_offset))
    return el


def build_service_journey(sj: ServiceJourney, lang: str = "pt") -> etree._Element:
    children: List[etree._Element] = []
    if sj.name:
        children.append(_multilingual("Name", sj.name, lang=lang))

    if sj.day_type_refs:
        day_types_wrap = etree.Element(f"{{{NETEX_NS}}}dayTypes")
        for dtref in sj.day_type_refs:
            dt_ref = etree.Element(f"{{{NETEX_NS}}}DayTypeRef")
            dt_ref.set("ref", dtref)
            dt_ref.set("version", "any")
            day_types_wrap.append(dt_ref)
        children.append(day_types_wrap)

    jp_ref = etree.Element(f"{{{NETEX_NS}}}JourneyPatternRef")
    jp_ref.set("ref", sj.journey_pattern_ref)
    jp_ref.set("version", "any")
    children.append(jp_ref)

    line_ref = etree.Element(f"{{{NETEX_NS}}}LineRef")
    line_ref.set("ref", sj.line_ref)
    line_ref.set("version", "any")
    children.append(line_ref)

    if sj.operator_ref:
        op_ref = etree.Element(f"{{{NETEX_NS}}}OperatorRef")
        op_ref.set("ref", sj.operator_ref)
        op_ref.set("version", "any")
        children.append(op_ref)

    passing_times_wrap = etree.Element(f"{{{NETEX_NS}}}passingTimes")
    for tpt in sj.passing_times:
        passing_times_wrap.append(build_timetabled_passing_time(tpt))
    children.append(passing_times_wrap)

    el = etree.Element(
        f"{{{NETEX_NS}}}ServiceJourney", id=sj.id, version=sj.version,
    )
    for c in children:
        el.append(c)
    return el


# ═══════════════════════════════════════════════════════════════════════════
# Serialisation
# ═══════════════════════════════════════════════════════════════════════════

def serialize(element: etree._Element) -> bytes:
    """Serialise an element tree to pretty-printed UTF-8 XML with declaration."""
    return etree.tostring(
        element,
        pretty_print=True,
        xml_declaration=True,
        encoding="UTF-8",
    )
