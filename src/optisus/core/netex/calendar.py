"""
GTFS calendar → NeTEx DayType / OperatingPeriod / DayTypeAssignment.

GTFS encodes service availability as a weekly bitmask (``monday``..
``sunday``) plus a ``(start_date, end_date)`` window, with optional
``calendar_dates.txt`` exceptions. NeTEx expresses the same concept as:

  * a ``DayType`` with a weekday pattern (``DaysOfWeek``),
  * an ``OperatingPeriod`` that bounds the pattern in time, and
  * one ``DayTypeAssignment`` linking the two, plus one extra assignment
    per calendar_dates exception (``isAvailable=true|false``).

This module holds the fold logic and nothing else — no database access,
no XML — so it can be unit-tested with plain dicts.
"""

from datetime import datetime
from typing import Dict, Iterable, List, Tuple

from optisus.core.netex.schemas import (
    DayType,
    DayTypeAssignment,
    OperatingPeriod,
)
from optisus.core.netex.urn import build_urn

_WEEKDAYS = (
    "monday", "tuesday", "wednesday", "thursday",
    "friday", "saturday", "sunday",
)
_WEEKDAY_TO_NETEX = {
    "monday": "Monday",
    "tuesday": "Tuesday",
    "wednesday": "Wednesday",
    "thursday": "Thursday",
    "friday": "Friday",
    "saturday": "Saturday",
    "sunday": "Sunday",
}


def _yyyymmdd_to_iso(value: str) -> str:
    """GTFS ``YYYYMMDD`` → ISO ``YYYY-MM-DD``. Required by NeTEx xsd:date."""
    dt = datetime.strptime(value, "%Y%m%d")
    return dt.strftime("%Y-%m-%d")


def _days_of_week(row: Dict[str, int]) -> List[str]:
    """Extract the NeTEx ``DaysOfWeek`` list from a GTFS calendar row."""
    return [_WEEKDAY_TO_NETEX[d] for d in _WEEKDAYS if int(row.get(d, 0)) == 1]


def translate_calendar(
    calendar_rows: Iterable[Dict],
    calendar_dates_rows: Iterable[Dict],
    codespace: str,
    version: str,
) -> Tuple[List[DayType], List[OperatingPeriod], List[DayTypeAssignment]]:
    """Fold GTFS calendar + calendar_dates into NeTEx calendar objects.

    Every ``service_id`` in either table yields one ``DayType``.
    Rows from ``calendar.txt`` additionally yield one ``OperatingPeriod``
    and one assignment linking the two. ``calendar_dates.txt`` rows yield
    per-date assignments with ``isAvailable`` set from ``exception_type``
    (1 = added, 2 = removed).

    Supports ``calendar_dates``-only services (no ``calendar.txt`` row):
    the DayType is emitted with an empty weekday pattern — the exceptions
    do all the work.
    """
    cal_rows = list(calendar_rows)
    cal_dates = list(calendar_dates_rows)

    seen_service_ids = {r["service_id"] for r in cal_rows}
    seen_service_ids |= {r["service_id"] for r in cal_dates}

    day_types: Dict[str, DayType] = {}
    for service_id in sorted(seen_service_ids):
        day_types[service_id] = DayType(
            id=build_urn(codespace, "DayType", service_id),
            version=version,
            name=service_id,
            days_of_week=[],  # filled below if a calendar row exists
        )

    operating_periods: List[OperatingPeriod] = []
    assignments: List[DayTypeAssignment] = []
    order = 1

    for row in cal_rows:
        sid = row["service_id"]
        day_types[sid].days_of_week = _days_of_week(row)

        op_id = build_urn(codespace, "OperatingPeriod", sid)
        operating_periods.append(OperatingPeriod(
            id=op_id,
            version=version,
            from_date=_yyyymmdd_to_iso(row["start_date"]),
            to_date=_yyyymmdd_to_iso(row["end_date"]),
        ))

        assignments.append(DayTypeAssignment(
            id=build_urn(codespace, "DayTypeAssignment", f"{sid}_period"),
            version=version,
            order=order,
            day_type_ref=day_types[sid].id,
            operating_period_ref=op_id,
            is_available=True,
        ))
        order += 1

    for row in cal_dates:
        sid = row["service_id"]
        exc = int(row["exception_type"])
        assignments.append(DayTypeAssignment(
            id=build_urn(codespace, "DayTypeAssignment", f"{sid}_{row['date']}"),
            version=version,
            order=order,
            day_type_ref=day_types[sid].id,
            date=_yyyymmdd_to_iso(row["date"]),
            is_available=(exc == 1),
        ))
        order += 1

    return list(day_types.values()), operating_periods, assignments
