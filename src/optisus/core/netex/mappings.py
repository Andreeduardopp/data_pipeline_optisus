"""
Static GTFS → NeTEx lookup tables.

Isolated from the translator so they can be unit-tested for coverage
against the GTFS enums without spinning up a database.
"""

from typing import Dict

from optisus.core.schemas.gtfs import LocationType, RouteType, WheelchairAccessible


ROUTE_TYPE_TO_VEHICLE_MODE: Dict[int, str] = {
    RouteType.TRAM.value: "tram",
    RouteType.METRO.value: "metro",
    RouteType.RAIL.value: "rail",
    RouteType.BUS.value: "bus",
    RouteType.FERRY.value: "water",
    RouteType.CABLE_TRAM.value: "cableway",
    RouteType.AERIAL_LIFT.value: "cableway",
    RouteType.FUNICULAR.value: "funicular",
    RouteType.TROLLEYBUS.value: "trolleyBus",
    RouteType.MONORAIL.value: "metro",
}
"""GTFS ``route_type`` integer → NeTEx ``VehicleModeEnumeration`` string."""


LOCATION_TYPE_TO_STOP_PLACE_TYPE: Dict[int, str] = {
    LocationType.STOP.value: "Quay",
    LocationType.STATION.value: "StopPlace",
    LocationType.ENTRANCE_EXIT.value: "StopPlaceEntrance",
    LocationType.GENERIC_NODE.value: "PathJunction",
    LocationType.BOARDING_AREA.value: "BoardingPosition",
}
"""GTFS ``location_type`` integer → NeTEx element tag it maps to."""


WHEELCHAIR_TO_LIMITATION: Dict[int, str] = {
    WheelchairAccessible.NO_INFO.value: "unknown",
    WheelchairAccessible.ACCESSIBLE.value: "true",
    WheelchairAccessible.NOT_ACCESSIBLE.value: "false",
}
"""GTFS wheelchair flag → NeTEx ``AccessibilityLimitation`` boolean-enum string."""


def vehicle_mode_for_route_type(route_type: int) -> str:
    """Return the NeTEx VehicleMode for a GTFS route_type.

    Raises ``KeyError`` for unmapped values so the caller can surface a
    pre-flight validation error rather than silently defaulting to ``bus``.
    """
    return ROUTE_TYPE_TO_VEHICLE_MODE[int(route_type)]
