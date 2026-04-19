"""
Tests for gtfs_schemas.py — GTFS and GTFS-ride Pydantic models.

Covers:
  - All 15 models can be instantiated with valid data
  - Required fields raise ValidationError when missing
  - Optional fields default to None
  - Time format validation (HH:MM:SS, allows >24:00:00)
  - Date format validation (YYYYMMDD)
  - Hex color validation
  - Coordinate range enforcement
  - Enum integer ranges
  - GTFS_TABLE_MODELS registry completeness
"""

import pytest
from pydantic import ValidationError

from gtfs_schemas import (
    # Enums
    LocationType,
    RouteType,
    DirectionId,
    WheelchairAccessible,
    BikesAllowed,
    PickupDropOffType,
    ExceptionType,
    TransferType,
    ExactTimes,
    RecordUse,
    # Core GTFS
    GtfsAgency,
    GtfsStop,
    GtfsRoute,
    GtfsTrip,
    GtfsStopTime,
    GtfsCalendar,
    # Additional GTFS
    GtfsCalendarDate,
    GtfsShape,
    GtfsFrequency,
    GtfsTransfer,
    GtfsFeedInfo,
    # GTFS-ride
    GtfsBoardAlight,
    GtfsRidership,
    GtfsRideFeedInfo,
    GtfsTripCapacity,
    # Registry
    GTFS_TABLE_MODELS,
)


# ═══════════════════════════════════════════════════════════════════════════
# Enum Sanity Checks
# ═══════════════════════════════════════════════════════════════════════════

class TestEnums:
    def test_location_type_values(self):
        assert LocationType.STOP == 0
        assert LocationType.STATION == 1
        assert LocationType.BOARDING_AREA == 4

    def test_route_type_bus(self):
        assert RouteType.BUS == 3
        assert RouteType.TROLLEYBUS == 11

    def test_exception_type(self):
        assert ExceptionType.SERVICE_ADDED == 1
        assert ExceptionType.SERVICE_REMOVED == 2

    def test_record_use(self):
        assert RecordUse.BOARDINGS_AND_ALIGHTINGS == 0
        assert RecordUse.ALIGHTINGS_ONLY == 1
        assert RecordUse.BOARDINGS_ONLY == 2


# ═══════════════════════════════════════════════════════════════════════════
# GtfsAgency
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsAgency:
    def test_valid_minimal(self):
        a = GtfsAgency(
            agency_name="Metro do Porto",
            agency_url="https://www.metrodoporto.pt",
            agency_timezone="Europe/Lisbon",
        )
        assert a.agency_name == "Metro do Porto"
        assert a.agency_id is None
        assert a.agency_lang is None

    def test_valid_full(self):
        a = GtfsAgency(
            agency_id="MDP",
            agency_name="Metro do Porto",
            agency_url="https://www.metrodoporto.pt",
            agency_timezone="Europe/Lisbon",
            agency_lang="pt",
            agency_phone="+351 225 081 000",
            agency_fare_url="https://www.metrodoporto.pt/tarifas",
            agency_email="info@metrodoporto.pt",
        )
        assert a.agency_id == "MDP"

    def test_missing_required(self):
        with pytest.raises(ValidationError):
            GtfsAgency(agency_name="Test")  # missing url, timezone


# ═══════════════════════════════════════════════════════════════════════════
# GtfsStop
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsStop:
    def test_valid(self):
        s = GtfsStop(
            stop_id="S001",
            stop_name="Central Station",
            stop_lat=41.1496,
            stop_lon=-8.6109,
        )
        assert s.stop_id == "S001"
        assert s.location_type is None

    def test_lat_out_of_range(self):
        with pytest.raises(ValidationError):
            GtfsStop(stop_id="S002", stop_lat=91.0, stop_lon=0.0)

    def test_lon_out_of_range(self):
        with pytest.raises(ValidationError):
            GtfsStop(stop_id="S003", stop_lat=0.0, stop_lon=-181.0)

    def test_location_type_range(self):
        s = GtfsStop(stop_id="S004", location_type=4)
        assert s.location_type == 4
        with pytest.raises(ValidationError):
            GtfsStop(stop_id="S005", location_type=5)

    def test_only_stop_id_required(self):
        s = GtfsStop(stop_id="S006")
        assert s.stop_name is None
        assert s.stop_lat is None


# ═══════════════════════════════════════════════════════════════════════════
# GtfsRoute
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsRoute:
    def test_valid(self):
        r = GtfsRoute(
            route_id="R01",
            route_type=3,
            route_short_name="L1",
        )
        assert r.route_type == 3

    def test_missing_route_type(self):
        with pytest.raises(ValidationError):
            GtfsRoute(route_id="R02")

    def test_valid_hex_color(self):
        r = GtfsRoute(
            route_id="R03",
            route_type=3,
            route_color="FF0000",
            route_text_color="FFFFFF",
        )
        assert r.route_color == "FF0000"

    def test_invalid_hex_color(self):
        with pytest.raises(ValidationError):
            GtfsRoute(
                route_id="R04",
                route_type=3,
                route_color="#FF0000",  # has '#' prefix
            )

    def test_invalid_hex_color_short(self):
        with pytest.raises(ValidationError):
            GtfsRoute(
                route_id="R05",
                route_type=3,
                route_color="F00",  # only 3 chars
            )


# ═══════════════════════════════════════════════════════════════════════════
# GtfsTrip
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsTrip:
    def test_valid(self):
        t = GtfsTrip(
            route_id="R01",
            service_id="WEEKDAY",
            trip_id="R01_0_001",
            direction_id=0,
        )
        assert t.trip_id == "R01_0_001"

    def test_missing_required(self):
        with pytest.raises(ValidationError):
            GtfsTrip(route_id="R01")  # missing service_id, trip_id

    def test_direction_id_range(self):
        with pytest.raises(ValidationError):
            GtfsTrip(
                route_id="R01",
                service_id="WEEKDAY",
                trip_id="T1",
                direction_id=2,
            )


# ═══════════════════════════════════════════════════════════════════════════
# GtfsStopTime
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsStopTime:
    def test_valid(self):
        st = GtfsStopTime(
            trip_id="T1",
            arrival_time="08:30:00",
            departure_time="08:31:00",
            stop_id="S001",
            stop_sequence=1,
        )
        assert st.arrival_time == "08:30:00"

    def test_after_midnight_time(self):
        """GTFS allows times >24:00:00 for after-midnight trips."""
        st = GtfsStopTime(
            trip_id="T1",
            arrival_time="25:10:00",
            departure_time="25:12:00",
            stop_id="S001",
            stop_sequence=5,
        )
        assert st.arrival_time == "25:10:00"

    def test_invalid_time_format(self):
        with pytest.raises(ValidationError):
            GtfsStopTime(
                trip_id="T1",
                arrival_time="8:30",  # missing seconds
                stop_id="S001",
                stop_sequence=1,
            )

    def test_optional_times(self):
        """Times are conditionally required — model allows None."""
        st = GtfsStopTime(
            trip_id="T1",
            stop_id="S001",
            stop_sequence=1,
        )
        assert st.arrival_time is None
        assert st.departure_time is None


# ═══════════════════════════════════════════════════════════════════════════
# GtfsCalendar
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsCalendar:
    def test_valid(self):
        c = GtfsCalendar(
            service_id="WEEKDAY",
            monday=1, tuesday=1, wednesday=1, thursday=1, friday=1,
            saturday=0, sunday=0,
            start_date="20260101",
            end_date="20261231",
        )
        assert c.service_id == "WEEKDAY"
        assert c.saturday == 0

    def test_invalid_date_format(self):
        with pytest.raises(ValidationError):
            GtfsCalendar(
                service_id="WD",
                monday=1, tuesday=1, wednesday=1, thursday=1, friday=1,
                saturday=0, sunday=0,
                start_date="2026-01-01",  # dashes not allowed
                end_date="20261231",
            )

    def test_day_value_out_of_range(self):
        with pytest.raises(ValidationError):
            GtfsCalendar(
                service_id="WD",
                monday=2,  # only 0 or 1 allowed
                tuesday=1, wednesday=1, thursday=1, friday=1,
                saturday=0, sunday=0,
                start_date="20260101",
                end_date="20261231",
            )


# ═══════════════════════════════════════════════════════════════════════════
# GtfsCalendarDate
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsCalendarDate:
    def test_valid(self):
        cd = GtfsCalendarDate(
            service_id="WEEKDAY",
            date="20260601",
            exception_type=2,
        )
        assert cd.exception_type == 2

    def test_invalid_exception_type(self):
        with pytest.raises(ValidationError):
            GtfsCalendarDate(
                service_id="WD",
                date="20260601",
                exception_type=0,  # must be 1 or 2
            )


# ═══════════════════════════════════════════════════════════════════════════
# GtfsShape
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsShape:
    def test_valid(self):
        s = GtfsShape(
            shape_id="SHP1",
            shape_pt_lat=41.1496,
            shape_pt_lon=-8.6109,
            shape_pt_sequence=0,
        )
        assert s.shape_id == "SHP1"

    def test_coordinate_range(self):
        with pytest.raises(ValidationError):
            GtfsShape(
                shape_id="SHP2",
                shape_pt_lat=95.0,
                shape_pt_lon=0.0,
                shape_pt_sequence=0,
            )


# ═══════════════════════════════════════════════════════════════════════════
# GtfsFrequency
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsFrequency:
    def test_valid(self):
        f = GtfsFrequency(
            trip_id="T1",
            start_time="06:00:00",
            end_time="22:00:00",
            headway_secs=600,
        )
        assert f.headway_secs == 600

    def test_headway_must_be_positive(self):
        with pytest.raises(ValidationError):
            GtfsFrequency(
                trip_id="T1",
                start_time="06:00:00",
                end_time="22:00:00",
                headway_secs=0,
            )

    def test_invalid_time(self):
        with pytest.raises(ValidationError):
            GtfsFrequency(
                trip_id="T1",
                start_time="6am",
                end_time="22:00:00",
                headway_secs=600,
            )


# ═══════════════════════════════════════════════════════════════════════════
# GtfsTransfer
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsTransfer:
    def test_valid(self):
        t = GtfsTransfer(
            from_stop_id="S001",
            to_stop_id="S002",
            transfer_type=2,
            min_transfer_time=120,
        )
        assert t.min_transfer_time == 120

    def test_transfer_type_range(self):
        with pytest.raises(ValidationError):
            GtfsTransfer(
                from_stop_id="S001",
                to_stop_id="S002",
                transfer_type=4,
            )


# ═══════════════════════════════════════════════════════════════════════════
# GtfsFeedInfo
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsFeedInfo:
    def test_valid(self):
        fi = GtfsFeedInfo(
            feed_publisher_name="Optisus",
            feed_publisher_url="https://optisus.pt",
            feed_lang="pt",
            feed_start_date="20260101",
            feed_end_date="20261231",
            feed_version="1.0",
        )
        assert fi.feed_version == "1.0"

    def test_invalid_date(self):
        with pytest.raises(ValidationError):
            GtfsFeedInfo(
                feed_publisher_name="Test",
                feed_publisher_url="https://test.com",
                feed_lang="en",
                feed_start_date="2026/01/01",
            )


# ═══════════════════════════════════════════════════════════════════════════
# GTFS-ride Models
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsBoardAlight:
    def test_valid(self):
        ba = GtfsBoardAlight(
            trip_id="T1",
            stop_id="S001",
            stop_sequence=3,
            record_use=0,
            boardings=42,
            alightings=15,
            service_date="20260415",
        )
        assert ba.boardings == 42

    def test_service_time_validation(self):
        ba = GtfsBoardAlight(
            trip_id="T1",
            stop_id="S001",
            stop_sequence=0,
            record_use=0,
            service_arrival_time="08:30:00",
            service_departure_time="08:31:00",
        )
        assert ba.service_arrival_time == "08:30:00"

    def test_invalid_service_date(self):
        with pytest.raises(ValidationError):
            GtfsBoardAlight(
                trip_id="T1",
                stop_id="S001",
                stop_sequence=0,
                record_use=0,
                service_date="April 15",
            )


class TestGtfsRidership:
    def test_valid(self):
        r = GtfsRidership(
            total_boardings=15000,
            total_alightings=14800,
            ridership_start_date="20260401",
            ridership_end_date="20260430",
        )
        assert r.total_boardings == 15000


class TestGtfsRideFeedInfo:
    def test_valid(self):
        rfi = GtfsRideFeedInfo(
            ride_files="board_alight.txt,ridership.txt",
            ride_start_date="20260101",
            ride_end_date="20261231",
            default_currency_type="EUR",
        )
        assert rfi.default_currency_type == "EUR"


class TestGtfsTripCapacity:
    def test_valid(self):
        tc = GtfsTripCapacity(
            trip_id="T1",
            seated_capacity=40,
            standing_capacity=60,
            wheelchair_capacity=2,
            bike_capacity=4,
        )
        assert tc.seated_capacity == 40

    def test_only_trip_id_required(self):
        tc = GtfsTripCapacity(trip_id="T2")
        assert tc.seated_capacity is None


# ═══════════════════════════════════════════════════════════════════════════
# Registry
# ═══════════════════════════════════════════════════════════════════════════

class TestGtfsTableModels:
    def test_all_15_models_registered(self):
        assert len(GTFS_TABLE_MODELS) == 15

    def test_core_tables_present(self):
        for name in ("agency", "stops", "routes", "trips", "stop_times", "calendar"):
            assert name in GTFS_TABLE_MODELS

    def test_ride_tables_present(self):
        for name in ("board_alight", "ridership", "ride_feed_info", "trip_capacity"):
            assert name in GTFS_TABLE_MODELS

    def test_models_are_importable_classes(self):
        for name, model in GTFS_TABLE_MODELS.items():
            assert hasattr(model, "model_fields"), f"{name} is not a Pydantic model"
