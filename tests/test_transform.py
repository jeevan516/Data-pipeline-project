"""Tests for the cleaning rules and the daily aggregation."""

import pandas as pd
import pytest

from green_taxi.transform import clean, daily_revenue, remove_reversals

MONTHS = ("2025-01",)


def trip(pickup: str, amount: float, *, vendor: int = 2, pu: int = 1, do: int = 2) -> dict:
    ts = pd.Timestamp(pickup)
    return {
        "VendorID": vendor,
        "lpep_pickup_datetime": ts,
        "lpep_dropoff_datetime": ts + pd.Timedelta(minutes=10),
        "PULocationID": pu,
        "DOLocationID": do,
        "total_amount": amount,
    }


def frame(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def test_sums_revenue_per_pickup_day():
    trips = frame(
        trip("2025-01-01 08:00", 10.0),
        trip("2025-01-01 23:59", 5.5, pu=3),
        trip("2025-01-02 00:01", 20.0),
    )
    cleaned, _ = clean(trips, MONTHS)
    daily = daily_revenue(cleaned)

    assert daily["date"].dt.strftime("%Y-%m-%d").tolist() == ["2025-01-01", "2025-01-02"]
    assert daily["revenue"].tolist() == [15.5, 20.0]
    assert daily["trips"].tolist() == [2, 1]


def test_drops_pickups_outside_the_requested_months():
    trips = frame(
        trip("2024-12-31 23:00", 99.0),
        trip("2025-01-15 12:00", 10.0),
        trip("2025-02-01 00:00", 99.0),
    )
    cleaned, report = clean(trips, MONTHS)

    assert cleaned["total_amount"].tolist() == [10.0]
    assert report.removed["pickup outside reporting window"] == 2
    assert report.rows_read == 3 and report.rows_kept == 1


def test_refund_removes_its_original_charge():
    trips = frame(
        trip("2025-01-05 10:00", 25.0),
        trip("2025-01-05 10:00", -25.0),  # refund of the trip above
        trip("2025-01-05 11:00", 12.0, pu=7),
    )
    cleaned, report = clean(trips, MONTHS)
    daily = daily_revenue(cleaned)

    assert report.removed["refunded trips (charge + refund pairs)"] == 2
    assert daily["revenue"].tolist() == [12.0]
    assert daily["trips"].tolist() == [1]


def test_refunds_pair_one_to_one():
    # Charged twice, refunded once: one charge must remain.
    trips = frame(
        trip("2025-01-05 10:00", 25.0),
        trip("2025-01-05 10:00", 25.0),
        trip("2025-01-05 10:00", -25.0),
    )
    kept, pairs = remove_reversals(trips)

    assert pairs == 1
    assert kept["total_amount"].tolist() == [25.0]


def test_refund_without_a_charge_still_reduces_revenue():
    trips = frame(
        trip("2025-01-05 10:00", 30.0),
        trip("2025-01-05 12:00", -4.0, pu=9),  # no matching charge in the data
    )
    daily = daily_revenue(clean(trips, MONTHS)[0])

    assert daily["revenue"].tolist() == [26.0]
    assert daily["trips"].tolist() == [1]  # a refund is not a paid trip


def test_zero_amount_trip_adds_no_paid_trip():
    trips = frame(trip("2025-01-05 10:00", 0.0), trip("2025-01-05 11:00", 8.0, pu=4))
    daily = daily_revenue(clean(trips, MONTHS)[0])

    assert daily["trips"].tolist() == [1]
    assert daily["revenue"].tolist() == [8.0]


def test_missing_values_are_dropped_and_counted():
    trips = frame(trip("2025-01-05 10:00", 8.0), trip("2025-01-05 11:00", float("nan"), pu=4))
    cleaned, report = clean(trips, MONTHS)

    assert len(cleaned) == 1
    assert report.removed["missing pickup time or amount"] == 1


def test_read_staged_requires_files():
    from green_taxi.transform import read_staged

    with pytest.raises(ValueError):
        read_staged([])
